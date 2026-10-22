"""A local, isolated SQLite ETL demonstration with explicit run state.

Path checks prevent accidental traversal and symlink following in a trusted
local workspace. They are not an operating-system security sandbox. An
interrupted process can leave a run ``running``; recovery is not implemented.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
from types import MappingProxyType
from uuid import uuid4

from pipesleuth.domain import ORDER_COLUMNS, Order, OrderValidationError


MAX_SOURCE_BYTES = 2 * 1024 * 1024
MAX_SOURCE_ROWS = 10_000
MAX_EVENT_LIMIT = 1000
SQLITE_MAX_INTEGER = 2**63 - 1
_RUN_ID = re.compile(r"[0-9a-f]{32}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_STATUSES = {"new", "running", "succeeded", "failed"}
_DUPLICATE_POLICIES = {"reject", "skip_identical"}


class PipelineError(ValueError):
    """Invalid pipeline input, path or persisted run data."""


class PipelineStateError(PipelineError):
    """A run cannot be started from its current state."""


class _LoadFailure(PipelineError):
    """An observed load failure with a machine-readable diagnostic."""

    def __init__(self, code: str, message: str, **details: object) -> None:
        super().__init__(message)
        self.code = code
        self.details = details


@dataclass(frozen=True, slots=True)
class RunSummary:
    run_id: str
    status: str
    rows_loaded: int
    totals_minor: Mapping[str, int]
    source_sha256: str
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None
    duplicate_policy: str = "reject"
    duplicates_skipped: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "totals_minor", MappingProxyType(dict(self.totals_minor)))

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "run_id": self.run_id,
            "status": self.status,
            "rows_loaded": self.rows_loaded,
            "totals_minor": dict(self.totals_minor),
            "source_sha256": self.source_sha256,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duplicate_policy": self.duplicate_policy,
            "duplicates_skipped": self.duplicates_skipped,
        }
        if self.error is not None:
            result["error"] = self.error
        return result


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _check_no_symlinks(path: Path) -> None:
    for candidate in (*reversed(path.parents), path):
        if candidate.is_symlink():
            raise PipelineError(f"symlink paths are not allowed: {candidate}")


def _regular_file(path: Path) -> None:
    _check_no_symlinks(path)
    if not path.is_file():
        raise PipelineError(f"required file does not exist: {path.name}")


def _encode_source(payload: Mapping[str, object]) -> bytes:
    if not isinstance(payload, Mapping) or set(payload) != {"schema", "rows"}:
        raise PipelineError("source must contain exactly schema and rows; authoring metadata is not accepted")
    schema, rows = payload["schema"], payload["rows"]
    if (
        not isinstance(schema, list)
        or not schema
        or any(not isinstance(column, str) or not column for column in schema)
        or len(schema) != len(set(schema))
    ):
        raise PipelineError("schema must be a non-empty list of unique column names")
    if not isinstance(rows, list) or len(rows) > MAX_SOURCE_ROWS:
        raise PipelineError(f"rows must be a list with at most {MAX_SOURCE_ROWS} records")
    copied_rows = []
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, Mapping) or any(not isinstance(key, str) for key in row):
            raise PipelineError(f"row {index} must be an object with string keys")
        for value in row.values():
            if value is not None and type(value) not in (str, int, float, bool):
                raise PipelineError(f"row {index} values must be JSON scalars")
            if isinstance(value, float) and not math.isfinite(value):
                raise PipelineError(f"row {index} values must be finite JSON scalars")
        copied_rows.append(dict(row))
    try:
        encoded = json.dumps(
            {"schema": schema, "rows": copied_rows},
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise PipelineError(f"source cannot be encoded as JSON: {exc}") from exc
    if len(encoded) > MAX_SOURCE_BYTES:
        raise PipelineError(f"source JSON exceeds {MAX_SOURCE_BYTES} bytes")
    return encoded


class SandboxPipeline:
    """Create and inspect isolated ETL runs; verify successful replays."""

    def __init__(self, data_dir: Path | str) -> None:
        requested = Path(data_dir).expanduser().absolute()
        if requested.is_symlink():
            raise PipelineError("data_dir must not be a symlink")
        # Resolve trusted parent aliases once (macOS /var and /tmp), without
        # accepting a symlink for the configured root or any runtime run/file.
        self.data_dir = requested.parent.resolve() / requested.name
        _check_no_symlinks(self.data_dir)
        if self.data_dir.exists() and not self.data_dir.is_dir():
            raise PipelineError("data_dir must be a directory")

    def _run_directory(self, run_id: str) -> Path:
        if not isinstance(run_id, str) or _RUN_ID.fullmatch(run_id) is None:
            raise PipelineError("run_id must be exactly 32 lowercase hexadecimal characters")
        directory = self.data_dir / run_id
        _check_no_symlinks(directory)
        if not directory.is_dir():
            raise PipelineError(f"run does not exist: {run_id}")
        # Include SQLite sidecars, which must never be symlinks either.
        if any(child.is_symlink() for child in directory.iterdir()):
            raise PipelineError("symlink files are not allowed in a run directory")
        return directory

    def database_path(self, run_id: str) -> Path:
        path = self._run_directory(run_id) / "warehouse.sqlite3"
        _regular_file(path)
        return path

    def _source_path(self, run_id: str) -> Path:
        path = self._run_directory(run_id) / "source.json"
        _regular_file(path)
        return path

    def _connect(self, run_id: str, mode: str) -> sqlite3.Connection:
        path = self.database_path(run_id)
        try:
            connection = sqlite3.connect(f"{path.as_uri()}?mode={mode}", uri=True)
            connection.row_factory = sqlite3.Row
            return connection
        except sqlite3.Error as exc:
            raise PipelineError(f"cannot open run database: {exc}") from exc

    def create(
        self, source_payload: Mapping[str, object], *, duplicate_policy: str = "reject"
    ) -> RunSummary:
        if not isinstance(duplicate_policy, str) or duplicate_policy not in _DUPLICATE_POLICIES:
            raise PipelineError("duplicate_policy must be reject or skip_identical")
        encoded = _encode_source(source_payload)
        digest = hashlib.sha256(encoded).hexdigest()
        _check_no_symlinks(self.data_dir)
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            directory = self.data_dir / uuid4().hex
            directory.mkdir()
            (directory / "source.json").write_bytes(encoded)
            with closing(sqlite3.connect(directory / "warehouse.sqlite3")) as connection, connection:
                connection.execute(
                    "CREATE TABLE pipeline_run ("
                    "run_id TEXT PRIMARY KEY, status TEXT NOT NULL, source_sha256 TEXT NOT NULL, "
                    "created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT, error TEXT, "
                    "duplicate_policy TEXT NOT NULL DEFAULT 'reject', "
                    "duplicates_skipped INTEGER NOT NULL DEFAULT 0)"
                )
                connection.execute(
                    "CREATE TABLE warehouse_orders (order_id TEXT PRIMARY KEY NOT NULL, "
                    "customer_id TEXT NOT NULL, amount_minor INTEGER NOT NULL CHECK(amount_minor >= 0), "
                    "currency TEXT NOT NULL, created_at TEXT NOT NULL)"
                )
                connection.execute(
                    "CREATE TABLE pipeline_events (event_id INTEGER PRIMARY KEY, "
                    "event_type TEXT NOT NULL, created_at TEXT NOT NULL, details_json TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO pipeline_run (run_id, status, source_sha256, created_at, duplicate_policy) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (directory.name, "new", digest, _now(), duplicate_policy),
                )
                # Observe the serialized copy, not mutable caller-owned objects.
                observed = json.loads(encoded)
                self._record_event(
                    connection, "source_received", observed_schema=observed["schema"],
                    required_schema=list(ORDER_COLUMNS), raw_row_count=len(observed["rows"]),
                )
        except (OSError, sqlite3.Error) as exc:
            raise PipelineError(f"cannot create pipeline run: {exc}") from exc
        return self.inspect(directory.name)

    def inspect(self, run_id: str) -> RunSummary:
        self._source_path(run_id)
        connection = self._connect(run_id, "ro")
        try:
            connection.execute("BEGIN")
            return self._summary(connection, run_id)
        except sqlite3.Error as exc:
            raise PipelineError(f"cannot inspect run database: {exc}") from exc
        finally:
            connection.close()

    @staticmethod
    def _metadata(connection: sqlite3.Connection, run_id: str) -> dict[str, object]:
        records = connection.execute("SELECT * FROM pipeline_run").fetchall()
        if len(records) != 1 or records[0]["run_id"] != run_id:
            raise PipelineError("run database metadata does not match run_id")
        record = dict(records[0])
        record.setdefault("duplicate_policy", "reject")
        record.setdefault("duplicates_skipped", 0)
        if (
            record["status"] not in _STATUSES
            or not isinstance(record["source_sha256"], str)
            or _SHA256.fullmatch(record["source_sha256"]) is None
            or record["duplicate_policy"] not in _DUPLICATE_POLICIES
            or type(record["duplicates_skipped"]) is not int
            or record["duplicates_skipped"] < 0
        ):
            raise PipelineError("run database contains invalid status, checksum or duplicate policy metadata")
        return record

    def _summary(self, connection: sqlite3.Connection, run_id: str) -> RunSummary:
        record = self._metadata(connection, run_id)
        totals: dict[str, int] = {}
        rows_loaded = 0
        for row in connection.execute("SELECT currency, amount_minor FROM warehouse_orders"):
            totals[row["currency"]] = totals.get(row["currency"], 0) + row["amount_minor"]
            rows_loaded += 1
        return RunSummary(
            run_id=run_id, status=record["status"], rows_loaded=rows_loaded, totals_minor=totals,
            source_sha256=record["source_sha256"], created_at=record["created_at"],
            started_at=record["started_at"], finished_at=record["finished_at"], error=record["error"],
            duplicate_policy=record["duplicate_policy"], duplicates_skipped=record["duplicates_skipped"],
        )

    @staticmethod
    def _has_events(connection: sqlite3.Connection) -> bool:
        return connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", ("pipeline_events",)
        ).fetchone() is not None

    def _record_event(self, connection: sqlite3.Connection, event_type: str, **details: object) -> None:
        # Legacy databases remain readable without implicit schema migrations.
        if self._has_events(connection):
            connection.execute(
                "INSERT INTO pipeline_events (event_type, created_at, details_json) VALUES (?, ?, ?)",
                (event_type, _now(), json.dumps(details, separators=(",", ":"), allow_nan=False)),
            )

    def events(self, run_id: str, limit: int = 100) -> list[dict[str, object]]:
        """Read the first bounded set of persisted events in chronological order.

        Legacy runs without this table have no recorded events; return []
        rather than manufacturing a history or migrating their databases.
        """

        if type(limit) is not int or not 1 <= limit <= MAX_EVENT_LIMIT:
            raise PipelineError(f"event limit must be an integer between 1 and {MAX_EVENT_LIMIT}")
        self._source_path(run_id)
        connection = self._connect(run_id, "ro")
        try:
            connection.execute("BEGIN")
            self._metadata(connection, run_id)
            if not self._has_events(connection):
                return []
            return [
                {"event_id": row["event_id"], "event_type": row["event_type"],
                 "created_at": row["created_at"], "details": json.loads(row["details_json"])}
                for row in connection.execute(
                    "SELECT event_id, event_type, created_at, details_json "
                    "FROM pipeline_events ORDER BY event_id LIMIT ?", (limit,)
                )
            ]
        except (sqlite3.Error, ValueError) as exc:
            raise PipelineError(f"cannot read run events: {exc}") from exc
        finally:
            connection.close()

    def _read_orders(
        self, source_path: Path, expected_digest: str, duplicate_policy: str
    ) -> tuple[list[Order], int]:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        with os.fdopen(os.open(source_path, flags), "rb") as source_file:
            info = os.fstat(source_file.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_SOURCE_BYTES:
                raise PipelineError(f"source must be a regular file at most {MAX_SOURCE_BYTES} bytes")
            encoded = source_file.read(MAX_SOURCE_BYTES + 1)
        if len(encoded) > MAX_SOURCE_BYTES:
            raise PipelineError(f"source JSON exceeds {MAX_SOURCE_BYTES} bytes")
        observed_digest = hashlib.sha256(encoded).hexdigest()
        if observed_digest != expected_digest:
            raise _LoadFailure(
                "source_changed", "source checksum mismatch; source.json changed after run creation",
                expected_sha256=expected_digest, observed_sha256=observed_digest,
            )
        payload = json.loads(encoded)
        _encode_source(payload)
        if payload["schema"] != list(ORDER_COLUMNS):
            observed = payload["schema"]
            raise _LoadFailure(
                "schema_mismatch", "source schema does not match canonical order columns",
                observed_schema=observed, required_schema=list(ORDER_COLUMNS),
                missing_columns=sorted(set(ORDER_COLUMNS) - set(observed)),
                extra_columns=sorted(set(observed) - set(ORDER_COLUMNS)),
            )
        orders = []
        seen: dict[str, Order] = {}
        duplicates_skipped = 0
        for row_number, row in enumerate(payload["rows"], start=1):
            try:
                order = Order.from_record(row)
            except OrderValidationError as exc:
                raise _LoadFailure(
                    "invalid_row", f"row {row_number}: {exc}", row_number=row_number, reason=str(exc)
                ) from exc
            if order.amount_minor > SQLITE_MAX_INTEGER:
                message = "amount_minor exceeds SQLite signed 64-bit integer range"
                raise _LoadFailure(
                    "invalid_row", f"row {row_number}: {message}", row_number=row_number, reason=message
                )
            if order.order_id in seen:
                if seen[order.order_id] != order:
                    raise _LoadFailure(
                        "conflicting_duplicate", f"row {row_number}: conflicting values for order_id {order.order_id}",
                        row_number=row_number, order_id=order.order_id,
                    )
                if duplicate_policy == "reject":
                    raise _LoadFailure(
                        "duplicate_key", f"row {row_number}: duplicate order_id {order.order_id}",
                        row_number=row_number, order_id=order.order_id,
                    )
                duplicates_skipped += 1
                continue
            seen[order.order_id] = order
            orders.append(order)
        return orders, duplicates_skipped

    def _verify_replay(
        self, connection: sqlite3.Connection, source_path: Path, record: Mapping[str, object]
    ) -> None:
        try:
            orders, skipped = self._read_orders(source_path, record["source_sha256"], record["duplicate_policy"])
        except (OSError, ValueError, OverflowError) as exc:
            raise PipelineStateError(f"successful run cannot be replayed: {exc}") from exc
        expected = sorted(tuple(order.to_record()[column] for column in ORDER_COLUMNS) for order in orders)
        observed = [tuple(row) for row in connection.execute(
            "SELECT order_id, customer_id, amount_minor, currency, created_at "
            "FROM warehouse_orders ORDER BY order_id"
        )]
        if expected != observed or skipped != record["duplicates_skipped"]:
            raise PipelineStateError("successful run cannot be replayed: warehouse contents or skip count changed")

    def run(self, run_id: str) -> RunSummary:
        source_path = self._source_path(run_id)
        connection = self._connect(run_id, "rw")
        try:
            connection.execute("BEGIN IMMEDIATE")
            record = self._metadata(connection, run_id)
            if record["status"] == "succeeded":
                # Hold the database writer lock across the full comparison;
                # return without status, event, clock or warehouse writes.
                self._verify_replay(connection, source_path, record)
                summary = self._summary(connection, run_id)
                connection.rollback()
                return summary
            if record["status"] != "new":
                raise PipelineStateError("only new runs can start; failed or interrupted runs cannot restart")
            claimed = connection.execute(
                "UPDATE pipeline_run SET status = 'running', started_at = ? "
                "WHERE run_id = ? AND status = 'new'",
                (_now(), run_id),
            )
            if claimed.rowcount != 1:
                connection.rollback()
                raise PipelineStateError("run is no longer new")
            self._record_event(connection, "load_started", duplicate_policy=record["duplicate_policy"])
            connection.commit()
            try:
                orders, skipped = self._read_orders(
                    source_path, record["source_sha256"], record["duplicate_policy"]
                )
                connection.execute("BEGIN IMMEDIATE")
                connection.executemany(
                    "INSERT INTO warehouse_orders (order_id, customer_id, amount_minor, currency, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (tuple(order.to_record()[column] for column in ORDER_COLUMNS) for order in orders),
                )
                if "duplicates_skipped" in {row[1] for row in connection.execute("PRAGMA table_info(pipeline_run)")}:
                    connection.execute(
                        "UPDATE pipeline_run SET status = 'succeeded', finished_at = ?, duplicates_skipped = ? "
                        "WHERE run_id = ?", (_now(), skipped, run_id),
                    )
                else:
                    connection.execute(
                        "UPDATE pipeline_run SET status = 'succeeded', finished_at = ? WHERE run_id = ?",
                        (_now(), run_id),
                    )
                self._record_event(connection, "load_succeeded", rows_loaded=len(orders), duplicates_skipped=skipped)
                connection.commit()
            except (OSError, ValueError, sqlite3.Error, OverflowError) as exc:
                connection.rollback()
                with connection:
                    connection.execute("DELETE FROM warehouse_orders")
                    connection.execute(
                        "UPDATE pipeline_run SET status = 'failed', finished_at = ?, error = ? WHERE run_id = ?",
                        (_now(), str(exc), run_id),
                    )
                    self._record_event(
                        connection, "load_failed", code=exc.code if isinstance(exc, _LoadFailure) else "load_error",
                        message=str(exc), **(exc.details if isinstance(exc, _LoadFailure) else {}),
                    )
        except sqlite3.Error as exc:
            connection.rollback()
            raise PipelineError(f"cannot execute run database transaction: {exc}") from exc
        finally:
            connection.close()
        return self.inspect(run_id)
