"""Independently verify exact derived-run contents from the order contract.

No planner, executor, fixture oracle or pipeline load/projection helper is used.
An existing caller transaction is inspected without being committed or rolled
back. Standalone reads use a temporary read snapshot owned by this verifier.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
import hashlib
import json
import math
import re
import sqlite3

from pipesleuth.domain import ORDER_COLUMNS, Order, OrderValidationError


MAX_SOURCE_BYTES = 2 * 1024 * 1024
MAX_SOURCE_ROWS = 10_000
_INTEGER_MAX = 2**63 - 1
_UUID = re.compile(r"[0-9a-f]{32}")
_SHA = re.compile(r"[0-9a-f]{64}")
_SCHEMAS = {
    "pipeline_run": "CREATE TABLE pipeline_run (run_id TEXT PRIMARY KEY, status TEXT NOT NULL, source_sha256 TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT, error TEXT, duplicate_policy TEXT NOT NULL DEFAULT 'reject', duplicates_skipped INTEGER NOT NULL DEFAULT 0, invalid_policy TEXT NOT NULL DEFAULT 'reject', rows_quarantined INTEGER NOT NULL DEFAULT 0)",
    "warehouse_orders": "CREATE TABLE warehouse_orders (order_id TEXT PRIMARY KEY NOT NULL, customer_id TEXT NOT NULL, amount_minor INTEGER NOT NULL CHECK(amount_minor >= 0), currency TEXT NOT NULL, created_at TEXT NOT NULL)",
    "quarantined_records": "CREATE TABLE quarantined_records (row_number INTEGER PRIMARY KEY NOT NULL, reason_code TEXT NOT NULL, reason TEXT NOT NULL, raw_record_json TEXT NOT NULL)",
    "pipeline_events": "CREATE TABLE pipeline_events (event_id INTEGER PRIMARY KEY, event_type TEXT NOT NULL, created_at TEXT NOT NULL, details_json TEXT NOT NULL)",
}


class RepairVerificationError(ValueError):
    """The actual derived database does not match the independently expected data."""


def _json(value, *, sorted_keys=False):
    return json.dumps(value, ensure_ascii=False, sort_keys=sorted_keys,
                      separators=(",", ":"), allow_nan=False)


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RepairVerificationError("repaired event facts contain duplicate keys")
        result[key] = value
    return result


def _expected(payload, duplicate_policy, invalid_policy):
    if (not isinstance(payload, Mapping) or set(payload) != {"schema", "rows"}
            or payload["schema"] != list(ORDER_COLUMNS) or not isinstance(payload["rows"], list)
            or len(payload["rows"]) > MAX_SOURCE_ROWS):
        raise RepairVerificationError("repair source does not match the order envelope")
    rows = []
    for row in payload["rows"]:
        if not isinstance(row, Mapping) or any(not isinstance(key, str) for key in row):
            raise RepairVerificationError("repair source rows must be scalar JSON objects")
        if any(not (value is None or type(value) in (str, int, bool)
                    or type(value) is float and math.isfinite(value)) for value in row.values()):
            raise RepairVerificationError("repair source rows must contain finite scalars")
        rows.append(dict(row))
    encoded = _json({"schema": list(ORDER_COLUMNS), "rows": rows}).encode("utf-8")
    if len(encoded) > MAX_SOURCE_BYTES:
        raise RepairVerificationError("repair source exceeds its byte limit")
    accepted, quarantined, seen = [], [], {}
    skipped = 0
    for position, raw in enumerate(rows, 1):
        reason = None
        code = "invalid_row"
        try:
            order = Order.from_record(raw)
        except OrderValidationError as error:
            reason = str(error)
        else:
            if order.amount_minor > _INTEGER_MAX:
                code = "integer_overflow"
                reason = "amount_minor exceeds SQLite signed 64-bit integer range"
        if reason is not None:
            if invalid_policy != "quarantine":
                raise RepairVerificationError("repair source contains a rejected invalid row")
            quarantined.append((position, code, reason, _json(raw)))
            continue
        canonical = tuple(order.to_record()[column] for column in ORDER_COLUMNS)
        previous = seen.get(order.order_id)
        if previous is not None:
            if previous != canonical or duplicate_policy != "skip_identical":
                raise RepairVerificationError("repair source contains an unhandled duplicate")
            skipped += 1
            continue
        seen[order.order_id] = canonical
        accepted.append(canonical)
    return encoded, sorted(accepted), quarantined, skipped


def _same_rows(connection, query, expected, label):
    index = 0
    for row in connection.execute(query):
        if index >= len(expected) or tuple(row) != expected[index]:
            raise RepairVerificationError(f"repaired {label} contents differ")
        index += 1
    if index != len(expected):
        raise RepairVerificationError(f"repaired {label} rows are missing")


def _utc(value):
    if not isinstance(value, str) or len(value) > 64:
        raise RepairVerificationError("repair timestamps are invalid")
    try:
        instant = datetime.fromisoformat(value)
        if instant.utcoffset() != timedelta(0):
            raise ValueError()
        return instant
    except ValueError:
        raise RepairVerificationError("repair timestamps are invalid") from None


def verify_repair(connection, source_payload, *, run_id, duplicate_policy,
                  invalid_policy, source_sha256):
    """Return a logical content digest only after exact independent comparison."""
    if (not isinstance(run_id, str) or _UUID.fullmatch(run_id) is None
            or not isinstance(source_sha256, str) or _SHA.fullmatch(source_sha256) is None
            or not isinstance(duplicate_policy, str) or duplicate_policy not in {"reject", "skip_identical"}
            or not isinstance(invalid_policy, str) or invalid_policy not in {"reject", "quarantine"}):
        raise RepairVerificationError("repair verification arguments are invalid")
    try:
        encoded, accepted, quarantined, skipped = _expected(source_payload, duplicate_policy, invalid_policy)
        if hashlib.sha256(encoded).hexdigest() != source_sha256:
            raise RepairVerificationError("repair source fingerprint differs")
    except RepairVerificationError:
        raise
    except (ValueError, TypeError, OverflowError, UnicodeError, RecursionError):
        raise RepairVerificationError("repair source is invalid") from None
    if not isinstance(connection, sqlite3.Connection):
        raise RepairVerificationError("repair verification requires a SQLite connection")
    owns_snapshot = not connection.in_transaction
    previous_limit = connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_SOURCE_BYTES + 4096)
    try:
        if owns_snapshot:
            connection.execute("BEGIN")
        actual = list(connection.execute(
            "SELECT type,name,sql FROM main.sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 9"
        ))
        expected = sorted(("table", name, sql) for name, sql in _SCHEMAS.items())
        if (len(actual) != len(expected)
                or any((row[0], row[1], row[2]) != (kind, name, sql)
                       for row, (kind, name, sql) in zip(actual, expected))):
            raise RepairVerificationError("repaired database schema differs")
        _same_rows(connection,
            "SELECT order_id,customer_id,amount_minor,currency,created_at FROM main.warehouse_orders ORDER BY order_id LIMIT 10001",
            accepted, "warehouse")
        _same_rows(connection,
            "SELECT row_number,reason_code,reason,raw_record_json FROM main.quarantined_records ORDER BY row_number LIMIT 10001",
            quarantined, "quarantine")
        metadata = list(connection.execute(
            "SELECT run_id,status,source_sha256,created_at,started_at,finished_at,error,duplicate_policy,duplicates_skipped,invalid_policy,rows_quarantined FROM main.pipeline_run LIMIT 2"
        ))
        if len(metadata) != 1:
            raise RepairVerificationError("repaired metadata row count differs")
        record = tuple(metadata[0])
        if ((record[0], record[1], record[2], record[6], record[7], record[8], record[9], record[10]) !=
                (run_id, "succeeded", source_sha256, None, duplicate_policy, skipped, invalid_policy, len(quarantined))
                or type(record[8]) is not int or type(record[10]) is not int):
            raise RepairVerificationError("repaired metadata differs")
        events = [tuple(row) for row in connection.execute(
            "SELECT event_id,event_type,created_at,details_json FROM main.pipeline_events ORDER BY event_id LIMIT 4"
        )]
        expected_events = [
            (1, "source_received", {"observed_schema": list(ORDER_COLUMNS), "required_schema": list(ORDER_COLUMNS), "raw_row_count": len(source_payload["rows"])}),
            (2, "load_started", {"duplicate_policy": duplicate_policy, "invalid_policy": invalid_policy}),
            (3, "load_succeeded", {"rows_loaded": len(accepted), "duplicates_skipped": skipped, "rows_quarantined": len(quarantined)}),
        ]
        if len(events) != 3:
            raise RepairVerificationError("repaired event history differs")
        for event, (identifier, kind, facts) in zip(events, expected_events):
            # Canonical serialization preserves JSON types (true is not 1).
            parsed = json.loads(event[3], object_pairs_hook=_unique)
            if event[:2] != (identifier, kind) or _json(parsed, sorted_keys=True) != _json(facts, sorted_keys=True):
                raise RepairVerificationError("repaired event facts differ")
        clock = [_utc(record[3]), _utc(events[0][2]), _utc(record[4]),
                 _utc(events[1][2]), _utc(record[5]), _utc(events[2][2])]
        if clock != sorted(clock):
            raise RepairVerificationError("repaired timestamps are inconsistent")
        totals = {}
        for row in accepted:
            totals[row[3]] = totals.get(row[3], 0) + row[2]
        verified = {"run_id": run_id, "source_sha256": source_sha256,
                    "duplicate_policy": duplicate_policy, "invalid_policy": invalid_policy,
                    "warehouse": accepted, "quarantine": quarantined,
                    "metadata": record, "events": events}
        return {"verified": True, "run_id": run_id, "rows_loaded": len(accepted),
                "rows_quarantined": len(quarantined), "duplicates_skipped": skipped,
                "totals_minor": totals, "source_sha256": source_sha256,
                "content_sha256": hashlib.sha256(_json(verified, sorted_keys=True).encode("utf-8")).hexdigest()}
    except RepairVerificationError:
        raise
    except (sqlite3.Error, ValueError, TypeError, UnicodeError, OverflowError, RecursionError):
        raise RepairVerificationError("repaired database could not be independently verified") from None
    finally:
        if owns_snapshot and connection.in_transaction:
            connection.rollback()
        connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, previous_limit)
