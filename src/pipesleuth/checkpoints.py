"""Transactional local checkpoints with explicit, non-replayable pending claims.

Checks protect a trusted workspace from accidental corruption and unsafe paths;
the digest is not authentication against an owner able to rewrite the database.
"""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from uuid import uuid4

from pipesleuth.evidence import EvidenceError, _check_database_header
from pipesleuth.pipeline import PipelineError, _check_no_symlinks


MAX_JSON_BYTES = 180 * 1024
MAX_RECORD_BYTES = 2 * MAX_JSON_BYTES + 4096
MAX_DATABASE_BYTES = 32 * 1024 * 1024
_SCHEMA_VERSION = 1
_APPLICATION_ID = 0x50534350
_ID = re.compile(r"[0-9a-f]{32}")
_SHA = re.compile(r"[0-9a-f]{64}")
_FIELDS = {"investigation_id", "revision", "state", "binding", "pending"}


class CheckpointError(ValueError):
    """An invalid, stale, pending, unavailable or corrupt checkpoint operation."""


def _identifier(value):
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise CheckpointError("checkpoint identifier must be 32 lowercase hexadecimal characters")


def _revision(value):
    if type(value) is not int or not 1 <= value < 2**63 - 1:
        raise CheckpointError("checkpoint revision is invalid")


def _operation(value):
    if (not isinstance(value, str) or not 1 <= len(value) <= 64 or value.strip() != value
            or any(ord(character) < 32 or ord(character) == 127 for character in value)):
        raise CheckpointError("checkpoint operation is invalid")


def _finite_json(value, depth=0):
    if depth > 32:
        raise CheckpointError("checkpoint JSON exceeds nesting limits")
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise CheckpointError("checkpoint JSON keys must be strings")
            _finite_json(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            _finite_json(child, depth + 1)
    elif not (value is None or type(value) in (str, int, bool)
              or type(value) is float and math.isfinite(value)):
        raise CheckpointError("checkpoint values must be finite JSON")


def _encode(value, limit):
    _finite_json(value)
    try:
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise CheckpointError("checkpoint JSON is invalid") from None
    if len(encoded) > limit:
        raise CheckpointError("checkpoint JSON exceeds its byte limit")
    return encoded


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _nonfinite(_value):
    raise ValueError("nonfinite JSON")


def _validate(record):
    if not isinstance(record, dict) or set(record) != _FIELDS:
        raise CheckpointError("checkpoint record schema is invalid")
    _identifier(record["investigation_id"])
    _revision(record["revision"])
    for key in ("state", "binding"):
        if not isinstance(record[key], dict):
            raise CheckpointError("checkpoint state and binding must be JSON objects")
        _encode(record[key], MAX_JSON_BYTES)
    pending = record["pending"]
    if pending is not None:
        if not isinstance(pending, dict) or set(pending) != {"token", "operation"}:
            raise CheckpointError("checkpoint pending schema is invalid")
        _identifier(pending["token"])
        _operation(pending["operation"])
    return _encode(record, MAX_RECORD_BYTES)


def _digest(record):
    return hashlib.sha256(_encode({"schema_version": _SCHEMA_VERSION, **record},
                                 MAX_RECORD_BYTES)).hexdigest()


class CheckpointStore:
    """Keep current investigation state; claims never silently replay pending work."""

    def __init__(self, root, *, timeout_seconds=2.0):
        if (type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= 5
                or not math.isfinite(timeout_seconds)):
            raise CheckpointError("checkpoint timeout must be finite, positive and at most 5")
        if not isinstance(root, (str, Path)):
            raise CheckpointError("checkpoint root must be a local directory")
        requested = Path(root).expanduser().absolute()
        try:
            if requested.is_symlink():
                raise CheckpointError("checkpoint symlinks are not permitted")
            self.root = requested.parent.resolve() / requested.name
            _check_no_symlinks(self.root)
            if self.root.exists() and not self.root.is_dir():
                raise CheckpointError("checkpoint root must be a directory")
        except (OSError, PipelineError):
            raise CheckpointError("checkpoint root is unavailable or unsafe") from None
        self.timeout_seconds = float(timeout_seconds)

    def _database(self, *, create=False):
        _check_no_symlinks(self.root)
        if create:
            self.root.mkdir(parents=True, exist_ok=True)
        if not self.root.is_dir():
            raise CheckpointError("checkpoint store does not exist")
        _check_no_symlinks(self.root)
        path = self.root / "checkpoints.sqlite3"
        if path.is_symlink():
            raise CheckpointError("checkpoint symlinks are not permitted")
        new = False
        if create and not path.exists():
            try:
                descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
                os.close(descriptor)
                new = True
            except FileExistsError:
                pass
        if not path.exists():
            raise CheckpointError("checkpoint store does not exist")
        info = path.stat()
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_DATABASE_BYTES:
            raise CheckpointError("checkpoint database is unavailable or exceeds limits")
        if not new:
            _check_database_header(path)
        return path, new

    @staticmethod
    def _schema(connection):
        if connection.execute("PRAGMA application_id").fetchone()[0] != _APPLICATION_ID:
            raise CheckpointError("checkpoint database belongs to another application")
        names = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")}
        if names != {"checkpoint_meta", "investigations"}:
            raise CheckpointError("checkpoint database schema is invalid")
        rows = connection.execute("SELECT singleton, schema_version FROM checkpoint_meta LIMIT 2").fetchall()
        if rows != [(1, _SCHEMA_VERSION)]:
            raise CheckpointError("checkpoint database version is unsupported")

    @contextmanager
    def _transaction(self, *, write=False, create=False):
        deadline = time.monotonic() + self.timeout_seconds
        connection = None
        try:
            path, new = self._database(create=create)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CheckpointError("checkpoint operation exceeded its time limit")
            connection = sqlite3.connect(path.as_uri() + "?mode=" + ("rw" if write else "ro"),
                                         uri=True, timeout=min(0.1, remaining))
            connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_RECORD_BYTES + 1024)
            connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 100)
            connection.execute("PRAGMA trusted_schema = OFF")
            if write:
                connection.execute("PRAGMA synchronous = FULL")
            if not write:
                connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            if new:
                connection.execute(f"PRAGMA application_id = {_APPLICATION_ID}")
                connection.execute("CREATE TABLE checkpoint_meta (singleton INTEGER PRIMARY KEY, schema_version INTEGER NOT NULL)")
                connection.execute("INSERT INTO checkpoint_meta VALUES (1, ?)", (_SCHEMA_VERSION,))
                connection.execute("CREATE TABLE investigations (investigation_id TEXT PRIMARY KEY NOT NULL, "
                                   "revision INTEGER NOT NULL, record_json TEXT NOT NULL, content_sha256 TEXT NOT NULL)")
            self._schema(connection)
            yield connection
            if time.monotonic() >= deadline:
                raise CheckpointError("checkpoint operation exceeded its time limit")
            if write:
                connection.commit()
            else:
                connection.rollback()
        except CheckpointError:
            raise
        except (OSError, PipelineError, EvidenceError, sqlite3.Error, ValueError, TypeError, RecursionError):
            raise CheckpointError("checkpoint store is unavailable or corrupt") from None
        finally:
            if connection is not None:
                if connection.in_transaction:
                    connection.rollback()
                connection.close()

    @staticmethod
    def _load(connection, investigation_id):
        row = connection.execute("SELECT revision, record_json, content_sha256 FROM investigations "
                                 "WHERE investigation_id = ?", (investigation_id,)).fetchone()
        if row is None:
            raise CheckpointError("checkpoint was not found")
        if (not isinstance(row[1], str) or len(row[1].encode("utf-8")) > MAX_RECORD_BYTES
                or not isinstance(row[2], str) or _SHA.fullmatch(row[2]) is None):
            raise CheckpointError("checkpoint stored record is invalid")
        try:
            record = json.loads(row[1], object_pairs_hook=_object, parse_constant=_nonfinite)
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise CheckpointError("checkpoint stored JSON is invalid") from None
        _validate(record)
        if (record["investigation_id"] != investigation_id or record["revision"] != row[0]
                or _digest(record) != row[2]):
            raise CheckpointError("checkpoint content fingerprint does not match")
        return record

    @staticmethod
    def _update(connection, record, previous_revision):
        encoded = _validate(record).decode("utf-8")
        changed = connection.execute("UPDATE investigations SET revision = ?, record_json = ?, content_sha256 = ? "
                                     "WHERE investigation_id = ? AND revision = ?",
                                     (record["revision"], encoded, _digest(record), record["investigation_id"], previous_revision))
        if changed.rowcount != 1:
            raise CheckpointError("checkpoint revision is stale")

    def create(self, state, binding):
        record = {"investigation_id": uuid4().hex, "revision": 1,
                  "state": state, "binding": binding, "pending": None}
        encoded = _validate(record)
        record = json.loads(encoded)
        with self._transaction(write=True, create=True) as connection:
            connection.execute("INSERT INTO investigations VALUES (?, ?, ?, ?)",
                               (record["investigation_id"], 1, encoded.decode("utf-8"), _digest(record)))
        return json.loads(encoded)

    def load(self, investigation_id):
        _identifier(investigation_id)
        with self._transaction() as connection:
            return self._load(connection, investigation_id)

    def claim(self, investigation_id, revision, operation):
        _identifier(investigation_id)
        _revision(revision)
        _operation(operation)
        with self._transaction(write=True) as connection:
            record = self._load(connection, investigation_id)
            if record["revision"] != revision or record["pending"] is not None:
                raise CheckpointError("checkpoint revision is stale or an operation is already pending")
            record["revision"] += 1
            record["pending"] = {"token": uuid4().hex, "operation": operation}
            self._update(connection, record, revision)
            return record

    def complete(self, investigation_id, token, state):
        _identifier(investigation_id)
        _identifier(token)
        if not isinstance(state, dict):
            raise CheckpointError("checkpoint state must be a JSON object")
        state = json.loads(_encode(state, MAX_JSON_BYTES))
        with self._transaction(write=True) as connection:
            record = self._load(connection, investigation_id)
            if record["pending"] is None or record["pending"]["token"] != token:
                raise CheckpointError("checkpoint claim token is stale")
            revision = record["revision"]
            record.update(revision=revision + 1, state=state, pending=None)
            self._update(connection, record, revision)
            return json.loads(_encode(record, MAX_RECORD_BYTES))
