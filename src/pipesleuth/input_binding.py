"""Fingerprint source and raw warehouse bytes under a bounded read snapshot.

SQLite's shared read lock protects against ordinary SQLite writers. Stable paths
are required in this trusted workspace; hashes are not owner authentication.
"""

from __future__ import annotations

import hashlib
import os
import stat
import time

from pipesleuth.evidence import EvidenceError, EvidenceGateway
from pipesleuth.pipeline import MAX_SOURCE_BYTES


MAX_DATABASE_BYTES = 32 * 1024 * 1024


class InputBindingError(ValueError):
    """The input cannot be fingerprinted safely or changed during inspection."""


def _signature(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _check_deadline(deadline):
    if time.monotonic() >= deadline:
        raise InputBindingError("input fingerprint exceeded its time limit")


def _file_hash(path, limit, deadline):
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    with os.fdopen(os.open(path, flags), "rb") as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
            raise InputBindingError("input file is unavailable or exceeds its size limit")
        digest, size = hashlib.sha256(), 0
        while True:
            _check_deadline(deadline)
            block = source.read(min(65_536, limit + 1 - size))
            if not block:
                break
            size += len(block)
            if size > limit:
                raise InputBindingError("input file exceeds its size limit")
            digest.update(block)
        after = os.fstat(source.fileno())
        if size != before.st_size or _signature(before) != _signature(after):
            raise InputBindingError("input changed while being fingerprinted")
    _check_deadline(deadline)
    return digest.hexdigest(), _signature(after)


def fingerprint_input(gateway, run_id):
    """Return exact source/database SHA-256 values without changing pipeline files."""

    try:
        # Construct the trusted fixed reader, not a model-supplied tool callback.
        reader = EvidenceGateway(gateway.data_dir, timeout_seconds=gateway.timeout_seconds)
        reader._arguments("read_run", {"run_id": run_id})
        deadline = time.monotonic() + reader.timeout_seconds
        with reader._snapshot(run_id, deadline) as (snapshot, directory):
            metadata = reader._metadata(snapshot, run_id)  # Acquires the shared SQLite read lock.
            source = directory / "source.json"
            database = directory / "warehouse.sqlite3"
            source_sha, source_signature = _file_hash(source, MAX_SOURCE_BYTES, deadline)
            if source_sha != metadata["source_sha256"]:
                raise InputBindingError("source fingerprint no longer matches its recorded input")
            database_sha, database_signature = _file_hash(database, MAX_DATABASE_BYTES, deadline)
            for path, signature in ((source, source_signature), (database, database_signature)):
                info = path.stat(follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode) or _signature(info) != signature:
                    raise InputBindingError("input changed while being fingerprinted")
            _check_deadline(deadline)
            return {"source_sha256": source_sha, "database_sha256": database_sha}
    except InputBindingError:
        raise
    except (EvidenceError, OSError, AttributeError, ValueError, TypeError, OverflowError, RecursionError):
        raise InputBindingError("input fingerprint is unavailable or unsafe") from None
