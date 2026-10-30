"""Exact, read-only input hashes protected against ordinary SQLite writers."""

from contextlib import closing
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from pipesleuth.evidence import EvidenceGateway
from pipesleuth.input_binding import InputBindingError, MAX_DATABASE_BYTES, fingerprint_input
from pipesleuth.pipeline import MAX_SOURCE_BYTES, SandboxPipeline
import pipesleuth.input_binding as binding_module


class InputBindingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.pipeline = SandboxPipeline(self.root / "runs")
        source = {"schema": ["order_id", "customer_id", "amount_minor", "currency", "created_at"],
                  "rows": [{"order_id": "O-1", "customer_id": "C-1", "amount_minor": 100,
                            "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"}]}
        run = self.pipeline.create(source)
        self.pipeline.run(run.run_id)
        self.run_id = run.run_id
        self.database = self.pipeline.database_path(run.run_id)
        self.source = self.database.parent / "source.json"
        self.gateway = EvidenceGateway(self.pipeline.data_dir)

    def snapshot(self):
        return {path.name: (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
                for path in self.database.parent.iterdir() if path.is_file()}

    def test_hashes_match_exact_source_and_raw_database_bytes_without_writes(self):
        before = self.snapshot()
        expected = {"source_sha256": hashlib.sha256(self.source.read_bytes()).hexdigest(),
                    "database_sha256": hashlib.sha256(self.database.read_bytes()).hexdigest()}
        self.assertEqual(fingerprint_input(self.gateway, self.run_id), expected)
        self.assertEqual(fingerprint_input(self.gateway, self.run_id), expected)
        self.assertEqual(self.snapshot(), before)

    def test_actual_database_change_changes_binding_and_source_byte_change_is_rejected(self):
        first = fingerprint_input(self.gateway, self.run_id)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("UPDATE warehouse_orders SET amount_minor = 200")
        second = fingerprint_input(self.gateway, self.run_id)
        self.assertEqual(first["source_sha256"], second["source_sha256"])
        self.assertNotEqual(first["database_sha256"], second["database_sha256"])
        self.source.write_bytes(self.source.read_bytes() + b"\n")
        before = self.snapshot()
        with self.assertRaises(InputBindingError) as failure:
            fingerprint_input(self.gateway, self.run_id)
        self.assertNotIn(str(self.root), str(failure.exception))
        self.assertEqual(self.snapshot(), before)

    def test_raw_database_hash_runs_while_sqlite_shared_lock_blocks_writer_commit(self):
        original = binding_module._file_hash
        blocked = []
        before = self.snapshot()

        def inspect(path, limit, deadline):
            if path == self.database:
                with closing(sqlite3.connect(self.database, timeout=0.01)) as writer:
                    try:
                        writer.execute("UPDATE warehouse_orders SET amount_minor = 999")
                        with self.assertRaises(sqlite3.OperationalError):
                            writer.commit()
                        blocked.append(True)
                    finally:
                        writer.rollback()
            return original(path, limit, deadline)

        with patch.object(binding_module, "_file_hash", side_effect=inspect):
            result = fingerprint_input(self.gateway, self.run_id)
        self.assertEqual(blocked, [True])
        self.assertEqual(result["database_sha256"], before["warehouse.sqlite3"][0])
        self.assertEqual(self.snapshot(), before)

    def test_real_exclusive_lock_obeys_gateway_deadline(self):
        before = self.snapshot()
        gateway = EvidenceGateway(self.pipeline.data_dir, timeout_seconds=0.02)
        with closing(sqlite3.connect(self.database)) as writer:
            writer.execute("BEGIN EXCLUSIVE")
            try:
                started = time.monotonic()
                with self.assertRaises(InputBindingError):
                    fingerprint_input(gateway, self.run_id)
                self.assertLess(time.monotonic() - started, 0.30)
            finally:
                writer.rollback()
        self.assertEqual(self.snapshot(), before)

    def test_source_changed_during_raw_database_hash_is_rejected(self):
        original = binding_module._file_hash

        def change_source(path, limit, deadline):
            if path == self.database:
                self.source.write_bytes(self.source.read_bytes() + b"\n")
            return original(path, limit, deadline)

        with patch.object(binding_module, "_file_hash", side_effect=change_source):
            with self.assertRaises(InputBindingError):
                fingerprint_input(self.gateway, self.run_id)

    def test_symlinks_fifos_and_oversized_files_are_rejected_without_reader_changes(self):
        original_source = self.source.read_bytes()
        target = self.root / "external.json"
        target.write_bytes(original_source)
        self.source.unlink()
        self.source.symlink_to(target)
        with self.assertRaises(InputBindingError):
            fingerprint_input(self.gateway, self.run_id)
        self.assertEqual(target.read_bytes(), original_source)
        self.source.unlink()
        if hasattr(os, "mkfifo"):
            os.mkfifo(self.source)
            before = self.source.stat()
            with self.assertRaises(InputBindingError):
                fingerprint_input(self.gateway, self.run_id)
            self.assertEqual(self.source.stat().st_ino, before.st_ino)
            self.source.unlink()
        self.source.write_bytes(b"x" * (MAX_SOURCE_BYTES + 1))
        before = self.snapshot()
        with self.assertRaises(InputBindingError):
            fingerprint_input(self.gateway, self.run_id)
        self.assertEqual(self.snapshot(), before)
        self.source.write_bytes(original_source)
        with self.database.open("r+b") as source:
            source.seek(MAX_DATABASE_BYTES)
            source.write(b"\0")
        before = self.snapshot()
        with self.assertRaises(InputBindingError):
            fingerprint_input(self.gateway, self.run_id)
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
