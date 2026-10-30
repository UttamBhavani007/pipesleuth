"""Checkpoint transactions, read-only loads, integrity and claim ownership."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

from pipesleuth.checkpoints import CheckpointError, CheckpointStore, MAX_JSON_BYTES


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.store = CheckpointStore(self.root / "state")
        self.binding = {"input": {"source_sha256": "a" * 64, "database_sha256": "b" * 64},
                        "policy": "durable-agent-v1"}

    @property
    def database(self):
        return self.store.root / "checkpoints.sqlite3"

    def snapshot(self):
        return {str(path.relative_to(self.root)): (path.read_bytes(), path.stat().st_mtime_ns)
                for path in self.root.rglob("*") if path.is_file()}

    def mutate(self, sql, parameters=()):
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(sql, parameters)

    def test_constructor_and_missing_load_create_no_files(self):
        self.assertEqual(self.store.root, self.root / "state")
        self.assertFalse(self.store.root.exists())
        for identifier in ("0" * 32, "../outside", True):
            with self.subTest(identifier=identifier), self.assertRaises(CheckpointError):
                self.store.load(identifier)
        self.assertFalse(self.store.root.exists())

    def test_round_trip_claim_complete_and_load_are_detached_and_read_only(self):
        state = {"step": 0, "items": []}
        record = self.store.create(state, self.binding)
        self.assertEqual(record["revision"], 1)
        self.assertIsNone(record["pending"])
        identifier = record["investigation_id"]
        state["items"].append("changed")
        self.binding["policy"] = "changed"
        record["state"]["step"] = 99
        before = self.snapshot()
        saved = self.store.load(identifier)
        self.assertEqual(saved["state"], {"step": 0, "items": []})
        self.assertEqual(saved["binding"]["policy"], "durable-agent-v1")
        self.assertEqual(self.snapshot(), before)
        claimed = self.store.claim(identifier, 1, "generate")
        self.assertEqual(claimed["revision"], 2)
        self.assertEqual(set(claimed["pending"]), {"token", "operation"})
        self.assertEqual(claimed["pending"]["operation"], "generate")
        new_state = {"step": 1}
        completed = self.store.complete(identifier, claimed["pending"]["token"], new_state)
        new_state["step"] = 9
        self.assertEqual((completed["revision"], completed["state"], completed["pending"]), (3, {"step": 1}, None))
        self.assertEqual(self.store.load(identifier), completed)

    def test_stale_revisions_tokens_and_pending_takeover_preserve_record(self):
        record = self.store.create({}, self.binding)
        identifier = record["investigation_id"]
        claimed = self.store.claim(identifier, 1, "tool:read_run")
        before = self.snapshot()
        for revision in (1, 2):
            with self.assertRaises(CheckpointError):
                self.store.claim(identifier, revision, "generate")
        with self.assertRaises(CheckpointError):
            self.store.complete(identifier, "0" * 32, {"wrong": True})
        self.assertEqual(self.store.load(identifier), claimed)
        self.assertEqual(self.snapshot(), before)
        self.store.complete(identifier, claimed["pending"]["token"], {})
        with self.assertRaises(CheckpointError):
            self.store.complete(identifier, claimed["pending"]["token"], {})

    def test_simultaneous_claims_have_exactly_one_owner(self):
        record = self.store.create({}, self.binding)
        barrier = threading.Barrier(2)

        def claim(_):
            store = CheckpointStore(self.store.root)
            barrier.wait(timeout=2)
            try:
                return store.claim(record["investigation_id"], 1, "generate")
            except CheckpointError:
                return None

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(claim, range(2)))
        winners = [item for item in results if item is not None]
        self.assertEqual(len(winners), 1)
        self.assertEqual(self.store.load(record["investigation_id"]), winners[0])

    def test_invalid_json_and_operations_fail_before_artifacts(self):
        for state, binding in (([], {}), ({}, []), ({"n": float("nan")}, {}),
                               ({1: "key"}, {}), ({"large": "x" * MAX_JSON_BYTES}, {})):
            with self.subTest(state_type=type(state).__name__), self.assertRaises(CheckpointError):
                self.store.create(state, binding)
        self.assertFalse(self.store.root.exists())
        for operation in ("", " generate", "tool\nread", None):
            with self.assertRaises(CheckpointError):
                self.store.claim("0" * 32, 1, operation)
        self.assertFalse(self.store.root.exists())

    def test_digest_duplicate_json_and_schema_tampering_are_rejected_without_writes(self):
        for case in ("state", "revision", "duplicate", "version"):
            with self.subTest(case=case):
                self.store = CheckpointStore(self.root / case)
                record = self.store.create({"step": 1}, self.binding)
                if case == "version":
                    self.mutate("UPDATE checkpoint_meta SET schema_version = 2")
                elif case == "revision":
                    self.mutate("UPDATE investigations SET revision = 8")
                else:
                    changed = json.dumps(record | {"state": {"step": 2}})
                    if case == "duplicate":
                        changed = '{"state":{},"state":{}}'
                    self.mutate("UPDATE investigations SET record_json = ?", (changed,))
                before = self.snapshot()
                with self.assertRaises(CheckpointError) as failure:
                    self.store.load(record["investigation_id"])
                self.assertNotIn(str(self.root), str(failure.exception))
                self.assertEqual(self.snapshot(), before)

    def test_foreign_and_wal_databases_are_rejected_without_modification(self):
        self.store.root.mkdir()
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
            connection.execute("INSERT INTO unrelated VALUES ('preserve')")
        before = self.snapshot()
        with self.assertRaises(CheckpointError):
            self.store.create({}, self.binding)
        self.assertEqual(self.snapshot(), before)
        self.store = CheckpointStore(self.root / "wal")
        record = self.store.create({}, self.binding)
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
        before = self.snapshot()
        with self.assertRaises(CheckpointError):
            self.store.load(record["investigation_id"])
        self.assertEqual(self.snapshot(), before)

    def test_symlinked_store_database_is_rejected_without_touching_target(self):
        record = self.store.create({}, self.binding)
        target = self.root / "external.sqlite3"
        self.database.rename(target)
        self.database.symlink_to(target)
        before = self.snapshot()
        with self.assertRaises(CheckpointError):
            self.store.load(record["investigation_id"])
        self.assertEqual(self.snapshot(), before)

    def test_sql_failure_rolls_back_claim_and_preserves_previous_record(self):
        record = self.store.create({}, self.binding)
        self.mutate("CREATE TRIGGER reject_claim BEFORE UPDATE ON investigations "
                    "BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END")
        before = self.snapshot()
        with self.assertRaises(CheckpointError):
            self.store.claim(record["investigation_id"], 1, "generate")
        self.assertEqual(self.store.load(record["investigation_id"]), record)
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
