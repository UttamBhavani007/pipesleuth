"""Derived-target execution, transaction faults and truthful outcome receipts."""

from contextlib import closing
from copy import deepcopy
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import PipelineError, SandboxPipeline
import pipesleuth.repair_executor as executor_module
from pipesleuth.repair_executor import RepairExecutionError, execute_repair
from pipesleuth.repair_verifier import RepairVerificationError, verify_repair


class RepairExecutorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.output = self.root / "derived"

    def prepared(self, scenario="healthy", duplicate_policy="reject", invalid_policy="reject"):
        return {"source": generate_fixture(scenario, row_count=3).to_source_payload(),
                "duplicate_policy": duplicate_policy, "invalid_policy": invalid_policy,
                "effects": {"rows_loaded": 3, "rows_quarantined": 4 if invalid_policy == "quarantine" else 0,
                            "duplicates_skipped": 3 if duplicate_policy == "skip_identical" else 0},
                "warnings": []}

    def tables(self, run_id):
        path = self.output / run_id / "warehouse.sqlite3"
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
            return {"status": connection.execute("SELECT status FROM pipeline_run").fetchone()[0],
                    "loaded": connection.execute("SELECT COUNT(*) FROM warehouse_orders").fetchone()[0],
                    "quarantined": connection.execute("SELECT COUNT(*) FROM quarantined_records").fetchone()[0],
                    "events": connection.execute("SELECT COUNT(*) FROM pipeline_events").fetchone()[0]}

    def test_supported_derived_loads_have_fresh_committed_verification_and_unchanged_input(self):
        for prepared in (self.prepared(), self.prepared("duplicate_ingestion", duplicate_policy="skip_identical"),
                         self.prepared("invalid_records", invalid_policy="quarantine")):
            with self.subTest(policy=(prepared["duplicate_policy"], prepared["invalid_policy"])):
                original = deepcopy(prepared)
                target = uuid4().hex
                receipt = execute_repair(self.output, target, prepared)
                self.assertEqual((receipt["status"], receipt["committed"], receipt["rolled_back"]), ("verified", True, False))
                self.assertFalse(receipt["outcome_uncertain"])
                self.assertTrue(receipt["verification"]["verified"])
                for key, value in prepared["effects"].items():
                    self.assertEqual(receipt["verification"][key], value)
                self.assertEqual(prepared, original)
                self.assertEqual(self.tables(target)["status"], "succeeded")

    def test_trusted_run_id_validation_precedes_writes_and_collision_never_overwrites(self):
        payload = self.prepared()["source"]
        pipeline = SandboxPipeline(self.output)
        for run_id in ("../outside", "A" * 32, "0" * 31, True, 123):
            with self.subTest(run_id=run_id), self.assertRaises(PipelineError):
                pipeline.create(payload, run_id=run_id)
        self.assertFalse(self.output.exists())
        target = uuid4().hex
        execute_repair(self.output, target, self.prepared())
        before = {path.name: (path.read_bytes(), path.stat().st_mtime_ns) for path in (self.output / target).iterdir()}
        with self.assertRaises(RepairExecutionError) as failed:
            execute_repair(self.output, target, self.prepared())
        self.assertEqual((failed.exception.receipt["committed"], failed.exception.receipt["rolled_back"]), (False, False))
        self.assertEqual({path.name: (path.read_bytes(), path.stat().st_mtime_ns) for path in (self.output / target).iterdir()}, before)

    def test_independent_verifier_failure_rolls_back_rows_metadata_and_success_events(self):
        target = uuid4().hex
        with patch.object(executor_module, "verify_repair", side_effect=RepairVerificationError("explicit fixture")):
            with self.assertRaises(RepairExecutionError) as failed:
                execute_repair(self.output, target, self.prepared())
        self.assertEqual((failed.exception.receipt["committed"], failed.exception.receipt["rolled_back"]), (False, True))
        self.assertEqual(self.tables(target), {"status": "new", "loaded": 0, "quarantined": 0, "events": 1})

    def test_real_sql_failure_after_partial_insertion_rolls_back_the_load(self):
        target = uuid4().hex
        original_create = SandboxPipeline.create

        def create_with_trigger(pipeline, *args, **kwargs):
            result = original_create(pipeline, *args, **kwargs)
            with closing(sqlite3.connect(pipeline.database_path(result.run_id))) as connection, connection:
                connection.execute("CREATE TRIGGER stop_second BEFORE INSERT ON warehouse_orders WHEN NEW.order_id='ORD-0002' BEGIN "
                                   "SELECT CASE WHEN (SELECT COUNT(*) FROM warehouse_orders)=1 THEN RAISE(ABORT,'partial insert fixture') "
                                   "ELSE RAISE(ABORT,'fixture precondition missing') END; END")
            return result

        with patch.object(SandboxPipeline, "create", create_with_trigger):
            with self.assertRaises(RepairExecutionError) as failed:
                execute_repair(self.output, target, self.prepared())
        self.assertEqual((failed.exception.receipt["committed"], failed.exception.receipt["rolled_back"]), (False, True))
        self.assertEqual(self.tables(target), {"status": "new", "loaded": 0, "quarantined": 0, "events": 1})

    def test_post_commit_verification_failure_never_claims_rollback(self):
        target = uuid4().hex
        calls = 0

        def fail_fresh_connection(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RepairVerificationError("explicit postcommit fixture")
            return verify_repair(*args, **kwargs)

        with patch.object(executor_module, "verify_repair", side_effect=fail_fresh_connection):
            with self.assertRaises(RepairExecutionError) as failed:
                execute_repair(self.output, target, self.prepared())
        receipt = failed.exception.receipt
        self.assertEqual((receipt["status"], receipt["committed"], receipt["rolled_back"]), ("failed", True, False))
        self.assertEqual(self.tables(target), {"status": "succeeded", "loaded": 3, "quarantined": 0, "events": 3})

    def test_commit_acknowledgement_loss_is_explicitly_uncertain(self):
        target = uuid4().hex
        original_connection = executor_module._connection

        class LostAcknowledgement(sqlite3.Connection):
            def commit(self):
                super().commit()
                raise OSError("explicit lost acknowledgement fixture")

        def uncertain_connection(pipeline, run_id, mode, deadline):
            if mode != "rw":
                return original_connection(pipeline, run_id, mode, deadline)
            connection = sqlite3.connect(pipeline.database_path(run_id), factory=LostAcknowledgement)
            connection.row_factory = sqlite3.Row
            return connection

        with patch.object(executor_module, "_connection", side_effect=uncertain_connection):
            with self.assertRaises(RepairExecutionError) as failed:
                execute_repair(self.output, target, self.prepared())
        receipt = failed.exception.receipt
        self.assertTrue(receipt["outcome_uncertain"])
        self.assertIsNone(receipt["committed"])
        self.assertFalse(receipt["rolled_back"])
        self.assertEqual(self.tables(target)["loaded"], 3)

    def test_late_trusted_guard_or_wrong_approved_effects_prevent_commit(self):
        target = uuid4().hex
        with self.assertRaises(RepairExecutionError) as failed:
            execute_repair(self.output, target, self.prepared(), timeout_seconds=0.02,
                           pre_commit=lambda: time.sleep(0.04))
        self.assertEqual(failed.exception.receipt["failure_code"], "elapsed_budget_exceeded")
        self.assertEqual((failed.exception.receipt["committed"], failed.exception.receipt["rolled_back"]), (False, True))
        self.assertEqual(self.tables(target)["loaded"], 0)
        bad = self.prepared()
        bad["effects"]["rows_loaded"] = 99
        with self.assertRaises(RepairExecutionError) as failed:
            execute_repair(self.output, uuid4().hex, bad)
        self.assertEqual(failed.exception.receipt["failure_code"], "approved_effects_mismatch")
        self.assertTrue(failed.exception.receipt["rolled_back"])


if __name__ == "__main__":
    unittest.main()
