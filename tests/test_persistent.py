"""Durable coordinator faults with real input files and labelled model fixtures."""

from contextlib import closing
from dataclasses import replace
from pathlib import Path
import shutil
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from pipesleuth.checkpoints import CheckpointError, CheckpointStore
from pipesleuth.evidence import EvidenceGateway
from pipesleuth.fixtures import generate_fixture
from pipesleuth.investigator import BoundedInvestigator
from pipesleuth.local_model import LocalModelConfig
from pipesleuth.persistent import PersistentError, PersistentInvestigator
from pipesleuth.pipeline import SandboxPipeline
from test_investigator import ScriptedClient, finish


class CountingGateway(EvidenceGateway):
    def __init__(self, directory):
        super().__init__(directory)
        self.calls = []
        self.after_read = None

    def call(self, name, arguments, **options):
        self.calls.append(name)
        result = super().call(name, arguments, **options)
        if self.after_read:
            self.after_read()
        return result


class FailingCompleteStore(CheckpointStore):
    fail = False

    def complete(self, identifier, token, state):
        if self.fail:
            raise CheckpointError("controlled storage failure before complete")
        return super().complete(identifier, token, state)


class PersistentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.pipeline = SandboxPipeline(self.root / "runs")
        run = self.pipeline.create(generate_fixture("healthy", row_count=2).to_source_payload())
        self.run_id = self.pipeline.run(run.run_id).run_id
        self.gateway = CountingGateway(self.pipeline.data_dir)
        self.client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"}, finish()])
        self.client.config = LocalModelConfig(expected_digest="a" * 64)
        self.store = FailingCompleteStore(self.root / "state")
        self.engine = BoundedInvestigator(self.gateway, self.client)
        self.runner = PersistentInvestigator(self.engine, self.store)

    def test_start_only_binds_input_and_terminal_resume_does_not_write_or_call(self):
        record = self.runner.start(self.run_id)
        self.assertEqual((self.gateway.calls, self.client.calls), ([], []))
        result = self.runner.resume(record["investigation_id"])
        self.assertTrue(result["state"]["done"])
        self.assertEqual(result["state"]["result"]["status"], "no_incident")
        self.assertEqual((result["state"]["result"]["tool_calls"],
                          result["state"]["result"]["model_calls"]), (2, 2))
        before = (self.store.root / "checkpoints.sqlite3").read_bytes()
        counts = (len(self.gateway.calls), len(self.client.calls))
        self.assertEqual(self.runner.resume(record["investigation_id"]), result)
        self.assertEqual((len(self.gateway.calls), len(self.client.calls)), counts)
        self.assertEqual((self.store.root / "checkpoints.sqlite3").read_bytes(), before)

    def test_configuration_change_is_rejected_before_operation_or_history_change(self):
        record = self.runner.start(self.run_id)
        self.client.config = replace(self.client.config, expected_digest="b" * 64)
        with self.assertRaises(PersistentError):
            self.runner.resume(record["investigation_id"])
        self.assertEqual((self.gateway.calls, self.client.calls), ([], []))
        self.assertEqual(self.store.load(record["investigation_id"]), record)

    def test_mutated_gateway_root_cannot_escape_binding_even_with_identical_files(self):
        record = self.runner.start(self.run_id)
        other = self.root / "copied-runs"
        shutil.copytree(self.pipeline.data_dir, other)
        self.gateway.data_dir = other
        with self.assertRaises(PersistentError):
            self.runner.resume(record["investigation_id"])
        self.assertEqual((self.gateway.calls, self.client.calls), ([], []))
        self.assertEqual(self.store.load(record["investigation_id"]), record)

    def test_input_changed_during_read_saves_observed_counts_as_terminal_unknown(self):
        record = self.runner.start(self.run_id)
        def mutate():
            with closing(sqlite3.connect(self.pipeline.database_path(self.run_id))) as connection, connection:
                connection.execute("UPDATE warehouse_orders SET amount_minor = amount_minor + 1")
        self.gateway.after_read = mutate
        stopped = self.runner.resume(record["investigation_id"])
        self.assertIsNone(stopped["pending"])
        self.assertTrue(stopped["state"]["done"])
        result = stopped["state"]["result"]
        self.assertEqual((result["status"], result["tool_calls"], result["model_calls"]), ("unknown", 1, 0))
        self.assertIn("stale_input", result["limitations"])
        self.assertEqual(len(result["evidence"]), 1)
        self.assertEqual(self.client.calls, [])

    def test_unavailable_postcheck_retains_evidence_and_clears_observed_claim(self):
        record = self.runner.start(self.run_id)
        original = self.runner._fingerprint
        calls = []
        def unavailable(run_id):
            calls.append(run_id)
            if len(calls) == 2:
                raise PersistentError("controlled unavailable postcheck")
            return original(run_id)
        with patch.object(self.runner, "_fingerprint", side_effect=unavailable):
            result = self.runner.resume(record["investigation_id"])
        self.assertIsNone(result["pending"])
        self.assertTrue(result["state"]["done"])
        self.assertIn("input_check_unavailable", result["state"]["result"]["limitations"])
        self.assertEqual(result["state"]["result"]["tool_calls"], 1)
        self.assertEqual(len(result["state"]["result"]["evidence"]), 1)

    def test_failed_commit_after_model_request_keeps_pending_and_never_replays(self):
        record = self.runner.start(self.run_id)
        checkpoint = self.runner.resume(record["investigation_id"], max_operations=1)
        self.store.fail = True
        with self.assertRaises(CheckpointError):
            self.runner.resume(record["investigation_id"], max_operations=1)
        pending = self.store.load(record["investigation_id"])
        self.assertEqual(pending["state"], checkpoint["state"])
        self.assertEqual(pending["pending"]["operation"], "generate")
        self.assertEqual(self.client.generation_requests, 1)
        self.assertEqual(pending["state"]["result"]["model_calls"], 0)  # Only durable counts are known after a crash.
        self.store.fail = False
        counts = (len(self.gateway.calls), len(self.client.calls))
        with self.assertRaisesRegex(PersistentError, "uncertain"):
            self.runner.resume(record["investigation_id"])
        self.assertEqual((len(self.gateway.calls), len(self.client.calls)), counts)
        self.assertEqual(self.store.load(record["investigation_id"]), pending)

    def test_postcheck_time_is_charged_and_cannot_leave_a_late_conclusion(self):
        self.engine.timeout_seconds = 0.05
        record = self.runner.start(self.run_id)
        original = self.runner._fingerprint
        calls = []
        def slow_postcheck(run_id):
            calls.append(run_id)
            if len(calls) == 2:
                time.sleep(0.06)
            return original(run_id)
        with patch.object(self.runner, "_fingerprint", side_effect=slow_postcheck):
            result = self.runner.resume(record["investigation_id"])
        self.assertTrue(result["state"]["done"])
        self.assertEqual(result["state"]["result"]["status"], "unknown")
        self.assertIn("elapsed_budget_exceeded", result["state"]["result"]["limitations"])
        self.assertGreaterEqual(result["state"]["elapsed_seconds"], 0.05)
        self.assertEqual(self.client.calls, [])

    def test_unpinned_model_and_bad_pause_limit_fail_before_storage_creation(self):
        self.client.config = LocalModelConfig()
        with self.assertRaises(PersistentError):
            PersistentInvestigator(self.engine, self.store)
        self.assertFalse(self.store.root.exists())
        self.client.config = LocalModelConfig(expected_digest="a" * 64)
        for limit in (True, 0, 17, 1.0):
            with self.subTest(limit=limit), self.assertRaises(PersistentError):
                self.runner.resume("0" * 32, max_operations=limit)
        self.assertFalse(self.store.root.exists())


if __name__ == "__main__":
    unittest.main()
