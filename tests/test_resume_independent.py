"""Independent durable-resume boundaries using actual SQLite and pipeline files."""

from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

from pipesleuth.checkpoints import CheckpointStore
from pipesleuth.evidence import EvidenceGateway
from pipesleuth.investigator import BoundedInvestigator
from pipesleuth.local_model import LocalModelConfig
from pipesleuth.persistent import PersistentInvestigator
from pipesleuth.pipeline import SandboxPipeline
from test_investigator import ScriptedClient


class RecordingGateway(EvidenceGateway):
    def __init__(self, directory, entered=None, release=None):
        super().__init__(directory)
        self.calls = []
        self.entered, self.release = entered, release

    def call(self, name, arguments, **options):
        self.calls.append((name, dict(arguments)))
        if self.entered is not None:
            self.entered.set()
            if not self.release.wait(2):
                raise RuntimeError("controlled test did not release the operation")
        return super().call(name, arguments, **options)


class ResumeIndependentTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.pipeline = SandboxPipeline(self.root / "runs")
        self.state_dir = self.root / "state"
        self.source = {
            "schema": ["order_id", "customer_id", "amount_minor", "currency", "created_at"],
            "rows": [
                {"order_id": "O-1", "customer_id": "C-1", "amount_minor": 100,
                 "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"},
                {"order_id": "O-2", "customer_id": "C-2", "amount_minor": 300,
                 "currency": "EUR", "created_at": "2026-01-02T09:00:00Z"},
            ],
        }

    def create_run(self):
        created = self.pipeline.create(self.source)
        return self.pipeline.run(created.run_id).run_id

    def runner(self, script=(), gateway=None, state_dir=None):
        gateway = gateway or RecordingGateway(self.pipeline.data_dir)
        client = ScriptedClient(script)
        client.config = LocalModelConfig(expected_digest="a" * 64)
        store = CheckpointStore(state_dir or self.state_dir)
        runner = PersistentInvestigator(BoundedInvestigator(gateway, client), store, retry_limit=1)
        return runner, store, gateway, client

    @staticmethod
    def snapshot(directory):
        return {str(path.relative_to(directory)): (path.read_bytes(), path.stat().st_mtime_ns)
                for path in directory.rglob("*") if path.is_file()}

    def test_same_count_same_total_or_same_json_source_changes_cannot_resume(self):
        for mutation in ("database_swap", "source_newline"):
            with self.subTest(mutation=mutation):
                run_id = self.create_run()
                runner, store, gateway, client = self.runner()
                created = runner.start(run_id)
                saved = runner.resume(created["investigation_id"], max_operations=1)
                directory = self.pipeline.data_dir / run_id
                if mutation == "database_swap":
                    with closing(sqlite3.connect(directory / "warehouse.sqlite3")) as connection, connection:
                        connection.execute("UPDATE warehouse_orders SET amount_minor = CASE order_id "
                                           "WHEN 'O-1' THEN 300 ELSE 100 END")
                        self.assertEqual(connection.execute("SELECT COUNT(*),SUM(amount_minor) FROM warehouse_orders").fetchone(),
                                         (2, 400))
                else:
                    source = directory / "source.json"
                    source.write_bytes(source.read_bytes() + b"\n")
                pipeline_before = self.snapshot(self.pipeline.data_dir)
                checkpoint_before = self.snapshot(self.state_dir)
                calls = list(gateway.calls)
                with self.assertRaises(ValueError):
                    runner.resume(created["investigation_id"])
                self.assertEqual(gateway.calls, calls)
                self.assertEqual(client.calls, [])
                self.assertEqual(store.load(created["investigation_id"]), saved)
                self.assertEqual(self.snapshot(self.pipeline.data_dir), pipeline_before)
                self.assertEqual(self.snapshot(self.state_dir), checkpoint_before)

    def test_claim_is_durable_before_operation_and_competing_runner_does_no_calls(self):
        run_id = self.create_run()
        entered, release = threading.Event(), threading.Event()
        slow_gateway = RecordingGateway(self.pipeline.data_dir, entered, release)
        first, store, _, first_client = self.runner(gateway=slow_gateway)
        created = first.start(run_id)
        identifier = created["investigation_id"]
        pipeline_before = self.snapshot(self.pipeline.data_dir)
        completed, errors = [], []

        def work():
            try:
                completed.append(first.resume(identifier, max_operations=1))
            except Exception as error:
                errors.append(type(error).__name__)

        worker = threading.Thread(target=work)
        worker.start()
        try:
            self.assertTrue(entered.wait(1), "operation did not start")
            pending = store.load(identifier)
            self.assertIsNotNone(pending["pending"])
            self.assertEqual(pending["pending"]["operation"], "tool:read_run")
            second, _, second_gateway, second_client = self.runner()
            with self.assertRaises(ValueError):
                second.resume(identifier, max_operations=1)
            self.assertEqual(second_gateway.calls, [])
            self.assertEqual(second_client.calls, [])
            self.assertEqual(store.load(identifier), pending)
        finally:
            release.set()
            worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(completed), 1)
        self.assertIsNone(completed[0]["pending"])
        self.assertEqual(completed[0]["state"]["result"]["tool_calls"], 1)
        self.assertEqual(first_client.calls, [])
        self.assertEqual(self.snapshot(self.pipeline.data_dir), pipeline_before)

    def test_terminal_show_and_resume_preserve_checkpoint_bytes_without_new_calls(self):
        run_id = self.create_run()
        runner, store, gateway, client = self.runner([{"action": "tool", "tool": "execute_sql"}])
        created = runner.start(run_id)
        terminal = runner.resume(created["investigation_id"])
        self.assertTrue(terminal["state"]["done"])
        self.assertEqual(terminal["state"]["result"]["status"], "unknown")
        self.assertEqual(terminal["state"]["result"]["model_calls"], 1)
        pipeline_before = self.snapshot(self.pipeline.data_dir)
        checkpoint_before = self.snapshot(self.state_dir)
        fresh, _, fresh_gateway, fresh_client = self.runner()
        self.assertEqual(fresh.show(created["investigation_id"]), terminal)
        self.assertEqual(fresh.resume(created["investigation_id"]), terminal)
        self.assertEqual(store.load(created["investigation_id"]), terminal)
        self.assertEqual(fresh_gateway.calls, [])
        self.assertEqual(fresh_client.calls, [])
        self.assertEqual(self.snapshot(self.pipeline.data_dir), pipeline_before)
        self.assertEqual(self.snapshot(self.state_dir), checkpoint_before)

    def test_checkpoint_root_cannot_overlap_pipeline_files(self):
        run_id = self.create_run()
        before = self.snapshot(self.pipeline.data_dir)
        for directory in (self.pipeline.data_dir, self.pipeline.data_dir / run_id / "state"):
            with self.subTest(directory=directory):
                with self.assertRaises(ValueError):
                    runner, _, _, _ = self.runner(state_dir=directory)
                    runner.start(run_id)
        self.assertEqual(self.snapshot(self.pipeline.data_dir), before)


if __name__ == "__main__":
    unittest.main()
