"""Fresh-process checkpoint commands with a controlled loopback model fixture."""

from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import SandboxPipeline
import test_investigator_cli as cli_fixtures

ROOT = Path(__file__).resolve().parents[1]


class ResumeCliTests(unittest.TestCase):
    server = cli_fixtures.InvestigatorCliTests.server
    snapshot = cli_fixtures.InvestigatorCliTests.snapshot

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name).resolve()
        self.data_dir = self.directory / "runs"
        self.state_dir = self.directory / "state"
        pipeline = SandboxPipeline(self.data_dir)
        created = pipeline.create(generate_fixture("healthy", row_count=2).to_source_payload())
        self.run_id = pipeline.run(created.run_id).run_id

    def command(self, operation, identifier, *options):
        return subprocess.run(
            [sys.executable, "-m", "pipesleuth", "investigation", operation, identifier,
             "--state-dir", str(self.state_dir), *options], cwd=ROOT,
            env=dict(os.environ, PYTHONPATH=str(ROOT / "src"), PYTHONDONTWRITEBYTECODE="1"),
            capture_output=True, text=True, timeout=10,
        )

    def start(self, endpoint, *options):
        return self.command("start", self.run_id, "--data-dir", str(self.data_dir),
                            "--endpoint", endpoint, "--model-digest", "a" * 64, *options)

    def test_saved_model_choice_survives_fresh_processes_without_repeating_completed_work(self):
        endpoint, model = self.server()
        before = self.snapshot()
        started = self.start(endpoint, "--max-operations", "2")
        self.assertEqual(started.returncode, 0, started.stderr)
        saved = json.loads(started.stdout)
        identifier = saved["investigation_id"]
        partial = saved["state"]["result"]
        self.assertEqual((partial["tool_calls"], partial["model_calls"]), (1, 1))
        self.assertFalse(saved["state"]["done"])
        self.assertIsNone(saved["pending"])
        self.assertEqual(model["generations"], 1)
        finished = self.command("resume", identifier)
        self.assertEqual(finished.returncode, 0, finished.stderr)
        completed = json.loads(finished.stdout)
        result = completed["state"]["result"]
        self.assertTrue(completed["state"]["done"])
        self.assertEqual((result["status"], result["tool_calls"], result["model_calls"]),
                         ("no_incident", 2, 2))
        self.assertEqual(result["evidence"][0], partial["evidence"][0])
        self.assertEqual(model["generations"], 2)
        calls = list(model["calls"])
        self.assertEqual(json.loads(self.command("resume", identifier).stdout), completed)
        self.assertEqual(json.loads(self.command("show", identifier).stdout), completed)
        self.assertEqual(model["calls"], calls)
        self.assertEqual(self.snapshot(), before)

    def test_stale_database_is_rejected_before_any_more_model_calls(self):
        endpoint, model = self.server()
        started = self.start(endpoint, "--max-operations", "1")
        self.assertEqual(started.returncode, 0, started.stderr)
        identifier = json.loads(started.stdout)["investigation_id"]
        database = self.data_dir / self.run_id / "warehouse.sqlite3"
        with closing(sqlite3.connect(database)) as connection, connection:
            connection.execute("UPDATE warehouse_orders SET amount_minor = amount_minor + 1")
        before = self.snapshot()
        result = self.command("resume", identifier)
        self.assertEqual(result.returncode, 2)
        self.assertNotIn("Traceback", result.stderr)
        self.assertEqual(model["calls"], [])
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(json.loads(self.command("show", identifier).stdout)["revision"],
                         json.loads(started.stdout)["revision"])

    def test_pending_operation_is_visible_and_never_replayed(self):
        from pipesleuth.checkpoints import CheckpointStore
        endpoint, model = self.server()
        started = self.start(endpoint, "--max-operations", "1")
        record = json.loads(started.stdout)
        store = CheckpointStore(self.state_dir)
        claimed = store.claim(record["investigation_id"], record["revision"], "generate")
        result = self.command("resume", record["investigation_id"])
        self.assertEqual(result.returncode, 2)
        self.assertNotIn("Traceback", result.stderr)
        shown = self.command("show", record["investigation_id"])
        self.assertEqual(shown.returncode, 0, shown.stderr)
        self.assertEqual(json.loads(shown.stdout)["pending"], claimed["pending"])
        self.assertEqual(model["calls"], [])

    def test_missing_state_and_invalid_limits_do_not_create_storage(self):
        endpoint, model = self.server()
        missing = self.command("show", "0" * 32)
        self.assertEqual(missing.returncode, 2)
        self.assertFalse(self.state_dir.exists())
        for option, value in (("--max-operations", "0"), ("--max-operations", "17"),
                              ("--retry-limit", "2"), ("--agent-timeout", "nan"),
                              ("--model-digest", "invalid")):
            with self.subTest(option=option):
                result = self.start(endpoint, option, value)
                self.assertEqual(result.returncode, 2)
                self.assertNotIn("Traceback", result.stderr)
                self.assertFalse(self.state_dir.exists())
        self.assertEqual(model["calls"], [])

    def test_rejected_generation_is_saved_and_inspectable_without_more_calls(self):
        endpoint, model = self.server(malicious=True)
        response = self.start(endpoint)
        self.assertEqual(response.returncode, 1, response.stderr)
        saved = json.loads(response.stdout)
        self.assertEqual(saved["state"]["result"]["status"], "unknown")
        self.assertIn("invalid_decision", saved["state"]["result"]["limitations"])
        shown = self.command("show", saved["investigation_id"])
        self.assertEqual(shown.returncode, 0, shown.stderr)
        self.assertEqual(json.loads(shown.stdout), saved)
        resumed = self.command("resume", saved["investigation_id"])
        self.assertEqual(resumed.returncode, 1, resumed.stderr)
        self.assertEqual(json.loads(resumed.stdout), saved)
        self.assertEqual(model["generations"], 1)


if __name__ == "__main__":
    unittest.main()
