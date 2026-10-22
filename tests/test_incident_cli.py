"""Execute actual fault CLI paths and inspect the database independently."""

from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class IncidentCliTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data_dir = Path(self.temp.name).resolve() / "runs"

    def cli(self, *args):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT / "src")
        return subprocess.run(
            [sys.executable, "-m", "pipesleuth", "pipeline", *args,
             "--data-dir", str(self.data_dir)],
            cwd=ROOT, env=env, text=True, capture_output=True, timeout=10,
        )

    def database(self, run_id):
        return self.data_dir / run_id / "warehouse.sqlite3"

    def test_schema_incident_records_observed_columns_and_no_loaded_rows(self):
        result = self.cli("run", "--scenario", "schema_drift", "--rows", "3")
        self.assertEqual(result.returncode, 1, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["rows_loaded"], 0)
        database = self.database(summary["run_id"])
        before = database.read_bytes()
        response = self.cli("events", summary["run_id"])
        self.assertEqual(response.returncode, 0, response.stderr)
        events = json.loads(response.stdout)["events"]
        failed = next(event for event in events if event["event_type"] == "load_failed")
        details = failed["details"]
        self.assertEqual(details["code"], "schema_mismatch")
        self.assertEqual(details["missing_columns"], ["amount_minor"])
        self.assertEqual(details["extra_columns"], ["gross_total_minor"])
        self.assertIn("gross_total_minor", details["observed_schema"])
        self.assertIn("amount_minor", details["required_schema"])
        self.assertNotIn('"oracle"', response.stdout)
        self.assertNotIn('"scenario"', response.stdout)
        self.assertNotIn('"seed"', response.stdout)
        self.assertNotIn("schema_drift", response.stdout)
        self.assertEqual(database.read_bytes(), before)
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM warehouse_orders").fetchone(), (0,))

    def test_duplicate_input_fails_by_default_and_reports_the_observed_key(self):
        result = self.cli("run", "--scenario", "duplicate_ingestion", "--rows", "3")
        self.assertEqual(result.returncode, 1, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["rows_loaded"], 0)
        response = self.cli("events", summary["run_id"])
        self.assertEqual(response.returncode, 0, response.stderr)
        events = json.loads(response.stdout)["events"]
        failure = next(event for event in events if event["event_type"] == "load_failed")
        self.assertEqual(failure["details"]["code"], "duplicate_key")
        self.assertEqual(failure["details"]["order_id"], "ORD-0001")

    def test_opt_in_retry_deduplication_and_replay_do_not_duplicate_rows(self):
        result = self.cli("run", "--scenario", "duplicate_ingestion", "--deduplicate")
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["status"], "succeeded")
        self.assertEqual(summary["duplicate_policy"], "skip_identical")
        self.assertEqual(summary["duplicates_skipped"], 3)
        self.assertEqual(summary["rows_loaded"], 12)
        database = self.database(summary["run_id"])
        before = database.read_bytes()
        rerun = self.cli("rerun", summary["run_id"])
        self.assertEqual(rerun.returncode, 0, rerun.stderr)
        self.assertEqual(json.loads(rerun.stdout), summary)
        self.assertEqual(database.read_bytes(), before)
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as db:
            self.assertEqual(db.execute(
                "SELECT COUNT(*),COUNT(DISTINCT order_id),SUM(amount_minor) FROM warehouse_orders"
            ).fetchone(), (12, 12, 73525))

    def test_failed_run_cannot_be_replayed_as_a_repair(self):
        result = self.cli("run", "--scenario", "schema_drift")
        summary = json.loads(result.stdout)
        before = self.database(summary["run_id"]).read_bytes()
        retry = self.cli("rerun", summary["run_id"])
        self.assertEqual(retry.returncode, 2)
        self.assertNotIn("Traceback", retry.stderr)
        self.assertEqual(self.database(summary["run_id"]).read_bytes(), before)

    def test_future_scenario_and_invalid_event_limit_fail_cleanly(self):
        future = self.cli("run", "--scenario", "invalid_records")
        self.assertEqual(future.returncode, 2)
        self.assertFalse(self.data_dir.exists())
        run = json.loads(self.cli("run", "--rows", "1").stdout)
        before = self.database(run["run_id"]).read_bytes()
        for limit in ("0", "1001"):
            with self.subTest(limit=limit):
                result = self.cli("events", run["run_id"], "--limit", limit)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertNotIn("Traceback", result.stderr)
        self.assertEqual(self.database(run["run_id"]).read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
