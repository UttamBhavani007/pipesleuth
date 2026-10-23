"""Run actual CLI validation paths and inspect warehouse/quarantine contents."""

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


class QuarantineCliTests(unittest.TestCase):
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

    def test_invalid_records_load_valid_rows_and_preserve_rejected_inputs(self):
        result = self.cli("run", "--scenario", "invalid_records", "--quarantine", "--rows", "3")
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["status"], "succeeded")
        self.assertEqual(summary["invalid_policy"], "quarantine")
        self.assertEqual(summary["rows_loaded"], 3)
        self.assertEqual(summary["rows_quarantined"], 4)
        self.assertEqual(summary["totals_minor"], {"EUR": 8207})
        database = self.database(summary["run_id"])
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as db:
            accepted = db.execute("SELECT order_id, amount_minor FROM warehouse_orders ORDER BY order_id").fetchall()
            rejected = db.execute(
                "SELECT row_number,reason_code,reason,raw_record_json FROM quarantined_records ORDER BY row_number"
            ).fetchall()
        self.assertEqual(accepted, [("ORD-0001", 909), ("ORD-0002", 4512), ("ORD-0003", 2786)])
        self.assertEqual([row[0] for row in rejected], [4, 5, 6, 7])
        self.assertEqual([row[1] for row in rejected], ["invalid_row"] * 4)
        source = json.loads((database.parent / "source.json").read_text())["rows"]
        self.assertEqual([json.loads(row[3]) for row in rejected], source[3:])
        for row, reason_fragment in zip(rejected, ("columns", "non-negative", "currency", "UTC")):
            self.assertIn(reason_fragment, row[2])
        before = database.read_bytes()
        shown = self.cli("quarantine", summary["run_id"])
        self.assertEqual(shown.returncode, 0, shown.stderr)
        records = json.loads(shown.stdout)["records"]
        self.assertEqual([r["raw_record"] for r in records], source[3:])
        self.assertEqual([r["reason"] for r in records], [r[2] for r in rejected])
        self.assertEqual(database.read_bytes(), before)
        replay = self.cli("rerun", summary["run_id"])
        self.assertEqual(replay.returncode, 0, replay.stderr)
        self.assertEqual(json.loads(replay.stdout), summary)
        self.assertEqual(database.read_bytes(), before)

    def test_invalid_records_still_fail_without_explicit_quarantine_policy(self):
        result = self.cli("run", "--scenario", "invalid_records", "--rows", "3")
        self.assertEqual(result.returncode, 1, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["invalid_policy"], "reject")
        self.assertEqual((summary["rows_loaded"], summary["rows_quarantined"]), (0, 0))
        shown = self.cli("quarantine", summary["run_id"])
        self.assertEqual(shown.returncode, 0, shown.stderr)
        self.assertEqual(json.loads(shown.stdout)["records"], [])

    def test_quarantine_reads_are_bounded_and_invalid_limits_do_not_write(self):
        result = self.cli("run", "--scenario", "invalid_records", "--quarantine", "--rows", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        before = self.database(summary["run_id"]).read_bytes()
        limited = self.cli("quarantine", summary["run_id"], "--limit", "1")
        self.assertEqual(limited.returncode, 0, limited.stderr)
        records = json.loads(limited.stdout)["records"]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["row_number"], 2)
        for limit in ("0", "1001"):
            with self.subTest(limit=limit):
                invalid = self.cli("quarantine", summary["run_id"], "--limit", limit)
                self.assertEqual(invalid.returncode, 2)
                self.assertEqual(invalid.stdout, "")
                self.assertNotIn("Traceback", invalid.stderr)
        self.assertEqual(self.database(summary["run_id"]).read_bytes(), before)

    def test_missing_quarantine_run_never_creates_files(self):
        missing = self.cli("quarantine", "0" * 32)
        self.assertEqual(missing.returncode, 2)
        self.assertEqual(missing.stdout, "")
        self.assertNotIn("Traceback", missing.stderr)
        self.assertFalse(self.data_dir.exists())


if __name__ == "__main__":
    unittest.main()
