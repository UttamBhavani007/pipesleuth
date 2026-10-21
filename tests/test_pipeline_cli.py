"""Run the real CLI and inspect SQLite with an independent connection."""

import json
import os
from contextlib import closing
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class PipelineCliTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data_dir = Path(self.temp.name) / "sandbox data"

    def cli(self, *args):
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT / "src")
        return subprocess.run(
            [sys.executable, "-m", "pipesleuth", "pipeline", *args, "--data-dir", str(self.data_dir)],
            cwd=ROOT, env=env, text=True, capture_output=True, timeout=10, check=False,
        )

    def test_run_loads_exact_orders_and_show_is_read_only(self):
        result = self.cli("run", "--seed", "42", "--rows", "3")
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["status"], "succeeded")
        self.assertEqual(summary["rows_loaded"], 3)
        self.assertEqual(summary["totals_minor"], {"EUR": 8207})
        directory = self.data_dir / summary["run_id"]
        database = directory / "warehouse.sqlite3"
        before = database.read_bytes()
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as conn:
            rows = conn.execute(
                "SELECT order_id, customer_id, amount_minor, currency, created_at "
                "FROM warehouse_orders ORDER BY order_id"
            ).fetchall()
        self.assertEqual(rows, [
            ("ORD-0001", "CUST-002", 909, "EUR", "2026-01-01T09:00:00Z"),
            ("ORD-0002", "CUST-005", 4512, "EUR", "2026-01-01T09:07:00Z"),
            ("ORD-0003", "CUST-004", 2786, "EUR", "2026-01-01T09:14:00Z"),
        ])
        shown = self.cli("show", summary["run_id"])
        self.assertEqual(shown.returncode, 0, shown.stderr)
        self.assertEqual(json.loads(shown.stdout), summary)
        self.assertEqual(database.read_bytes(), before)
        self.assertEqual(set(json.loads((directory / "source.json").read_text())), {"schema", "rows"})

    def test_each_invocation_creates_a_separate_sandbox(self):
        first, second = self.cli("run", "--rows", "1"), self.cli("run", "--rows", "2")
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        one, two = json.loads(first.stdout), json.loads(second.stdout)
        self.assertNotEqual(one["run_id"], two["run_id"])
        self.assertEqual(json.loads(self.cli("show", one["run_id"]).stdout)["rows_loaded"], 1)
        self.assertEqual(json.loads(self.cli("show", two["run_id"]).stdout)["rows_loaded"], 2)

    def test_bad_generation_arguments_create_no_files(self):
        for args in (("--rows", "0"), ("--rows", "10001"), ("--seed", "-1")):
            with self.subTest(args=args):
                result = self.cli("run", *args)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertNotIn("Traceback", result.stderr)
                self.assertFalse(self.data_dir.exists())

    def test_missing_or_invalid_run_is_not_created_by_show(self):
        for identifier in ("0" * 32, "../other", "schema_drift"):
            with self.subTest(identifier=identifier):
                result = self.cli("show", identifier)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertNotIn("Traceback", result.stderr)
                self.assertFalse(self.data_dir.exists())


if __name__ == "__main__":
    unittest.main()
