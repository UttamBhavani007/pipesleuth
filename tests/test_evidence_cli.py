"""Exercise the read-only evidence interface through real CLI processes."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class EvidenceCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.data_dir = Path(self.temporary.name).resolve() / "runs"

    def cli(self, *args):
        env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
        return subprocess.run(
            [sys.executable, "-m", "pipesleuth", *args],
            cwd=ROOT, env=env, text=True, capture_output=True, timeout=10,
        )

    def snapshot(self):
        return {
            str(path.relative_to(self.data_dir)): (path.read_bytes(), path.stat().st_mtime_ns)
            for path in self.data_dir.rglob("*") if path.is_file()
        }

    def run_batch(self, scenario, *flags):
        result = self.cli("pipeline", "run", "--scenario", scenario, "--rows", "3", *flags,
                          "--data-dir", str(self.data_dir))
        self.assertIn(result.returncode, (0, 1), result.stderr)
        return json.loads(result.stdout)

    def test_catalog_is_available_without_creating_run_files(self):
        result = self.cli("evidence", "tools")
        self.assertEqual(result.returncode, 0, result.stderr)
        names = {tool["name"] for tool in json.loads(result.stdout)["tools"]}
        self.assertEqual(names, {"read_run", "read_logs", "inspect_schema", "inspect_validation"})
        self.assertFalse(self.data_dir.exists())

    def test_all_four_tools_return_bounded_provenance_without_writes(self):
        run = self.run_batch("invalid_records", "--quarantine")
        before = self.snapshot()
        for tool in ("read_run", "read_logs", "inspect_schema", "inspect_validation"):
            with self.subTest(tool=tool):
                result = self.cli("evidence", tool, run["run_id"], "--data-dir", str(self.data_dir))
                self.assertEqual(result.returncode, 0, result.stderr)
                evidence = json.loads(result.stdout)
                self.assertEqual(evidence["tool"], tool)
                self.assertEqual(evidence["run_id"], run["run_id"])
                self.assertEqual(evidence["arguments"]["run_id"], run["run_id"])
                self.assertTrue(evidence["evidence_id"])
                self.assertEqual(len(evidence["content_sha256"]), 64)
                self.assertIsInstance(evidence["data"], dict)
                self.assertIs(type(evidence["truncated"]), bool)
                self.assertLessEqual(len(result.stdout.rstrip("\n").encode("utf-8")), 16384)
                self.assertNotIn(str(self.data_dir), result.stdout)
                for hidden in ('"oracle"', '"expected_orders"', '"scenario"', '"seed"'):
                    self.assertNotIn(hidden, result.stdout)
        self.assertEqual(self.snapshot(), before)

    def test_invalid_calls_fail_cleanly_and_leave_files_unchanged(self):
        run = self.run_batch("healthy")
        before = self.snapshot()
        invalid_calls = [
            ("execute_sql", run["run_id"]),
            ("read_run", "../../outside"),
            ("read_logs", run["run_id"], "--limit", "0"),
            ("read_logs", run["run_id"], "--limit", "21"),
            ("inspect_validation", run["run_id"], "--limit", "-1"),
            ("inspect_schema", run["run_id"], "--limit", "1"),
            ("read_run", run["run_id"], "--sql", "DELETE FROM warehouse_orders"),
        ]
        for args in invalid_calls:
            with self.subTest(args=args):
                result = self.cli("evidence", *args, "--data-dir", str(self.data_dir))
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertNotIn("Traceback", result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_failed_runs_are_readable_and_missing_runs_do_not_create_files(self):
        failed = self.run_batch("schema_drift")
        self.assertEqual(failed["status"], "failed")
        before = self.snapshot()
        result = self.cli("evidence", "read_logs", failed["run_id"], "--limit", "1",
                          "--data-dir", str(self.data_dir))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["arguments"]["limit"], 1)
        self.assertEqual(self.snapshot(), before)
        missing = self.data_dir.parent / "missing"
        result = self.cli("evidence", "read_run", "0" * 32, "--data-dir", str(missing))
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertNotIn("Traceback", result.stderr)
        self.assertFalse(missing.exists())


if __name__ == "__main__":
    unittest.main()
