"""Run the deterministic checklist through the real command interface."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ChecklistCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.data_dir = Path(self.temporary.name).resolve() / "runs"

    def cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "pipesleuth", *args], cwd=ROOT,
            env=dict(os.environ, PYTHONPATH=str(ROOT / "src"), PYTHONDONTWRITEBYTECODE="1"),
            text=True, capture_output=True, timeout=15,
        )

    def create(self, scenario, *flags):
        result = self.cli("pipeline", "run", "--scenario", scenario, "--rows", "3", *flags,
                          "--data-dir", str(self.data_dir))
        self.assertIn(result.returncode, (0, 1), result.stderr)
        return json.loads(result.stdout)["run_id"]

    def snapshot(self):
        return {str(path.relative_to(self.data_dir)): (path.read_bytes(), path.stat().st_mtime_ns)
                for path in self.data_dir.rglob("*") if path.is_file()}

    def diagnose(self, run_id):
        return self.cli("diagnose", run_id, "--data-dir", str(self.data_dir))

    def test_clean_and_identical_retry_controls_need_no_intervention(self):
        for scenario, flags in (("healthy", ()), ("duplicate_ingestion", ("--deduplicate",))):
            with self.subTest(scenario=scenario):
                run_id = self.create(scenario, *flags)
                before = self.snapshot()
                result = self.diagnose(run_id)
                self.assertEqual(result.returncode, 0, result.stderr)
                diagnosis = json.loads(result.stdout)
                self.assertEqual(diagnosis["method"], "deterministic_checklist")
                self.assertEqual(diagnosis["status"], "no_incident")
                self.assertEqual(diagnosis["findings"], [])
                self.assertEqual(diagnosis["tool_calls"], 4)
                self.assertEqual(len(diagnosis["evidence"]), 4)
                if flags:
                    self.assertIn("identical_retries_skipped", diagnosis["notes"])
                self.assertEqual(self.snapshot(), before)
                self.assertLessEqual(len(result.stdout.rstrip("\n").encode("utf-8")), 81920)

    def test_supported_faults_return_findings_with_real_evidence_references(self):
        cases = (("schema_drift", (), "schema_mismatch"),
                 ("duplicate_ingestion", (), "duplicate_key"),
                 ("invalid_records", (), "invalid_row"),
                 ("invalid_records", ("--quarantine",), "invalid_row"))
        for scenario, flags, expected in cases:
            with self.subTest(scenario=scenario, flags=flags):
                run_id = self.create(scenario, *flags)
                before = self.snapshot()
                result = self.diagnose(run_id)
                self.assertEqual(result.returncode, 0, result.stderr)
                diagnosis = json.loads(result.stdout)
                self.assertEqual(diagnosis["status"], "diagnosed")
                self.assertEqual([finding["code"] for finding in diagnosis["findings"]], [expected])
                references = {evidence["evidence_id"] for evidence in diagnosis["evidence"]}
                for finding in diagnosis["findings"]:
                    self.assertTrue(finding["evidence_refs"])
                    self.assertTrue(set(finding["evidence_refs"]) <= references)
                self.assertEqual(self.snapshot(), before)

    def test_missing_evidence_returns_unknown_without_creating_files(self):
        result = self.diagnose("0" * 32)
        self.assertEqual(result.returncode, 1, result.stderr)
        diagnosis = json.loads(result.stdout)
        self.assertEqual(diagnosis["status"], "unknown")
        self.assertEqual(diagnosis["findings"], [])
        self.assertTrue(diagnosis["limitations"])
        self.assertFalse(self.data_dir.exists())
        self.assertNotIn(str(self.data_dir), result.stdout)

    def test_invalid_ids_and_execution_flags_are_rejected_without_mutation(self):
        run_id = self.create("healthy")
        before = self.snapshot()
        for args in (("diagnose", "../../outside"), ("diagnose", run_id, "--approve"),
                     ("diagnose", run_id, "--sql", "DELETE FROM warehouse_orders")):
            with self.subTest(args=args):
                result = self.cli(*args, "--data-dir", str(self.data_dir))
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertNotIn("Traceback", result.stderr)
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
