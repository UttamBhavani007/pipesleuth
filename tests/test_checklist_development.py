"""Check development-report arithmetic and the evaluator/evidence boundary."""

import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


def _probe_module():
    specification = importlib.util.spec_from_file_location(
        "checklist_development_probe", ROOT / "scripts" / "checklist_development.py"
    )
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


class ChecklistDevelopmentTests(unittest.TestCase):
    def test_report_counts_match_actual_cases_and_evidence_excludes_evaluator_labels(self):
        report = _probe_module().build_report()
        cases, summary = report["cases"], report["summary"]
        self.assertEqual(len(cases), 10)
        self.assertEqual(summary["denominator"], len(cases))
        self.assertEqual(summary["total_cases"], len(cases))
        self.assertEqual(summary["exceptions"], sum(case["error"] is not None for case in cases))
        self.assertEqual(summary["completed_cases"] + summary["exceptions"], len(cases))
        self.assertEqual(summary["expectation_matches"], sum(case["expectation_match"] for case in cases))
        self.assertEqual(summary["all_checks_passed"], sum(case["all_checks_passed"] for case in cases))
        self.assertEqual(summary["files_unchanged"], sum(case["files_unchanged"] for case in cases))
        self.assertEqual(summary["total_tool_calls"], sum(case["tool_calls"] for case in cases))
        self.assertEqual(sum(summary["status_counts"].values()), sum(case["observed"] is not None for case in cases))
        self.assertEqual(report["config"]["model_calls"], 0)
        for name in ("baseline_source_sha256", "evidence_source_sha256", "probe_source_sha256"):
            self.assertRegex(report["config"][name], r"^[0-9a-f]{64}$")
        for case in cases:
            with self.subTest(case=case["case_id"]):
                self.assertIsNone(case["error"], case["error"])
                self.assertTrue(case["files_unchanged"])
                self.assertTrue(case["independent_database_counts"]["matches_expected"])
                self.assertTrue(case["independent_database_counts"]["unchanged"])
                evidence = case["observed"]["diagnosis"]["evidence"]
                encoded = json.dumps(evidence, sort_keys=True)
                for label in ('"case_id"', '"expected"', '"oracle"', '"scenario"', '"case_authoring"'):
                    self.assertNotIn(label, encoded)
                self.assertNotIn(case["case_id"], encoded)
                self.assertNotIn(str(ROOT), encoded)
                self.assertGreater(case["tool_calls"], 0)


if __name__ == "__main__":
    unittest.main()
