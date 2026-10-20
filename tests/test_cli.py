"""Exercise the actual source-only entry point and its trust boundary."""

import json
import os
from pathlib import Path
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]


class CliTests(unittest.TestCase):
    def run_cli(self, *args: str) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT / "src")
        return subprocess.run(
            [sys.executable, "-m", "pipesleuth", *args],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    def test_scenarios_declares_implemented_stage(self):
        result = self.run_cli("scenarios")
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["stage"], "fixture_foundation")
        self.assertEqual(set(payload["scenarios"]), {
            "healthy", "schema_drift", "duplicate_ingestion", "invalid_records",
        })

    def test_default_fixture_never_emits_labels_or_oracle(self):
        for scenario in ("healthy", "schema_drift", "duplicate_ingestion", "invalid_records"):
            with self.subTest(scenario=scenario):
                result = self.run_cli("fixture", "--scenario", scenario, "--rows", "3")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(set(json.loads(result.stdout)), {"schema", "rows"})
                self.assertNotIn('"oracle"', result.stdout)
                self.assertNotIn('"metadata"', result.stdout)
                self.assertEqual(result.stderr, "")

    def test_same_seed_replays_identical_json(self):
        args = ("fixture", "--scenario", "duplicate_ingestion", "--seed", "582", "--rows", "4")
        first, second = self.run_cli(*args), self.run_cli(*args)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(first.stdout, second.stdout)

    def test_authoring_oracle_requires_explicit_flag(self):
        result = self.run_cli("fixture", "--scenario", "schema_drift", "--include-oracle")
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["metadata"]["scenario"], "schema_drift")
        self.assertIn("oracle", payload)
        self.assertIn("ground truth", result.stderr)

    def test_invalid_cli_arguments_fail_cleanly(self):
        cases = [
            ("fixture", "--scenario", "schema_drift", "--rows", "0"),
            ("fixture", "--scenario", "healthy", "--rows", "10001"),
            ("fixture", "--scenario", "healthy", "--seed", "-1"),
            ("fixture", "--scenario", "healthy", "--seed", "4294967296"),
            ("fixture", "--scenario", "unknown"),
            ("fixture",),
        ]
        for args in cases:
            with self.subTest(args=args):
                result = self.run_cli(*args)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertNotIn("Traceback", result.stderr)

    def test_version_works_without_network_or_model(self):
        result = self.run_cli("--version")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "0.1.0.dev1")


if __name__ == "__main__":
    unittest.main()
