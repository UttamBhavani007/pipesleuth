"""Real fresh-process repair commands; approvals are scripted software checks."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import SandboxPipeline

ROOT = Path(__file__).resolve().parents[1]


class RepairCliTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.data, self.output, self.state = (self.root / name for name in ('input', 'output', 'state'))
        pipeline = SandboxPipeline(self.data)
        run = pipeline.create(generate_fixture('schema_drift', row_count=2).to_source_payload())
        self.run_id = pipeline.run(run.run_id).run_id

    def cli(self, *arguments):
        return subprocess.run([sys.executable, '-m', 'pipesleuth', 'repair', *arguments], cwd=ROOT,
            env=dict(os.environ, PYTHONPATH=str(ROOT / 'src'), PYTHONDONTWRITEBYTECODE='1'),
            text=True, capture_output=True, timeout=10)

    def plan(self):
        result = self.cli('plan', self.run_id, '--action', 'rename_amount_column', '--data-dir', str(self.data),
                          '--output-dir', str(self.output), '--state-dir', str(self.state))
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_explicit_hash_approval_then_verified_execution_across_processes(self):
        planned = self.plan()
        identifier = planned['investigation_id']
        state = planned['state']
        denied = self.cli('execute', identifier, '--state-dir', str(self.state))
        self.assertEqual(denied.returncode, 2)
        self.assertFalse(self.output.exists())
        approved = self.cli('approve', identifier, '--state-dir', str(self.state),
                            '--plan-sha256', state['plan_sha256'],
                            '--input-fingerprint-sha256', state['input_fingerprint_sha256'])
        self.assertEqual(approved.returncode, 0, approved.stderr)
        self.assertEqual(json.loads(approved.stdout)['state']['approval']['origin'], 'trusted_cli')
        executed = self.cli('execute', identifier, '--state-dir', str(self.state))
        self.assertEqual(executed.returncode, 0, executed.stderr)
        result = json.loads(executed.stdout)
        self.assertEqual(result['state']['status'], 'succeeded')
        self.assertTrue(result['state']['execution']['verification']['verified'])
        shown = self.cli('show', identifier, '--state-dir', str(self.state))
        self.assertEqual(shown.returncode, 0, shown.stderr)
        self.assertEqual(json.loads(shown.stdout), result)
        duplicate = self.cli('execute', identifier, '--state-dir', str(self.state))
        self.assertEqual(duplicate.returncode, 2)
        self.assertNotIn('Traceback', duplicate.stderr)

    def test_missing_and_unknown_requests_create_no_artifacts(self):
        result = self.cli('show', 'a' * 32, '--state-dir', str(self.state))
        self.assertEqual(result.returncode, 2)
        result = self.cli('plan', self.run_id, '--action', 'execute_sql', '--state-dir', str(self.state))
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.state.exists())
        self.assertFalse(self.output.exists())
        actions = self.cli('actions')
        self.assertEqual(actions.returncode, 0, actions.stderr)
        self.assertEqual(len(json.loads(actions.stdout)['actions']), 3)

    def test_wrong_acknowledgement_hash_keeps_plan_unapproved(self):
        planned = self.plan()
        identifier = planned['investigation_id']
        result = self.cli('approve', identifier, '--state-dir', str(self.state), '--plan-sha256', '0' * 64,
                          '--input-fingerprint-sha256', planned['state']['input_fingerprint_sha256'])
        self.assertEqual(result.returncode, 2)
        self.assertNotIn('Traceback', result.stderr)
        shown = self.cli('show', identifier, '--state-dir', str(self.state))
        self.assertEqual(json.loads(shown.stdout), planned)
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
