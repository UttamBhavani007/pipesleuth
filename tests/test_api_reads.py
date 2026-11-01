"""Read API integration over actual synthetic files; no inference requests."""

from dataclasses import FrozenInstanceError, replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

try:
    from fastapi.testclient import TestClient
except ImportError as error:
    raise unittest.SkipTest("optional API dependencies are not installed") from error

from pipesleuth.api import ApiSettings, create_app
from pipesleuth.checkpoints import CheckpointStore
from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import SandboxPipeline
from pipesleuth.repairs import RepairService


class ApiReadTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.settings = ApiSettings(*(self.root / name for name in ('input', 'output', 'investigations', 'repairs')))
        self.pipeline = SandboxPipeline(self.settings.data_dir)
        run = self.pipeline.create(generate_fixture('schema_drift', row_count=2).to_source_payload())
        self.summary = self.pipeline.run(run.run_id)
        self.run_id = run.run_id
        self.read_token, self.operation_token, self.approval_token = ('r' * 40, 'o' * 40, 'a' * 40)
        app = create_app(self.settings, read_token=self.read_token, operation_token=self.operation_token,
                         approval_token=self.approval_token)
        self.client = TestClient(app, base_url='http://127.0.0.1:8765', client=('127.0.0.1', 50000),
                                 raise_server_exceptions=False)
        self.addCleanup(self.client.close)
        self.headers = {'Authorization': 'Bearer ' + self.read_token}

    def get(self, path, **kwargs):
        return self.client.get(path, headers=self.headers, **kwargs)

    def snapshot(self):
        return {str(path.relative_to(self.root)): (path.read_bytes(), path.stat().st_mtime_ns)
                for path in self.root.rglob('*') if path.is_file()}

    def test_settings_are_immutable_disjoint_and_construction_has_no_writes(self):
        fresh = self.root / 'fresh'
        settings = ApiSettings(*(fresh / name for name in ('input', 'output', 'investigations', 'repairs')))
        create_app(settings, read_token=self.read_token, operation_token=self.operation_token,
                   approval_token=self.approval_token)
        self.assertFalse(fresh.exists())
        with self.assertRaises(ValueError):
            create_app(settings, read_token='short', operation_token=self.operation_token,
                       approval_token=self.approval_token)
        self.assertFalse(fresh.exists())
        with self.assertRaises(FrozenInstanceError):
            settings.port = 8766
        for changes in ({'port': True}, {'port': 80}, {'retry_limit': True}, {'max_steps': 9},
                        {'max_tool_calls': 5}, {'agent_timeout': float('nan')},
                        {'model_digest': 'bad'}, {'endpoint': 'https://example.org'},
                        {'output_dir': self.settings.data_dir / 'child'},
                        {'repair_state_dir': self.settings.investigation_state_dir}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(self.settings, **changes)

    def test_minimal_health_and_secured_schema_have_no_runtime_or_disk_side_effects(self):
        before = self.snapshot()
        with patch('pipesleuth.local_model.LocalModelClient.health', side_effect=AssertionError('no runtime')):
            response = self.client.get('/health')
            self.assertEqual(response.json(), {'status': 'ok', 'api_version': 1})
            self.assertEqual(self.client.get('/openapi.json').status_code, 401)
            schema = self.get('/openapi.json')
            self.assertEqual(schema.status_code, 200)
            self.assertIn('/v1/runs/{run_id}', schema.json()['paths'])
            self.assertEqual(self.get('/docs').json(), {'error': {'code': 'not_found'}})
            self.assertEqual(self.get('/docs').status_code, 404)
            head = self.client.head('/health', headers=self.headers)
            self.assertEqual(head.status_code, 405)
            self.assertEqual(self.get('/redoc').status_code, 404)
        self.assertEqual(self.snapshot(), before)
        self.assertFalse(self.settings.investigation_state_dir.exists())
        self.assertFalse(self.settings.repair_state_dir.exists())

    def test_run_and_four_tools_match_bounded_core_without_path_or_error_leaks(self):
        before = self.snapshot()
        response = self.get('/v1/runs/' + self.run_id)
        self.assertEqual(response.status_code, 200, response.text)
        summary = response.json()
        self.assertEqual((summary['run_id'], summary['status'], summary['rows_loaded']),
                         (self.run_id, 'failed', 0))
        self.assertNotIn('error', summary)
        self.assertNotIn(str(self.root), response.text)
        from pipesleuth.evidence import EvidenceGateway
        gateway = EvidenceGateway(self.settings.data_dir)
        for tool in ('read_run', 'read_logs', 'inspect_schema', 'inspect_validation'):
            response = self.get(f'/v1/runs/{self.run_id}/evidence/{tool}')
            self.assertEqual(response.status_code, 200, response.text)
            actual, expected = response.json(), gateway.call(tool, {'run_id': self.run_id})
            actual.pop('observed_at')
            expected.pop('observed_at')
            self.assertEqual(actual, expected)
        limited = self.get(f'/v1/runs/{self.run_id}/evidence/read_logs?limit=1').json()
        self.assertEqual(len(limited['data']['events']), 1)
        self.assertTrue(limited['truncated'])
        self.assertEqual(self.snapshot(), before)

    def test_fixed_capabilities_and_invalid_fields_cannot_select_queries_paths_or_roots(self):
        capability = self.get('/v1/capabilities').json()
        self.assertEqual(capability['diagnosis_methods'], ['checklist', 'local', 'agent'])
        self.assertFalse(capability['model_configured'])
        self.assertEqual(len(capability['repair_actions']), 3)
        for path in ('/v1/runs/not-a-uuid', f'/v1/runs/{self.run_id}/evidence/arbitrary_sql',
                     f'/v1/runs/{self.run_id}/evidence/read_run?limit=2',
                     f'/v1/runs/{self.run_id}/evidence/read_logs?limit=0',
                     f'/v1/runs/{self.run_id}/evidence/read_logs?limit=21',
                     f'/v1/runs/{self.run_id}/evidence/read_logs?limit=true'):
            with self.subTest(path=path):
                response = self.get(path)
                self.assertEqual(response.status_code, 422, response.text)
                self.assertEqual(response.json(), {'error': {'code': 'invalid_request'}})
        for path in (f'/v1/runs/{self.run_id}?data_dir=/private',
                     f'/v1/runs/{self.run_id}/evidence/read_logs?limit=1&limit=2'):
            response = self.get(path)
            self.assertEqual((response.status_code, response.json()),
                             (400, {'error': {'code': 'invalid_query'}}))
        self.assertEqual(self.get('/v1/runs/' + self.run_id + '/').status_code, 404)
        before = self.snapshot()
        missing = self.get('/v1/runs/' + 'f' * 32)
        self.assertEqual((missing.status_code, missing.json()), (404, {'error': {'code': 'read_unavailable'}}))
        self.assertEqual(self.snapshot(), before)

    def test_saved_records_load_only_without_model_or_input_reads(self):
        store = CheckpointStore(self.settings.investigation_state_dir)
        record = store.create({'saved': 'fixture_only'}, {'purpose': 'read contract'})
        planned = RepairService(self.settings.data_dir, self.settings.output_dir,
                                self.settings.repair_state_dir).plan(self.run_id, 'rename_amount_column')
        before = self.snapshot()
        with patch('pipesleuth.local_model.LocalModelClient', side_effect=AssertionError('no client')):
            response = self.get('/v1/investigations/' + record['investigation_id'])
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json(), record)
            response = self.get('/v1/repairs/' + planned['investigation_id'])
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json(), planned)
        self.assertEqual(self.snapshot(), before)
        for resource in ('investigations', 'repairs'):
            self.assertEqual(self.get('/v1/' + resource + '/' + 'f' * 32).status_code, 404)

    def test_root_replacement_and_internal_errors_fail_closed(self):
        link = self.settings.output_dir
        link.symlink_to(self.settings.data_dir, target_is_directory=True)
        self.assertEqual(self.get('/v1/runs/' + self.run_id).status_code, 404)
        link.unlink()
        with patch('pipesleuth.evidence.EvidenceGateway.call', side_effect=RuntimeError('/private/secret-data')):
            response = self.get('/v1/runs/' + self.run_id)
        self.assertEqual((response.status_code, response.json()), (500, {'error': {'code': 'internal_error'}}))
        self.assertNotIn('secret', response.text)
