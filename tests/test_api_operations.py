"""HTTP operation boundaries over real local files and labelled model fixtures."""

from dataclasses import replace
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
from pipesleuth.local_model import LocalModelConfig
from pipesleuth.pipeline import SandboxPipeline
from test_investigator import ScriptedClient, finish


class ApiOperationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.settings = ApiSettings(*(self.root / name for name in ('input', 'output', 'investigations', 'repairs')))
        self.pipeline = SandboxPipeline(self.settings.data_dir)
        self.run_id = self.pipeline.create(generate_fixture('schema_drift', row_count=2).to_source_payload()).run_id
        self.pipeline.run(self.run_id)
        self.tokens = {'read': 'r' * 40, 'operation': 'o' * 40, 'approval': 'a' * 40}
        self.client = self.make_client(self.settings)

    def make_client(self, settings):
        client = TestClient(create_app(settings, read_token=self.tokens['read'],
            operation_token=self.tokens['operation'], approval_token=self.tokens['approval']),
            base_url='http://127.0.0.1:8765', client=('127.0.0.1', 50000), raise_server_exceptions=False)
        self.addCleanup(client.close)
        return client

    def post(self, path, body, *, role='operation', client=None):
        return (client or self.client).post(path, json=body,
            headers={'Authorization': 'Bearer ' + self.tokens[role]})

    def read(self, path, *, client=None):
        return (client or self.client).get(path, headers={'Authorization': 'Bearer ' + self.tokens['read']})

    def snapshot_input(self):
        return {str(path.relative_to(self.settings.data_dir)): (path.read_bytes(), path.stat().st_mtime_ns)
                for path in self.settings.data_dir.rglob('*') if path.is_file()}

    def plan(self):
        response = self.post('/v1/repairs', {'run_id': self.run_id, 'action': 'rename_amount_column'})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def test_checklist_diagnosis_works_without_model_pin_or_runtime(self):
        before = self.snapshot_input()
        with patch('pipesleuth.local_model.LocalModelClient', side_effect=AssertionError('no model client')):
            response = self.post(f'/v1/runs/{self.run_id}/diagnosis', {'method': 'checklist'})
        self.assertEqual(response.status_code, 200, response.text)
        value = response.json()
        self.assertEqual((value['method'], value['status'], value['tool_calls']),
                         ('deterministic_checklist', 'diagnosed', 4))
        self.assertEqual(value['findings'][0]['code'], 'schema_mismatch')
        self.assertNotIn('model_calls', value)
        self.assertEqual(self.snapshot_input(), before)
        self.assertFalse(self.settings.investigation_state_dir.exists())

    def test_missing_model_pin_rejects_all_model_operations_before_admission(self):
        requests = [(f'/v1/runs/{self.run_id}/diagnosis', {'method': 'local'}),
                    (f'/v1/runs/{self.run_id}/diagnosis', {'method': 'agent'}),
                    ('/v1/investigations', {'run_id': self.run_id}),
                    ('/v1/investigations/' + 'f' * 32 + '/resume', {'max_operations': 1})]
        before = self.snapshot_input()
        with patch('pipesleuth.local_model.LocalModelClient', side_effect=AssertionError('no model client')):
            for path, body in requests:
                response = self.post(path, body)
                self.assertEqual((response.status_code, response.json()),
                                 (409, {'error': {'code': 'model_unconfigured'}}))
        self.assertEqual(self.snapshot_input(), before)
        self.assertFalse(self.settings.investigation_state_dir.exists())
        self.assertFalse(self.settings.output_dir.exists())

    def test_start_only_checkpoints_and_resume_preserves_bound_config_and_counts(self):
        settings = replace(self.settings, model_digest='a' * 64)
        client = self.make_client(settings)
        scripted = ScriptedClient([{'action': 'tool', 'tool': 'inspect_schema'},
                                   finish('diagnosed', 'schema_mismatch')])
        scripted.config = settings.model_config()
        before = self.snapshot_input()
        with patch('pipesleuth.local_model.LocalModelClient', return_value=scripted):
            started = self.post('/v1/investigations', {'run_id': self.run_id}, client=client)
            self.assertEqual(started.status_code, 200, started.text)
            record = started.json()
            identifier = record['investigation_id']
            self.assertEqual((record['state']['result']['tool_calls'], scripted.generation_requests), (0, 0))
            resumed = self.post(f'/v1/investigations/{identifier}/resume', {'max_operations': 1}, client=client)
            self.assertEqual(resumed.status_code, 200, resumed.text)
            self.assertEqual((resumed.json()['state']['result']['tool_calls'], scripted.generation_requests), (1, 0))
            finished = self.post(f'/v1/investigations/{identifier}/resume', {}, client=client)
            self.assertEqual(finished.status_code, 200, finished.text)
            self.assertTrue(finished.json()['state']['done'])
            self.assertEqual(finished.json()['state']['result']['model_calls'], 2)
            store = settings.investigation_state_dir / 'checkpoints.sqlite3'
            before_store = (store.read_bytes(), store.stat().st_mtime_ns)
            terminal = self.post(f'/v1/investigations/{identifier}/resume', {}, client=client)
            self.assertEqual(terminal.json(), finished.json())
            self.assertEqual((store.read_bytes(), store.stat().st_mtime_ns), before_store)
            self.assertEqual(scripted.generation_requests, 2)
        self.assertEqual(self.snapshot_input(), before)

    def test_resume_cannot_restore_an_endpoint_or_changed_digest_from_saved_record(self):
        settings = replace(self.settings, model_digest='a' * 64)
        client = self.make_client(settings)
        record = self.post('/v1/investigations', {'run_id': self.run_id}, client=client).json()
        binding = record['binding']
        binding['config']['endpoint'] = 'http://127.0.0.1:9'
        store = CheckpointStore(settings.investigation_state_dir)
        forged = store.create(record['state'], binding)
        path = settings.investigation_state_dir / 'checkpoints.sqlite3'
        before = (path.read_bytes(), path.stat().st_mtime_ns)
        with patch('pipesleuth.local_model.LocalModelClient.health', side_effect=AssertionError('no runtime')):
            response = self.post('/v1/investigations/' + forged['investigation_id'] + '/resume', {}, client=client)
            self.assertEqual((response.status_code, response.json()),
                             (409, {'error': {'code': 'operation_rejected'}}))
            different = self.make_client(replace(settings, model_digest='b' * 64))
            response = self.post('/v1/investigations/' + record['investigation_id'] + '/resume', {}, client=different)
            self.assertEqual(response.status_code, 409)
        self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), before)

    def test_repair_requires_separate_exact_approval_and_preserves_original(self):
        before = self.snapshot_input()
        planned = self.plan()
        identifier = planned['investigation_id']
        path = f'/v1/repairs/{identifier}'
        approval = {name: planned['state'][name] for name in ('plan_sha256', 'input_fingerprint_sha256')}
        self.assertEqual(self.post(path + '/execute', {}).status_code, 409)
        self.assertEqual(self.post(path + '/approve', approval).status_code, 403)
        self.assertEqual(self.post(path + '/approve', approval, role='read').status_code, 403)
        wrong = dict(approval, plan_sha256='f' * 64)
        self.assertEqual(self.post(path + '/approve', wrong, role='approval').status_code, 409)
        approved = self.post(path + '/approve', approval, role='approval')
        self.assertEqual(approved.status_code, 200, approved.text)
        self.assertEqual(approved.json()['state']['approval']['origin'], 'trusted_api')
        self.assertEqual(self.post(path + '/execute', {}, role='approval').status_code, 403)
        executed = self.post(path + '/execute', {})
        self.assertEqual(executed.status_code, 200, executed.text)
        value = executed.json()
        self.assertEqual(value['state']['status'], 'succeeded')
        self.assertTrue(value['state']['execution']['committed'])
        self.assertEqual(self.read(path).json(), value)
        self.assertEqual(self.post(path + '/execute', {}).status_code, 409)
        self.assertEqual(self.snapshot_input(), before)
        target = value['state']['plan']['target_run_id']
        self.assertEqual(SandboxPipeline(self.settings.output_dir).inspect(target).rows_loaded, 2)

    def test_stale_input_rejects_execution_before_claim_or_target_creation(self):
        planned = self.plan()
        identifier = planned['investigation_id']
        path = f'/v1/repairs/{identifier}'
        approval = {name: planned['state'][name] for name in ('plan_sha256', 'input_fingerprint_sha256')}
        approved = self.post(path + '/approve', approval, role='approval').json()
        source = self.settings.data_dir / self.run_id / 'source.json'
        source.write_bytes(source.read_bytes() + b'\n')
        response = self.post(path + '/execute', {})
        self.assertEqual((response.status_code, response.json()), (409, {'error': {'code': 'operation_rejected'}}))
        self.assertEqual(self.read(path).json(), approved)
        self.assertFalse(self.settings.output_dir.exists())

    def test_closed_bodies_reject_authority_and_configuration_fields_without_echo(self):
        requests = [(f'/v1/runs/{self.run_id}/diagnosis', {'method': 'checklist', 'endpoint': '/private/secret'}),
                    ('/v1/investigations', {'run_id': self.run_id, 'data_dir': '/private/secret'}),
                    ('/v1/investigations/' + 'f' * 32 + '/resume', {'max_operations': True}),
                    ('/v1/investigations/' + 'f' * 32 + '/resume', {'max_operations': 17}),
                    ('/v1/repairs', {'run_id': self.run_id, 'action': 'arbitrary_sql'}),
                    ('/v1/repairs/' + 'f' * 32 + '/execute', {'approval': True, 'sql': 'DELETE'})]
        for path, body in requests:
            response = self.post(path, body)
            self.assertEqual((response.status_code, response.json()), (422, {'error': {'code': 'invalid_request'}}))
            self.assertNotIn('private', response.text)
        self.assertFalse(self.settings.investigation_state_dir.exists())
        self.assertFalse(self.settings.repair_state_dir.exists())
        self.assertFalse(self.settings.output_dir.exists())

    def test_business_and_internal_errors_are_generic_and_wrong_role_cannot_mutate(self):
        body = {'run_id': self.run_id, 'action': 'rename_amount_column'}
        self.assertEqual(self.post('/v1/repairs', body, role='read').status_code, 403)
        self.assertEqual(self.post('/v1/repairs', body, role='approval').status_code, 403)
        with patch('pipesleuth.repairs.RepairService.plan', side_effect=ValueError('/private/secret')):
            response = self.post('/v1/repairs', body)
        self.assertEqual((response.status_code, response.json()), (409, {'error': {'code': 'operation_rejected'}}))
        with patch('pipesleuth.repairs.RepairService.plan', side_effect=RuntimeError('/private/secret')):
            response = self.post('/v1/repairs', body)
        self.assertEqual((response.status_code, response.json()), (500, {'error': {'code': 'internal_error'}}))
        self.assertFalse(self.settings.repair_state_dir.exists())
