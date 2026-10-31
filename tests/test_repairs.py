"""Approval, concurrency and before/after-commit fault checks; no human review."""

from contextlib import closing
from copy import deepcopy
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import SandboxPipeline
from pipesleuth.repairs import RepairError, RepairService, _digest


class RepairServiceTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.data, self.output, self.state = (self.root / name for name in ('input', 'output', 'state'))
        self.pipeline = SandboxPipeline(self.data)
        run = self.pipeline.create(generate_fixture('schema_drift', row_count=2).to_source_payload())
        self.run_id = self.pipeline.run(run.run_id).run_id
        self.service = RepairService(self.data, self.output, self.state)

    def planned(self):
        return self.service.plan(self.run_id, 'rename_amount_column')

    def approved(self):
        planned = self.planned()
        state = planned['state']
        return self.service.approve(planned['investigation_id'], state['plan_sha256'],
                                    state['input_fingerprint_sha256'], origin='scripted_test')

    def target(self, record):
        return self.output / record['state']['plan']['target_run_id'] / 'warehouse.sqlite3'

    def test_failed_incident_required_and_invalid_origin_cannot_approve(self):
        healthy = self.pipeline.create(generate_fixture('healthy', row_count=2).to_source_payload())
        self.pipeline.run(healthy.run_id)
        with self.assertRaises(RepairError):
            self.service.plan(healthy.run_id, 'rename_amount_column')
        self.assertFalse(self.state.exists())
        record = self.planned()
        for origin in ('model', '', True):
            with self.subTest(origin=origin), self.assertRaises(RepairError):
                self.service.approve(record['investigation_id'], record['state']['plan_sha256'],
                                     record['state']['input_fingerprint_sha256'], origin=origin)
            self.assertEqual(self.service.show(record['investigation_id']), record)

    def test_changed_plan_cannot_reuse_an_old_approval(self):
        record = self.approved()
        state = deepcopy(record['state'])
        state['plan']['target_run_id'] = 'f' * 32
        state['plan_sha256'] = _digest(state['plan'])
        claim = self.service.store.claim(record['investigation_id'], record['revision'], 'controlled_corruption')
        self.service.store.complete(record['investigation_id'], claim['pending']['token'], state)
        with self.assertRaises(RepairError):
            self.service.execute(record['investigation_id'])
        self.assertFalse(self.output.exists())

    def test_extra_plan_commands_are_rejected_even_with_consistent_storage_checksum(self):
        record = self.planned()
        state = deepcopy(record['state'])
        state['plan']['sql'] = 'DELETE FROM warehouse_orders'
        state['plan_sha256'] = _digest(state['plan'])
        claim = self.service.store.claim(record['investigation_id'], record['revision'], 'controlled_corruption')
        self.service.store.complete(record['investigation_id'], claim['pending']['token'], state)
        with self.assertRaises(RepairError):
            self.service.show(record['investigation_id'])
        self.assertFalse(self.output.exists())

    def test_unresolved_execution_intent_refuses_replay_before_output(self):
        record = self.approved()
        self.service.store.claim(record['investigation_id'], record['revision'], 'execute_repair')
        pending = self.service.show(record['investigation_id'])
        with self.assertRaisesRegex(RepairError, 'uncertain'):
            self.service.execute(record['investigation_id'])
        self.assertEqual(self.service.show(record['investigation_id']), pending)
        self.assertFalse(self.output.exists())

    def test_source_change_at_precommit_rolls_back_output_rows(self):
        import pipesleuth.repair_executor as module
        real = module.execute_repair
        record = self.approved()
        def changed(output, target, prepared, *, timeout_seconds, pre_commit):
            def mutate_then_check():
                source = self.data / self.run_id / 'source.json'
                source.write_bytes(source.read_bytes() + b'\n')
                pre_commit()
            return real(output, target, prepared, timeout_seconds=timeout_seconds, pre_commit=mutate_then_check)
        with patch.object(module, 'execute_repair', side_effect=changed):
            stopped = self.service.execute(record['investigation_id'])
        self.assertEqual(stopped['state']['status'], 'failed')
        receipt = stopped['state']['execution']
        self.assertFalse(receipt['committed'])
        self.assertTrue(receipt['rolled_back'])
        with closing(sqlite3.connect(self.target(record))) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM warehouse_orders').fetchone(), (0,))
            self.assertEqual(connection.execute('SELECT status FROM pipeline_run').fetchone(), ('new',))

    def test_postcommit_drift_is_failed_without_a_false_rollback_claim(self):
        import pipesleuth.repair_executor as module
        real = module.execute_repair
        record = self.approved()
        def changed(*args, **kwargs):
            receipt = real(*args, **kwargs)
            source = self.data / self.run_id / 'source.json'
            source.write_bytes(source.read_bytes() + b'\n')
            return receipt
        with patch.object(module, 'execute_repair', side_effect=changed):
            stopped = self.service.execute(record['investigation_id'])
        self.assertEqual(stopped['state']['status'], 'failed')
        receipt = stopped['state']['execution']
        self.assertTrue(receipt['committed'])
        self.assertFalse(receipt['rolled_back'])
        self.assertFalse(receipt['post_input_verified'])
        self.assertEqual(receipt['failure_code'], 'input_changed_after_commit')
        with closing(sqlite3.connect(self.target(record))) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM warehouse_orders').fetchone(), (2,))

    def test_concurrent_execution_has_one_claim_and_one_target(self):
        import pipesleuth.repair_executor as module
        real = module.execute_repair
        record = self.approved()
        entered, release = threading.Event(), threading.Event()
        completed, errors = [], []
        def slow(*args, **kwargs):
            entered.set()
            if not release.wait(2):
                raise RuntimeError('controlled worker was not released')
            return real(*args, **kwargs)
        def worker():
            try:
                completed.append(self.service.execute(record['investigation_id']))
            except Exception as failure:
                errors.append(type(failure).__name__)
        with patch.object(module, 'execute_repair', side_effect=slow):
            thread = threading.Thread(target=worker)
            thread.start()
            try:
                self.assertTrue(entered.wait(1))
                fresh = RepairService(self.data, self.output, self.state)
                with self.assertRaisesRegex(RepairError, 'uncertain'):
                    fresh.execute(record['investigation_id'])
            finally:
                release.set()
                thread.join(timeout=4)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0]['state']['status'], 'succeeded')
        self.assertEqual([p.name for p in self.output.iterdir()], [record['state']['plan']['target_run_id']])

    def test_uncertain_commit_receipt_retains_intent_without_retry(self):
        from pipesleuth.repair_executor import RepairExecutionError
        record = self.approved()
        receipt = {'target_run_id': record['state']['plan']['target_run_id'], 'status': 'failed',
                   'committed': None, 'rolled_back': False, 'outcome_uncertain': True,
                   'failure_code': 'controlled_missing_commit_acknowledgement'}
        with patch('pipesleuth.repair_executor.execute_repair', side_effect=RepairExecutionError(receipt)) as execute:
            with self.assertRaisesRegex(RepairError, 'uncertain'):
                self.service.execute(record['investigation_id'])
            pending = self.service.show(record['investigation_id'])
            self.assertIsNotNone(pending['pending'])
            self.assertEqual(pending['state'], record['state'])
            with self.assertRaisesRegex(RepairError, 'uncertain'):
                self.service.execute(record['investigation_id'])
            self.assertEqual(execute.call_count, 1)


if __name__ == '__main__':
    unittest.main()
