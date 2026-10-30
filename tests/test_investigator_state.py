"""JSON cursor, lifetime budget and safe retry checks with scripted fixtures."""

from copy import deepcopy
import json
import time
import unittest
from unittest.mock import patch

from pipesleuth.evidence import EvidenceError
from pipesleuth.investigator import BoundedInvestigator, InvestigatorError
from pipesleuth.local_model import LocalModelError
from test_investigator import RecordedGateway, ScriptedClient, finish
import test_investigator as fixtures


class FlakyGateway(RecordedGateway):
    def __init__(self, real, failures):
        super().__init__(real)
        self.failures = set(failures)

    def call(self, name, arguments, *, timeout_seconds=None):
        if len(self.calls) in self.failures:
            self.calls.append((name, dict(arguments), timeout_seconds))
            raise EvidenceError("explicit transient fixture")
        return super().call(name, arguments, timeout_seconds=timeout_seconds)


class InvestigatorStateTests(unittest.TestCase):
    setUp = fixtures.InvestigatorTests.setUp
    create = fixtures.InvestigatorTests.create
    snapshot = fixtures.InvestigatorTests.snapshot
    def test_json_operations_are_detached_and_selected_tools_resume_without_regeneration(self):
        run_id = self.create()
        first_client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"}])
        engine = BoundedInvestigator(self.gateway, first_client)
        state = engine.initial_state(run_id)
        self.assertEqual((self.gateway.calls, first_client.calls), ([], []))
        self.assertEqual(set(state), {"version", "result", "next", "elapsed_seconds", "done", "retries"})
        original = deepcopy(state)
        state = engine.advance(state)
        self.assertEqual(original, engine.initial_state(run_id))
        self.assertEqual(state["next"], {"action": "generate"})
        previous = deepcopy(state)
        state = engine.advance(state)
        self.assertEqual(previous["result"]["steps"], 0)
        self.assertEqual(state["next"], {"action": "tool", "tool": "inspect_schema", "step": 1})
        state = json.loads(json.dumps(state))
        next_client = ScriptedClient([finish()])
        resumed = BoundedInvestigator(self.gateway, next_client)
        state = resumed.advance(state)
        self.assertEqual(next_client.calls, [])
        self.assertEqual([call[0] for call in self.gateway.calls], ["read_run", "inspect_schema"])
        self.assertEqual(state["next"], {"action": "generate"})
        state = resumed.advance(state)
        self.assertTrue(state["done"])
        self.assertIsNone(state["next"])
        self.assertEqual(state["result"]["status"], "no_incident")
        self.assertEqual((state["result"]["tool_calls"], state["result"]["model_calls"], state["result"]["steps"]), (2, 2, 2))
        calls = (len(self.gateway.calls), len(next_client.calls))
        self.assertEqual(resumed.advance(state), state)
        self.assertEqual((len(self.gateway.calls), len(next_client.calls)), calls)

    def test_each_advance_performs_at_most_one_component_operation(self):
        run_id = self.create()
        client = ScriptedClient([{"action": "tool", "tool": "read_logs"}, finish()])
        engine = BoundedInvestigator(self.gateway, client)
        state = engine.initial_state(run_id)
        while not state["done"]:
            before = len(self.gateway.calls) + len(client.calls)
            state = engine.advance(state)
            after = len(self.gateway.calls) + len(client.calls)
            self.assertLessEqual(after - before, 1)
        self.assertEqual((state["result"]["tool_calls"], state["result"]["model_calls"]), (2, 2))

    def test_pauses_do_not_count_as_active_time_and_lifetime_exhaustion_does_not_reset(self):
        run_id = self.create()
        client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"}, finish()])
        engine = BoundedInvestigator(self.gateway, client, timeout_seconds=1)
        state = engine.advance(engine.initial_state(run_id))
        active_before = state["elapsed_seconds"]
        started = time.monotonic()
        time.sleep(0.05)
        state = engine.advance(json.loads(json.dumps(state)))
        elapsed = time.monotonic() - started
        self.assertLess(state["elapsed_seconds"] - active_before, elapsed - 0.025)
        state["elapsed_seconds"] = 1.0
        state["result"]["elapsed_ms"] = 1000.0
        calls = (len(self.gateway.calls), len(client.calls))
        state = engine.advance(state)
        self.assertTrue(state["done"])
        self.assertIn("elapsed_budget_exceeded", state["result"]["limitations"])
        self.assertEqual((len(self.gateway.calls), len(client.calls)), calls)

    def test_lifetime_step_budget_still_admits_an_already_selected_read_once(self):
        run_id = self.create()
        client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"}])
        engine = BoundedInvestigator(self.gateway, client, max_steps=1)
        state = engine.advance(engine.advance(engine.initial_state(run_id)))
        self.assertEqual(state["next"]["tool"], "inspect_schema")
        state = engine.advance(json.loads(json.dumps(state)))
        self.assertTrue(state["done"])
        self.assertEqual((state["result"]["steps"], state["result"]["tool_calls"]), (1, 2))
        self.assertIn("step_budget_exceeded", state["result"]["limitations"])

    def test_one_observed_failed_read_can_retry_without_hiding_the_failure(self):
        run_id = self.create()
        gateway = FlakyGateway(self.gateway.real, {0})
        client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"}, finish()])
        engine = BoundedInvestigator(gateway, client)
        state = engine.initial_state(run_id)
        state = engine.advance(state, retry_limit=1)
        self.assertFalse(state["done"])
        self.assertEqual(state["retries"], 1)
        self.assertEqual(state["result"]["trace"][0]["failure_code"], "evidence_unavailable")
        while not state["done"]:
            state = engine.advance(state, retry_limit=1)
        self.assertEqual(state["result"]["status"], "no_incident")
        self.assertEqual(state["result"]["tool_calls"], 3)
        self.assertIn("retry_recovered", state["result"]["limitations"])
        self.assertEqual(state["result"]["trace"][0]["failure_code"], "evidence_unavailable")

    def test_retry_is_global_and_generation_attempts_are_never_repeated(self):
        run_id = self.create()
        gateway = FlakyGateway(self.gateway.real, {0, 2})
        client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"}])
        engine = BoundedInvestigator(gateway, client)
        state = engine.initial_state(run_id)
        while not state["done"]:
            state = engine.advance(state, retry_limit=1)
        self.assertEqual((state["retries"], state["result"]["tool_calls"]), (1, 3))
        self.assertEqual(state["result"]["status"], "unknown")
        client = ScriptedClient([LocalModelError("attempted generation fixture")])
        engine = BoundedInvestigator(self.gateway, client)
        state = engine.advance(engine.advance(engine.initial_state(run_id)), retry_limit=1)
        self.assertTrue(state["done"])
        self.assertEqual((state["retries"], state["result"]["model_calls"]), (0, 1))
        self.assertEqual(len(client.calls), 1)

    def test_pre_generation_health_failure_can_use_the_single_retry(self):
        run_id = self.create()
        client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"}, finish()], fail_health=True)
        engine = BoundedInvestigator(self.gateway, client)
        state = engine.advance(engine.initial_state(run_id))
        state = engine.advance(state, retry_limit=1)
        self.assertFalse(state["done"])
        self.assertEqual((state["retries"], state["result"]["steps"], state["result"]["model_calls"]), (1, 1, 0))
        client.fail_health = False
        while not state["done"]:
            state = engine.advance(state, retry_limit=1)
        self.assertEqual(state["result"]["status"], "no_incident")
        self.assertEqual((state["result"]["steps"], state["result"]["model_calls"]), (3, 2))

    def test_corrupt_cursor_counts_policy_and_budget_fail_before_components(self):
        run_id = self.create()
        client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"}])
        engine = BoundedInvestigator(self.gateway, client)
        state = engine.advance(engine.advance(engine.initial_state(run_id)))
        variants = []
        wrong = deepcopy(state); wrong["version"] = 2; variants.append(wrong)
        wrong = deepcopy(state); wrong["next"]["tool"] = "execute_sql"; variants.append(wrong)
        wrong = deepcopy(state); wrong["next"]["tool"] = "read_logs"; variants.append(wrong)
        wrong = deepcopy(state); wrong["next"]["arguments"] = {"run_id": "f" * 32}; variants.append(wrong)
        wrong = deepcopy(state); wrong["result"]["steps"] = 0; variants.append(wrong)
        wrong = deepcopy(state); wrong["elapsed_seconds"] = 0; wrong["result"]["elapsed_ms"] = 0; variants.append(wrong)
        wrong = deepcopy(state); wrong["retries"] = True; variants.append(wrong)
        calls = (len(self.gateway.calls), len(client.calls))
        for wrong in variants:
            with self.subTest(cursor=wrong["next"]), self.assertRaises(InvestigatorError):
                engine.advance(wrong)
            self.assertEqual((len(self.gateway.calls), len(client.calls)), calls)
        for retry_limit in (True, -1, 2, 1.0):
            with self.subTest(retry_limit=retry_limit), self.assertRaises(InvestigatorError):
                engine.advance(state, retry_limit=retry_limit)


    def test_completed_unavailable_evidence_is_not_retried_or_duplicated(self):
        run_id = self.create()
        from contextlib import closing
        import sqlite3
        with closing(sqlite3.connect(self.pipeline.database_path(run_id))) as connection, connection:
            connection.execute("DROP TABLE pipeline_events")
        client = ScriptedClient([])
        engine = BoundedInvestigator(self.gateway, client)
        state = engine.advance(engine.initial_state(run_id), retry_limit=1)
        self.assertTrue(state["done"])
        self.assertEqual((state["retries"], state["result"]["tool_calls"]), (0, 1))
        self.assertEqual(len(state["result"]["evidence"]), 1)
        self.assertEqual(engine.advance(state, retry_limit=1), state)

    def test_budget_expiry_before_an_attempt_never_reuses_an_old_failure_trace(self):
        run_id = self.create()
        client = ScriptedClient([])
        engine = BoundedInvestigator(self.gateway, client)
        with patch.object(engine, "_budget", side_effect=InvestigatorError("elapsed_budget_exceeded")):
            state = engine.advance(engine.initial_state(run_id), retry_limit=1)
        self.assertTrue(state["done"])
        self.assertEqual((state["retries"], state["result"]["tool_calls"], state["result"]["model_calls"]), (0, 0, 0))
        self.assertEqual(state["result"]["trace"], [])


if __name__ == "__main__":
    unittest.main()
