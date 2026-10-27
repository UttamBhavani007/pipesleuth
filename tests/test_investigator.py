"""Bounded investigator checks using real evidence and scripted model fixtures.

Scripted responses establish software behavior, not local-model accuracy.
"""

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from pipesleuth.evidence import EvidenceGateway
from pipesleuth.fixtures import generate_fixture
from pipesleuth.investigator import BoundedInvestigator, InvestigatorError
from pipesleuth.local_model import LocalModelConfig, LocalModelError, ModelResponse
import pipesleuth.investigator as investigator_module
from pipesleuth.pipeline import SandboxPipeline


class RecordedGateway:
    def __init__(self, real, delay=0):
        self.real = real
        self.timeout_seconds = real.timeout_seconds
        self.calls = []
        self.delay = delay

    def call(self, name, arguments, *, timeout_seconds=None):
        self.calls.append((name, dict(arguments), timeout_seconds))
        if self.delay:
            time.sleep(self.delay)
        return self.real.call(name, arguments, timeout_seconds=timeout_seconds)


def model_response(text, reason="stop"):
    return ModelResponse(text=text, model="qwen3:1.7b", digest="a" * 64,
                         prompt_tokens=100, generated_tokens=20,
                         total_duration_ns=10000000, load_duration_ns=1000000,
                         prompt_eval_duration_ns=2000000, eval_duration_ns=7000000,
                         done_reason=reason)


class ScriptedClient:
    def __init__(self, script, delay=0, fail_health=False):
        self.config = LocalModelConfig()
        self.script = list(script)
        self.delay, self.fail_health = delay, fail_health
        self.calls = []
        self.generation_requests = 0

    def generate(self, system, prompt, schema, *, timeout_seconds=None):
        self.calls.append({"system": system, "prompt": json.loads(prompt),
                           "schema": deepcopy(schema), "timeout_seconds": timeout_seconds})
        if self.fail_health:
            raise LocalModelError("fixture health failure")
        self.generation_requests += 1
        if self.delay:
            time.sleep(self.delay)
        action = self.script.pop(0)
        if isinstance(action, Exception):
            raise action
        if callable(action):
            action = action(self.calls[-1]["prompt"], schema)
        if isinstance(action, ModelResponse):
            return action
        return model_response(action if isinstance(action, str) else json.dumps(action))


def finish(status="no_incident", code=None):
    def decision(context, schema):
        findings = []
        if code is not None:
            findings = [{"code": code, "summary": "The observed columns differ from the order contract.",
                         "evidence_refs": [context["evidence"][-1]["evidence_id"]]}]
        return {"action": "finish", "diagnosis": {"status": status, "findings": findings}}
    return decision


class InvestigatorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.pipeline = SandboxPipeline(Path(self.temporary.name).resolve() / "runs")
        self.gateway = RecordedGateway(EvidenceGateway(self.pipeline.data_dir))

    def create(self, scenario="healthy", **policies):
        created = self.pipeline.create(generate_fixture(scenario, row_count=3).to_source_payload(), **policies)
        self.pipeline.run(created.run_id)
        return created.run_id

    def snapshot(self):
        return {str(path.relative_to(self.pipeline.data_dir)): (path.read_bytes(), path.stat().st_mtime_ns)
                for path in self.pipeline.data_dir.rglob("*") if path.is_file()}

    def test_configuration_and_uuid_reject_invalid_values_before_components(self):
        for key, values in (("max_steps", (0, 9, True, 1.0, "4")),
                            ("max_tool_calls", (0, 5, True, 1.0, "4")),
                            ("timeout_seconds", (0, -1, 121, True, float("nan"), float("inf"), 10**10000))):
            for value in values:
                with self.subTest(key=key, value_type=type(value)), self.assertRaises(InvestigatorError):
                    BoundedInvestigator(None, None, **{key: value})
        agent = BoundedInvestigator(None, None)
        for run_id in (None, "../outside", "A" * 32, "0" * 31):
            with self.subTest(run_id=run_id), self.assertRaises(InvestigatorError):
                agent.investigate(run_id)
        self.assertFalse(self.pipeline.data_dir.exists())

    def test_adaptive_schema_read_and_finish_keep_real_references_and_files(self):
        run_id = self.create("schema_drift")
        before = self.snapshot()
        client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"},
                                 finish("diagnosed", "schema_mismatch")])
        result = BoundedInvestigator(self.gateway, client, max_tool_calls=2).investigate(run_id)
        self.assertEqual(result["status"], "diagnosed")
        self.assertEqual((result["tool_calls"], result["model_calls"], result["steps"]), (2, 2, 2))
        self.assertEqual([call[0] for call in self.gateway.calls], ["read_run", "inspect_schema"])
        self.assertEqual([call[1] for call in self.gateway.calls], [{"run_id": run_id}] * 2)
        self.assertEqual(len(result["trace"]), 4)
        references = {item["evidence_id"] for item in result["evidence"]}
        self.assertTrue(set(result["findings"][0]["evidence_refs"]) <= references)
        self.assertFalse(result["semantic_support_verified"])
        self.assertFalse(result["trace_truncated"])
        first, second = (call["prompt"] for call in client.calls)
        self.assertEqual(first["allowed_actions"], ["tool"])
        self.assertEqual(second["allowed_actions"], ["finish"])
        self.assertNotIn("deterministic_checklist", json.dumps(client.calls))
        self.assertNotIn('"scenario"', json.dumps(client.calls))
        self.assertEqual(self.snapshot(), before)

    def test_trusted_fixed_arguments_and_unvisited_tool_catalog(self):
        run_id = self.create()
        client = ScriptedClient([{"action": "tool", "tool": "read_logs"},
                                 {"action": "tool", "tool": "inspect_validation"}, finish()])
        result = BoundedInvestigator(self.gateway, client).investigate(run_id)
        self.assertEqual(result["status"], "no_incident")
        self.assertEqual((result["tool_calls"], result["model_calls"], result["steps"]), (3, 3, 3))
        self.assertEqual(self.gateway.calls[1][1], {"run_id": run_id, "limit": 20})
        self.assertEqual(self.gateway.calls[2][1], {"run_id": run_id, "limit": 20})
        self.assertEqual([item["name"] for item in client.calls[0]["prompt"]["available_tools"]],
                         ["read_logs", "inspect_schema", "inspect_validation"])
        self.assertNotIn("read_logs", [item["name"] for item in client.calls[1]["prompt"]["available_tools"]])
        self.assertNotIn("inspect_validation", [item["name"] for item in client.calls[2]["prompt"]["available_tools"]])

    def test_early_finish_unknown_tools_cross_run_arguments_and_repeats_are_rejected(self):
        run_id = self.create()
        invalid_scripts = [
            [{"action": "finish", "diagnosis": {"status": "no_incident", "findings": []}}],
            [{"action": "tool", "tool": "execute_sql"}],
            [{"action": "tool", "tool": "read_run"}],
            [{"action": "tool", "tool": "inspect_schema", "arguments": {"run_id": "f" * 32}}],
            [{"action": "tool", "tool": "read_logs", "sql": "DELETE FROM warehouse_orders"}],
            [{"action": "tool", "tool": "inspect_schema"}, {"action": "tool", "tool": "inspect_schema"}],
            [{"action": "tool", "tool": "inspect_schema"},
             {"action": "finish", "diagnosis": {"status": "no_incident", "findings": []}, "approve": True}],
        ]
        before = self.snapshot()
        for script in invalid_scripts:
            with self.subTest(script=script):
                gateway = RecordedGateway(self.gateway.real)
                client = ScriptedClient(script)
                result = BoundedInvestigator(gateway, client).investigate(run_id)
                self.assertEqual((result["status"], result["findings"]), ("unknown", []))
                self.assertIn("invalid_decision", result["limitations"])
                self.assertEqual(result["model_calls"], len(script))
                self.assertEqual(result["tool_calls"], 1 if len(script) == 1 else 2)
                self.assertEqual(self.snapshot(), before)

    def test_tool_and_step_budgets_preserve_actual_counts(self):
        run_id = self.create()
        client = ScriptedClient([])
        limited = BoundedInvestigator(self.gateway, client, max_tool_calls=1).investigate(run_id)
        self.assertEqual((limited["tool_calls"], limited["steps"], limited["model_calls"]), (1, 0, 0))
        self.assertIn("tool_budget_exceeded", limited["limitations"])
        self.assertEqual(client.calls, [])
        client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"}])
        limited = BoundedInvestigator(self.gateway, client, max_steps=1).investigate(run_id)
        self.assertEqual((limited["status"], limited["tool_calls"], limited["steps"], limited["model_calls"]),
                         ("unknown", 2, 1, 1))
        self.assertIn("step_budget_exceeded", limited["limitations"])

    def test_missing_and_truncated_evidence_stop_without_further_inference(self):
        missing = BoundedInvestigator(self.gateway, ScriptedClient([])).investigate("0" * 32)
        self.assertEqual((missing["status"], missing["tool_calls"], missing["model_calls"]), ("unknown", 1, 0))
        self.assertFalse(self.pipeline.data_dir.exists())
        source = generate_fixture("healthy", row_count=1).to_source_payload()
        source["rows"].extend(dict(source["rows"][0], order_id=f"R-{index}", amount_minor=True)
                              for index in range(21))
        created = self.pipeline.create(source, invalid_policy="quarantine")
        self.pipeline.run(created.run_id)
        before = self.snapshot()
        client = ScriptedClient([{"action": "tool", "tool": "inspect_validation"}])
        result = BoundedInvestigator(self.gateway, client).investigate(created.run_id)
        self.assertEqual((result["status"], result["tool_calls"], result["model_calls"]), ("unknown", 2, 1))
        self.assertIn("evidence_truncated", result["limitations"])
        self.assertEqual(self.snapshot(), before)

    def test_model_errors_length_and_malformed_json_fail_without_retry(self):
        run_id = self.create()
        for action, reason in ((LocalModelError("fixture failure"), "local_model_error"),
                               (model_response('{"action":', "length"), "generation_truncated"),
                               ("not-json", "invalid_decision")):
            with self.subTest(reason=reason):
                client = ScriptedClient([action])
                result = BoundedInvestigator(self.gateway, client).investigate(run_id)
                self.assertEqual((result["status"], result["steps"], result["model_calls"]), ("unknown", 1, 1))
                self.assertIn(reason, result["limitations"])
                self.assertEqual(len(client.calls), 1)
        client = ScriptedClient([], fail_health=True)
        result = BoundedInvestigator(self.gateway, client).investigate(run_id)
        self.assertEqual((result["steps"], result["model_calls"]), (1, 0))
        self.assertIn("local_model_error", result["limitations"])

    def test_all_component_timeouts_are_clamped_to_remaining_absolute_budget(self):
        run_id = self.create()
        client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"}, finish()], delay=0.005)
        result = BoundedInvestigator(self.gateway, client, timeout_seconds=2).investigate(run_id)
        self.assertEqual(result["status"], "no_incident")
        budgets = [self.gateway.calls[0][2], client.calls[0]["timeout_seconds"],
                   self.gateway.calls[1][2], client.calls[1]["timeout_seconds"]]
        self.assertTrue(all(0 < budget <= 2 for budget in budgets))
        self.assertTrue(all(first > second for first, second in zip(budgets, budgets[1:])))
        self.assertEqual(self.gateway.timeout_seconds, 2)
        self.assertEqual(client.config.timeout_seconds, 120)

    def test_elapsed_budget_is_checked_after_late_components_before_any_next_action(self):
        run_id = self.create()
        gateway = RecordedGateway(self.gateway.real, delay=0.04)
        client = ScriptedClient([])
        result = BoundedInvestigator(gateway, client, timeout_seconds=0.02).investigate(run_id)
        self.assertEqual((result["status"], result["tool_calls"], result["model_calls"]), ("unknown", 1, 0))
        self.assertIn("elapsed_budget_exceeded", result["limitations"])
        client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"}], delay=0.04)
        result = BoundedInvestigator(self.gateway, client, timeout_seconds=0.02).investigate(run_id)
        self.assertEqual((result["status"], result["tool_calls"], result["model_calls"]), ("unknown", 1, 1))
        self.assertIn("elapsed_budget_exceeded", result["limitations"])

    def test_input_and_report_caps_abstain_without_hiding_counts_or_truncation(self):
        run_id = self.create()
        client = ScriptedClient([])
        with patch.object(investigator_module, "MAX_INPUT_BYTES", 1):
            result = BoundedInvestigator(self.gateway, client).investigate(run_id)
        self.assertEqual((result["model_calls"], result["steps"], result["tool_calls"]), (0, 0, 1))
        self.assertIn("input_budget_exceeded", result["limitations"])
        padded = model_response('{"action":"tool","tool":"inspect_schema"}' + " " * 5000)
        client = ScriptedClient([padded, finish()])
        result = BoundedInvestigator(self.gateway, client).investigate(run_id)
        self.assertEqual(result["status"], "no_incident")
        self.assertTrue(result["trace_truncated"])
        response = next(event["model_response"] for event in result["trace"] if "model_response" in event)
        self.assertEqual(len(response["text"].encode()), 4096)
        self.assertTrue(response["raw_text_truncated"])
        client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"}, finish()])
        with patch.object(investigator_module, "MAX_RESULT_BYTES", 2048):
            result = BoundedInvestigator(self.gateway, client).investigate(run_id)
        self.assertEqual((result["status"], result["tool_calls"], result["model_calls"], result["steps"]),
                         ("unknown", 2, 2, 2))
        self.assertTrue(result["trace_truncated"])
        self.assertIn("result_limit_exceeded", result["limitations"])
        encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
        self.assertLessEqual(len(encoded), 2048)

    def test_finish_validation_crossing_the_deadline_cannot_return_a_conclusion(self):
        run_id = self.create()
        client = ScriptedClient([{"action": "tool", "tool": "inspect_schema"}, finish()])
        real_parse = investigator_module.parse_decision

        def slow_finish(*arguments):
            decision = real_parse(*arguments)
            if decision["action"] == "finish":
                time.sleep(0.04)
            return decision

        with patch.object(investigator_module, "parse_decision", side_effect=slow_finish):
            result = BoundedInvestigator(self.gateway, client, timeout_seconds=0.02).investigate(run_id)
        self.assertEqual((result["status"], result["findings"]), ("unknown", []))
        self.assertIn("elapsed_budget_exceeded", result["limitations"])
        self.assertEqual((result["tool_calls"], result["model_calls"], result["steps"]), (2, 2, 2))


if __name__ == "__main__":
    unittest.main()
