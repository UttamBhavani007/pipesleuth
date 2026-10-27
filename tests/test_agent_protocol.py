"""Pure protocol checks: offered actions, closed shapes and real references."""

from copy import deepcopy
import json
import unittest
from unittest.mock import patch

from pipesleuth.agent_protocol import (
    AgentProtocolError, MAX_DECISION_BYTES, ProtocolError, decision_schema, parse_decision,
)


REFS = ["ev_" + "a" * 32, "ev_" + "b" * 32]
TOOLS = ["read_logs", "inspect_schema"]


def diagnosis(status="diagnosed"):
    return {"status": status, "findings": [] if status != "diagnosed" else [
        {"code": "schema_mismatch", "summary": "The source columns differ from the order contract.",
         "evidence_refs": list(REFS)},
    ]}


class AgentProtocolTests(unittest.TestCase):
    def parse(self, value, *, tools=TOOLS, refs=REFS, allow_finish=True):
        text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        return parse_decision(text, tools, refs, allow_finish)

    def test_schema_exposes_only_offered_actions_and_is_fresh(self):
        schema = decision_schema(TOOLS, REFS, True)
        self.assertEqual(schema["type"], "object")
        self.assertIsInstance(schema["properties"], dict)
        self.assertEqual(len(schema["anyOf"]), 2)
        tool, finish = schema["anyOf"]
        self.assertEqual(tool["properties"]["tool"]["enum"], TOOLS)
        self.assertEqual(set(tool["properties"]), {"action", "tool"})
        self.assertFalse(tool["additionalProperties"])
        self.assertEqual(set(finish["properties"]), {"action", "diagnosis"})
        self.assertFalse(finish["additionalProperties"])
        nested = finish["properties"]["diagnosis"]
        self.assertFalse(nested["additionalProperties"])
        self.assertEqual(nested["properties"]["findings"]["items"]["properties"]["evidence_refs"]["items"]["enum"], REFS)
        tool["properties"]["tool"]["enum"].append("execute_sql")
        self.assertEqual(decision_schema(TOOLS, REFS, True)["anyOf"][0]["properties"]["tool"]["enum"], TOOLS)
        self.assertEqual(json.loads(json.dumps(schema)), schema)

    def test_finish_is_omitted_until_allowed_and_only_finish_remains_without_tools(self):
        tool_only = decision_schema(TOOLS, REFS, False)
        self.assertEqual(len(tool_only["anyOf"]), 1)
        self.assertEqual(tool_only["anyOf"][0]["properties"]["action"]["enum"], ["tool"])
        finish_only = decision_schema([], REFS, True)
        self.assertEqual(len(finish_only["anyOf"]), 1)
        self.assertEqual(finish_only["anyOf"][0]["properties"]["action"]["enum"], ["finish"])
        with self.assertRaises(AgentProtocolError):
            self.parse({"action": "finish", "diagnosis": diagnosis()}, allow_finish=False)

    def test_offered_tools_and_valid_diagnoses_round_trip_without_mutating_host_inputs(self):
        available, references = list(TOOLS), list(REFS)
        expected_tools, expected_refs = list(available), list(references)
        for tool in available:
            value = {"action": "tool", "tool": tool}
            self.assertEqual(parse_decision(json.dumps(value), available, references, True), value)
        for status in ("diagnosed", "unknown", "no_incident"):
            value = {"action": "finish", "diagnosis": diagnosis(status)}
            self.assertEqual(self.parse(value), value)
        self.assertEqual(available, expected_tools)
        self.assertEqual(references, expected_refs)
        self.assertIs(ProtocolError, AgentProtocolError)

    def test_unavailable_and_previously_visited_tools_are_rejected(self):
        for tool in ("read_run", "inspect_validation", "execute_sql", "shell", "../outside", None, []):
            with self.subTest(tool_type=type(tool).__name__), self.assertRaises(AgentProtocolError):
                self.parse({"action": "tool", "tool": tool})
        with self.assertRaises(AgentProtocolError):
            self.parse({"action": "tool", "tool": "inspect_schema"}, tools=["read_logs"])

    def test_tool_arguments_run_paths_and_approval_fields_cannot_be_injected(self):
        base = {"action": "tool", "tool": "read_logs"}
        extras = {"arguments": {"run_id": "f" * 32}, "run_id": "f" * 32,
                  "path": "/private/evaluator.json", "sql": "DELETE FROM warehouse_orders",
                  "shell": "touch an-unrelated-file", "approve": True,
                  "timeout_seconds": 120, "diagnosis": diagnosis()}
        for key, value in extras.items():
            with self.subTest(field=key), self.assertRaises(AgentProtocolError) as failure:
                self.parse(base | {key: value})
            self.assertNotIn(str(value), str(failure.exception))
        with self.assertRaises(AgentProtocolError):
            self.parse({"action": "finish", "diagnosis": diagnosis(), "tool": "read_logs"})

    def test_invalid_and_fabricated_finish_findings_fail_closed(self):
        cases = []
        for status, findings in (("diagnosed", []), ("unknown", diagnosis()["findings"]),
                                 ("no_incident", diagnosis()["findings"]), ("approved", [])):
            cases.append({"status": status, "findings": findings})
        unknown_ref = diagnosis()
        unknown_ref["findings"][0]["evidence_refs"] = ["ev_" + "c" * 32]
        cases.append(unknown_ref)
        duplicate_ref = diagnosis()
        duplicate_ref["findings"][0]["evidence_refs"] = [REFS[0], REFS[0]]
        cases.append(duplicate_ref)
        extra_approval = diagnosis()
        extra_approval["approve"] = True
        cases.append(extra_approval)
        extra_action = diagnosis()
        extra_action["findings"][0]["sql"] = "DELETE FROM warehouse_orders"
        cases.append(extra_action)
        bad_code = diagnosis()
        bad_code["findings"][0]["code"] = "execute_repair"
        cases.append(bad_code)
        for index, value in enumerate(cases):
            with self.subTest(case=index), self.assertRaises(AgentProtocolError):
                self.parse({"action": "finish", "diagnosis": value})

    def test_duplicate_keys_nonfinite_values_and_malformed_json_are_rejected(self):
        values = [
            'not-json', '[]', 'null', '"tool"', '{"action":"tool","action":"finish","tool":"read_logs"}',
            '{"action":"tool","act\\u0069on":"tool","tool":"read_logs"}',
            '{"action":"finish","diagnosis":{"status":"unknown","status":"no_incident","findings":[]}}',
            '{"action":"finish","diagnosis":{"status":"unknown","findings":NaN}}',
            '{"action":"finish","diagnosis":{"status":"unknown","findings":Infinity}}',
            '{"action":"finish","diagnosis":' + '[' * 1100 + '0' + ']' * 1100 + '}',
        ]
        for index, value in enumerate(values):
            with self.subTest(case=index), self.assertRaises(AgentProtocolError):
                self.parse(value)

    def test_utf8_byte_limit_and_surrogates_are_checked_before_returning_decisions(self):
        valid = '{"action":"tool","tool":"read_logs"}'
        at_limit = valid + " " * (MAX_DECISION_BYTES - len(valid.encode("utf-8")))
        self.assertEqual(self.parse(at_limit), {"action": "tool", "tool": "read_logs"})
        for text in (at_limit + " ", "\ud800", b'{"action":"tool","tool":"read_logs"}'):
            with self.subTest(text_type=type(text).__name__), self.assertRaises(AgentProtocolError):
                parse_decision(text, TOOLS, REFS, True)
        invalid_summary = diagnosis()
        invalid_summary["findings"][0]["summary"] = "\ud800"
        with self.assertRaises(AgentProtocolError):
            self.parse(json.dumps({"action": "finish", "diagnosis": invalid_summary}))

    def test_invalid_host_offers_are_rejected_for_schema_and_parser(self):
        configurations = [("read_logs", REFS, True), (["unknown_tool"], REFS, True),
                          (["read_logs", "read_logs"], REFS, True), (TOOLS, "not-list", True),
                          (TOOLS, [REFS[0], REFS[0]], True), (TOOLS, ["fabricated"], True),
                          (TOOLS, REFS, 1), (TOOLS, [], True), ([], REFS, False),
                          (TOOLS, [None], False), ([{}], REFS, False)]
        for available, references, allow_finish in configurations:
            with self.subTest(available_type=type(available).__name__), self.assertRaises(AgentProtocolError):
                decision_schema(available, references, allow_finish)
            with self.assertRaises(AgentProtocolError):
                parse_decision('{"action":"tool","tool":"read_logs"}', available, references, allow_finish)

    def test_valid_references_do_not_authenticate_or_filter_a_models_claim(self):
        value = {"action": "finish", "diagnosis": diagnosis()}
        value["diagnosis"]["findings"][0]["summary"] = "An unsupported claim with an existing reference."
        with patch("builtins.open", side_effect=AssertionError("protocol must not read files")), \
                patch("socket.create_connection", side_effect=AssertionError("protocol must not connect")):
            self.assertEqual(self.parse(value), value)


if __name__ == "__main__":
    unittest.main()
