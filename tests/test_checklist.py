"""Grounded checklist rules and fail-closed evidence protocol checks."""

from contextlib import closing
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from pipesleuth.checklist import ChecklistBaseline, ChecklistError, MAX_CHECKLIST_BYTES
from pipesleuth.evidence import EvidenceError, EvidenceGateway
from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import SandboxPipeline


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def rehash(response):
    binding = {key: response[key] for key in ("run_id", "tool", "arguments", "data", "truncated")}
    response["content_sha256"] = hashlib.sha256(canonical(binding)).hexdigest()
    response["evidence_id"] = "ev_" + response["content_sha256"][:32]
    return response


class BundleGateway:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def call(self, tool, arguments):
        self.calls.append((tool, deepcopy(arguments)))
        value = self.responses[tool]
        if isinstance(value, Exception):
            raise value
        return deepcopy(value)


class ChecklistTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.pipeline = SandboxPipeline(Path(temporary.name) / "runs")
        self.gateway = EvidenceGateway(self.pipeline.data_dir)

    def run_fixture(self, kind="healthy", **policies):
        source = generate_fixture(kind, row_count=3).to_source_payload()
        created = self.pipeline.create(source, **policies)
        self.pipeline.run(created.run_id)
        return created.run_id

    def bundle(self, run_id):
        return {tool: self.gateway.call(tool, {"run_id": run_id, **extra}) for tool, extra in (
            ("read_run", {}), ("read_logs", {"limit": 20}),
            ("inspect_schema", {}), ("inspect_validation", {"limit": 20}))}

    def diagnose_bundle(self, responses, run_id):
        gateway = BundleGateway(responses)
        return ChecklistBaseline(gateway).diagnose(run_id), gateway

    def assert_unknown(self, result):
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(result["findings"], [])
        self.assertTrue(result["limitations"])
        self.assertEqual(result["tool_calls"], 4)

    def test_clean_control_has_exact_four_calls_and_no_incident(self):
        run_id = self.run_fixture()
        responses = self.bundle(run_id)
        result, gateway = self.diagnose_bundle(responses, run_id)
        self.assertEqual(result["method"], "deterministic_checklist")
        self.assertEqual(result["status"], "no_incident")
        self.assertEqual(result["findings"], [])
        self.assertEqual(result["limitations"], [])
        self.assertEqual(result["notes"], ["separate_evidence_snapshots"])
        self.assertEqual(gateway.calls, [("read_run", {"run_id": run_id}), ("read_logs", {"run_id": run_id, "limit": 20}),
                                        ("inspect_schema", {"run_id": run_id}), ("inspect_validation", {"run_id": run_id, "limit": 20})])
        self.assertEqual({item["tool"]: item for item in result["evidence"]}, responses)
        self.assertGreaterEqual(result["elapsed_ms"], 0)
        self.assertLessEqual(len(canonical(result)), MAX_CHECKLIST_BYTES)
        self.assertEqual(json.loads(canonical(result)), result)

    def test_identical_retries_are_a_success_note_and_not_a_repair_finding(self):
        run_id = self.run_fixture("duplicate_ingestion", duplicate_policy="skip_identical")
        result = ChecklistBaseline(self.gateway).diagnose(run_id)
        self.assertEqual((result["status"], result["findings"]), ("no_incident", []))
        self.assertIn("identical_retries_skipped", result["notes"])
        self.assertNotIn("repair", canonical(result).decode())

    def test_supported_failed_diagnostics_have_static_findings_and_real_refs(self):
        sources = []
        for kind, code in (("schema_drift", "schema_mismatch"), ("duplicate_ingestion", "duplicate_key"),
                           ("invalid_records", "invalid_row")):
            sources.append((generate_fixture(kind, row_count=3).to_source_payload(), code))
        conflicting = generate_fixture("healthy", row_count=3).to_source_payload()
        conflicting["rows"].append(conflicting["rows"][0] | {"amount_minor": 910})
        sources.append((conflicting, "conflicting_duplicate"))
        overflow = generate_fixture("healthy", row_count=3).to_source_payload()
        overflow["rows"][2]["amount_minor"] = 2**63
        sources.append((overflow, "integer_overflow"))
        for source, code in sources:
            with self.subTest(code=code):
                created = self.pipeline.create(source)
                self.pipeline.run(created.run_id)
                result = ChecklistBaseline(self.gateway).diagnose(created.run_id)
                self.assertEqual(result["status"], "diagnosed")
                self.assertEqual([finding["code"] for finding in result["findings"]], [code])
                refs = {item["evidence_id"] for item in result["evidence"]}
                self.assertTrue(set(result["findings"][0]["evidence_refs"]) <= refs)
                self.assertEqual(result["limitations"], [])

    def test_quarantine_groups_exact_reasons_and_supports_all_invalid(self):
        source = generate_fixture("invalid_records", row_count=3).to_source_payload()
        source["rows"].append(source["rows"][0] | {"order_id": "OVERFLOW", "amount_minor": 2**63})
        for all_invalid in (False, True):
            with self.subTest(all_invalid=all_invalid):
                selected = {"schema": source["schema"], "rows": source["rows"][3:] if all_invalid else source["rows"]}
                created = self.pipeline.create(selected, invalid_policy="quarantine")
                self.pipeline.run(created.run_id)
                result = ChecklistBaseline(self.gateway).diagnose(created.run_id)
                self.assertEqual(result["status"], "diagnosed")
                self.assertEqual([finding["code"] for finding in result["findings"]], ["invalid_row", "integer_overflow"])
                invalid, overflow = [finding["details"] for finding in result["findings"]]
                first = 1 if all_invalid else 4
                self.assertEqual(invalid["count"], 4)
                self.assertEqual([row["row_number"] for row in invalid["rows"]], list(range(first, first + 4)))
                self.assertEqual(overflow["count"], 1)
                self.assertEqual(overflow["rows"], [{"row_number": first + 4, "reason": "amount_minor exceeds SQLite signed 64-bit integer range"}])

    def test_invalid_run_ids_are_rejected_before_tools_are_called(self):
        gateway = BundleGateway({})
        for run_id in (None, True, "../outside", "A" * 32, "a" * 31):
            with self.subTest(run_id=run_id), self.assertRaises(ChecklistError):
                ChecklistBaseline(gateway).diagnose(run_id)
        self.assertEqual(gateway.calls, [])

    def test_missing_runs_and_tool_errors_abstain_with_four_calls_and_safe_codes(self):
        missing = ChecklistBaseline(self.gateway).diagnose("0" * 32)
        self.assert_unknown(missing)
        self.assertEqual(missing["evidence"], [])
        self.assertEqual(missing["limitations"], ["evidence_unavailable"])
        run_id = self.run_fixture()
        responses = self.bundle(run_id)
        responses["inspect_schema"] = EvidenceError("private exception /Users/private/secret")
        result, gateway = self.diagnose_bundle(responses, run_id)
        self.assert_unknown(result)
        self.assertEqual(len(gateway.calls), 4)
        self.assertNotIn("private exception", canonical(result).decode())

    def test_new_running_and_legacy_unavailable_evidence_are_unknown(self):
        for status in ("new", "running"):
            created = self.pipeline.create(generate_fixture("healthy").to_source_payload())
            if status == "running":
                with closing(sqlite3.connect(self.pipeline.database_path(created.run_id))) as connection, connection:
                    connection.execute("UPDATE pipeline_run SET status = 'running'")
            result = ChecklistBaseline(self.gateway).diagnose(created.run_id)
            self.assert_unknown(result)
            self.assertIn("run_not_terminal", result["limitations"])
        run_id = self.run_fixture()
        responses = self.bundle(run_id)
        responses["read_logs"]["data"].update(available=False, events_total=0, events=[])
        rehash(responses["read_logs"])
        result, _ = self.diagnose_bundle(responses, run_id)
        self.assert_unknown(result)
        self.assertIn("evidence_unavailable", result["limitations"])

    def test_unsupported_source_and_storage_failures_abstain(self):
        for code, diagnostic in (("source_changed", {"expected_sha256": "0" * 64, "observed_sha256": "1" * 64}),
                                 ("load_error", {})):
            run_id = self.run_fixture("duplicate_ingestion")
            responses = self.bundle(run_id)
            failure = {"code": code, **diagnostic}
            responses["read_logs"]["data"]["events"][-1]["details"] = failure
            responses["inspect_validation"]["data"]["failure"] = failure
            rehash(responses["read_logs"])
            rehash(responses["inspect_validation"])
            result, _ = self.diagnose_bundle(responses, run_id)
            self.assert_unknown(result)
            self.assertIn("unsupported_diagnostic", result["limitations"])

    def test_truncation_and_tampered_or_malformed_envelopes_never_produce_findings(self):
        run_id = self.run_fixture("duplicate_ingestion")
        original = self.bundle(run_id)
        mutations = [
            lambda response: response.update(content_sha256="0" * 64),
            lambda response: response.update(evidence_id="ev_" + "0" * 32),
            lambda response: response.update(run_id="0" * 32),
            lambda response: response.update(tool="execute_sql"),
            lambda response: response.update(truncated=True),
            lambda response: response["arguments"].update(limit=20.0),
            lambda response: response["data"].update(oracle="private-answer"),
            lambda response: response["data"].update(events=[{"private": "answer"}]),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                responses = deepcopy(original)
                response = responses["read_logs"]
                mutate(response)
                if index >= 4:
                    rehash(response)
                result, gateway = self.diagnose_bundle(responses, run_id)
                self.assert_unknown(result)
                self.assertEqual(len(gateway.calls), 4)
                self.assertNotIn("private-answer", canonical(result).decode())

    def test_self_consistent_hashes_do_not_override_cross_tool_counts_schema_or_lifecycle(self):
        run_id = self.run_fixture()
        original = self.bundle(run_id)
        for change in ("count", "schema", "terminal", "policy", "raw_count", "quarantine", "timestamps", "totals"):
            with self.subTest(change=change):
                responses = deepcopy(original)
                if change == "count":
                    responses["inspect_validation"]["data"]["rows_loaded"] = 2
                elif change == "schema":
                    responses["inspect_schema"]["data"]["source_columns"][2] = "gross_total_minor"
                elif change == "terminal":
                    responses["inspect_validation"]["data"]["failure"] = {"code": "load_error"}
                elif change == "policy":
                    responses["read_run"]["data"]["duplicate_policy"] = "skip_identical"
                elif change == "raw_count":
                    responses["read_logs"]["data"]["events"][0]["details"]["raw_row_count"] = 99
                elif change == "quarantine":
                    responses["read_run"]["data"]["rows_quarantined"] = 1
                elif change == "timestamps":
                    responses["read_run"]["data"]["created_at"] = "2099-01-01T00:00:00Z"
                else:
                    responses["read_run"]["data"]["totals_minor"] = {"EUR": -1}
                for response in responses.values():
                    rehash(response)
                result, _ = self.diagnose_bundle(responses, run_id)
                self.assert_unknown(result)

    def test_quarantine_reasons_must_match_retained_raw_record_validation(self):
        run_id = self.run_fixture("invalid_records", invalid_policy="quarantine")
        for change in ("reason", "raw", "row_number", "missing"):
            responses = self.bundle(run_id)
            record = responses["inspect_validation"]["data"]["records"][0]
            if change == "reason":
                record["reason"] = "an unsupported claim"
            elif change == "raw":
                record["raw_record"]["amount_minor"] = 1250
            elif change == "row_number":
                record["row_number"] = 99
            else:
                responses["inspect_validation"]["data"]["records"].pop()
            rehash(responses["inspect_validation"])
            result, _ = self.diagnose_bundle(responses, run_id)
            self.assert_unknown(result)

    def test_oversized_or_nested_untrusted_payloads_are_discarded(self):
        run_id = self.run_fixture()
        for extra in ("x" * 100_000, {"oracle": "private-answer"}):
            responses = self.bundle(run_id)
            responses["read_logs"]["data"]["events"][0]["details"]["observed_schema"] = extra
            rehash(responses["read_logs"])
            result, _ = self.diagnose_bundle(responses, run_id)
            self.assert_unknown(result)
            self.assertLessEqual(len(canonical(result)), MAX_CHECKLIST_BYTES)
            self.assertNotIn("private-answer", canonical(result).decode())

    def test_impossible_totals_abstain_even_when_all_counts_and_hashes_agree(self):
        for row_count, totals in ((0, {"EUR": 100}), (1, {"EUR": 0, "USD": 0}), (1, {"EUR": 2**63})):
            source = generate_fixture("healthy", row_count=1).to_source_payload()
            if row_count == 0:
                source["rows"] = []
            created = self.pipeline.create(source)
            self.pipeline.run(created.run_id)
            responses = self.bundle(created.run_id)
            responses["read_run"]["data"]["totals_minor"] = totals
            rehash(responses["read_run"])
            result, _ = self.diagnose_bundle(responses, created.run_id)
            self.assert_unknown(result)

    def test_duplicate_diagnostics_require_a_possible_row_and_trimmed_identifier(self):
        run_id = self.run_fixture("duplicate_ingestion")
        for field, value in (("row_number", 1), ("order_id", ""), ("order_id", " padded ")):
            responses = self.bundle(run_id)
            responses["read_logs"]["data"]["events"][-1]["details"][field] = value
            responses["inspect_validation"]["data"]["failure"][field] = value
            rehash(responses["read_logs"])
            rehash(responses["inspect_validation"])
            result, _ = self.diagnose_bundle(responses, run_id)
            self.assert_unknown(result)


if __name__ == "__main__":
    unittest.main()
