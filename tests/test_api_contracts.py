"""HTTP schema boundaries do not expose trusted host configuration."""

import unittest

try:
    from pydantic import ValidationError
    from pipesleuth.api_contracts import (
        ApiError, ApprovalRequest, DiagnosisRequest, DiagnosisResult,
        EvidenceEnvelope, ExecuteRequest, InvestigationStart, RepairPlanRequest,
        ResumeRequest, RunSummary, SavedRecord,
    )
except ModuleNotFoundError as exc:
    if exc.name != "pydantic":
        raise
    ValidationError = None


RUN = "a" * 32
SHA = "b" * 64


@unittest.skipIf(ValidationError is None, "optional API dependencies are not installed")
class ApiContractTests(unittest.TestCase):
    def rejects(self, model, value):
        with self.assertRaises(ValidationError):
            model.model_validate(value)

    def test_requests_are_closed_and_have_conventional_defaults(self):
        self.assertEqual(DiagnosisRequest().method, "checklist")
        self.assertIsNone(ResumeRequest().max_operations)
        for model, payload in (
            (DiagnosisRequest, {"method": "checklist"}),
            (InvestigationStart, {"run_id": RUN}),
            (ResumeRequest, {"max_operations": 3}),
            (RepairPlanRequest, {"run_id": RUN, "action": "skip_identical_duplicates"}),
            (ApprovalRequest, {"plan_sha256": SHA, "input_fingerprint_sha256": SHA}),
            (ExecuteRequest, {}),
        ):
            self.assertIsInstance(model.model_validate(payload), model)
            for field in ("path", "sql", "endpoint", "model", "approval_origin", "approve"):
                self.rejects(model, {**payload, field: "untrusted"})

    def test_identifiers_digests_and_operation_limit_never_coerce(self):
        for value in (True, 1, "a" * 31, "A" * 32, RUN + "\n", "../../source"):
            self.rejects(InvestigationStart, {"run_id": value})
        for value in (True, "1", 1.0, 0, 17, float("nan")):
            self.rejects(ResumeRequest, {"max_operations": value})
        for value in (SHA.upper(), SHA[:-1], SHA + "\n", 42):
            self.rejects(ApprovalRequest, {"plan_sha256": value, "input_fingerprint_sha256": SHA})
        self.rejects(RepairPlanRequest, {"run_id": RUN, "action": "execute_sql"})

    def test_run_summary_matches_core_and_rejects_boolean_counters(self):
        payload = {"run_id": RUN, "status": "failed", "rows_loaded": 0,
                   "totals_minor": {}, "source_sha256": SHA, "created_at": "2026-11-01T18:00:00Z"}
        self.assertEqual(RunSummary.model_validate(payload).run_id, RUN)
        for field, value in (("rows_loaded", True), ("totals_minor", {"EUR": False}),
                             ("totals_minor", {"JPY": 1}), ("status", "complete")):
            self.rejects(RunSummary, {**payload, field: value})

    @staticmethod
    def envelope():
        return {"evidence_id": "ev_" + "c" * 32, "run_id": RUN,
                "tool": "read_run", "arguments": {"run_id": RUN},
                "observed_at": "2026-11-01T18:00:00Z", "content_sha256": SHA,
                "data": {"untrusted_data": True, "nested": [1, None, "text"]}, "truncated": False}

    def test_envelope_saved_record_preserve_json_and_reject_nonfinite(self):
        envelope = self.envelope()
        self.assertEqual(EvidenceEnvelope.model_validate(envelope).model_dump(), envelope)
        self.rejects(EvidenceEnvelope, {**envelope, "truncated": 1})
        self.rejects(EvidenceEnvelope, {**envelope, "data": {"nested": [float("inf")]}})
        record = {"investigation_id": RUN, "revision": 1, "state": {"result": {}},
                  "binding": {"version": 1}, "pending": {"token": RUN, "operation": "generate"}}
        self.assertEqual(SavedRecord.model_validate(record).model_dump(), record)
        self.rejects(SavedRecord, {**record, "revision": True})
        self.rejects(SavedRecord, {**record, "state": {"value": float("nan")}})

    def test_diagnosis_core_formats_and_reference_consistency(self):
        result = {"method": "deterministic_checklist", "run_id": RUN, "status": "diagnosed",
                  "findings": [{"code": "schema_mismatch", "summary": "Column changed",
                                "evidence_refs": [self.envelope()["evidence_id"]], "details": {"columns": []}}],
                  "limitations": [], "evidence": [self.envelope()], "tool_calls": 4,
                  "elapsed_ms": 0.1, "notes": ["separate_evidence_snapshots"]}
        self.assertEqual(DiagnosisResult.model_validate(result).status, "diagnosed")
        for method in ("single_call_local_model", "bounded_local_agent"):
            self.assertEqual(DiagnosisResult.model_validate({**result, "method": method,
                "model_calls": 1, "semantic_support_verified": False}).model_calls, 1)
        self.rejects(DiagnosisResult, {**result, "status": "unknown"})
        self.rejects(DiagnosisResult, {**result, "elapsed_ms": float("nan")})
        self.rejects(DiagnosisResult, {**result, "semantic_support_verified": True})
        finding = {**result["findings"][0], "evidence_refs": ["ev_" + "d" * 32]}
        self.rejects(DiagnosisResult, {**result, "findings": [finding]})

    def test_errors_have_only_sanitized_machine_codes(self):
        self.assertEqual(ApiError.model_validate({"error": {"code": "invalid_request"}}).model_dump(),
                         {"error": {"code": "invalid_request"}})
        self.rejects(ApiError, {"error": {"code": "bad", "input": "secret"}})
        self.rejects(ApiError, {"error": {"code": "/Users/private/path"}})


if __name__ == "__main__":
    unittest.main()
