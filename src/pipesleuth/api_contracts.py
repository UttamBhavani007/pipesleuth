"""Closed HTTP contracts; filesystem and model configuration stay host-owned."""

from __future__ import annotations

import math
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator


RunId = Annotated[str, Field(strict=True, pattern=r"^[0-9a-f]{32}$")]
Sha256 = Annotated[str, Field(strict=True, pattern=r"^[0-9a-f]{64}$")]
EvidenceId = Annotated[str, Field(strict=True, pattern=r"^ev_[0-9a-f]{32}$")]
Count = Annotated[int, Field(strict=True, ge=0)]
ToolName = Literal["read_run", "read_logs", "inspect_schema", "inspect_validation"]
RepairAction = Literal["rename_amount_column", "skip_identical_duplicates", "quarantine_invalid_rows"]


def _finite_json(value):
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("JSON numbers must be finite")
    if isinstance(value, str):
        value.encode("utf-8")
    elif isinstance(value, dict):
        for key, item in value.items():
            key.encode("utf-8")
            _finite_json(item)
    elif isinstance(value, list):
        for item in value:
            _finite_json(item)


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    @model_validator(mode="after")
    def finite_json(self):
        _finite_json(self.model_dump())
        return self


class DiagnosisRequest(ApiModel):
    method: Literal["checklist", "local", "agent"] = "checklist"


class InvestigationStart(ApiModel):
    run_id: RunId


class ResumeRequest(ApiModel):
    max_operations: Annotated[int, Field(strict=True, ge=1, le=16)] | None = None


class RepairPlanRequest(ApiModel):
    run_id: RunId
    action: RepairAction


class ApprovalRequest(ApiModel):
    plan_sha256: Sha256
    input_fingerprint_sha256: Sha256


class ExecuteRequest(ApiModel):
    """Execution cannot carry an implicit approval or an action override."""


class RunSummary(ApiModel):
    run_id: RunId
    status: Literal["new", "running", "succeeded", "failed"]
    rows_loaded: Count
    totals_minor: dict[Literal["EUR", "USD", "GBP"], Count]
    source_sha256: Sha256
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None
    duplicate_policy: Literal["reject", "skip_identical"] = "reject"
    duplicates_skipped: Count = 0
    invalid_policy: Literal["reject", "quarantine"] = "reject"
    rows_quarantined: Count = 0


class EvidenceEnvelope(ApiModel):
    evidence_id: EvidenceId
    run_id: RunId
    tool: ToolName
    arguments: dict[str, JsonValue]
    observed_at: str
    content_sha256: Sha256
    data: dict[str, JsonValue]
    truncated: bool


class SavedRecord(ApiModel):
    investigation_id: RunId
    revision: Annotated[int, Field(strict=True, ge=1)]
    state: dict[str, JsonValue]
    binding: dict[str, JsonValue]
    pending: dict[str, JsonValue] | None


class Finding(ApiModel):
    code: Literal["schema_mismatch", "duplicate_key", "conflicting_duplicate", "invalid_row", "integer_overflow"]
    summary: Annotated[str, Field(min_length=1, max_length=240)]
    evidence_refs: Annotated[list[EvidenceId], Field(min_length=1, max_length=4)]
    details: dict[str, JsonValue] | None = None

    @model_validator(mode="after")
    def unique_references(self):
        if not self.summary.strip() or len(set(self.evidence_refs)) != len(self.evidence_refs):
            raise ValueError("invalid finding")
        return self


class DiagnosisResult(ApiModel):
    method: Literal["deterministic_checklist", "single_call_local_model", "bounded_local_agent"]
    run_id: RunId
    status: Literal["diagnosed", "no_incident", "unknown"]
    findings: Annotated[list[Finding], Field(max_length=4)]
    limitations: list[str]
    evidence: Annotated[list[EvidenceEnvelope], Field(max_length=4)]
    tool_calls: Count
    elapsed_ms: Annotated[float, Field(ge=0)]
    notes: list[str] | None = None
    model_calls: Count | None = None
    model_response: dict[str, JsonValue] | None = None
    semantic_support_verified: Literal[False] | None = None
    trace: list[dict[str, JsonValue]] | None = None
    steps: Count | None = None
    trace_truncated: bool | None = None

    @model_validator(mode="after")
    def consistent_findings(self):
        if bool(self.findings) != (self.status == "diagnosed"):
            raise ValueError("diagnosis status and findings disagree")
        references = {item.evidence_id for item in self.evidence}
        if any(ref not in references for finding in self.findings for ref in finding.evidence_refs):
            raise ValueError("unknown evidence reference")
        if any(item.run_id != self.run_id for item in self.evidence):
            raise ValueError("evidence belongs to another run")
        return self


class ErrorDetail(ApiModel):
    code: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")]


class ApiError(ApiModel):
    error: ErrorDetail


class JsonResult(ApiModel):
    """Named host operation with a bounded JSON result, for auxiliary reads."""

    operation: str
    result: dict[str, JsonValue]
