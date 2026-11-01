"""Fixed-root local HTTP interface over PipeSleuth's existing bounded core.

Tokens are supplied by the trusted launcher. Requests cannot choose filesystem
roots, model configuration, evidence arguments, SQL or execution authority.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Path as ApiPath, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from pipesleuth.api_contracts import (
    ApiError, ApprovalRequest, DiagnosisRequest, DiagnosisResult, EvidenceEnvelope,
    ExecuteRequest, InvestigationStart, RepairPlanRequest, ResumeRequest, RunSummary, SavedRecord,
)
from pipesleuth.api_security import ApiSecurity
from pipesleuth.checkpoints import CheckpointStore
from pipesleuth.evidence import EvidenceGateway
from pipesleuth.local_model import ALLOWED_MODEL, LocalModelConfig
from pipesleuth.pipeline import SandboxPipeline
from pipesleuth.repair_plan import ACTIONS
from pipesleuth.repairs import RepairService


@dataclass(frozen=True, slots=True)
class ApiSettings:
    data_dir: Path | str
    output_dir: Path | str
    investigation_state_dir: Path | str
    repair_state_dir: Path | str
    port: int = 8765
    endpoint: str = "http://127.0.0.1:11436"
    model: str = ALLOWED_MODEL
    model_digest: str | None = None
    timeout_seconds: float = 120.0
    num_ctx: int = 8192
    max_output_tokens: int = 768
    max_steps: int = 4
    max_tool_calls: int = 4
    agent_timeout: float = 60.0
    retry_limit: int = 1
    gateway_timeout_seconds: float = 2.0
    gateway_max_response_bytes: int = 16_384

    def __post_init__(self):
        from pipesleuth.investigator import BoundedInvestigator
        from pipesleuth.local_model import LocalModelClient

        if type(self.port) is not int or not 1024 <= self.port <= 65_535:
            raise ValueError("API port must be an integer between 1024 and 65535")
        if type(self.retry_limit) is not int or not 0 <= self.retry_limit <= 1:
            raise ValueError("API retry limit must be zero or one")
        model = self.model_config()
        roots = (
            SandboxPipeline(self.data_dir).data_dir,
            SandboxPipeline(self.output_dir).data_dir,
            CheckpointStore(self.investigation_state_dir).root,
            CheckpointStore(self.repair_state_dir).root,
        )
        for index, left in enumerate(roots):
            for right in roots[index + 1:]:
                if left == right or left in right.parents or right in left.parents:
                    raise ValueError("API input, output and state roots must not overlap")
        for name, root in zip(("data_dir", "output_dir", "investigation_state_dir", "repair_state_dir"), roots):
            object.__setattr__(self, name, root)
        gateway = EvidenceGateway(roots[0], timeout_seconds=self.gateway_timeout_seconds,
                                  max_response_bytes=self.gateway_max_response_bytes)
        BoundedInvestigator(gateway, LocalModelClient(model), max_steps=self.max_steps,
                            max_tool_calls=self.max_tool_calls, timeout_seconds=self.agent_timeout)

    def model_config(self):
        return LocalModelConfig(endpoint=self.endpoint, model=self.model,
            expected_digest=self.model_digest, timeout_seconds=self.timeout_seconds,
            num_ctx=self.num_ctx, max_output_tokens=self.max_output_tokens)

    def guard_roots(self):
        """Recheck configured paths against symlink/file replacement without creating them."""
        current = (SandboxPipeline(self.data_dir).data_dir, SandboxPipeline(self.output_dir).data_dir,
                   CheckpointStore(self.investigation_state_dir).root,
                   CheckpointStore(self.repair_state_dir).root)
        if current != (self.data_dir, self.output_dir, self.investigation_state_dir, self.repair_state_dir):
            raise ValueError("configured API roots have changed")


class _ApiFailure(Exception):
    def __init__(self, status, code):
        self.status, self.code = status, code


RunId = Annotated[str, ApiPath(pattern=r"^[0-9a-f]{32}$", min_length=32, max_length=32)]
Tool = Literal["read_run", "read_logs", "inspect_schema", "inspect_validation"]


def _closed_query(request: Request):
    items = list(request.query_params.multi_items())
    allowed = request.url.path.endswith(("/evidence/read_logs", "/evidence/inspect_validation"))
    if items and (not allowed or len(items) != 1 or items[0][0] != "limit"):
        raise _ApiFailure(422, "invalid_request")


def create_app(settings: ApiSettings, *, read_token, operation_token, approval_token):
    """Construct an app without starting a runtime or creating data/state directories."""
    if not isinstance(settings, ApiSettings):
        raise ValueError("settings must be validated ApiSettings")
    settings.guard_roots()
    app = FastAPI(title="PipeSleuth local API", version="1", docs_url=None,
                  redoc_url=None, openapi_url=None, redirect_slashes=False, dependencies=[Depends(_closed_query)],
                  responses={status: {"model": ApiError} for status in (400, 401, 403, 404, 409, 413, 422, 500, 503)})
    security = {"host": "127.0.0.1", "port": settings.port, "read_token": read_token,
                "operation_token": operation_token, "approval_token": approval_token}
    # Validate credentials immediately; Starlette otherwise builds middleware
    # lazily on the first request, after the launcher has reported startup.
    ApiSecurity(app, **security)
    app.add_middleware(ApiSecurity, **security)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(_request: Request, _error):
        return JSONResponse(status_code=422, content={"error": {"code": "invalid_request"}})

    @app.exception_handler(HTTPException)
    async def http_error(_request: Request, error):
        code = {404: "not_found", 405: "method_not_allowed"}.get(error.status_code, "request_rejected")
        return JSONResponse(status_code=error.status_code, content={"error": {"code": code}})

    @app.exception_handler(_ApiFailure)
    async def known_failure(_request: Request, error):
        return JSONResponse(status_code=error.status, content={"error": {"code": error.code}})

    @app.exception_handler(Exception)
    async def internal_error(_request: Request, _error):
        return JSONResponse(status_code=500, content={"error": {"code": "internal_error"}})

    def gateway():
        settings.guard_roots()
        return EvidenceGateway(settings.data_dir, timeout_seconds=settings.gateway_timeout_seconds,
                               max_response_bytes=settings.gateway_max_response_bytes)

    def read(operation):
        try:
            settings.guard_roots()
            return operation()
        except (ValueError, OSError):
            raise _ApiFailure(404, "read_unavailable") from None

    @app.get("/health")
    def health():
        return {"status": "ok", "api_version": 1}

    @app.get("/openapi.json", include_in_schema=False)
    def secured_openapi():
        return app.openapi()

    @app.get("/v1/capabilities")
    def capabilities():
        return {"diagnosis_methods": ["checklist", "local", "agent"],
                "evidence_tools": EvidenceGateway.tools(), "repair_actions": list(ACTIONS),
                "model_configured": settings.model_digest is not None}

    @app.get("/v1/runs/{run_id}", response_model=RunSummary, response_model_exclude_unset=True)
    def run_summary(run_id: RunId):
        def load():
            envelope = gateway().call("read_run", {"run_id": run_id})
            if envelope["truncated"]:
                raise ValueError("run summary is truncated")
            fields = {name: value for name, value in envelope["data"].items() if name in RunSummary.model_fields}
            return RunSummary.model_validate({"run_id": run_id, **fields})
        return read(load)

    @app.get("/v1/runs/{run_id}/evidence/{tool}", response_model=EvidenceEnvelope)
    def evidence(run_id: RunId, tool: Tool, limit: Annotated[int | None, Query(ge=1, le=20)] = None):
        if limit is not None and tool not in {"read_logs", "inspect_validation"}:
            raise _ApiFailure(422, "invalid_request")
        arguments = {"run_id": run_id}
        if limit is not None:
            arguments["limit"] = limit
        return read(lambda: EvidenceEnvelope.model_validate(gateway().call(tool, arguments)))

    @app.get("/v1/investigations/{investigation_id}", response_model=SavedRecord)
    def investigation(investigation_id: RunId):
        return read(lambda: SavedRecord.model_validate(CheckpointStore(settings.investigation_state_dir).load(investigation_id)))

    @app.get("/v1/repairs/{repair_id}", response_model=SavedRecord)
    def repair(repair_id: RunId):
        return read(lambda: SavedRecord.model_validate(RepairService(settings.data_dir, settings.output_dir,
            settings.repair_state_dir).show(repair_id)))

    def operate(operation):
        try:
            settings.guard_roots()
            return operation()
        except (ValueError, OSError):
            raise _ApiFailure(409, "operation_rejected") from None

    def require_model():
        if settings.model_digest is None:
            raise _ApiFailure(409, "model_unconfigured")

    def engine():
        from pipesleuth.investigator import BoundedInvestigator
        from pipesleuth.local_model import LocalModelClient
        require_model()
        return BoundedInvestigator(gateway(), LocalModelClient(settings.model_config()),
            max_steps=settings.max_steps, max_tool_calls=settings.max_tool_calls,
            timeout_seconds=settings.agent_timeout)

    def persistent():
        from pipesleuth.persistent import PersistentInvestigator
        # Restore only the server's own settings. The coordinator compares the
        # saved configuration before admitting any evidence/model operation.
        return PersistentInvestigator(engine(), CheckpointStore(settings.investigation_state_dir),
                                      retry_limit=settings.retry_limit)

    def repair_service():
        return RepairService(settings.data_dir, settings.output_dir, settings.repair_state_dir)

    @app.post("/v1/runs/{run_id}/diagnosis", response_model=DiagnosisResult,
              response_model_exclude_unset=True)
    def diagnosis(run_id: RunId, body: DiagnosisRequest):
        if body.method != "checklist":
            require_model()

        def diagnose():
            if body.method == "checklist":
                from pipesleuth.checklist import ChecklistBaseline
                result = ChecklistBaseline(gateway()).diagnose(run_id)
            elif body.method == "local":
                from pipesleuth.local_model import LocalModelClient
                from pipesleuth.single_call import SingleCallBaseline
                result = SingleCallBaseline(gateway(), LocalModelClient(settings.model_config())).diagnose(run_id)
            else:
                result = engine().investigate(run_id)
            return DiagnosisResult.model_validate(result)
        return operate(diagnose)

    @app.post("/v1/investigations", response_model=SavedRecord)
    def start_investigation(body: InvestigationStart):
        require_model()
        return operate(lambda: SavedRecord.model_validate(persistent().start(body.run_id)))

    @app.post("/v1/investigations/{investigation_id}/resume", response_model=SavedRecord)
    def resume_investigation(investigation_id: RunId, body: ResumeRequest):
        require_model()
        return operate(lambda: SavedRecord.model_validate(persistent().resume(
            investigation_id, max_operations=body.max_operations)))

    @app.post("/v1/repairs", response_model=SavedRecord)
    def plan_repair(body: RepairPlanRequest):
        return operate(lambda: SavedRecord.model_validate(repair_service().plan(body.run_id, body.action)))

    @app.post("/v1/repairs/{repair_id}/approve", response_model=SavedRecord)
    def approve_repair(repair_id: RunId, body: ApprovalRequest):
        return operate(lambda: SavedRecord.model_validate(repair_service().approve(
            repair_id, body.plan_sha256, body.input_fingerprint_sha256, origin="trusted_api")))

    @app.post("/v1/repairs/{repair_id}/execute", response_model=SavedRecord)
    def execute_repair(repair_id: RunId, body: ExecuteRequest):
        return operate(lambda: SavedRecord.model_validate(repair_service().execute(repair_id)))

    return app
