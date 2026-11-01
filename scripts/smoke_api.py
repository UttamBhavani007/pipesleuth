"""Audit an owned foreground local API over HTTP using scripted approvals.

No model backend is configured or called. Tokens stay in memory/private files;
reports omit Authorization headers and path-bearing core records.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
from http.client import HTTPConnection
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time

from pipesleuth.pipeline import SandboxPipeline
from smoke_repair import ACTIONS, COLUMNS, EXPECTED, ROWS, _audit, _database, _files, _hash, _source


ROOT = Path(__file__).resolve().parents[1]
SOURCES = ("src/pipesleuth/api.py", "src/pipesleuth/api_contracts.py",
           "src/pipesleuth/api_security.py", "src/pipesleuth/api_server.py",
           "src/pipesleuth/cli.py", "requirements-api.txt",
           "src/pipesleuth/repairs.py", "src/pipesleuth/repair_plan.py",
           "src/pipesleuth/repair_executor.py", "src/pipesleuth/repair_verifier.py",
           "src/pipesleuth/checkpoints.py", "src/pipesleuth/input_binding.py",
           "src/pipesleuth/evidence.py", "src/pipesleuth/checklist.py",
           "src/pipesleuth/pipeline.py", "src/pipesleuth/domain.py",
           "scripts/smoke_repair.py", "scripts/smoke_api.py")


class Audit:
    def __init__(self, port, session):
        self.port, self.session, self.requests = port, session, []

    def call(self, name, method, path, *, role="operation", payload=None,
             raw=None, headers=None, duplicate_authorization=False):
        configured = {"Accept": "application/json", **(headers or {})}
        if role is not None:
            configured["Authorization"] = "Bearer " + self.session[role + "_token"]
        if payload is not None:
            raw = json.dumps(payload, allow_nan=False, separators=(",", ":")).encode()
        if raw is not None:
            configured.setdefault("Content-Type", "application/json")
        started = time.monotonic()
        connection = HTTPConnection("127.0.0.1", self.port, timeout=30)
        entry = {"name": name, "method": method, "status": None, "capability_role": role,
                 "response_bytes": None, "response_sha256": None, "error_code": None,
                 "transport_or_decode_error": None, "elapsed_ms": None}
        self.requests.append(entry)
        try:
            if duplicate_authorization:
                connection.putrequest(method, path)
                for key, value in configured.items():
                    connection.putheader(key, value)
                connection.putheader("Authorization", "Bearer " + self.session["read_token"])
                connection.endheaders(raw)
            else:
                connection.request(method, path, body=raw, headers=configured)
            response = connection.getresponse()
            content = response.read(2 * 1024 * 1024 + 1)
            if len(content) > 2 * 1024 * 1024:
                raise ValueError("API response exceeded audit bound")
            entry.update(status=response.status, response_bytes=len(content),
                         response_sha256=hashlib.sha256(content).hexdigest())
            body = json.loads(content)
            if not isinstance(body, dict):
                raise ValueError("API response is not an object")
            error = body.get("error")
            entry["error_code"] = error.get("code") if isinstance(error, dict) else None
            # An audit failure never prints private token/body contents.
            assert all(token not in content.decode("utf-8") for key, token in self.session.items() if key.endswith("_token"))
            return response.status, body
        except Exception as failure:
            entry["transport_or_decode_error"] = type(failure).__name__
            raise
        finally:
            entry["elapsed_ms"] = round((time.monotonic() - started) * 1000, 3)
            connection.close()


def _expect(result, status):
    actual, body = result
    assert actual == status, "unexpected HTTP status"
    if status >= 400:
        assert set(body) == {"error"} and set(body["error"]) == {"code"}
    return body


def _port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _wait_session(process, path, port):
    deadline = time.monotonic() + 12
    probes, responses = 0, []
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("owned API process exited during startup")
        if path.exists():
            info = path.stat(follow_symlinks=False)
            assert stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600
            try:
                session = json.loads(path.read_text())
            except json.JSONDecodeError:
                # Existence precedes a completed private startup write.
                time.sleep(0.03)
                continue
            assert set(session) == {"base_url", "read_token", "operation_token", "approval_token"}
            assert session["base_url"] == f"http://127.0.0.1:{port}"
            values = [session[key] for key in ("read_token", "operation_token", "approval_token")]
            assert all(isinstance(value, str) and len(value) >= 32 for value in values) and len(set(values)) == 3
            try:
                connection = HTTPConnection("127.0.0.1", port, timeout=0.3)
                try:
                    probes += 1
                    connection.request("GET", "/health")
                    status = connection.getresponse().status
                    responses.append(status)
                    ready = status == 200
                finally:
                    connection.close()
            except OSError:
                ready = False
            if ready:
                return session, {"health_probe_attempts": probes, "health_response_statuses": responses,
                                 "private_session_mode_0600": True, "distinct_capabilities_verified": True}
        time.sleep(0.03)
    raise TimeoutError("owned API startup exceeded its audit deadline")


def _stop(process, port, session_file):
    sent_interrupt = process.poll() is None
    if sent_interrupt:
        process.send_signal(signal.SIGINT)
    forced = False
    try:
        process.wait(timeout=12)
    except subprocess.TimeoutExpired:
        forced = True
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            closed = False
    except ConnectionRefusedError:
        closed = True
    return {"owned_process_stopped": process.poll() is not None,
            "interrupt_sent_to_owned_process": sent_interrupt,
            "forced_shutdown_required": forced,
            "loopback_port_connection_refused": closed,
            "private_session_file_removed": not session_file.exists()}


def _repair_case(audit, pipeline, output, action, run_id, *, concurrent=False):
    outcome = {"action": action, "error": None, "checks": {}}
    try:
        originals = _files(pipeline.data_dir)
        before_database = _database(pipeline.database_path(run_id))
        planned = _expect(audit.call("plan_" + action, "POST", "/v1/repairs",
            payload={"run_id": run_id, "action": action}), 200)
        identifier, state = planned["investigation_id"], planned["state"]
        path = f"/v1/repairs/{identifier}"
        approval = {"plan_sha256": state["plan_sha256"],
                    "input_fingerprint_sha256": state["input_fingerprint_sha256"]}
        _expect(audit.call("unapproved_execute_" + action, "POST", path + "/execute", payload={}), 409)
        _expect(audit.call("operator_cannot_approve_" + action, "POST", path + "/approve", payload=approval), 403)
        _expect(audit.call("wrong_hash_" + action, "POST", path + "/approve", role="approval",
                           payload={**approval, "plan_sha256": "0" * 64}), 409)
        approved = _expect(audit.call("scripted_approval_" + action, "POST", path + "/approve",
                                     role="approval", payload=approval), 200)
        if concurrent:
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(lambda _: audit.call("concurrent_execute_" + action,
                    "POST", path + "/execute", payload={}), range(2)))
            statuses = [result[0] for result in results]
            assert statuses.count(200) == 1 and next(code for code in statuses if code != 200) in (409, 503)
            completed = next(body for status, body in results if status == 200)
            outcome["concurrent_statuses"] = statuses
        else:
            completed = _expect(audit.call("execute_" + action, "POST", path + "/execute", payload={}), 200)
        actual = _database(output / state["plan"]["target_run_id"] / "warehouse.sqlite3")
        rejected = ([[4, "invalid_row", "amount_minor must be a non-negative integer",
                      dict(ROWS[0], order_id="L-X", amount_minor=-7)]]
                    if action == "quarantine_invalid_rows" else [])
        expected_run = [[state["plan"]["target_run_id"], "succeeded",
                         state["plan"]["policies"]["duplicate_policy"], state["plan"]["policies"]["invalid_policy"],
                         2 if action == "skip_identical_duplicates" else 0,
                         1 if action == "quarantine_invalid_rows" else 0]]
        duplicate_before = _files(output)
        _expect(audit.call("duplicate_execute_" + action, "POST", path + "/execute", payload={}), 409)
        shown = _expect(audit.call("show_repair_" + action, "GET", path, role="read"), 200)
        outcome.update({"plan": _audit(planned), "approval": _audit(approved), "completed": _audit(completed),
                        "actual_database": actual,
                        "approval_is_scripted_http_audit": True, "human_approval_claimed": False})
        outcome["checks"] = {"succeeded": completed["state"]["status"] == "succeeded",
                             "post_input_verified": completed["state"]["execution"]["post_input_verified"] is True,
                             "exact_warehouse_rows": actual["warehouse_orders"] == EXPECTED,
                             "exact_quarantine_rows": actual["quarantined_records"] == rejected,
                             "exact_run_metadata": actual["pipeline_run"] == expected_run,
                             "original_and_unrelated_files_unchanged": _files(pipeline.data_dir) == originals,
                             "original_database_exactly_unchanged": _database(pipeline.database_path(run_id)) == before_database,
                             "duplicate_execution_output_unchanged": _files(output) == duplicate_before,
                             "read_matches_completed_record": shown == completed}
    except Exception as failure:
        outcome["error"] = {"type": type(failure).__name__}
    outcome["passed"] = outcome["error"] is None and bool(outcome["checks"]) and all(outcome["checks"].values())
    return outcome


def _stale(audit, pipeline, output, run_id):
    planned = _expect(audit.call("stale_plan", "POST", "/v1/repairs",
        payload={"run_id": run_id, "action": "rename_amount_column"}), 200)
    identifier, state = planned["investigation_id"], planned["state"]
    path = f"/v1/repairs/{identifier}"
    approved = _expect(audit.call("stale_scripted_approval", "POST", path + "/approve", role="approval",
        payload={"plan_sha256": state["plan_sha256"], "input_fingerprint_sha256": state["input_fingerprint_sha256"]}), 200)
    source = pipeline.data_dir / run_id / "source.json"
    source.write_bytes(source.read_bytes() + b"\n")
    after_fixture_mutation, output_before = _files(pipeline.data_dir), _files(output)
    _expect(audit.call("stale_execution_refused", "POST", path + "/execute", payload={}), 409)
    shown = _expect(audit.call("stale_show_unchanged", "GET", path, role="read"), 200)
    return {"explicit_fixture_mutation": "Added source whitespace after scripted approval; no logical row change.",
            "saved_record_unchanged": shown == approved,
            "output_files_unchanged": _files(output) == output_before,
            "original_files_unchanged_during_refusal": _files(pipeline.data_dir) == after_fixture_mutation}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "reports/local/api-smoke.json")
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("output already exists; choose a new path to preserve earlier outcomes")
    hashes = {name: _hash(ROOT / name) for name in SOURCES}
    report = {"generated_at": None, "scope": "Actual loopback API HTTP audit using synthetic data and scripted approvals; no actual model inference or human approval claim.",
              "config": {"python_version": platform.python_version(), "machine": platform.machine(),
                         "source_sha256": hashes,
                         "packages": {name: importlib.metadata.version(name) for name in ("fastapi", "uvicorn", "pydantic", "starlette")}},
              "requests": [], "repair_cases": [], "checks": {}, "stale_input_refusal": None,
              "cleanup": None, "startup": None, "errors": [], "actual_model_generation_requests": 0}
    with tempfile.TemporaryDirectory(prefix="pipesleuth-api-smoke-") as temporary:
        root = Path(temporary).resolve()
        pipeline, output = SandboxPipeline(root / "incidents"), root / "derived"
        healthy = pipeline.create({"schema": COLUMNS[:], "rows": deepcopy(ROWS)})
        pipeline.run(healthy.run_id)
        incidents = {}
        for action in ACTIONS:
            created = pipeline.create(_source(action))
            assert pipeline.run(created.run_id).status == "failed"
            incidents[action] = created.run_id
        stale = pipeline.create(_source("rename_amount_column"))
        pipeline.run(stale.run_id)
        original_files = _files(pipeline.data_dir)
        unrelated_before = _files(pipeline.data_dir / healthy.run_id)
        port, session_file = _port(), root / "api-session.json"
        environment = {name: value for name, value in os.environ.items()
                       if not name.upper().endswith("_PROXY") and not name.upper().startswith(("OPENAI_", "ANTHROPIC_", "OTEL_"))}
        environment.update(PYTHONPATH=str(ROOT / "src"), PYTHONDONTWRITEBYTECODE="1", OTEL_SDK_DISABLED="true")
        command = [sys.executable, "-m", "pipesleuth", "serve", "--port", str(port),
                   "--data-dir", str(pipeline.data_dir), "--output-dir", str(output),
                   "--investigation-state-dir", str(root / "investigations"),
                   "--repair-state-dir", str(root / "repairs"), "--session-file", str(session_file)]
        process = None
        try:
            process = subprocess.Popen(command, cwd=ROOT, env=environment, stdin=subprocess.DEVNULL,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            session, report["startup"] = _wait_session(process, session_file, port)
            audit = Audit(port, session)
            report["requests"] = audit.requests
            report["checks"]["session_mode_0600_and_distinct_capabilities"] = True
            health = _expect(audit.call("public_health", "GET", "/health", role=None), 200)
            assert health == {"status": "ok", "api_version": 1}
            report["checks"]["minimal_public_health_verified"] = True
            capabilities = _expect(audit.call("capabilities", "GET", "/v1/capabilities", role="read"), 200)
            assert capabilities["model_configured"] is False
            report["capabilities"] = capabilities
            _expect(audit.call("run_summary", "GET", f"/v1/runs/{healthy.run_id}", role="read"), 200)
            evidence = _expect(audit.call("bounded_logs", "GET", f"/v1/runs/{healthy.run_id}/evidence/read_logs?limit=2", role="read"), 200)
            assert evidence["tool"] == "read_logs" and evidence["arguments"]["limit"] == 2
            healthy_diagnosis = _expect(audit.call("healthy_checklist", "POST", f"/v1/runs/{healthy.run_id}/diagnosis", payload={"method": "checklist"}), 200)
            assert healthy_diagnosis["status"] == "no_incident"
            schema_diagnosis = _expect(audit.call("schema_checklist", "POST", f"/v1/runs/{incidents['rename_amount_column']}/diagnosis", payload={"method": "checklist"}), 200)
            assert schema_diagnosis["status"] == "diagnosed"
            assert [finding["code"] for finding in schema_diagnosis["findings"]] == ["schema_mismatch"]
            report["checklist_observations"] = {"healthy_status": healthy_diagnosis["status"],
                                                 "schema_status": schema_diagnosis["status"],
                                                 "schema_finding_codes": [finding["code"] for finding in schema_diagnosis["findings"]]}
            diagnosis_path = f"/v1/runs/{healthy.run_id}/diagnosis"
            negatives = [
                ("missing_capability", "GET", "/v1/capabilities", {"role": None}, 401),
                ("reader_cannot_mutate", "POST", diagnosis_path, {"role": "read", "payload": {"method": "checklist"}}, 403),
                ("approval_cannot_mutate", "POST", diagnosis_path, {"role": "approval", "payload": {"method": "checklist"}}, 403),
                ("foreign_origin", "GET", "/v1/capabilities", {"headers": {"Origin": "https://example.invalid"}}, 403),
                ("foreign_host", "GET", "/v1/capabilities", {"headers": {"Host": "example.invalid"}}, 400),
                ("forwarded_header", "GET", "/v1/capabilities", {"headers": {"X-Forwarded-For": "127.0.0.1"}}, 400),
                ("duplicate_authorization", "GET", "/v1/capabilities", {"duplicate_authorization": True}, 400),
                ("duplicate_json_keys", "POST", diagnosis_path, {"raw": b'{"method":"checklist","method":"local"}'}, 400),
                ("nonfinite_json", "POST", diagnosis_path, {"raw": b'{"method":NaN}'}, 400),
                ("oversized_json", "POST", diagnosis_path, {"raw": b" " * 4097}, 413),
                ("non_json_media", "POST", diagnosis_path, {"raw": b"hello", "headers": {"Content-Type": "text/plain"}}, 415),
                ("unknown_request_field", "POST", diagnosis_path, {"payload": {"method": "checklist", "path": "PRIVATE_INPUT_MARKER"}}, 422),
                ("unknown_query_field", "GET", f"/v1/runs/{healthy.run_id}?data_dir=PRIVATE_INPUT_MARKER", {}, 400),
                ("model_unconfigured", "POST", diagnosis_path, {"payload": {"method": "local"}}, 409),
                ("investigation_model_unconfigured", "POST", "/v1/investigations", {"payload": {"run_id": healthy.run_id}}, 409),
                ("missing_noncreating_read", "GET", "/v1/investigations/" + "0" * 32, {"role": "read"}, 404),
            ]
            for name, method, path, options, status in negatives:
                body = _expect(audit.call(name, method, path, **options), status)
                assert "PRIVATE_INPUT_MARKER" not in json.dumps(body)
                if name in {"model_unconfigured", "investigation_model_unconfigured"}:
                    assert body["error"]["code"] == "model_unconfigured"
            report["checks"]["all_sixteen_request_boundary_checks_passed"] = True
            report["checks"]["model_unconfigured_verified"] = True
            report["checks"]["unconfigured_investigation_read_created_no_store"] = not (root / "investigations").exists()
            for index, action in enumerate(ACTIONS):
                report["repair_cases"].append(_repair_case(audit, pipeline, output, action, incidents[action], concurrent=index == 0))
            report["checks"]["all_original_files_unchanged_before_explicit_stale_fixture"] = _files(pipeline.data_dir) == original_files
            report["stale_input_refusal"] = _stale(audit, pipeline, output, stale.run_id)
            report["checks"]["unrelated_healthy_run_unchanged"] = _files(pipeline.data_dir / healthy.run_id) == unrelated_before
        except Exception as failure:
            report["errors"].append({"phase": "owned_http_audit", "type": type(failure).__name__})
        finally:
            if process is not None:
                report["cleanup"] = _stop(process, port, session_file)
    report["generated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    report["source_hashes_unchanged_during_run"] = all(_hash(ROOT / name) == digest for name, digest in hashes.items())
    cases, cleanup = report["repair_cases"], report["cleanup"] or {}
    report["summary"] = {"audited_http_request_attempts": len(report["requests"]),
                         "audited_http_responses": sum(request["status"] is not None for request in report["requests"]),
                         "startup_health_probe_attempts": (report["startup"] or {}).get("health_probe_attempts", 0),
                         "planned_repair_cases": 3,
                         "recorded_repair_cases": len(cases), "verified_derived_runs": sum(case["passed"] for case in cases),
                         "case_errors": sum(case["error"] is not None for case in cases), "driver_errors": len(report["errors"]),
                         "actual_model_generation_requests": 0, "human_approval_claimed": False}
    report["api_checks_passed"] = (len(cases) == 3 and all(case["passed"] for case in cases)
        and bool(report["checks"]) and all(report["checks"].values())
        and bool(report["stale_input_refusal"]) and all(value for key, value in report["stale_input_refusal"].items() if key != "explicit_fixture_mutation")
        and cleanup.get("owned_process_stopped") is True and cleanup.get("loopback_port_connection_refused") is True
        and cleanup.get("private_session_file_removed") is True and cleanup.get("forced_shutdown_required") is False
        and report["source_hashes_unchanged_during_run"] and not report["errors"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n")
    print(json.dumps(report["summary"], sort_keys=True))
    return 0 if report["api_checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
