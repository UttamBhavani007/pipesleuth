"""Read-only audit of a prepared local UI acceptance workspace.

This does not drive the browser, approve/execute repairs, run a model or score
model quality. Browser observations must be supplied separately and are labelled
as operator-recorded observations. Evaluator truth stays out of HTTP/model input.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
from http.client import HTTPConnection
import json
from pathlib import Path
import platform
import re
import sqlite3

from pipesleuth.checkpoints import CheckpointStore
from pipesleuth.web_assets import CSP, UI_ASSETS

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKSPACE = ROOT / "reports/local/ui-smoke"
UUID = re.compile(r"[0-9a-f]{32}")
COLUMNS = ["order_id", "customer_id", "amount_minor", "currency", "created_at"]
SOURCES = tuple("src/pipesleuth/web/" + filename for filename, _ in UI_ASSETS.values()) + (
    "src/pipesleuth/api.py", "src/pipesleuth/api_contracts.py", "src/pipesleuth/api_security.py",
    "src/pipesleuth/api_server.py", "src/pipesleuth/web_assets.py", "src/pipesleuth/cli.py",
    "src/pipesleuth/checkpoints.py", "src/pipesleuth/persistent.py", "src/pipesleuth/investigator.py",
    "src/pipesleuth/local_model.py", "src/pipesleuth/evidence.py", "src/pipesleuth/checklist.py",
    "src/pipesleuth/repairs.py", "src/pipesleuth/repair_plan.py", "src/pipesleuth/repair_executor.py",
    "src/pipesleuth/repair_verifier.py", "src/pipesleuth/input_binding.py", "src/pipesleuth/pipeline.py",
    "src/pipesleuth/domain.py", "src/pipesleuth/fixtures.py", "pyproject.toml", "requirements-api.txt",
    "tests/test_ui_serving.py", "tests/ui_core.mjs", "tests/ui_investigations.mjs", "tests/ui_repairs.mjs",
    "scripts/prepare_ui_smoke.py", "scripts/audit_ui_state.py", "scripts/check_ui_client.mjs",
)


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _files(directory):
    return {str(path.relative_to(directory)): _hash(path) for path in directory.rglob("*") if path.is_file()}


def _database(path):
    """Literal independent SELECTs; no executor/verifier success flags are used."""
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        connection.execute("PRAGMA query_only = ON")
        orders = connection.execute("SELECT order_id,customer_id,amount_minor,currency,created_at FROM warehouse_orders ORDER BY order_id").fetchall()
        order_types = connection.execute("SELECT typeof(order_id),typeof(customer_id),typeof(amount_minor),typeof(currency),typeof(created_at) FROM warehouse_orders ORDER BY order_id").fetchall()
        rejected = connection.execute("SELECT row_number,reason_code,reason,raw_record_json FROM quarantined_records ORDER BY row_number").fetchall()
        run = connection.execute("SELECT run_id,status,duplicate_policy,invalid_policy,duplicates_skipped,rows_quarantined FROM pipeline_run").fetchall()
    return {"warehouse_orders": [list(row) for row in orders],
            "warehouse_value_types": [list(row) for row in order_types],
            "quarantined_records": [list(row[:3]) + [json.loads(row[3])] for row in rejected],
            "pipeline_run": [list(row) for row in run]}


def _records(directory):
    path = directory / "checkpoints.sqlite3"
    if not path.exists():
        return []
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        identifiers = [row[0] for row in connection.execute("SELECT investigation_id FROM investigations ORDER BY investigation_id")]
    store = CheckpointStore(directory)
    return [store.load(identifier) for identifier in identifiers]


def _expected_quarantine(case, source):
    if not case["oracle"]["invalid_row_numbers"]:
        return []
    reasons = ["order columns do not match contract (missing=['amount_minor'], extra=[])",
               "amount_minor must be a non-negative integer", "currency must be one of EUR, GBP, USD",
               "created_at must be timezone-aware and in UTC"]
    invalid = case["oracle"]["invalid_row_numbers"]
    if case["scenario"] != "invalid_records" or len(invalid) != 4:
        raise ValueError("unsupported evaluator fixture")
    return [[number, "invalid_row", reason, source["rows"][number - 1]] for number, reason in zip(invalid, reasons)]


def _repair(record, workspace, authoring):
    state, plan = record["state"], record["state"]["plan"]
    identifier = record["investigation_id"]
    source_id, target_id = plan["source_run_id"], plan["target_run_id"]
    if not UUID.fullmatch(source_id) or not UUID.fullmatch(target_id):
        raise ValueError("invalid saved run identifier")
    case = authoring[source_id]
    source = json.loads((workspace / "input" / source_id / "source.json").read_text())
    result = {"repair_id": identifier, "revision": record["revision"], "status": state["status"],
              "pending_operation": record["pending"]["operation"] if record["pending"] else None,
              "source_run_id": source_id, "target_run_id": target_id, "action": plan["action"],
              "plan_sha256": state["plan_sha256"], "input_fingerprint_sha256": state["input_fingerprint_sha256"],
              "approval_origin": state["approval"]["origin"] if state["approval"] else None,
              "execution_recorded": state["execution"] is not None,
              "independent_database_checks": None}
    target = workspace / "output" / target_id
    if not target.exists():
        result["derived_run_present"] = False
        if state["status"] == "succeeded":
            result["independently_verified"] = False
        return result
    result["derived_run_present"] = True
    actual = _database(target / "warehouse.sqlite3")
    expected_rows = sorted([[order[column] for column in COLUMNS] for order in case["oracle"]["expected_orders"]])
    policies = ("skip_identical", "reject") if plan["action"] == "skip_identical_duplicates" else ("reject", "quarantine") if plan["action"] == "quarantine_invalid_rows" else ("reject", "reject")
    if plan["action"] == "rename_amount_column":
        expected_source = {"schema": COLUMNS, "rows": [{("amount_minor" if key == "gross_total_minor" else key): value for key, value in row.items()} for row in source["rows"]]}
    else:
        expected_source = source
    expected_quarantine = _expected_quarantine(case, source) if plan["action"] == "quarantine_invalid_rows" else []
    expected_run = [[target_id, "succeeded", *policies,
                    len(case["oracle"]["duplicate_order_ids"]) if plan["action"] == "skip_identical_duplicates" else 0,
                    len(expected_quarantine)]]
    actual_source = json.loads((target / "source.json").read_text())
    result["actual_database"] = actual
    checks = {"target_is_distinct": target_id != source_id,
              "exact_warehouse_rows_from_fixture_oracle": actual["warehouse_orders"] == expected_rows,
              "exact_warehouse_value_types": actual["warehouse_value_types"] == [["text", "text", "integer", "text", "text"]] * len(expected_rows),
              "exact_quarantine_rows_and_reasons": actual["quarantined_records"] == expected_quarantine,
              "exact_pipeline_metadata": actual["pipeline_run"] == expected_run,
              "exact_derived_source": actual_source == expected_source}
    result["independent_database_checks"] = checks
    result["independently_verified"] = all(checks.values())
    return result


def _investigation(record):
    state, result = record["state"], record["state"]["result"]
    return {"investigation_id": record["investigation_id"], "revision": record["revision"],
            "done": state["done"], "status": result["status"], "run_id": result["run_id"],
            "pending_operation": record["pending"]["operation"] if record["pending"] else None,
            "tool_calls": result["tool_calls"], "model_generation_requests": result["model_calls"],
            "generation_count_is_lower_bound": record["pending"] is not None,
            "limitations": result["limitations"], "evidence_count": len(result["evidence"]),
            "semantic_support_verified": result["semantic_support_verified"],
            "trace": [{key: event.get(key) for key in ("step", "action", "tool", "status", "failure_code", "model_calls")} for event in result["trace"]]}


def audit_http(port):
    """Only public GETs to a literal loopback server, with no private token read."""
    if type(port) is not int or not 1024 <= port <= 65535:
        raise ValueError("invalid local audit port")
    observations = []
    cases = [(path, 200, {}) for path in UI_ASSETS] + [
        ("/.runtime/api-session.json", 401, {}), ("/session", 401, {}),
        ("/v1/capabilities", 401, {}), ("/ui/missing.mjs", 401, {}),
        ("/ui/%61pp.mjs", 401, {}), ("/?token=not-a-credential", 400, {}),
        ("/", 403, {"Origin": "https://foreign.example"}),
        ("/", 400, {"Host": "localhost:" + str(port)}),
    ]
    for path, expected, headers in cases:
        with closing(HTTPConnection("127.0.0.1", port, timeout=5)) as connection:
            connection.request("GET", path, headers=headers)
            response = connection.getresponse()
            content = response.read(262145)
            policy = response.getheader("Content-Security-Policy")
            checks = {"expected_status": response.status == expected, "exact_csp": policy == CSP,
                      "no_store": response.getheader("Cache-Control") == "no-store",
                      "no_referrer": response.getheader("Referrer-Policy") == "no-referrer",
                      "no_cookie": response.getheader("Set-Cookie") is None,
                      "bounded_response": len(content) <= 262144}
            if expected == 200:
                filename, media = UI_ASSETS[path]
                checks.update(exact_asset_bytes=_hash(ROOT / "src/pipesleuth/web" / filename) == hashlib.sha256(content).hexdigest(),
                              exact_media_type=response.getheader("Content-Type") == media + "; charset=utf-8")
            else:
                value = json.loads(content)
                checks["generic_error_only"] = (isinstance(value, dict) and set(value) == {"error"}
                                                 and isinstance(value["error"], dict) and set(value["error"]) == {"code"})
            observations.append({"path": path, "method": "GET", "status": response.status,
                                 "response_sha256": hashlib.sha256(content).hexdigest(), "response_bytes": len(content),
                                 "checks": checks, "passed": all(checks.values())})
    return {"scope": "Independent public GET audit; no capability tokens loaded or mutations sent.",
            "responses": observations, "passed": all(item["passed"] for item in observations)}


def audit(workspace=DEFAULT_WORKSPACE, *, browser_observations=None, client_probe=None, port=None):
    workspace = Path(workspace).resolve()
    before = _files(workspace)
    hashes = {name: _hash(ROOT / name) for name in SOURCES}
    manifest = json.loads((workspace / "manifest.json").read_text())
    authoring = {case["run_id"]: case for case in json.loads((workspace / "authoring.json").read_text())["cases"]}
    report = {"scope": "Local UI integration acceptance; not model accuracy, human review, production recovery or deployment evidence.",
              "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
              "config": {"python_version": platform.python_version(), "machine": platform.machine(), "source_sha256": hashes},
              "model_quality_evaluated": False, "human_review_claimed": False,
              "browser_observations": browser_observations or [],
              "browser_observation_scope": "Agent-operated native-browser observations, supplied separately; this read-only audit does not drive or score a browser. No human review is established.",
              "input_cases": [], "saved_investigations": [], "repair_records": [], "errors": []}
    for case in manifest["cases"]:
        identifier = case["run_id"]
        if not UUID.fullmatch(identifier):
            raise ValueError("invalid prepared run identifier")
        directory = workspace / "input" / identifier
        actual_hashes = {name: _hash(directory / name) for name in ("source.json", "warehouse.sqlite3")}
        report["input_cases"].append({"case_id": case["case_id"], "run_id": identifier,
                                      "initial_pipeline_status": case["observed_pipeline_status"],
                                      "initial_input_sha256": case["input_sha256"], "observed_input_sha256": actual_hashes,
                                      "original_files_exactly_preserved": actual_hashes == case["input_sha256"]})
    for record in _records(workspace / "investigations"):
        report["saved_investigations"].append(_investigation(record))
    for record in _records(workspace / "repairs"):
        try:
            report["repair_records"].append(_repair(record, workspace, authoring))
        except Exception as error:
            report["errors"].append({"phase": "independent_repair_read", "repair_id": record["investigation_id"], "type": type(error).__name__})
    if port is not None:
        report["public_http_audit"] = audit_http(port)
    if client_probe is not None:
        if (not isinstance(client_probe, dict) or client_probe.get("browser_driven") is not False
                or client_probe.get("passed") is not True
                or not client_probe.get("workspace_files_unchanged")
                or any(hashes.get(name) != digest for name, digest in client_probe.get("source_sha256", {}).items())):
            raise ValueError("client probe is unsuccessful or belongs to different sources")
        report["exact_javascript_client_probe"] = client_probe
    report["workspace_unchanged_during_readonly_audit"] = _files(workspace) == before
    report["bound_sources_unchanged_during_audit"] = hashes == {name: _hash(ROOT / name) for name in SOURCES}
    report["summary"] = {"prepared_input_runs": len(report["input_cases"]),
                          "original_inputs_exactly_preserved": sum(case["original_files_exactly_preserved"] for case in report["input_cases"]),
                          "saved_investigations": len(report["saved_investigations"]),
                          "recorded_model_generation_requests": sum(item["model_generation_requests"] for item in report["saved_investigations"]),
                          "generation_count_is_lower_bound": any(item["generation_count_is_lower_bound"] for item in report["saved_investigations"]),
                          "repair_records": len(report["repair_records"]),
                          "independently_verified_derived_runs": sum(item.get("independently_verified", False) for item in report["repair_records"]),
                          "public_http_responses": len(report.get("public_http_audit", {}).get("responses", [])),
                          "audit_errors": len(report["errors"])}
    report["readonly_audit_passed"] = (all(item["original_files_exactly_preserved"] for item in report["input_cases"])
        and all(item.get("independently_verified", True) for item in report["repair_records"])
        and report["workspace_unchanged_during_readonly_audit"] and report["bound_sources_unchanged_during_audit"]
        and report.get("public_http_audit", {"passed": True})["passed"] and not report["errors"])
    # Do not serialize path-bearing saved state, session files or credentials.
    content = _encoded(report).decode()
    if any(fragment in content for fragment in ("/Users/", "/private/", "/tmp/", "Bearer ")):
        raise ValueError("report contains private path or credential-like text")
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    parser.add_argument("--output", type=Path, default=ROOT / "reports/local/ui-state-audit.json")
    parser.add_argument("--browser-observations", type=Path)
    parser.add_argument("--client-probe", type=Path)
    parser.add_argument("--port", type=int, help="optional public read-only loopback HTTP checks")
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("output exists; choose a new path to preserve the previous observation")
    observations = json.loads(args.browser_observations.read_text()) if args.browser_observations else None
    probe = json.loads(args.client_probe.read_text()) if args.client_probe else None
    report = audit(args.workspace, browser_observations=observations, client_probe=probe, port=args.port)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as output:
        output.write(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n")
    print(json.dumps(report["summary"], sort_keys=True))
    return 0 if report["readonly_audit_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
