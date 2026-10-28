"""Record two actual bounded-agent development smokes without retries or repairs.

Literal setup and expected outcomes are evaluator-only. The investigator sees a
neutral run ID and whichever allowlisted evidence it actually requests.
"""

from __future__ import annotations

import argparse
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import sqlite3
import tempfile
import time

from pipesleuth.evidence import EvidenceGateway
from pipesleuth.investigator import BoundedInvestigator
from pipesleuth.local_model import LocalModelClient, LocalModelConfig
from pipesleuth.pipeline import SandboxPipeline

# Only metadata/snapshot helpers are reused; importing this module performs no
# single-call baseline or model generation. Its source is hashed below as well.
from smoke_local import _api, _files, _run_files, _startup


ROOT = Path(__file__).resolve().parents[1]
SOURCES = ("src/pipesleuth/investigator.py", "src/pipesleuth/agent_protocol.py",
           "src/pipesleuth/local_model.py", "src/pipesleuth/evidence.py",
           "src/pipesleuth/checklist.py", "scripts/smoke_investigator.py",
           "scripts/smoke_local.py", "scripts/local_runtime.py")
COLUMNS = ("order_id", "customer_id", "amount_minor", "currency", "created_at")


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _database_state(database):
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
        return {
            "warehouse": connection.execute(
                "SELECT order_id,customer_id,amount_minor,currency,created_at "
                "FROM warehouse_orders ORDER BY order_id").fetchall(),
            "quarantine": connection.execute(
                "SELECT row_number,reason_code,reason,raw_record_json "
                "FROM quarantined_records ORDER BY row_number").fetchall(),
        }


class RecordingGateway(EvidenceGateway):
    def __init__(self, directory):
        super().__init__(directory)
        self.calls = []

    def call(self, tool, arguments, **options):
        self.calls.append({"tool": tool, "arguments": dict(arguments),
                           "timeout_seconds": options.get("timeout_seconds")})
        return super().call(tool, arguments, **options)


class RecordingClient(LocalModelClient):
    def __init__(self, config):
        super().__init__(config)
        self.attempts = []

    def generate(self, system, prompt, schema, **options):
        self.attempts.append({
            "system_sha256": hashlib.sha256(system.encode("utf-8")).hexdigest(),
            "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
            "format_schema_sha256": hashlib.sha256(_encoded(schema)).hexdigest(),
            "schema_has_any_of": isinstance(schema.get("anyOf"), list),
            "input_bytes": len(system.encode("utf-8")) + len(prompt.encode("utf-8")) + len(_encoded(schema)),
            "timeout_seconds": options.get("timeout_seconds"),
        })
        return super().generate(system, prompt, schema, **options)


def _case(pipeline, config, renamed):
    clean_rows = [
        {"order_id": "A-1", "customer_id": "C-1", "amount_minor": 1250,
         "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"},
        {"order_id": "A-2", "customer_id": "C-2", "amount_minor": 750,
         "currency": "EUR", "created_at": "2026-01-02T09:00:00Z"},
    ]
    source = {"schema": list(COLUMNS), "rows": deepcopy(clean_rows)}
    if renamed:
        source["schema"][2] = "gross_total_minor"
        for row in source["rows"]:
            row["gross_total_minor"] = row.pop("amount_minor")
    expected = {"status": "diagnosed" if renamed else "no_incident",
                "codes": ["schema_mismatch"] if renamed else []}
    created = pipeline.create(source)
    pipeline.run(created.run_id)
    database = pipeline.database_path(created.run_id)
    before_files, before_state = _run_files(pipeline.data_dir), _database_state(database)
    expected_rows = [] if renamed else [tuple(row[column] for column in COLUMNS) for row in clean_rows]
    exact_setup = before_state == {"warehouse": expected_rows, "quarantine": []}
    gateway, client = RecordingGateway(pipeline.data_dir), RecordingClient(config)
    observed, error = None, None
    started = time.perf_counter()
    try:
        observed = BoundedInvestigator(gateway, client, max_steps=4, max_tool_calls=4,
                                      timeout_seconds=60).investigate(created.run_id)
    except Exception as failure:
        # Preserve the failed case, without exception text or local paths.
        error = {"type": type(failure).__name__}
    elapsed = time.perf_counter() - started
    after_state = _database_state(database)
    trace = observed["trace"] if observed is not None else []
    adaptive = [event["tool"] for event in trace if event.get("action") == "tool"
                and event.get("step", 0) > 0 and event.get("status") == "ok"]
    accepted_finish = any(event.get("decision", {}).get("action") == "finish" for event in trace)
    codes = sorted({finding["code"] for finding in observed["findings"]}) if observed is not None else None
    match = observed is not None and observed["status"] == expected["status"] and codes == expected["codes"]
    scope_ok = all(call["arguments"] == {"run_id": created.run_id,
                    **({"limit": 20} if call["tool"] in {"read_logs", "inspect_validation"} else {})}
                   for call in gateway.calls)
    counters_ok = (observed is not None and observed["tool_calls"] == len(gateway.calls)
                   and observed["model_calls"] == client.generation_requests
                   and observed["steps"] == len(client.attempts))
    return {
        "case": "renamed_amount_column" if renamed else "healthy_input", "run_id": created.run_id,
        "expected_literal_behavior": expected, "literal_development_expectation_match": match,
        "observed": observed, "error": error, "elapsed_seconds": elapsed,
        "gateway_calls": gateway.calls, "generation_attempts": client.attempts,
        "actual_generation_requests": client.generation_requests, "adaptive_tool_choices": adaptive,
        "accepted_finish": accepted_finish, "fixed_scope_arguments_verified": scope_ok,
        "reported_counters_match_observed": counters_ok,
        "any_of_model_response_observed": any(item["schema_has_any_of"] for item in client.attempts)
            and any(event.get("model_response") is not None for event in trace),
        "files_unchanged": _run_files(pipeline.data_dir) == before_files,
        "exact_database_setup_verified": exact_setup,
        "exact_database_state_unchanged": before_state == after_state,
        "database_state_sha256_before": hashlib.sha256(_encoded(before_state)).hexdigest(),
        "database_state_sha256_after": hashlib.sha256(_encoded(after_state)).hexdigest(),
        "semantic_quality_evaluated": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--endpoint", default="http://127.0.0.1:11436")
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--models", type=Path)
    parser.add_argument("--runtime-log", type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists; choose a new path to preserve previous outcomes")
    source_hashes = {name: _hash(ROOT / name) for name in SOURCES}
    cache_before = _files(args.models) if args.models is not None else None
    config = LocalModelConfig(endpoint=args.endpoint)
    report = {
        "generated_at": None,
        "scope": "Two synthetic bounded-agent integration smokes; not a holdout, accuracy estimate, or recovery result.",
        "config": {"python_version": platform.python_version(), "machine": platform.machine(),
                   "physical_memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
                   "endpoint": config.endpoint, "model": config.model, "context_tokens": config.num_ctx,
                   "output_token_cap": config.max_output_tokens, "temperature": 0, "think": False,
                   "max_steps": 4, "max_tool_calls": 4, "agent_timeout_seconds": 60,
                   "client_timeout_cap_seconds": config.timeout_seconds,
                   "runtime_binary_sha256": _hash(args.binary) if args.binary is not None else None,
                   "source_sha256": source_hashes},
        "runtime": None, "cases": [], "errors": [],
    }
    try:
        health = LocalModelClient(config).health()
        status, shown = _api(config.endpoint, "POST", "/api/show", {"model": config.model, "verbose": False})
        license_text = shown.get("license", "")
        if status != 200 or not isinstance(license_text, str) or not all(
            phrase in license_text for phrase in ("Apache License", "Version 2.0")
        ):
            raise ValueError("served local model license was not verified")
        report["runtime"] = {"health": health, "license_is_apache_2": True,
                             "license_sha256": hashlib.sha256(license_text.encode()).hexdigest(),
                             "capabilities": shown.get("capabilities", []),
                             "model_info": {key: value for key, value in shown.get("model_info", {}).items()
                                            if key in {"general.architecture", "general.parameter_count", "qwen3.context_length"}},
                             "details": shown.get("details", {})}
        report["config"]["pinned_digest"] = health["digest"]
        pinned = LocalModelConfig(endpoint=config.endpoint, expected_digest=health["digest"])
        with tempfile.TemporaryDirectory(prefix="pipesleuth-investigator-smoke-") as temporary:
            pipeline = SandboxPipeline(Path(temporary).resolve() / "runs")
            for renamed in (False, True):
                report["cases"].append(_case(pipeline, pinned, renamed))
    except Exception as failure:
        report["errors"].append({"phase": "actual_ollama_smoke", "type": type(failure).__name__})
    report["startup_observations"] = _startup(args.runtime_log)
    report["cache_metadata_unchanged"] = _files(args.models) == cache_before if args.models is not None else None
    report["source_hashes_unchanged_during_run"] = all(_hash(ROOT / name) == value for name, value in source_hashes.items())
    if not report["source_hashes_unchanged_during_run"]:
        report["errors"].append({"phase": "source_binding", "type": "ImplementationChangedDuringSmoke"})
    report["generated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    cases = report["cases"]
    report["summary"] = {
        "planned_cases": 2, "recorded_cases": len(cases),
        "actual_generation_requests": sum(case["actual_generation_requests"] for case in cases),
        "admitted_adaptive_tool_reads": sum(len(case["adaptive_tool_choices"]) for case in cases),
        "cases_with_adaptive_reads": sum(bool(case["adaptive_tool_choices"]) for case in cases),
        "accepted_finishes": sum(case["accepted_finish"] for case in cases),
        "literal_development_expectation_matches": sum(case["literal_development_expectation_match"] for case in cases),
        "unchanged_run_cases": sum(case["files_unchanged"] and case["exact_database_state_unchanged"] for case in cases),
        "case_errors": sum(case["error"] is not None for case in cases), "driver_errors": len(report["errors"]),
        "status_counts": {name: sum(case["observed"] is not None and case["observed"]["status"] == name for case in cases)
                          for name in ("diagnosed", "no_incident", "unknown")},
        "semantic_quality_evaluated": False,
    }
    startup_ok = args.runtime_log is None or all(report["startup_observations"].get(key) is True for key in (
        "cloud_disabled_observed", "loopback_binding_observed", "request_body_debug_disabled_observed",
        "cache_pruning_disabled_observed"))
    scope_ok = len(cases) == 2 and all(case["files_unchanged"] and case["exact_database_state_unchanged"]
                  and case["exact_database_setup_verified"] and case["fixed_scope_arguments_verified"]
                  and case["reported_counters_match_observed"] and case["error"] is None for case in cases)
    report["integration_and_scope_checks_passed"] = bool(scope_ok and startup_ok and not report["errors"]
        and report["cache_metadata_unchanged"] is not False
        and report["summary"]["admitted_adaptive_tool_reads"] > 0)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps(report["summary"], sort_keys=True))
    return 0 if report["integration_and_scope_checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
