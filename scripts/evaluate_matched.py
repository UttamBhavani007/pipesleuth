"""Compare the three existing investigators on frozen synthetic inputs.

Only neutral run IDs and real gateway observations cross into the methods.
The diagnosis-to-action policy and its exact-plan approvals are scripted.
"""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
from statistics import median
import sqlite3
import sys
import tempfile
import time

from pipesleuth.checklist import ChecklistBaseline
from pipesleuth.evidence import EvidenceGateway
from pipesleuth.investigator import BoundedInvestigator
from pipesleuth.local_model import LocalModelClient, LocalModelConfig, LocalModelError
from pipesleuth.pipeline import SandboxPipeline
from pipesleuth.repairs import RepairError, RepairService
from pipesleuth.single_call import SingleCallBaseline


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = "evaluation/matched-v1.json"
MANIFEST_SHA256 = "f96dacf28e1c8a64beb872230fc9bc1d3243b6a15742bbff7768647da48c4f9e"
METHODS = ("checklist", "single_call", "bounded_agent")
ACTIONS = {"schema_mismatch": "rename_amount_column", "duplicate_key": "skip_identical_duplicates",
           "invalid_row": "quarantine_invalid_rows"}
COLUMNS = ["order_id", "customer_id", "amount_minor", "currency", "created_at"]
TOOLS = {"read_run", "read_logs", "inspect_schema", "inspect_validation"}
FAILED_LIMITATIONS = {"local_model_error", "invalid_decision", "invalid_generation",
                      "generation_truncated", "input_budget_exceeded", "context_budget_exceeded",
                      "elapsed_budget_exceeded", "trace_budget_exceeded", "result_limit_exceeded",
                      "report_limit_exceeded", "evidence_malformed", "evidence_truncated"}
BOUND_FILES = (MANIFEST, "scripts/evaluate_matched.py", *(f"src/pipesleuth/{name}.py" for name in (
    "agent_protocol", "checklist", "domain", "evidence", "investigator", "local_model", "pipeline",
    "checkpoints", "input_binding", "repairs", "repair_plan", "repair_executor", "repair_verifier",
    "single_call")))


def _encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _now():
    return datetime.now(timezone.utc).isoformat()


def load_manifest():
    raw = (ROOT / MANIFEST).read_bytes()
    if _sha(raw) != MANIFEST_SHA256:
        raise ValueError("frozen evaluator manifest hash mismatch")
    manifest = json.loads(raw)
    if manifest["schema_version"] != 1 or Counter(case["split"] for case in manifest["cases"]) != {"development": 3, "holdout": 6}:
        raise ValueError("invalid frozen split counts")
    for case in manifest["cases"]:
        if set(case["source"]) != {"schema", "rows"} or _sha(_encoded(case["source"])) != case["canonical_source_sha256"]:
            raise ValueError("frozen source payload hash mismatch")
    return manifest


def _snapshot(root):
    if not root.exists():
        return {}
    return {str(path.relative_to(root)): (_sha(path.read_bytes()), path.stat().st_mtime_ns)
            for path in root.rglob("*") if path.is_file()}


def _database(path):
    """Inspect actual SQLite rows and storage types independently of the repair verifier."""
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        def read(statement):
            return [list(row) for row in connection.execute(statement)]

        orders = read("SELECT order_id,customer_id,amount_minor,currency,created_at "
                      "FROM warehouse_orders ORDER BY order_id")
        order_types = read("SELECT typeof(order_id),typeof(customer_id),typeof(amount_minor),"
                           "typeof(currency),typeof(created_at) FROM warehouse_orders ORDER BY order_id")
        quarantined = read("SELECT row_number,reason_code,reason,raw_record_json "
                           "FROM quarantined_records ORDER BY row_number")
        metadata = read("SELECT run_id,status,source_sha256,duplicate_policy,invalid_policy,"
                        "duplicates_skipped,rows_quarantined FROM pipeline_run")
        columns = read("PRAGMA table_info(warehouse_orders)")
        has_logs = bool(read("SELECT name FROM sqlite_master WHERE type='table' AND name='pipeline_events'"))
        events = read("SELECT event_type,details_json FROM pipeline_events ORDER BY event_id") if has_logs else []
    return {"warehouse_orders": orders, "warehouse_types": order_types,
            "warehouse_schema": [row[1:4] + [row[5]] for row in columns],
            "quarantined_records": [row[:3] + [json.loads(row[3])] for row in quarantined],
            "metadata": metadata, "logs_available": has_logs,
            "events": [[event, json.loads(details)] for event, details in events]}


def _contains_fields(value, expected):
    return isinstance(value, dict) and all(key in value and _encoded(value[key]) == _encoded(item)
                                           for key, item in expected.items())


def _warehouse_matches(actual, expected):
    schema = [[name, "INTEGER" if name == "amount_minor" else "TEXT", 1, int(name == "order_id")]
              for name in COLUMNS]
    return (actual["warehouse_orders"] == expected
            and actual["warehouse_types"] == [["text", "text", "integer", "text", "text"] for _ in expected]
            and actual["warehouse_schema"] == schema
            and len({row[0] for row in actual["warehouse_orders"]}) == len(expected))


def _prepare_case(pipeline, case):
    run_id = pipeline.create(case["source"]).run_id
    database = pipeline.database_path(run_id)
    if case["injection"] == "abort_insert":
        with closing(sqlite3.connect(database)) as connection, connection:
            connection.execute("CREATE TRIGGER guard_before_write BEFORE INSERT ON warehouse_orders "
                               "BEGIN SELECT RAISE(ABORT, 'write blocked'); END")
    pipeline.run(run_id)
    if case["injection"] == "drop_events":
        with closing(sqlite3.connect(database)) as connection, connection:
            connection.execute("DROP TABLE pipeline_events")
    actual = _database(database)
    source = pipeline.data_dir / run_id / "source.json"
    expected = case["expected"]
    metadata = [[run_id, expected["initial_status"], _sha(source.read_bytes()), "reject", "reject", 0, 0]]
    terminal = actual["events"][-1] if actual["events"] else None
    verified = (json.loads(source.read_bytes()) == case["source"] and actual["metadata"] == metadata
                and _warehouse_matches(actual, expected["initial_orders"]) and not actual["quarantined_records"]
                and actual["logs_available"] is expected["logs_available"]
                and (expected["diagnostic"] is None or (terminal is not None and terminal[0] == "load_failed"
                     and _contains_fields(terminal[1], expected["diagnostic"]))))
    if not verified:
        raise ValueError("frozen setup does not match independently observed state")
    return run_id


class RecordingGateway(EvidenceGateway):
    """Observe actual calls and envelopes without changing evidence or deadlines."""

    def __init__(self, root):
        super().__init__(root, timeout_seconds=2, max_response_bytes=16_384)
        self.calls = []
        self.envelopes = []

    def call(self, tool_name, arguments, *, timeout_seconds=None):
        record = {"tool": tool_name, "arguments": dict(arguments), "timeout_seconds": timeout_seconds,
                  "evidence_id": None}
        self.calls.append(record)
        response = super().call(tool_name, arguments, timeout_seconds=timeout_seconds)
        record["evidence_id"] = response["evidence_id"]
        self.envelopes.append(deepcopy(response))
        return response


def _failure_kind(error, elapsed, budget):
    # The transport deliberately redacts paths and failures. Preserve only known categories.
    if (isinstance(error, TimeoutError) or isinstance(error.__context__, TimeoutError)
            or str(error) == "local model operation exceeded its time limit" or elapsed >= budget):
        return "timeout"
    if isinstance(error.__context__, ConnectionRefusedError):
        return "connection_refused"
    return "local_model_error"


class ObservedModelClient(LocalModelClient):
    """Record input scope, timing and failures from the same production client."""

    def __init__(self, config, gateway, run_id):
        super().__init__(config)
        self.gateway, self.run_id = gateway, run_id
        self.attempts = []

    def generate(self, system, prompt, schema, *, timeout_seconds=None):
        supplied = json.loads(prompt)
        allowed_keys = {"run_id", "evidence", "available_tools", "allowed_actions"}
        scoped = (set(supplied) <= allowed_keys and supplied.get("run_id") == self.run_id
                  and all(item in self.gateway.envelopes for item in supplied.get("evidence", [])))
        budget = self.config.timeout_seconds if timeout_seconds is None else timeout_seconds
        before = self.generation_requests
        record = {"prompt_sha256": _sha(prompt.encode()), "system_sha256": _sha(system.encode()),
                  "format_sha256": _sha(_encoded(schema)), "timeout_seconds": budget,
                  "prompt_scope_verified": scoped, "evidence_ids": [e["evidence_id"] for e in supplied["evidence"]],
                  "failure": None, "prompt_tokens": None, "generated_tokens": None}
        self.attempts.append(record)
        started = time.perf_counter()
        try:
            if not scoped:
                raise LocalModelError("evaluation prompt escaped the observed evidence scope")
            response = super().generate(system, prompt, schema, timeout_seconds=timeout_seconds)
            record.update(prompt_tokens=response.prompt_tokens, generated_tokens=response.generated_tokens)
            return response
        except LocalModelError as error:
            record["failure"] = _failure_kind(error, time.perf_counter() - started, budget)
            raise
        finally:
            record["elapsed_seconds"] = time.perf_counter() - started
            record["generation_requests"] = self.generation_requests - before


def reference_checks(result, run_id, observed):
    envelopes = result["evidence"]
    available = {}
    for item in envelopes:
        binding = {key: item[key] for key in ("run_id", "tool", "arguments", "data", "truncated")}
        digest = _sha(_encoded(binding))
        if (item["run_id"] != run_id or item["tool"] not in TOOLS or item not in observed
                or item["content_sha256"] != digest or item["evidence_id"] != "ev_" + digest[:32]
                or item["evidence_id"] in available):
            return False, {}
        available[item["evidence_id"]] = item
    valid = all(finding["evidence_refs"] and len(finding["evidence_refs"]) == len(set(finding["evidence_refs"]))
                and all(ref in available and not available[ref]["truncated"] for ref in finding["evidence_refs"])
                for finding in result["findings"])
    return valid, available


def _fact_matches(item, fact):
    data = item["data"]
    if item["tool"] == "read_logs":
        return any(event["event_type"] == "load_failed" and _contains_fields(event["details"], fact)
                   for event in data["events"])
    if item["tool"] == "inspect_validation":
        return _contains_fields(data["failure"], fact)
    if item["tool"] == "inspect_schema" and fact["code"] == "schema_mismatch":
        return (data["source_columns"] == fact["observed_schema"]
                and data["required_columns"] == fact["required_schema"])
    return False


def scripted_action(result, references_valid):
    if not references_valid or result["status"] != "diagnosed" or len(result["findings"]) != 1:
        return None
    return ACTIONS.get(result["findings"][0]["code"])


def _repair(root, pipeline, run_id, action, expected):
    output = root / "derived"
    service = RepairService(pipeline.data_dir, output, root / "state")
    repair = {"action": action, "status": "plan_refused", "verified_recovery": False,
              "approval_origin": None, "checks": {}, "error": None}
    phase = "plan"
    started = time.perf_counter()
    try:
        planned = service.plan(run_id, action)
        phase = "approve"
        state, identifier = planned["state"], planned["investigation_id"]
        approved = service.approve(identifier, state["plan_sha256"], state["input_fingerprint_sha256"],
                                   origin="scripted_test")
        repair["approval_origin"] = approved["state"]["approval"]["origin"]
        phase = "execute"
        completed = service.execute(identifier)["state"]
        receipt = completed["execution"]
        repair["status"] = completed["status"]
        repair["receipt"] = {key: receipt.get(key) for key in ("status", "committed", "rolled_back",
                             "outcome_uncertain", "post_input_verified", "failure_code")}
        if completed["status"] == "succeeded":
            phase = "verify"
            target = state["plan"]["target_run_id"]
            source = output / target / "source.json"
            database = output / target / "warehouse.sqlite3"
            actual = _database(database)
            repair["actual_database"] = actual
            repair["plan_sha256"] = state["plan_sha256"]
            repair["input_fingerprint_sha256"] = state["input_fingerprint_sha256"]
            checks = repair["checks"]
            checks["distinct_derived_run"] = target != run_id
            checks["expected_action"] = expected is not None and expected["action"] == action
            checks["exact_rows_and_types"] = expected is not None and _warehouse_matches(actual, expected["warehouse_orders"])
            checks["exact_quarantine"] = expected is not None and _encoded(actual["quarantined_records"]) == _encoded(expected["quarantined_records"])
            checks["exact_metadata"] = expected is not None and actual["metadata"] == [[
                target, "succeeded", _sha(source.read_bytes()), expected["duplicate_policy"], expected["invalid_policy"],
                expected["duplicates_skipped"], len(expected["quarantined_records"])]]
            before_replay = _snapshot(output)
            SandboxPipeline(output).run(target)
            checks["clean_replay_unchanged"] = _snapshot(output) == before_replay
            try:
                service.execute(identifier)
            except RepairError:
                checks["duplicate_execution_refused"] = _snapshot(output) == before_replay
            else:
                checks["duplicate_execution_refused"] = False
            checks["exact_approval_and_commit"] = (repair["approval_origin"] == "scripted_test"
                and receipt["committed"] is True and receipt["post_input_verified"] is True)
            repair["verified_recovery"] = all(checks.values())
    except Exception as error:
        repair["error"] = {"phase": phase, "type": type(error).__name__}
        repair["status"] = f"{phase}_refused" if isinstance(error, RepairError) else f"{phase}_error"
    repair["elapsed_seconds"] = time.perf_counter() - started
    return repair


def _trial(case, run_id, pipeline, trial_root, method, config, agent_timeout, original_files):
    gateway = RecordingGateway(pipeline.data_dir)
    client = ObservedModelClient(config, gateway, run_id) if method != "checklist" else None
    expected = case["expected"]
    trial = {"case_id": case["case_id"], "split": case["split"], "method": method, "run_id": run_id,
             "expected": expected, "result": None, "error": None, "repair": None, "false_intervention": False,
             "literal_outcome_match": False, "diagnosis_match": False, "diagnostic_fact_referenced": None,
             "reference_integrity_verified": None}
    started = time.perf_counter()
    try:
        if method == "checklist":
            result = ChecklistBaseline(gateway).diagnose(run_id)
        elif method == "single_call":
            result = SingleCallBaseline(gateway, client).diagnose(run_id)
        else:
            result = BoundedInvestigator(gateway, client, max_steps=4, max_tool_calls=4,
                                         timeout_seconds=agent_timeout).investigate(run_id)
        trial["result"] = result
    except Exception as error:
        trial["error"] = {"phase": "diagnosis", "type": type(error).__name__}
    trial["diagnosis_elapsed_seconds"] = time.perf_counter() - started
    attempts = [] if client is None else client.attempts
    trial["model_attempts"] = attempts
    trial["generation_requests"] = 0 if client is None else client.generation_requests
    trial["tool_calls"] = gateway.calls
    trial["prompt_scope_verified"] = all(item["prompt_scope_verified"] for item in attempts)
    trial["evidence_excludes_labels"] = all(case["case_id"] not in json.dumps(item) for item in gateway.envelopes)
    trial["source_files_unchanged_after_diagnosis"] = _snapshot(pipeline.data_dir) == original_files
    if trial["result"] is not None:
        try:
            codes = sorted(finding["code"] for finding in result["findings"])
            literal = result["status"] == expected["status"] and codes == expected["codes"]
            trial["literal_outcome_match"] = literal
            valid, evidence = reference_checks(result, run_id, gateway.envelopes)
            trial["reference_integrity_verified"] = valid
            fact_match = None
            if expected["status"] == "diagnosed":
                fact_match = (literal and valid and any(_fact_matches(evidence[ref], expected["diagnostic"])
                    for finding in result["findings"] for ref in finding["evidence_refs"]))
            trial["diagnostic_fact_referenced"] = fact_match
            technical_failure = (any(item["failure"] is not None for item in attempts)
                                 or bool(set(result["limitations"]) & FAILED_LIMITATIONS))
            trial["diagnosis_match"] = (literal and valid and not technical_failure and fact_match is not False)
            if (trial["source_files_unchanged_after_diagnosis"] and trial["prompt_scope_verified"]
                    and trial["evidence_excludes_labels"]):
                action = scripted_action(result, valid)
                if action:
                    expected_repair = expected["repair"]
                    trial["false_intervention"] = expected_repair is None or expected_repair["action"] != action
                    trial["repair"] = _repair(trial_root, pipeline, run_id, action, expected_repair)
        except Exception as error:
            trial["error"] = {"phase": "score", "type": type(error).__name__}
    trial["source_files_unchanged_after_trial"] = _snapshot(pipeline.data_dir) == original_files
    return trial


def _summary(trials):
    statuses = Counter(item["result"]["status"] if item["result"] else "error" for item in trials)
    failures = Counter(attempt["failure"] for item in trials for attempt in item.get("model_attempts", []) if attempt["failure"])
    limitations = Counter(code for item in trials if item["result"] for code in set(item["result"]["limitations"]))
    times = [item["diagnosis_elapsed_seconds"] for item in trials if item.get("diagnosis_elapsed_seconds") is not None]
    token_records = [attempt for item in trials for attempt in item.get("model_attempts", []) if attempt["prompt_tokens"] is not None]
    return {"trials": len(trials), "status_counts": dict(statuses), "model_attempt_failures": dict(failures),
            "limitation_counts": dict(limitations), "errors": sum(item["error"] is not None for item in trials),
            "literal_outcome_matches": sum(item["literal_outcome_match"] for item in trials),
            "diagnosis_matches": sum(item["diagnosis_match"] for item in trials),
            "referenced_diagnostic_facts": sum(item["diagnostic_fact_referenced"] is True for item in trials),
            "recoverable_cases": sum(item["expected"]["repair"] is not None for item in trials),
            "repair_policy_interventions": sum(item["repair"] is not None for item in trials),
            "verified_recoveries": sum(bool(item["repair"] and item["repair"]["verified_recovery"]) for item in trials),
            "false_interventions": sum(item["false_intervention"] for item in trials),
            "generation_requests": sum(item.get("generation_requests", 0) for item in trials),
            "tool_calls": sum(len(item.get("tool_calls", [])) for item in trials),
            "recorded_token_responses": len(token_records),
            "prompt_tokens_on_recorded_responses": sum(item["prompt_tokens"] for item in token_records),
            "generated_tokens_on_recorded_responses": sum(item["generated_tokens"] for item in token_records),
            "median_diagnosis_seconds": median(times) if times else None}


def build_report(config, agent_timeout, suite="all", *, progress=None):
    manifest = load_manifest()
    cases = [case for case in manifest["cases"] if suite == "all" or case["split"] == suite]
    hashes = {name: _sha((ROOT / name).read_bytes()) for name in BOUND_FILES}
    started = _now()
    preflight_started = time.perf_counter()
    try:
        health = LocalModelClient(config).health()
    except LocalModelError as error:
        health = {"failure": _failure_kind(error, time.perf_counter() - preflight_started, config.timeout_seconds)}
    trials = []
    with tempfile.TemporaryDirectory(prefix="pipesleuth-match-") as temporary:
        root = Path(temporary).resolve()
        pipeline = SandboxPipeline(root / "inputs")
        prepared = []
        for case in cases:
            try:
                prepared.append((case, _prepare_case(pipeline, case), None))
            except Exception as error:
                prepared.append((case, None, {"phase": "setup", "type": type(error).__name__}))
        original_files = _snapshot(pipeline.data_dir)
        for index, (case, run_id, error) in enumerate(prepared):
            # Rotate order deterministically so the same method does not always go first.
            rotation = index % len(METHODS)
            for method in METHODS[rotation:] + METHODS[:rotation]:
                if error is None:
                    trial = _trial(case, run_id, pipeline, root / "trials" / f"{index}-{method}",
                                   method, config, agent_timeout, original_files)
                else:
                    trial = {"case_id": case["case_id"], "split": case["split"], "method": method,
                             "run_id": None, "expected": case["expected"], "result": None, "error": error,
                             "diagnostic_fact_referenced": None, "literal_outcome_match": False,
                             "diagnosis_match": False, "repair": None, "false_intervention": False}
                trials.append(trial)
                if progress is not None:
                    progress(len(trials), len(cases) * len(METHODS), method, trial)
        unchanged = _snapshot(pipeline.data_dir) == original_files
    hashes_unchanged = all(_sha((ROOT / name).read_bytes()) == digest for name, digest in hashes.items())
    splits = list(dict.fromkeys(case["split"] for case in cases))
    summaries = {split: {method: _summary([item for item in trials if item["split"] == split and item["method"] == method])
                          for method in METHODS} for split in splits}
    audit_ok = (unchanged and hashes_unchanged and len(trials) == len(cases) * len(METHODS)
                and all(item["error"] is None and item.get("source_files_unchanged_after_trial") is True
                        and item.get("source_files_unchanged_after_diagnosis") is True
                        and item.get("evidence_excludes_labels") is True and item.get("prompt_scope_verified") is True
                        and item.get("reference_integrity_verified") is True for item in trials))
    return {"schema_version": 1, "started_at_utc": started, "finished_at_utc": _now(), "suite": suite,
            "manifest": {"path": MANIFEST, "sha256": MANIFEST_SHA256, "frozen_at_utc": manifest["frozen_at_utc"]},
            "config": {"python_version": platform.python_version(), "system": platform.system(), "machine": platform.machine(),
                       "model": config.model, "expected_model_digest": config.expected_digest, "model_health": health,
                       "model_timeout_seconds": config.timeout_seconds, "agent_timeout_seconds": agent_timeout,
                       "model_context_tokens": config.num_ctx, "max_output_tokens": config.max_output_tokens,
                       "model_temperature": 0, "model_thinking": False, "agent_max_steps": 4, "agent_max_tool_calls": 4,
                       "gateway_timeout_seconds": 2, "gateway_max_response_bytes": 16_384, "repeats": 1,
                       "approval_origin": "scripted_test", "scripted_actions_by_finding": ACTIONS, "source_sha256": hashes},
            "planned_trials": len(cases) * len(METHODS), "observed_trials": len(trials), "trials": trials,
            "summaries": summaries, "original_input_files_unchanged": unchanged,
            "implementation_files_unchanged": hashes_unchanged, "audit_checks_passed": audit_ok,
            "semantic_support_evaluated": False, "human_review_performed": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:11436")
    parser.add_argument("--model-digest", required=True)
    parser.add_argument("--model-timeout", type=float, default=25)
    parser.add_argument("--agent-timeout", type=float, default=45)
    parser.add_argument("--suite", choices=("all", "development", "holdout"), default="all")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    if not 0 < args.agent_timeout <= 120:
        parser.error("agent timeout must be positive and at most 120 seconds")
    try:
        config = LocalModelConfig(endpoint=args.endpoint, expected_digest=args.model_digest,
                                  timeout_seconds=args.model_timeout)
    except LocalModelError as error:
        parser.error(str(error))
    if args.report.exists():
        parser.error("report already exists; choose a new path to preserve prior observations")

    def progress(index, total, method, trial):
        status = trial["result"]["status"] if trial["result"] else "error"
        print(f"Trial {index}/{total} {method}: {status}", file=sys.stderr, flush=True)

    report = build_report(config, args.agent_timeout, args.suite, progress=progress)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    with args.report.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
        stream.write("\n")
    print(json.dumps({"audit_checks_passed": report["audit_checks_passed"],
                      "observed_trials": report["observed_trials"], "summaries": report["summaries"]}, sort_keys=True))
    return 0 if report["audit_checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
