"""Audit trusted repairs on literal synthetic inputs without model inference.

Approvals are scripted software fixtures, not a claim of human review. Exact
expected rows are driver-only assertions and never investigator evidence.
"""

from __future__ import annotations

import argparse
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import sqlite3
import tempfile

from pipesleuth.pipeline import SandboxPipeline
from pipesleuth.repairs import RepairError, RepairService


ROOT = Path(__file__).resolve().parents[1]
SOURCES = ("src/pipesleuth/repairs.py", "src/pipesleuth/repair_executor.py",
           "src/pipesleuth/repair_plan.py", "src/pipesleuth/repair_verifier.py",
           "src/pipesleuth/checkpoints.py",
           "src/pipesleuth/input_binding.py", "src/pipesleuth/evidence.py",
           "src/pipesleuth/pipeline.py",
           "src/pipesleuth/domain.py", "scripts/smoke_repair.py")
ACTIONS = ("rename_amount_column", "skip_identical_duplicates", "quarantine_invalid_rows")
COLUMNS = ["order_id", "customer_id", "amount_minor", "currency", "created_at"]
ROWS = [
    {"order_id": "L-1", "customer_id": "C-A", "amount_minor": 125,
     "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"},
    {"order_id": "L-2", "customer_id": "C-B", "amount_minor": 375,
     "currency": "EUR", "created_at": "2026-01-02T09:00:00Z"},
    {"order_id": "L-3", "customer_id": "C-C", "amount_minor": 0,
     "currency": "USD", "created_at": "2026-01-03T09:00:00Z"},
]
EXPECTED = [[row[column] for column in COLUMNS] for row in ROWS]


def _encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, separators=(",", ":")).encode("utf-8")


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _files(directory):
    return {str(path.relative_to(directory)): {"sha256": _hash(path), "size": path.stat().st_size,
                                              "mtime_ns": path.stat().st_mtime_ns}
            for path in directory.rglob("*") if path.is_file()} if directory.exists() else {}


def _source(action):
    payload = {"schema": COLUMNS[:], "rows": deepcopy(ROWS)}
    if action == "rename_amount_column":
        payload["schema"][2] = "gross_total_minor"
        for row in payload["rows"]:
            row["gross_total_minor"] = row.pop("amount_minor")
    elif action == "skip_identical_duplicates":
        payload["rows"] += [deepcopy(ROWS[0]), deepcopy(ROWS[2])]
    elif action == "quarantine_invalid_rows":
        payload["rows"].append(dict(ROWS[0], order_id="L-X", amount_minor=-7))
    return payload


def _database(path):
    # Independent fixed SELECTs; no verifier/executor success flag is trusted.
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        orders = connection.execute(
            "SELECT order_id, customer_id, amount_minor, currency, created_at "
            "FROM warehouse_orders ORDER BY order_id").fetchall()
        rejected = connection.execute(
            "SELECT row_number, reason_code, reason, raw_record_json "
            "FROM quarantined_records ORDER BY row_number").fetchall()
        run = connection.execute(
            "SELECT run_id,status,duplicate_policy,invalid_policy,duplicates_skipped,rows_quarantined "
            "FROM pipeline_run").fetchall()
    return {"warehouse_orders": [list(row) for row in orders],
            "quarantined_records": [list(row[:3]) + [json.loads(row[3])] for row in rejected],
            "pipeline_run": [list(row) for row in run]}


def _audit(record):
    state, plan = record["state"], record["state"]["plan"]
    return {"repair_id": record["investigation_id"], "revision": record["revision"],
            "status": state["status"], "pending": record["pending"],
            "plan_sha256": state["plan_sha256"],
            "input_fingerprint_sha256": state["input_fingerprint_sha256"],
            "action": plan["action"], "source_run_id": plan["source_run_id"],
            "target_run_id": plan["target_run_id"], "policies": plan["policies"],
            "effects": plan["effects"],
            "approval_origin": state["approval"]["origin"] if state["approval"] else None,
            "execution": ({key: state["execution"].get(key)
                           for key in ("status", "committed", "rolled_back", "outcome_uncertain", "post_input_verified", "failure_code")}
                          if state["execution"] else None),
            "state_sha256": hashlib.sha256(_encoded(state)).hexdigest()}


def _refusal(service, identifier, operation, output):
    before = service.show(identifier)
    output_before = _files(output)
    try:
        operation()
    except RepairError:
        refused = True
    else:
        refused = False
    return {"refused": refused, "record_unchanged": service.show(identifier) == before,
            "output_files_unchanged": _files(output) == output_before}


def _case(pipeline, service, action, output):
    observed = {"action": action, "error": None, "checks": {}}
    try:
        created = pipeline.create(_source(action))
        initial = pipeline.run(created.run_id)
        assert initial.status == "failed"
        before_files = _files(pipeline.data_dir)
        before_database = _database(pipeline.database_path(created.run_id))
        planned = service.plan(created.run_id, action)
        identifier, state = planned["investigation_id"], planned["state"]
        observed["plan"] = _audit(planned)
        observed["refusals"] = {
            "unapproved_execution": _refusal(service, identifier, lambda: service.execute(identifier), output),
            "mismatched_plan_hash": _refusal(service, identifier, lambda: service.approve(
                identifier, "0" * 64, state["input_fingerprint_sha256"], origin="scripted_test"), output)}
        approved = service.approve(identifier, state["plan_sha256"], state["input_fingerprint_sha256"],
                                   origin="scripted_test")
        completed = service.execute(identifier)
        actual = _database(output / state["plan"]["target_run_id"] / "warehouse.sqlite3")
        expected_rejected = ([[4, "invalid_row", "amount_minor must be a non-negative integer",
                              dict(ROWS[0], order_id="L-X", amount_minor=-7)]]
                             if action == "quarantine_invalid_rows" else [])
        observed.update({"approval": _audit(approved), "completed": _audit(completed),
                         "actual_database": actual,
                         "expected_driver_only": {"warehouse_orders": EXPECTED,
                                                  "quarantined_records": expected_rejected}})
        policies = state["plan"]["policies"]
        expected_run = [[state["plan"]["target_run_id"], "succeeded",
                         policies["duplicate_policy"], policies["invalid_policy"],
                         2 if action == "skip_identical_duplicates" else 0,
                         1 if action == "quarantine_invalid_rows" else 0]]
        observed["expected_driver_only"]["pipeline_run"] = expected_run
        observed["refusals"]["duplicate_execution"] = _refusal(
            service, identifier, lambda: service.execute(identifier), output)
        checks = observed["checks"]
        checks.update({"completed_successfully": completed["state"]["status"] == "succeeded",
                       "scripted_approval_recorded": approved["state"]["approval"]["origin"] == "scripted_test",
                       "post_input_verified": completed["state"]["execution"].get("post_input_verified") is True,
                       "derived_uuid_distinct": state["plan"]["target_run_id"] != created.run_id,
                       "exact_warehouse_rows": actual["warehouse_orders"] == EXPECTED,
                       "exact_run_metadata": actual["pipeline_run"] == expected_run,
                       "exact_quarantine_rows": actual["quarantined_records"] == expected_rejected,
                       "original_and_unrelated_files_unchanged": _files(pipeline.data_dir) == before_files,
                       "original_database_exactly_unchanged": _database(pipeline.database_path(created.run_id)) == before_database,
                       "all_refusals_verified": all(all(result.values()) for result in observed["refusals"].values())})
    except Exception as failure:
        observed["error"] = {"type": type(failure).__name__}
    observed["passed"] = observed["error"] is None and bool(observed["checks"]) and all(observed["checks"].values())
    return observed


def _stale_case(pipeline, service, output):
    created = pipeline.create(_source("rename_amount_column"))
    pipeline.run(created.run_id)
    record = service.plan(created.run_id, "rename_amount_column")
    state, identifier = record["state"], record["investigation_id"]
    service.approve(identifier, state["plan_sha256"], state["input_fingerprint_sha256"], origin="scripted_test")
    source = pipeline.data_dir / created.run_id / "source.json"
    source.write_bytes(source.read_bytes() + b"\n")
    after_fixture_mutation = _files(pipeline.data_dir)
    outcome = _refusal(service, identifier, lambda: service.execute(identifier), output)
    return {"fixture_mutation": "Added source whitespace after scripted approval; observed rows unchanged.",
            **outcome, "original_files_unchanged_during_refusal": _files(pipeline.data_dir) == after_fixture_mutation,
            "passed": all(outcome.values()) and _files(pipeline.data_dir) == after_fixture_mutation}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "reports/local/repair-smoke.json")
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("output already exists; choose a new path to preserve previous outcomes")
    hashes = {name: _hash(ROOT / name) for name in SOURCES}
    report = {"generated_at": None, "scope": "Deterministic trusted repair integration; no model diagnosis or human approval claim.",
              "config": {"python_version": platform.python_version(), "machine": platform.machine(),
                         "approval_origin": "scripted_test", "source_sha256": hashes},
              "model_inference_performed": False, "human_review_claimed": False,
              "cases": [], "stale_input_refusal": None, "errors": []}
    try:
        with tempfile.TemporaryDirectory(prefix="pipesleuth-repair-smoke-") as temporary:
            root = Path(temporary).resolve()
            pipeline = SandboxPipeline(root / "incidents")
            output = root / "derived"
            service = RepairService(pipeline.data_dir, output, root / "state")
            unrelated = pipeline.create({"schema": COLUMNS[:], "rows": deepcopy(ROWS)})
            pipeline.run(unrelated.run_id)
            for action in ACTIONS:
                report["cases"].append(_case(pipeline, service, action, output))
            report["stale_input_refusal"] = _stale_case(pipeline, service, output)
    except Exception as failure:
        report["errors"].append({"phase": "repair_smoke", "type": type(failure).__name__})
    unchanged = all(_hash(ROOT / name) == digest for name, digest in hashes.items())
    report["source_hashes_unchanged_during_run"] = unchanged
    report["generated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    report["summary"] = {"planned_repair_cases": 3, "recorded_repair_cases": len(report["cases"]),
                         "verified_derived_runs": sum(case["passed"] for case in report["cases"]),
                         "verified_refusals": sum(all(check.values()) for case in report["cases"]
                                                  for check in case.get("refusals", {}).values())
                                               + int(bool(report["stale_input_refusal"] and report["stale_input_refusal"]["passed"])),
                         "case_errors": sum(case["error"] is not None for case in report["cases"]),
                         "driver_errors": len(report["errors"]), "model_generation_requests": 0,
                         "semantic_quality_evaluated": False}
    report["repair_checks_passed"] = (len(report["cases"]) == 3 and all(case["passed"] for case in report["cases"])
                                      and bool(report["stale_input_refusal"] and report["stale_input_refusal"]["passed"])
                                      and unchanged and not report["errors"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n")
    print(json.dumps(report["summary"], sort_keys=True))
    return 0 if report["repair_checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
