"""Run ten source-checked synthetic DEVELOPMENT cases against the checklist.

This evaluator owns its labels, setup mutations, and independent SQL checks.
Only a neutral run identifier crosses into the baseline. It is a small local
probe, not a holdout evaluation or evidence of general diagnostic accuracy.
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
import time

from pipesleuth.checklist import ChecklistBaseline
from pipesleuth.evidence import EvidenceGateway
from pipesleuth.pipeline import SandboxPipeline


REPOSITORY = Path(__file__).resolve().parents[1]
COLUMNS = ["order_id", "customer_id", "amount_minor", "currency", "created_at"]
CASES = (
    ("healthy", "no_incident", (), (), 3, 0),
    ("identical_retry_deduplicated", "no_incident", (), ("identical_retries_skipped",), 3, 0),
    ("schema_failure", "diagnosed", ("schema_mismatch",), (), 0, 0),
    ("duplicate_failure", "diagnosed", ("duplicate_key",), (), 0, 0),
    ("invalid_row_rejected", "diagnosed", ("invalid_row",), (), 0, 0),
    ("invalid_row_quarantined", "diagnosed", ("invalid_row",), (), 3, 1),
    ("storage_trigger_failure", "unknown", (), (), 0, 0),
    ("legacy_logs_unavailable", "unknown", (), (), 3, 0),
    ("quarantine_evidence_truncated", "unknown", (), (), 3, 21),
    ("warehouse_count_conflict", "unknown", (), (), 2, 0),
)


class RecordingGateway(EvidenceGateway):
    """Count actual tool calls without changing their inputs or responses."""

    def __init__(self, data_dir: Path):
        super().__init__(data_dir, timeout_seconds=2.0, max_response_bytes=16_384)
        self.calls: list[str] = []

    def call(self, tool_name, arguments):
        self.calls.append(tool_name)
        return super().call(tool_name, arguments)


def _source() -> dict:
    # Literal records and expected counts were checked against the order contract.
    return {"schema": list(COLUMNS), "rows": [
        {"order_id": "O-1", "customer_id": "C-1", "amount_minor": 100,
         "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"},
        {"order_id": "O-2", "customer_id": "C-2", "amount_minor": 200,
         "currency": "EUR", "created_at": "2026-01-02T09:00:00Z"},
        {"order_id": "O-3", "customer_id": "C-3", "amount_minor": 300,
         "currency": "USD", "created_at": "2026-01-03T09:00:00Z"},
    ]}


def _create_case(pipeline: SandboxPipeline, case_id: str) -> str:
    """Trusted evaluator setup; no case label is persisted in the run."""

    source = _source()
    duplicate_policy, invalid_policy = "reject", "reject"
    if case_id == "identical_retry_deduplicated":
        source["rows"].append(deepcopy(source["rows"][0]))
        duplicate_policy = "skip_identical"
    elif case_id == "schema_failure":
        source["schema"][2] = "gross_total_minor"
        for row in source["rows"]:
            row["gross_total_minor"] = row.pop("amount_minor")
    elif case_id == "duplicate_failure":
        source["rows"].append(deepcopy(source["rows"][0]))
    elif case_id in {"invalid_row_rejected", "invalid_row_quarantined", "quarantine_evidence_truncated"}:
        count = 21 if case_id == "quarantine_evidence_truncated" else 1
        for index in range(count):
            source["rows"].append(dict(source["rows"][0], order_id=f"R-{index + 1}", amount_minor=True))
        if case_id != "invalid_row_rejected":
            invalid_policy = "quarantine"
    created = pipeline.create(source, duplicate_policy=duplicate_policy, invalid_policy=invalid_policy)
    database = pipeline.database_path(created.run_id)
    if case_id == "storage_trigger_failure":
        with closing(sqlite3.connect(database)) as connection, connection:
            connection.execute(
                "CREATE TRIGGER abort_load BEFORE INSERT ON warehouse_orders "
                "BEGIN SELECT RAISE(ABORT, 'simulated storage failure'); END"
            )
    pipeline.run(created.run_id)
    if case_id == "legacy_logs_unavailable":
        with closing(sqlite3.connect(database)) as connection, connection:
            connection.execute("DROP TABLE pipeline_events")
    elif case_id == "warehouse_count_conflict":
        with closing(sqlite3.connect(database)) as connection, connection:
            connection.execute("DELETE FROM warehouse_orders WHERE order_id = ?", ("O-1",))
    return created.run_id


def _snapshot(root: Path) -> dict:
    """Include every run file, including unexpected sidecars, without publishing paths."""

    return {
        str(path.relative_to(root)): (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
        for path in root.rglob("*") if path.is_file()
    }


def _database_counts(database: Path) -> dict[str, int]:
    # Independent fixed SQL outside the gateway and baseline.
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
        return {
            "warehouse_rows": connection.execute("SELECT COUNT(*) FROM warehouse_orders").fetchone()[0],
            "quarantined_rows": connection.execute("SELECT COUNT(*) FROM quarantined_records").fetchone()[0],
        }


def _source_hash(module_name: str) -> str:
    return hashlib.sha256((REPOSITORY / "src" / "pipesleuth" / module_name).read_bytes()).hexdigest()


def _run_case(pipeline: SandboxPipeline, specification: tuple) -> dict:
    case_id, status, codes, notes, accepted, rejected = specification
    expected_counts = {"warehouse_rows": accepted, "quarantined_rows": rejected}
    result = {
        "case_id": case_id,
        "expected": {"status": status, "codes": list(codes), "required_notes": list(notes),
                     "database_counts": expected_counts},
        "observed": None,
        "elapsed_seconds": None,
        "tool_calls": 0,
        "tool_names": [],
        "files_unchanged": False,
        "independent_database_counts": {"before": None, "after": None,
                                        "matches_expected": False, "unchanged": False},
        "expectation_match": False,
        "all_checks_passed": False,
        "error": None,
    }
    before = None
    database = None
    gateway = None
    started = None
    phase = "setup"
    try:
        run_id = _create_case(pipeline, case_id)
        database = pipeline.database_path(run_id)
        result["run_id"] = run_id
        phase = "independent_precheck"
        initial_counts = _database_counts(database)
        result["independent_database_counts"]["before"] = initial_counts
        result["independent_database_counts"]["matches_expected"] = initial_counts == expected_counts
        before = _snapshot(pipeline.data_dir)
        gateway = RecordingGateway(pipeline.data_dir)
        phase = "diagnose"
        started = time.perf_counter()
        diagnosis = ChecklistBaseline(gateway).diagnose(run_id)
        result["elapsed_seconds"] = time.perf_counter() - started
        phase = "result_validation"
        observed_codes = sorted({finding["code"] for finding in diagnosis["findings"]})
        observed_notes = diagnosis["notes"]
        result["observed"] = {"status": diagnosis["status"], "codes": observed_codes,
                              "notes": observed_notes, "diagnosis": diagnosis}
        result["expectation_match"] = (
            diagnosis["status"] == status and observed_codes == sorted(codes)
            and set(notes).issubset(observed_notes)
        )
    except Exception as error:
        # Keep failed cases in the denominator. Do not publish exception text,
        # tracebacks, source payloads or local filesystem paths.
        result["error"] = {"phase": phase, "type": type(error).__name__}
    finally:
        if started is not None and result["elapsed_seconds"] is None:
            result["elapsed_seconds"] = time.perf_counter() - started
        if gateway is not None:
            result["tool_calls"] = len(gateway.calls)
            result["tool_names"] = list(gateway.calls)
        if before is not None:
            try:
                result["files_unchanged"] = _snapshot(pipeline.data_dir) == before
            except OSError as error:
                result["error"] = result["error"] or {"phase": "file_postcheck", "type": type(error).__name__}
        if database is not None:
            try:
                counts = _database_counts(database)
                result["independent_database_counts"]["after"] = counts
                result["independent_database_counts"]["unchanged"] = (
                    counts == result["independent_database_counts"]["before"]
                )
            except sqlite3.Error as error:
                result["error"] = result["error"] or {"phase": "database_postcheck", "type": type(error).__name__}
    counts = result["independent_database_counts"]
    result["all_checks_passed"] = (
        result["error"] is None and result["expectation_match"] and result["files_unchanged"]
        and counts["matches_expected"] and counts["unchanged"]
    )
    return result


def build_report() -> dict:
    """Execute the fixed development probe; temporary databases are removed afterward."""

    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="pipesleuth-checklist-development-") as temporary:
        pipeline = SandboxPipeline(Path(temporary).resolve() / "runs")
        cases = [_run_case(pipeline, specification) for specification in CASES]
    status_counts = {status: sum(case["observed"] is not None and case["observed"]["status"] == status
                                 for case in cases)
                     for status in ("diagnosed", "no_incident", "unknown")}
    return {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "scope": "Small synthetic development probe; not a held-out evaluation or an accuracy estimate.",
        "config": {
            "case_authoring": "AI-authored literal inputs and expectations, checked against source; no human review claimed.",
            "case_count": len(CASES),
            "python_version": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "baseline_source_sha256": _source_hash("checklist.py"),
            "evidence_source_sha256": _source_hash("evidence.py"),
            "probe_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "gateway_timeout_seconds": 2.0,
            "gateway_max_response_bytes": 16_384,
            "model_calls": 0,
            "repairs_executed": 0,
        },
        "summary": {
            "total_cases": len(cases),
            "completed_cases": sum(case["error"] is None for case in cases),
            "exceptions": sum(case["error"] is not None for case in cases),
            "expectation_matches": sum(case["expectation_match"] for case in cases),
            "files_unchanged": sum(case["files_unchanged"] for case in cases),
            "database_counts_match": sum(case["independent_database_counts"]["matches_expected"] for case in cases),
            "database_counts_unchanged": sum(case["independent_database_counts"]["unchanged"] for case in cases),
            "all_checks_passed": sum(case["all_checks_passed"] for case in cases),
            "status_counts": status_counts,
            "total_tool_calls": sum(case["tool_calls"] for case in cases),
            "elapsed_seconds": time.perf_counter() - started,
            "denominator": len(cases),
        },
        "cases": cases,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Write the actual development report as JSON")
    arguments = parser.parse_args()
    report = build_report()
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False,
                                          allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps(report["summary"], sort_keys=True))
    return 0 if report["summary"]["all_checks_passed"] == report["summary"]["total_cases"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
