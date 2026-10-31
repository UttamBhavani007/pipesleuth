"""Trusted transactional creation of one approved derived pipeline run.

Approval and original-input protection belong to the calling repair service.
No action, path, SQL, callback or approval is selected from a model response.
An initialized target is retained on failure for audit; it is never overwritten.
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
import time

from pipesleuth.domain import ORDER_COLUMNS
from pipesleuth.pipeline import SandboxPipeline, _now
from pipesleuth.repair_verifier import verify_repair


_UUID = re.compile(r"[0-9a-f]{32}")


class RepairExecutionError(ValueError):
    """An observed execution failure with an explicit, safe commit/rollback receipt."""

    def __init__(self, receipt):
        super().__init__("derived repair did not complete verified execution")
        self.receipt = json.loads(json.dumps(receipt, allow_nan=False))


class _ExecutionFailure(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _remaining(deadline):
    value = deadline - time.monotonic()
    if value <= 0:
        raise _ExecutionFailure("elapsed_budget_exceeded")
    return value


def _connection(pipeline, run_id, mode, deadline):
    path = pipeline.database_path(run_id)
    connection = sqlite3.connect(path.as_uri() + "?mode=" + mode, uri=True,
                                 timeout=min(0.1, _remaining(deadline)))
    try:
        connection.row_factory = sqlite3.Row
        connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 100)
        connection.execute("PRAGMA trusted_schema = OFF")
        return connection
    except Exception:
        connection.close()
        raise


def execute_repair(output_dir, target_run_id, prepared, *, timeout_seconds=5.0, pre_commit=None):
    """Create a fresh trusted UUID target, verify before commit and again afterward.

    ``pre_commit`` is a trusted Python-only stale-input guard supplied by the
    repair service. It is never read from a plan, a model response, or JSON.
    """
    valid_id = isinstance(target_run_id, str) and _UUID.fullmatch(target_run_id) is not None
    receipt = {"target_run_id": target_run_id if valid_id else None, "status": "failed",
               "committed": False, "rolled_back": False, "verification": None,
               "failure_code": None, "outcome_uncertain": False}
    connection = None
    phase = "validation"
    try:
        if (not valid_id or type(timeout_seconds) not in (int, float)
                or not 0 < timeout_seconds <= 5 or not math.isfinite(timeout_seconds)
                or pre_commit is not None and not callable(pre_commit)
                or not isinstance(prepared, dict) or set(prepared) !=
                {"source", "duplicate_policy", "invalid_policy", "effects", "warnings"}):
            raise _ExecutionFailure("invalid_execution_arguments")
        effects = prepared["effects"]
        if (not isinstance(effects, dict) or set(effects) !=
                {"rows_loaded", "rows_quarantined", "duplicates_skipped"}
                or any(type(value) is not int or not 0 <= value <= 10_000 for value in effects.values())
                or not isinstance(prepared["warnings"], list) or len(prepared["warnings"]) > 16
                or any(not isinstance(value, str) or len(value.encode("utf-8")) > 1024
                       for value in prepared["warnings"])):
            raise _ExecutionFailure("invalid_prepared_effects")
        deadline = time.monotonic() + float(timeout_seconds)
        pipeline = SandboxPipeline(output_dir)
        phase = "target_initialization"
        _remaining(deadline)
        created = pipeline.create(prepared["source"], duplicate_policy=prepared["duplicate_policy"],
                                  invalid_policy=prepared["invalid_policy"], run_id=target_run_id)
        _remaining(deadline)
        source_path = pipeline._source_path(target_run_id)
        phase = "load"
        connection = _connection(pipeline, target_run_id, "rw", deadline)
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("UPDATE pipeline_run SET status='running',started_at=? WHERE run_id=?",
                           (_now(), target_run_id))
        pipeline._record_event(connection, "load_started", duplicate_policy=prepared["duplicate_policy"],
                               invalid_policy=prepared["invalid_policy"])
        orders, skipped, quarantined = pipeline._read_orders(
            source_path, created.source_sha256, prepared["duplicate_policy"], prepared["invalid_policy"])
        _remaining(deadline)
        connection.executemany(
            "INSERT INTO warehouse_orders(order_id,customer_id,amount_minor,currency,created_at) VALUES(?,?,?,?,?)",
            (tuple(order.to_record()[column] for column in ORDER_COLUMNS) for order in orders))
        connection.executemany(
            "INSERT INTO quarantined_records(row_number,reason_code,reason,raw_record_json) VALUES(?,?,?,?)",
            quarantined)
        connection.execute(
            "UPDATE pipeline_run SET status='succeeded',finished_at=?,duplicates_skipped=?,rows_quarantined=? WHERE run_id=?",
            (_now(), skipped, len(quarantined), target_run_id))
        pipeline._record_event(connection, "load_succeeded", rows_loaded=len(orders),
                               duplicates_skipped=skipped, rows_quarantined=len(quarantined))
        phase = "verification"
        verified = verify_repair(connection, prepared["source"], run_id=target_run_id,
            duplicate_policy=prepared["duplicate_policy"], invalid_policy=prepared["invalid_policy"],
            source_sha256=created.source_sha256)
        if any(verified[key] != value for key, value in effects.items()):
            raise _ExecutionFailure("approved_effects_mismatch")
        _remaining(deadline)
        if pre_commit is not None:
            phase = "pre_commit"
            if pre_commit() is False:
                raise _ExecutionFailure("pre_commit_rejected")
        _remaining(deadline)
        phase = "commit"
        connection.commit()
        receipt["committed"] = True
        connection.close()
        connection = None
        phase = "post_commit_verification"
        # Recheck the actual target source bytes as well as its committed data.
        pipeline._read_orders(source_path, created.source_sha256,
                              prepared["duplicate_policy"], prepared["invalid_policy"])
        _remaining(deadline)
        connection = _connection(pipeline, target_run_id, "ro", deadline)
        connection.execute("PRAGMA query_only = ON")
        connection.execute("BEGIN")
        committed = verify_repair(connection, prepared["source"], run_id=target_run_id,
            duplicate_policy=prepared["duplicate_policy"], invalid_policy=prepared["invalid_policy"],
            source_sha256=created.source_sha256)
        _remaining(deadline)
        if committed["content_sha256"] != verified["content_sha256"]:
            raise _ExecutionFailure("committed_content_changed")
        receipt.update(status="verified", verification=committed)
        return receipt
    except Exception as error:
        if connection is not None and not receipt["committed"] and connection.in_transaction:
            try:
                connection.rollback()
                receipt["rolled_back"] = True
            except sqlite3.Error:
                receipt["rolled_back"] = False
                receipt["committed"] = None
                receipt["outcome_uncertain"] = True
        elif phase == "commit" and receipt["committed"] is not True:
            # A commit may have happened even though its acknowledgement was
            # lost. Do not turn that observed uncertainty into a known failure.
            receipt["committed"] = None
            receipt["outcome_uncertain"] = True
        receipt["failure_code"] = error.code if isinstance(error, _ExecutionFailure) else phase + "_failed"
        raise RepairExecutionError(receipt) from None
    finally:
        if connection is not None:
            connection.close()
