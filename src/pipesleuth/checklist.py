"""A deterministic troubleshooting baseline using only bounded evidence tools.

The four calls use separate snapshots. Agreement is checked explicitly; this
baseline neither claims an atomic bundle nor executes repairs or model calls.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
import math
import re
import time

from pipesleuth.domain import ORDER_COLUMNS, SUPPORTED_CURRENCIES, Order, OrderValidationError
from pipesleuth.evidence import EvidenceError, MAX_RESPONSE_BYTES, MAX_TEXT_BYTES


MAX_CHECKLIST_BYTES = 80 * 1024
_UUID = re.compile(r"[0-9a-f]{32}")
_SHA = re.compile(r"[0-9a-f]{64}")
_CALLS = (("read_run", {}), ("read_logs", {"limit": 20}),
          ("inspect_schema", {}), ("inspect_validation", {"limit": 20}))
_FAILURE_FIELDS = {
    "schema_mismatch": {"code", "observed_schema", "required_schema", "missing_columns", "extra_columns"},
    "duplicate_key": {"code", "row_number", "order_id"},
    "conflicting_duplicate": {"code", "row_number", "order_id"},
    "invalid_row": {"code", "row_number", "reason"},
    "integer_overflow": {"code", "row_number", "reason"},
    "source_changed": {"code", "expected_sha256", "observed_sha256"},
    "load_error": {"code"},
}
_SUMMARIES = {
    "schema_mismatch": "The reported schema failure matches the observed source columns.",
    "duplicate_key": "The load reported a repeated order identifier under the reject policy.",
    "conflicting_duplicate": "The load reported conflicting valid records for one order identifier.",
    "invalid_row": "Records failed the order contract.",
    "integer_overflow": "Order amounts exceeded the SQLite integer range.",
}


class ChecklistError(ValueError):
    """An invalid checklist request."""


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _require(condition):
    if not condition:
        raise ValueError("invalid evidence")


def _count(value):
    return type(value) is int and 0 <= value <= 10_000


def _utc(value):
    if not isinstance(value, str) or len(value) > 64:
        return False
    try:
        return datetime.fromisoformat(value).utcoffset() == timedelta(0)
    except ValueError:
        return False


def _bounded(value, depth=0):
    _require(depth <= 9)
    if isinstance(value, dict):
        _require(len(value) <= 32)
        for key, child in value.items():
            _require(isinstance(key, str) and len(key.encode()) <= 64)
            _bounded(child, depth + 1)
    elif isinstance(value, list):
        _require(len(value) <= 20)
        for child in value:
            _bounded(child, depth + 1)
    elif isinstance(value, str):
        _require(len(value.encode()) <= MAX_TEXT_BYTES)
    elif type(value) is float:
        _require(math.isfinite(value))
    else:
        _require(value is None or type(value) in (int, bool))


def _shape(tool, data, truncated):
    """Validate closed tool shapes, including explicit gateway placeholders."""

    def check(value, predicate):
        placeholder = truncated and isinstance(value, str) and "[truncated]" in value
        _require(placeholder or predicate(value))

    def strings(value):
        return isinstance(value, list) and all(isinstance(item, str) for item in value)

    def mapping(value, keys):
        _require(isinstance(value, dict) and set(value) == set(keys))

    def diagnostic(value):
        if value is None:
            return
        if truncated and isinstance(value, str) and "[truncated]" in value:
            return
        _require(isinstance(value, dict))
        if truncated and value == {"details_truncated": True}:
            return
        code = value.get("code")
        _require(isinstance(code, str) and code in _FAILURE_FIELDS)
        _require(set(value) == _FAILURE_FIELDS[code])
        for key, item in value.items():
            if key in {"observed_schema", "required_schema", "missing_columns", "extra_columns"}:
                check(item, strings)
            elif key == "row_number":
                check(item, lambda number: _count(number) and number > 0)
            elif key in {"expected_sha256", "observed_sha256"}:
                check(item, lambda text: isinstance(text, str) and _SHA.fullmatch(text) is not None)
            else:
                check(item, lambda text: isinstance(text, str))

    _require(isinstance(data, dict) and data.get("untrusted_data") is True)
    if tool == "read_run":
        mapping(data, {"untrusted_data", "status", "source_sha256", "created_at", "started_at", "finished_at",
                       "duplicate_policy", "invalid_policy", "duplicates_skipped", "rows_loaded", "totals_minor",
                       "rows_quarantined", "quarantine_available", "logs_available"})
        check(data["status"], lambda item: item in {"new", "running", "succeeded", "failed"})
        check(data["source_sha256"], lambda item: isinstance(item, str) and _SHA.fullmatch(item) is not None)
        check(data["created_at"], _utc)
        for key in ("started_at", "finished_at"):
            check(data[key], lambda item: item is None or _utc(item))
        check(data["duplicate_policy"], lambda item: item in {"reject", "skip_identical"})
        check(data["invalid_policy"], lambda item: item in {"reject", "quarantine"})
        for key in ("duplicates_skipped", "rows_loaded", "rows_quarantined"):
            check(data[key], _count)
        _require(type(data["quarantine_available"]) is bool and type(data["logs_available"]) is bool)
        totals = data["totals_minor"]
        _require(isinstance(totals, dict) and set(totals) <= SUPPORTED_CURRENCIES)
        _require(all(type(total) is int and 0 <= total <= 10_000 * (2**63 - 1) for total in totals.values()))
    elif tool == "inspect_schema":
        mapping(data, {"untrusted_data", "required_columns", "source_columns", "warehouse_columns", "warehouse_columns_omitted"})
        check(data["required_columns"], strings)
        check(data["source_columns"], strings)
        check(data["warehouse_columns_omitted"], _count)
        _require(isinstance(data["warehouse_columns"], list))
        for column in data["warehouse_columns"]:
            mapping(column, {"name", "type", "not_null", "primary_key"})
            check(column["name"], lambda item: item in ORDER_COLUMNS)
            check(column["type"], lambda item: isinstance(item, str))
            check(column["not_null"], lambda item: type(item) is bool)
            check(column["primary_key"], lambda item: type(item) is bool)
    elif tool == "read_logs":
        mapping(data, {"untrusted_data", "available", "events_total", "events"})
        _require(type(data["available"]) is bool and isinstance(data["events"], list))
        check(data["events_total"], _count)
        for event in data["events"]:
            mapping(event, {"event_id", "event_type", "created_at", "details"})
            check(event["event_id"], lambda item: type(item) is int and item > 0)
            check(event["created_at"], _utc)
            kind, details = event["event_type"], event["details"]
            _require(isinstance(kind, str) and kind in {"source_received", "load_started", "load_succeeded", "load_failed"})
            if kind == "load_failed":
                diagnostic(details)
                _require(details is not None)
                continue
            if truncated and details == {"details_truncated": True}:
                continue
            if kind == "source_received":
                mapping(details, {"observed_schema", "required_schema", "raw_row_count"})
                check(details["observed_schema"], strings)
                check(details["required_schema"], strings)
                check(details["raw_row_count"], _count)
            elif kind == "load_started":
                mapping(details, {"duplicate_policy", "invalid_policy"})
                check(details["duplicate_policy"], lambda item: item in {"reject", "skip_identical"})
                check(details["invalid_policy"], lambda item: item in {"reject", "quarantine"})
            else:
                mapping(details, {"rows_loaded", "duplicates_skipped", "rows_quarantined"})
                for item in details.values():
                    check(item, _count)
    else:
        mapping(data, {"untrusted_data", "available", "rows_quarantined", "warehouse_available", "rows_loaded", "records", "failure"})
        _require(type(data["available"]) is bool and type(data["warehouse_available"]) is bool)
        check(data["rows_loaded"], _count)
        check(data["rows_quarantined"], _count)
        diagnostic(data["failure"])
        _require(isinstance(data["records"], list))
        for record in data["records"]:
            if truncated and isinstance(record, str) and "[truncated]" in record:
                continue
            mapping(record, {"row_number", "reason_code", "reason", "raw_record", "raw_record_truncated"})
            check(record["row_number"], lambda item: _count(item) and item > 0)
            check(record["reason_code"], lambda item: item in {"invalid_row", "integer_overflow"})
            check(record["reason"], lambda item: isinstance(item, str))
            check(record["raw_record_truncated"], lambda item: type(item) is bool)
            raw = record["raw_record"]
            if truncated and (raw is None or isinstance(raw, str) and "[truncated]" in raw):
                continue
            _require(isinstance(raw, dict) and set(raw) <= set(ORDER_COLUMNS))
            _require(all(item is None or type(item) in (str, int, bool)
                         or type(item) is float and math.isfinite(item) for item in raw.values()))


def _envelope(response, tool, arguments):
    _bounded(response)
    _require(isinstance(response, dict) and set(response) == {
        "evidence_id", "run_id", "tool", "arguments", "observed_at", "content_sha256", "data", "truncated"})
    _require(len(_canonical(response)) <= MAX_RESPONSE_BYTES)
    _require(response["run_id"] == arguments["run_id"] and response["tool"] == tool)
    _require(_canonical(response["arguments"]) == _canonical(arguments) and _utc(response["observed_at"]))
    _require(type(response["truncated"]) is bool)
    binding = {key: response[key] for key in ("run_id", "tool", "arguments", "data", "truncated")}
    digest = hashlib.sha256(_canonical(binding)).hexdigest()
    _require(response["content_sha256"] == digest and response["evidence_id"] == "ev_" + digest[:32])
    _shape(tool, response["data"], response["truncated"])
    # Detach retained evidence from caller-owned dictionaries before rule evaluation.
    return json.loads(_canonical(response))


class ChecklistBaseline:
    """Run a fixed four-call deterministic checklist, abstaining on uncertainty."""

    def __init__(self, gateway):
        self.gateway = gateway

    def diagnose(self, run_id):
        if not isinstance(run_id, str) or _UUID.fullmatch(run_id) is None:
            raise ChecklistError("run_id must be 32 lowercase hexadecimal characters")
        started = time.monotonic()
        evidence, limitations = [], []
        for tool, extra in _CALLS:
            arguments = {"run_id": run_id, **extra}
            try:
                response = self.gateway.call(tool, arguments)
            except EvidenceError:
                limitations.append("evidence_unavailable")
                continue
            try:
                response = _envelope(response, tool, arguments)
            except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
                limitations.append("evidence_malformed")
                continue
            evidence.append(response)
            if response["truncated"]:
                limitations.append("evidence_truncated")
        result = {"method": "deterministic_checklist", "run_id": run_id, "status": "unknown", "findings": [],
                  "limitations": [], "notes": ["separate_evidence_snapshots"], "evidence": evidence,
                  "tool_calls": 4, "elapsed_ms": 0.0}
        if not limitations:
            try:
                self._diagnose_complete(result)
            except (ValueError, TypeError, KeyError, OverflowError):
                limitations.append("evidence_conflict")
                result["status"], result["findings"] = "unknown", []
        result["limitations"] = sorted(set(limitations + result["limitations"]))
        result["elapsed_ms"] = round((time.monotonic() - started) * 1000, 3)
        if len(_canonical(result)) > MAX_CHECKLIST_BYTES:
            result.update(status="unknown", findings=[], evidence=[], limitations=["report_limit_exceeded"])
        return result

    @staticmethod
    def _diagnose_complete(result):
        responses = {item["tool"]: item for item in result["evidence"]}
        _require(set(responses) == {tool for tool, _ in _CALLS})
        run, logs, schema, validation = (responses[name]["data"] for name, _ in _CALLS)
        if not (run["quarantine_available"] and run["logs_available"] and logs["available"]
                and validation["available"] and validation["warehouse_available"]):
            result["limitations"].append("evidence_unavailable")
            return
        if run["status"] not in {"succeeded", "failed"}:
            result["limitations"].append("run_not_terminal")
            return
        _require(run["started_at"] is not None and run["finished_at"] is not None)
        _require(run["rows_loaded"] == validation["rows_loaded"]
                 and run["rows_quarantined"] == validation["rows_quarantined"])
        totals, loaded = run["totals_minor"], run["rows_loaded"]
        _require(bool(totals) == (loaded > 0) and len(totals) <= loaded
                 and sum(totals.values()) <= loaded * (2**63 - 1))
        _require(schema["required_columns"] == list(ORDER_COLUMNS) and schema["warehouse_columns_omitted"] == 0)
        expected_columns = [{"name": name, "type": "INTEGER" if name == "amount_minor" else "TEXT",
                             "not_null": True, "primary_key": name == "order_id"} for name in ORDER_COLUMNS]
        _require(schema["warehouse_columns"] == expected_columns)
        events = logs["events"]
        terminal = "load_succeeded" if run["status"] == "succeeded" else "load_failed"
        _require(logs["events_total"] == 3 and len(events) == 3)
        _require([event["event_type"] for event in events] == ["source_received", "load_started", terminal])
        _require(events[0]["event_id"] < events[1]["event_id"] < events[2]["event_id"])
        times = [run["created_at"], events[0]["created_at"], run["started_at"],
                 events[1]["created_at"], run["finished_at"], events[2]["created_at"]]
        parsed_times = [datetime.fromisoformat(value) for value in times]
        _require(parsed_times == sorted(parsed_times))
        received, policy, finished = (event["details"] for event in events)
        _require(received["observed_schema"] == schema["source_columns"]
                 and received["required_schema"] == list(ORDER_COLUMNS))
        _require(policy["duplicate_policy"] == run["duplicate_policy"] and policy["invalid_policy"] == run["invalid_policy"])
        refs = [responses[name]["evidence_id"] for name, _ in _CALLS]
        if run["status"] == "succeeded":
            _require(schema["source_columns"] == list(ORDER_COLUMNS) and validation["failure"] is None)
            _require(finished == {key: run[key] for key in ("rows_loaded", "duplicates_skipped", "rows_quarantined")})
            _require(received["raw_row_count"] == run["rows_loaded"] + run["duplicates_skipped"] + run["rows_quarantined"])
            _require(run["duplicates_skipped"] == 0 or run["duplicate_policy"] == "skip_identical")
            _require(run["rows_quarantined"] == 0 or run["invalid_policy"] == "quarantine")
            records = validation["records"]
            _require(len(records) == run["rows_quarantined"])
            numbers = [row["row_number"] for row in records]
            _require(numbers == sorted(set(numbers)) and all(number <= received["raw_row_count"] for number in numbers))
            for row in records:
                _require(row["raw_record_truncated"] is False)
                try:
                    order = Order.from_record(row["raw_record"])
                except OrderValidationError as exc:
                    _require(row["reason_code"] == "invalid_row" and row["reason"] == str(exc))
                else:
                    _require(row["reason_code"] == "integer_overflow" and order.amount_minor > 2**63 - 1
                             and row["reason"] == "amount_minor exceeds SQLite signed 64-bit integer range")
            for code in ("invalid_row", "integer_overflow"):
                group = [row for row in records if row["reason_code"] == code]
                if group:
                    result["findings"].append({"code": code, "summary": _SUMMARIES[code], "evidence_refs": refs,
                        "details": {"count": len(group), "rows": [{"row_number": row["row_number"], "reason": row["reason"]} for row in group]}})
            if run["duplicates_skipped"]:
                result["notes"].append("identical_retries_skipped")
            result["status"] = "diagnosed" if records else "no_incident"
            return
        _require(run["rows_loaded"] == 0 and run["rows_quarantined"] == 0 and run["duplicates_skipped"] == 0
                 and validation["records"] == [] and finished == validation["failure"])
        code = finished["code"]
        if code not in _SUMMARIES:
            result["limitations"].append("unsupported_diagnostic")
            return
        if code == "schema_mismatch":
            observed = schema["source_columns"]
            _require(observed != list(ORDER_COLUMNS) and finished["observed_schema"] == observed
                     and finished["required_schema"] == list(ORDER_COLUMNS)
                     and finished["missing_columns"] == sorted(set(ORDER_COLUMNS) - set(observed))
                     and finished["extra_columns"] == sorted(set(observed) - set(ORDER_COLUMNS)))
        else:
            _require(schema["source_columns"] == list(ORDER_COLUMNS)
                     and 1 <= finished["row_number"] <= received["raw_row_count"])
            if code == "duplicate_key":
                _require(run["duplicate_policy"] == "reject")
            if code in {"duplicate_key", "conflicting_duplicate"}:
                identifier = finished["order_id"]
                _require(finished["row_number"] >= 2 and bool(identifier) and identifier.strip() == identifier)
            if code in {"invalid_row", "integer_overflow"}:
                _require(run["invalid_policy"] == "reject")
        result["status"] = "diagnosed"
        result["findings"] = [{"code": code, "summary": _SUMMARIES[code], "evidence_refs": refs,
                               "details": {key: value for key, value in finished.items() if key != "code"}}]
