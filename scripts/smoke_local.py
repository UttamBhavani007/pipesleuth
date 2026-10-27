"""Record two actual local-model smokes and separate controlled HTTP failures.

Labels and literal setup data belong only to this driver. The baseline receives
neutral run identifiers and tool evidence, never expected diagnoses. This is an
integration smoke, not a diagnostic-quality benchmark or a holdout evaluation.
"""

from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import platform
import re
import sqlite3
import tempfile
import threading
import time
from urllib.parse import urlsplit

from pipesleuth.evidence import EvidenceGateway
from pipesleuth.local_model import LocalModelClient, LocalModelConfig, LocalModelError
from pipesleuth.pipeline import SandboxPipeline
from pipesleuth.single_call import SingleCallBaseline


ROOT = Path(__file__).resolve().parents[1]


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _files(directory: Path) -> dict:
    return {str(path.relative_to(directory)): (path.stat().st_size, path.stat().st_mtime_ns)
            for path in directory.rglob("*") if path.is_file()}


def _run_files(directory: Path) -> dict:
    return {str(path.relative_to(directory)): (_hash(path), path.stat().st_mtime_ns)
            for path in directory.rglob("*") if path.is_file()}


def _api(endpoint: str, method: str, route: str, payload=None) -> tuple[int, dict]:
    # Endpoint validation is shared with the real client; no proxies or redirects.
    endpoint = LocalModelConfig(endpoint=endpoint).endpoint
    parsed = urlsplit(endpoint)
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=5)
    try:
        body = None if payload is None else json.dumps(payload).encode()
        connection.request(method, route, body=body, headers={"Content-Type": "application/json"})
        response = connection.getresponse()
        encoded = response.read(131_073)
        if len(encoded) > 131_072:
            raise ValueError("smoke metadata exceeded its byte limit")
        return response.status, json.loads(encoded)
    finally:
        connection.close()


def _counts(database: Path) -> dict:
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
        return {"warehouse": connection.execute("SELECT COUNT(*) FROM warehouse_orders").fetchone()[0],
                "quarantine": connection.execute("SELECT COUNT(*) FROM quarantined_records").fetchone()[0]}


def _case(pipeline: SandboxPipeline, config: LocalModelConfig, renamed: bool) -> dict:
    source = {"schema": ["order_id", "customer_id", "amount_minor", "currency", "created_at"], "rows": [
        {"order_id": "A-1", "customer_id": "C-1", "amount_minor": 1250,
         "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"},
        {"order_id": "A-2", "customer_id": "C-2", "amount_minor": 750,
         "currency": "EUR", "created_at": "2026-01-02T09:00:00Z"},
    ]}
    if renamed:
        source["schema"][2] = "gross_total_minor"
        for row in source["rows"]:
            row["gross_total_minor"] = row.pop("amount_minor")
    created = pipeline.create(source)
    run = pipeline.run(created.run_id)
    database = pipeline.database_path(created.run_id)
    before, counts = _run_files(pipeline.data_dir), _counts(database)
    client = LocalModelClient(config)
    started = time.perf_counter()
    observed, error = None, None
    try:
        observed = SingleCallBaseline(EvidenceGateway(pipeline.data_dir), client).diagnose(created.run_id)
    except Exception as failure:
        # Keep failures; do not publish exception text, temporary paths or tracebacks.
        error = {"type": type(failure).__name__}
    elapsed = time.perf_counter() - started
    expected = {"status": "diagnosed" if renamed else "no_incident",
                "codes": ["schema_mismatch"] if renamed else []}
    observed_codes = sorted({finding["code"] for finding in observed["findings"]}) if observed else None
    expectation_match = (observed is not None and observed["status"] == expected["status"]
                         and observed_codes == expected["codes"])
    return {"case": "renamed_amount_column" if renamed else "healthy_input", "run_id": created.run_id,
            "expected_literal_behavior": expected,
            "literal_development_expectation_match": expectation_match,
            "pipeline_status": run.status, "observed": observed, "error": error,
            "actual_generation_requests": client.generation_requests, "elapsed_seconds": elapsed,
            "files_unchanged": _run_files(pipeline.data_dir) == before,
            "database_counts_before": counts, "database_counts_after": _counts(database),
            "semantic_quality_evaluated": False}


class _ControlledHandler(BaseHTTPRequestHandler):
    """Local HTTP test fixtures; these responses are not Ollama/model output."""

    def log_message(self, *args):
        pass

    def _send(self, data, status=200):
        encoded = json.dumps(data).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        if self.path == "/api/version":
            self._send({"version": "0.40.7"})
        elif self.path == "/api/tags":
            models = [] if self.server.mode == "missing" else [
                {"name": "qwen3:1.7b", "model": "qwen3:1.7b", "digest": "1" * 64, "size": 1}]
            self._send({"models": models})
        else:
            self._send({"error": "unknown controlled route"}, 404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(min(length, 98_304))
        if self.path == "/api/show":
            self._send({"model_info": {"general.architecture": "qwen3"}, "capabilities": ["completion"]})
        elif self.path == "/api/generate":
            self.server.generate_attempts += 1
            time.sleep(0.3)
            self._send({"error": "controlled delayed response"})
        else:
            self._send({"error": "unknown controlled route"}, 404)


def _controlled_failure(mode: str) -> dict:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ControlledHandler)
    server.daemon_threads = True
    server.mode, server.generate_attempts = mode, 0
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    timeout = 0.1 if mode == "timeout" else 1.0
    client = LocalModelClient(LocalModelConfig(endpoint=f"http://127.0.0.1:{server.server_port}",
                                               timeout_seconds=timeout))
    started = time.perf_counter()
    error = None
    try:
        client.generate("Return JSON.", "Controlled transport probe.",
                        {"type": "object", "properties": {"value": {"type": "integer"}}, "required": ["value"]})
    except LocalModelError as failure:
        error = str(failure)
    finally:
        elapsed = time.perf_counter() - started
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)
    if mode == "missing":
        expected = error is not None and "cached locally" in error and client.generation_requests == 0
    else:
        expected = (error is not None and ("timed out" in error or "time limit" in error)
                    and client.generation_requests == 1 and server.generate_attempts == 1 and elapsed < 1.0)
    return {"check": "model_unavailable_inventory" if mode == "missing" else "generation_timeout",
            "backend": "controlled_local_http_test_server", "real_model_inference": False,
            "client_timeout_seconds": timeout, "server_delay_seconds": 0.3 if mode == "timeout" else 0,
            "error": error, "elapsed_seconds": elapsed,
            "actual_generation_requests": client.generation_requests,
            "server_generation_attempts": server.generate_attempts, "expected_transport_behavior_observed": expected}


def _startup(path: Path | None) -> dict:
    if path is None:
        return {"available": False}
    if path.stat().st_size > 2 * 1024 * 1024:
        raise ValueError("runtime log exceeds the inspection limit")
    encoded = path.read_bytes()
    text = encoded.decode("utf-8", errors="replace")
    contexts = {int(value) for value in re.findall(r"n_ctx(?:_per_seq)?\s*=\s*(\d+)", text)}
    contexts.update(int(value) for value in re.findall(r"--ctx-size\s+(\d+)", text))
    return {"available": True, "log_sha256": hashlib.sha256(encoded).hexdigest(),
            "cloud_disabled_observed": "Ollama cloud disabled: true" in text,
            "loopback_binding_observed": "Listening on 127.0.0.1:11436" in text,
            "request_body_debug_disabled_observed": "OLLAMA_DEBUG_LOG_REQUESTS:false" in text,
            "cache_pruning_disabled_observed": "OLLAMA_NOPRUNE:true" in text,
            "effective_contexts_observed": sorted(contexts)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--endpoint", default="http://127.0.0.1:11436")
    parser.add_argument("--binary", type=Path, help="Optional trusted runtime binary for provenance hashing")
    parser.add_argument("--models", type=Path, help="Optional existing cache for before/after metadata checks")
    parser.add_argument("--runtime-log", type=Path, help="Optional actual foreground startup/backend log")
    args = parser.parse_args()
    config = LocalModelConfig(endpoint=args.endpoint)
    cache_before = _files(args.models) if args.models is not None else None
    report = {"generated_at": None, "scope": "Two synthetic integration smokes; no diagnostic accuracy or semantic support claim.",
              "runtime": None, "cases": [], "failure_checks": [], "errors": [],
              "config": {"python_version": platform.python_version(), "machine": platform.machine(),
                         "physical_memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
                         "endpoint": config.endpoint, "model": config.model, "context_tokens": config.num_ctx,
                         "output_token_cap": config.max_output_tokens, "timeout_seconds": config.timeout_seconds,
                         "baseline_source_sha256": _hash(ROOT / "src/pipesleuth/single_call.py"),
                         "client_source_sha256": _hash(ROOT / "src/pipesleuth/local_model.py"),
                         "evidence_source_sha256": _hash(ROOT / "src/pipesleuth/evidence.py"),
                         "smoke_source_sha256": _hash(Path(__file__)),
                         "runtime_binary_sha256": _hash(args.binary) if args.binary is not None else None}}
    try:
        health = LocalModelClient(config).health()
        status, shown = _api(config.endpoint, "POST", "/api/show", {"model": config.model, "verbose": False})
        license_text = shown.get("license", "")
        apache = isinstance(license_text, str) and "Apache License" in license_text and "Version 2.0" in license_text
        if status != 200 or not apache:
            raise ValueError("served model license was not verified as Apache 2.0")
        report["runtime"] = {"health": health, "license_is_apache_2": apache,
                             "license_sha256": hashlib.sha256(license_text.encode()).hexdigest(),
                             "model_info": {key: value for key, value in shown.get("model_info", {}).items()
                                            if key in {"general.architecture", "general.parameter_count", "qwen3.context_length"}},
                             "details": shown.get("details", {}), "capabilities": shown.get("capabilities", [])}
        pinned = LocalModelConfig(endpoint=config.endpoint, expected_digest=health["digest"])
        report["config"]["pinned_digest"] = health["digest"]
        with tempfile.TemporaryDirectory(prefix="pipesleuth-model-smoke-") as temporary:
            pipeline = SandboxPipeline(Path(temporary).resolve() / "runs")
            for renamed in (False, True):
                report["cases"].append(_case(pipeline, pinned, renamed))
        missing_status, _ = _api(config.endpoint, "POST", "/api/show", {"model": "pipesleuth-missing-smoke"})
        report["failure_checks"].append({"check": "missing_model_show", "backend": "actual_ollama",
                                         "real_model_inference": False, "http_status": missing_status,
                                         "actual_generation_requests": 0,
                                         "expected_transport_behavior_observed": missing_status == 404})
    except Exception as failure:
        report["errors"].append({"phase": "actual_ollama_smoke", "type": type(failure).__name__})
    for mode in ("missing", "timeout"):
        try:
            report["failure_checks"].append(_controlled_failure(mode))
        except Exception as failure:
            report["errors"].append({"phase": "controlled_" + mode, "type": type(failure).__name__})
    report["startup_observations"] = _startup(args.runtime_log)
    report["cache_metadata_unchanged"] = _files(args.models) == cache_before if args.models is not None else None
    report["generated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    report["summary"] = {
        "planned_real_model_cases": 2, "recorded_cases": len(report["cases"]),
        "actual_ollama_generation_requests": sum(case["actual_generation_requests"] for case in report["cases"]),
        "completed_model_responses": sum(case["observed"] is not None and case["observed"].get("model_response") is not None for case in report["cases"]),
        "literal_development_expectation_matches": sum(case["literal_development_expectation_match"] for case in report["cases"]),
        "unchanged_run_cases": sum(case["files_unchanged"] for case in report["cases"]),
        "controlled_checks": sum(check["backend"] == "controlled_local_http_test_server" for check in report["failure_checks"]),
        "errors": len(report["errors"]) + sum(case["error"] is not None for case in report["cases"]),
        "semantic_quality_evaluated": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps(report["summary"], sort_keys=True))
    return 0 if (report["summary"]["actual_ollama_generation_requests"] == 2
                 and report["summary"]["completed_model_responses"] == 2
                 and report["summary"]["unchanged_run_cases"] == 2
                 and report["summary"]["errors"] == 0
                 and report["cache_metadata_unchanged"] is not False
                 and (args.runtime_log is None or all(report["startup_observations"].get(key) is True for key in (
                     "cloud_disabled_observed", "loopback_binding_observed",
                     "request_body_debug_disabled_observed", "cache_pruning_disabled_observed")))
                 and all(check["expected_transport_behavior_observed"] for check in report["failure_checks"])) else 1


if __name__ == "__main__":
    raise SystemExit(main())
