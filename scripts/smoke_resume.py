"""Record actual process interruption/resume and a separate uncertain fixture.

No repair, inference retry, prompt tuning or automatic replay is performed.
Workers use only the public persistent API and live in owned child processes.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import tempfile
import threading
import time

from pipesleuth.checkpoints import CheckpointError, CheckpointStore
from pipesleuth.evidence import EvidenceGateway
from pipesleuth.investigator import BoundedInvestigator
from pipesleuth.local_model import LocalModelClient, LocalModelConfig
from pipesleuth.persistent import PersistentError, PersistentInvestigator
from pipesleuth.pipeline import SandboxPipeline
from smoke_investigator import _database_state
from smoke_local import _api, _files, _run_files, _startup


ROOT = Path(__file__).resolve().parents[1]
DIGEST = "8f68893c685c3ddff2aa3fffce2aa60a30bb2da65ca488b61fff134a4d1730e7"
SOURCES = ("src/pipesleuth/checkpoints.py", "src/pipesleuth/persistent.py",
           "src/pipesleuth/input_binding.py",
           "src/pipesleuth/investigator.py", "src/pipesleuth/agent_protocol.py",
           "src/pipesleuth/local_model.py", "src/pipesleuth/evidence.py",
           "src/pipesleuth/checklist.py", "scripts/smoke_resume.py",
           "scripts/smoke_investigator.py", "scripts/smoke_local.py", "scripts/local_runtime.py")


def _encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write(path, value):
    pending = path.with_name(path.name + ".tmp")
    with pending.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, sort_keys=True, ensure_ascii=False, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(pending, path)


def _audit(record):
    # The private binding can contain local paths. Hash it; expose only runtime
    # state/results and neutral identifiers, never the raw path configuration.
    state = record["state"]
    return {"investigation_id": record["investigation_id"], "revision": record["revision"],
            "binding_sha256": hashlib.sha256(_encoded(record["binding"])).hexdigest(),
            "state_sha256": hashlib.sha256(_encoded(state)).hexdigest(),
            "pending": record["pending"], "done": state["done"],
            "next": state["next"], "result": state["result"]}


class RecordingGateway(EvidenceGateway):
    def __init__(self, directory, forbidden=False):
        super().__init__(directory)
        self.calls, self.forbidden = [], forbidden

    def call(self, tool, arguments, **options):
        self.calls.append({"tool": tool, "arguments": dict(arguments)})
        if self.forbidden:
            raise AssertionError("read-only/uncertain inspection unexpectedly called a tool")
        return super().call(tool, arguments, **options)


class FixtureClient:
    """Controlled software fixture; this class never performs model inference."""

    def __init__(self, config, callback=None):
        self.config, self.callback = config, callback
        self.generation_requests, self.calls = 0, 0

    def generate(self, *arguments, **options):
        self.calls += 1
        if self.callback is not None:
            self.callback()
            threading.Event().wait()
        raise AssertionError("inspection unexpectedly attempted generation")


def _components(args, fixture=False, forbidden=False):
    config = LocalModelConfig(endpoint=args.endpoint, expected_digest="a" * 64 if fixture else args.model_digest)
    gateway = RecordingGateway(args.data_dir, forbidden=forbidden)
    client = FixtureClient(config) if fixture or forbidden else LocalModelClient(config)
    store = CheckpointStore(args.state_dir)
    engine = BoundedInvestigator(gateway, client)
    return PersistentInvestigator(engine, store, retry_limit=1), store, gateway, client


def _worker(args):
    fixture = args.worker in {"pending_fixture", "fixture_resume"}
    forbidden = args.worker in {"terminal", "fixture_resume"}
    runner, store, gateway, client = _components(args, fixture=fixture, forbidden=forbidden)
    if args.worker in {"actual", "pending_fixture"}:
        record = runner.start(args.run_id)
        identifier = record["investigation_id"]
        if fixture:
            def entered():
                pending = store.load(identifier)
                if pending["pending"] is None:
                    raise AssertionError("model fixture entered before durable claim")
                _write(args.marker, {"phase": "controlled_pending_model_fixture", "record": _audit(pending),
                                     "real_model_inference": False, "gateway_calls": gateway.calls,
                                     "confirmed_generation_requests": 0})
            client.callback = entered
            runner.resume(identifier)
            raise AssertionError("controlled pending fixture returned")
        for _ in range(16):
            record = runner.resume(identifier, max_operations=1)
            observed = store.load(identifier)
            if observed != record or record["pending"] is not None:
                raise AssertionError("completed checkpoint was not durable")
            result = record["state"]["result"]
            chosen = any(event.get("action") == "generate" and event.get("status") == "tool"
                         for event in result["trace"])
            phase = None
            if chosen and result["tool_calls"] == 1 and result["model_calls"] >= 1:
                phase = "completed_model_selection_checkpoint"
            elif record["state"]["done"]:
                phase = "terminal_before_model_selection"
            if phase:
                _write(args.marker, {"phase": phase, "record": _audit(observed),
                                     "real_model_inference": True,
                                     "gateway_calls": gateway.calls,
                                     "confirmed_generation_requests": client.generation_requests})
                # Parent interrupts only after this marker, which follows a fresh
                # public store read after the completed operation's transaction.
                threading.Event().wait()
        raise AssertionError("model-selection checkpoint was not reached")
    before = store.load(args.investigation_id)
    error = None
    try:
        shown = runner.show(args.investigation_id)
        record = runner.resume(args.investigation_id)
        same_show = shown == record
    except (CheckpointError, PersistentError) as failure:
        error = {"type": type(failure).__name__,
                 "pending_refused": "pending" in str(failure).lower() or "uncertain" in str(failure).lower()}
        record, same_show = store.load(args.investigation_id), False
    _write(args.worker_output, {"record": _audit(record), "error": error,
                               "show_equals_resume": same_show, "record_unchanged": before == record,
                               "gateway_calls": gateway.calls,
                               "confirmed_generation_requests": client.generation_requests,
                               "fixture_generate_attempts": getattr(client, "calls", 0)})
    return 0


def _command(args, mode, data_dir, state_dir, **values):
    command = [sys.executable, str(Path(__file__).resolve()), "--worker", mode,
               "--data-dir", str(data_dir), "--state-dir", str(state_dir),
               "--endpoint", args.endpoint, "--model-digest", args.model_digest]
    for name, value in values.items():
        command.extend(["--" + name.replace("_", "-"), str(value)])
    return command


def _interrupt(args, mode, data_dir, state_dir, run_id, directory):
    marker = directory / (mode + "-marker.json")
    environment = dict(os.environ, PYTHONPATH=str(ROOT / "src"), PYTHONDONTWRITEBYTECODE="1")
    process = subprocess.Popen(_command(args, mode, data_dir, state_dir, run_id=run_id, marker=marker),
                               cwd=ROOT, env=environment, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        deadline = time.monotonic() + 150
        while not marker.exists():
            if process.poll() is not None:
                raise RuntimeError("owned worker exited before its checkpoint marker")
            if time.monotonic() >= deadline:
                raise TimeoutError("owned worker did not reach its marker")
            time.sleep(0.02)
        observed = json.loads(marker.read_text())
        process.terminate()
        code = process.wait(timeout=5)
        observed["owned_child_returncode"] = code
        observed["owned_child_terminated_by_sigterm"] = code == -signal.SIGTERM
        return observed
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def _fresh(args, mode, data_dir, state_dir, identifier, directory):
    output = directory / (mode + "-output.json")
    result = subprocess.run(_command(args, mode, data_dir, state_dir,
                                    investigation_id=identifier, worker_output=output),
                            cwd=ROOT, env=dict(os.environ, PYTHONPATH=str(ROOT / "src"),
                                               PYTHONDONTWRITEBYTECODE="1"),
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=150)
    if result.returncode != 0 or not output.exists():
        raise RuntimeError("fresh owned worker did not produce its result")
    return json.loads(output.read_text())


def _run(pipeline):
    source = {"schema": ["order_id", "customer_id", "amount_minor", "currency", "created_at"], "rows": [
        {"order_id": "A-1", "customer_id": "C-1", "amount_minor": 1250,
         "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"},
        {"order_id": "A-2", "customer_id": "C-2", "amount_minor": 750,
         "currency": "EUR", "created_at": "2026-01-02T09:00:00Z"}]}
    created = pipeline.create(source)
    return pipeline.run(created.run_id).run_id


def _actual_case(args, pipeline, run_id, state_dir, directory):
    before_files = _run_files(pipeline.data_dir)
    before_state = _database_state(pipeline.database_path(run_id))
    marker = _interrupt(args, "actual", pipeline.data_dir, state_dir, run_id, directory)
    identifier = marker["record"]["investigation_id"]
    resumed = _fresh(args, "resume", pipeline.data_dir, state_dir, identifier, directory)
    state_before_terminal_read = _run_files(state_dir)
    terminal = _fresh(args, "terminal", pipeline.data_dir, state_dir, identifier, directory)
    partial, final = marker["record"]["result"], resumed["record"]["result"]
    prefix = partial["trace"]
    cursor = marker["record"]["next"]
    new_trace = final["trace"][len(prefix):]
    saved_choice_consumed_first = (marker["phase"] == "completed_model_selection_checkpoint"
        and isinstance(cursor, dict) and cursor.get("action") == "tool"
        and bool(new_trace) and new_trace[0].get("action") == "tool"
        and new_trace[0].get("tool") == cursor.get("tool")
        and bool(resumed["gateway_calls"]) and resumed["gateway_calls"][0]["tool"] == cursor.get("tool"))
    saved_calls = marker["confirmed_generation_requests"]
    fresh_calls = resumed["confirmed_generation_requests"]
    counts_complete = marker["record"]["pending"] is None and resumed["record"]["pending"] is None
    return {
        "backend": "actual_local_ollama", "interruption": marker, "fresh_resume": resumed,
        "terminal_reinspection": terminal,
        "completed_model_selection_checkpoint_reached": marker["phase"] == "completed_model_selection_checkpoint",
        "saved_model_choice_consumed_before_any_generation": saved_choice_consumed_first,
        "confirmed_generation_request_attempts": saved_calls + fresh_calls,
        "request_count_complete": counts_complete,
        "uncertain_in_flight_request_outcome": None if counts_complete else "unknown",
        "completed_trace_prefix_preserved": final["trace"][:len(prefix)] == prefix,
        "bootstrap_evidence_preserved": bool(partial["evidence"]) and partial["evidence"][0] == final["evidence"][0],
        "no_bootstrap_replay": all(call["tool"] != "read_run" for call in resumed["gateway_calls"]),
        "cumulative_model_count_verified": final["model_calls"] == partial["model_calls"] + fresh_calls,
        "terminal_reread_without_calls": terminal["record"] == resumed["record"] and terminal["record_unchanged"]
            and terminal["confirmed_generation_requests"] == terminal["fixture_generate_attempts"] == 0
            and terminal["gateway_calls"] == [] and terminal["error"] is None,
        "terminal_store_bytes_unchanged": _run_files(state_dir) == state_before_terminal_read,
        "pipeline_files_unchanged": _run_files(pipeline.data_dir) == before_files,
        "exact_database_state_unchanged": _database_state(pipeline.database_path(run_id)) == before_state,
        "semantic_quality_evaluated": False,
    }


def _pending_case(args, pipeline, run_id, state_dir, directory):
    before_files = _run_files(pipeline.data_dir)
    marker = _interrupt(args, "pending_fixture", pipeline.data_dir, state_dir, run_id, directory)
    before_store = _run_files(state_dir)
    rejected = _fresh(args, "fixture_resume", pipeline.data_dir, state_dir,
                      marker["record"]["investigation_id"], directory)
    return {"backend": "controlled_model_fixture_no_inference", "interruption": marker, "fresh_resume": rejected,
            "confirmed_real_model_requests": 0, "real_model_inference": False,
            "pending_request_outcome_policy": "unknown; no automatic replay permitted",
            "pending_refused_without_calls": rejected["error"] is not None and rejected["error"]["pending_refused"]
                and rejected["gateway_calls"] == [] and rejected["fixture_generate_attempts"] == 0
                and rejected["confirmed_generation_requests"] == 0,
            "pending_record_unchanged": rejected["record"] == marker["record"],
            "store_bytes_unchanged_after_refusal": _run_files(state_dir) == before_store,
            "pipeline_files_unchanged": _run_files(pipeline.data_dir) == before_files}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--endpoint", default="http://127.0.0.1:11436")
    parser.add_argument("--model-digest", default=DIGEST)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--models", type=Path)
    parser.add_argument("--runtime-log", type=Path)
    parser.add_argument("--worker", choices=("actual", "resume", "terminal", "pending_fixture", "fixture_resume"), help=argparse.SUPPRESS)
    for name in ("data-dir", "state-dir", "marker", "worker-output"):
        parser.add_argument("--" + name, type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--run-id", help=argparse.SUPPRESS)
    parser.add_argument("--investigation-id", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        return _worker(args)
    if args.output is None or args.output.exists():
        parser.error("choose a new --output path to preserve prior outcomes")
    source_hashes = {name: _hash(ROOT / name) for name in SOURCES}
    config = LocalModelConfig(endpoint=args.endpoint, expected_digest=args.model_digest)
    cache_before = _files(args.models) if args.models is not None else None
    report = {"generated_at": None, "scope": "Actual saved-operation process restart plus separate deterministic uncertain-operation fixture; no accuracy or repair claim.",
              "config": {"python_version": platform.python_version(), "machine": platform.machine(),
                         "endpoint": config.endpoint, "model": config.model, "expected_digest": config.expected_digest,
                         "context_tokens": config.num_ctx, "output_tokens": config.max_output_tokens,
                         "temperature": 0, "think": False, "retry_limit": 1,
                         "source_sha256": source_hashes,
                         "runtime_binary_sha256": _hash(args.binary) if args.binary is not None else None},
              "runtime": None, "actual_restart": None, "pending_fixture": None, "errors": []}
    try:
        health = LocalModelClient(config).health()
        status, shown = _api(config.endpoint, "POST", "/api/show", {"model": config.model, "verbose": False})
        license_text = shown.get("license", "")
        if status != 200 or not isinstance(license_text, str) or not all(word in license_text for word in ("Apache License", "Version 2.0")):
            raise ValueError("cached model license could not be verified")
        report["runtime"] = {"health": health, "license_is_apache_2": True,
                             "license_sha256": hashlib.sha256(license_text.encode()).hexdigest()}
        with tempfile.TemporaryDirectory(prefix="pipesleuth-resume-smoke-") as temporary:
            directory = Path(temporary).resolve()
            pipeline = SandboxPipeline(directory / "runs")
            actual_id, fixture_id = _run(pipeline), _run(pipeline)
            for key, function, run_id in (("actual_restart", _actual_case, actual_id),
                                          ("pending_fixture", _pending_case, fixture_id)):
                try:
                    report[key] = function(args, pipeline, run_id, directory / key, directory)
                except Exception as failure:
                    report["errors"].append({"phase": key, "type": type(failure).__name__})
    except Exception as failure:
        report["errors"].append({"phase": "runtime_setup", "type": type(failure).__name__})
    report["startup_observations"] = _startup(args.runtime_log)
    report["cache_metadata_unchanged"] = _files(args.models) == cache_before if args.models is not None else None
    report["source_hashes_unchanged_during_run"] = all(_hash(ROOT / name) == digest for name, digest in source_hashes.items())
    actual, pending = report["actual_restart"], report["pending_fixture"]
    actual_keys = ("completed_model_selection_checkpoint_reached", "saved_model_choice_consumed_before_any_generation",
                   "completed_trace_prefix_preserved", "bootstrap_evidence_preserved", "no_bootstrap_replay",
                   "cumulative_model_count_verified", "terminal_reread_without_calls", "terminal_store_bytes_unchanged",
                   "pipeline_files_unchanged", "exact_database_state_unchanged")
    pending_keys = ("pending_refused_without_calls", "pending_record_unchanged",
                    "store_bytes_unchanged_after_refusal", "pipeline_files_unchanged")
    report["restart_checks_passed"] = bool(actual and pending and not report["errors"]
        and all(actual[key] for key in actual_keys) and all(pending[key] for key in pending_keys)
        and report["source_hashes_unchanged_during_run"] and report["cache_metadata_unchanged"] is not False)
    report["generated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    report["summary"] = {"confirmed_actual_generation_request_attempts": actual["confirmed_generation_request_attempts"] if actual else 0,
                         "actual_model_selection_checkpoint_reached": actual["completed_model_selection_checkpoint_reached"] if actual else False,
                         "pending_fixture_refused": pending["pending_refused_without_calls"] if pending else False,
                         "restart_checks_passed": report["restart_checks_passed"], "errors": len(report["errors"]),
                         "semantic_quality_evaluated": False}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps(report["summary"], sort_keys=True))
    return 0 if report["restart_checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
