"""Prepare isolated synthetic inputs for a real local browser acceptance run.

Run once with PYTHONPATH=src .venv/bin/python scripts/prepare_ui_smoke.py.
This does not start a server, call a model or approve a repair. Input directories
are opaque run IDs; authoring labels/oracles remain outside the API input root.
The ignored workspace is exclusive: existing work is never overwritten.
"""

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import SandboxPipeline


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT / "reports" / "local" / "ui-smoke"
PORT = 8766
CASES = (
    ("case-01", "schema_drift"),
    ("case-02", "duplicate_ingestion"),
    ("case-03", "invalid_records"),
    ("case-04", "healthy"),
)


def _write_private(path, value):
    data = (json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
    except Exception:
        # No replacement, retry or rollback of already observed fixture work.
        raise


def prepare():
    for parent in (ROOT / "reports", ROOT / "reports" / "local"):
        if parent.is_symlink():
            raise ValueError("smoke parent cannot be a symbolic link")
        parent.mkdir(mode=0o700, exist_ok=True)
        if not parent.is_dir():
            raise ValueError("smoke parent must be a directory")
    # exist_ok=False also refuses a pre-existing directory or symbolic link.
    WORKSPACE.mkdir(mode=0o700)
    pipeline = SandboxPipeline(WORKSPACE / "input")
    manifest = {"version": 1, "prepared_at": datetime.now(timezone.utc).isoformat(),
                "purpose": "synthetic local browser acceptance; not a model-quality evaluation",
                "actual_model_requests": 0, "cases": []}
    authoring = {"warning": "Evaluator-only truth. Never expose through API/model evidence.", "cases": []}
    for case_id, kind in CASES:
        fixture = generate_fixture(kind, seed=42, row_count=6)
        run = pipeline.create(fixture.to_source_payload())
        observed = pipeline.run(run.run_id)
        directory = pipeline.data_dir / run.run_id
        hashes = {name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
                  for name in ("source.json", "warehouse.sqlite3")}
        manifest["cases"].append({"case_id": case_id, "run_id": run.run_id,
                                  "observed_pipeline_status": observed.status,
                                  "input_sha256": hashes})
        authoring["cases"].append({"case_id": case_id, "run_id": run.run_id,
                                  "scenario": kind, "oracle": fixture.oracle.to_dict()})
    _write_private(WORKSPACE / "manifest.json", manifest)
    _write_private(WORKSPACE / "authoring.json", authoring)
    return manifest


def main():
    manifest = prepare()
    # Print only non-secret launch coordination, never session contents.
    print(json.dumps({"port": PORT,
                      "runs": {case["case_id"]: case["run_id"] for case in manifest["cases"]},
                      "session_file": str(WORKSPACE / "private-session.json")}, indent=2))


if __name__ == "__main__":
    main()
