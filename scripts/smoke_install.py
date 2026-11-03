"""Build a wheel offline and audit it in a fresh local runtime environment."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import tomllib
from zipfile import ZipFile

from audit_ui_state import _database, _files, audit_http
from pipesleuth.web_assets import UI_ASSETS
from smoke_api import Audit, _expect, _port, _stop, _wait_session


ROOT = Path(__file__).resolve().parents[1]
PROJECT_FILES = ("pyproject.toml", "README.md", "LICENSE")
REQUIREMENTS = "requirements-api.txt"
WHEEL_MANIFEST = "reports/api-dependencies.json"
AUDIT_FILES = ("scripts/smoke_install.py", "scripts/audit_ui_state.py",
               "scripts/smoke_api.py", "scripts/smoke_repair.py")
COLUMNS = ("order_id", "customer_id", "amount_minor", "currency", "created_at")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def package_sources():
    package = ROOT / "src/pipesleuth"
    files = sorted(path.relative_to(ROOT).as_posix() for path in package.rglob("*.py"))
    files += ["src/pipesleuth/web/" + filename for filename, _ in UI_ASSETS.values()]
    for name in files + list(PROJECT_FILES):
        path = ROOT / name
        if not path.is_file() or path.resolve() != path:
            raise ValueError("package source must be a regular file inside the checkout")
    return files


def reviewed_wheels(wheelhouse):
    def canonical(name):
        return re.sub(r"[-_.]+", "-", name).lower()

    expected = json.loads((ROOT / WHEEL_MANIFEST).read_text())["packages"]
    pins = dict(line.split("==") for raw in (ROOT / REQUIREMENTS).read_text().splitlines()
                if (line := raw.partition("#")[0].strip()))
    versions = {canonical(item["name"]): item["version"] for item in expected}
    if versions != {canonical(name): version for name, version in pins.items()}:
        raise ValueError("reviewed wheels and requirements disagree")
    candidates = {sha256(path): path for path in wheelhouse.glob("*.whl")
                  if path.is_file() and not path.is_symlink()}
    paths = []
    for item in expected:
        path = candidates.get(item["wheel_sha256"])
        if path is None or path.stat().st_size != item["wheel_bytes"]:
            raise ValueError("a reviewed dependency wheel is absent or changed")
        paths.append(path)
    return paths, versions


def offline_environment():
    env = {key: os.environ[key] for key in ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL") if key in os.environ}
    env.update(PIP_CONFIG_FILE=os.devnull, PIP_DISABLE_PIP_VERSION_CHECK="1",
               PIP_NO_CACHE_DIR="1", PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1")
    return env


def command(label, args, *, cwd, env, expected=0):
    result = subprocess.run([str(arg) for arg in args], cwd=cwd, env=env,
                            capture_output=True, text=True, timeout=90, check=False)
    if result.returncode != expected:
        # Never print raw launcher output or subprocess command lines.
        raise RuntimeError(f"{label} exited with {result.returncode}; expected {expected}")
    return result.stdout.strip()


def build_and_inspect(root, sources, env):
    checkout, wheels = root / "source", root / "built"
    for name in [*PROJECT_FILES, *sources]:
        destination = checkout / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, destination)
    command("wheel build", [sys.executable, "-I", "-m", "pip", "wheel", "--no-index", "--no-deps",
                            "--no-build-isolation", "--wheel-dir", wheels, "."], cwd=checkout, env=env)
    wheel, = wheels.glob("pipesleuth-*.whl")
    expected = {name.removeprefix("src/"): checkout / name for name in sources}
    with ZipFile(wheel) as archive:
        entries = archive.namelist()
        assert len(set(entries)) == len(entries), "duplicate wheel entries"
        assert {name for name in entries if name.startswith("pipesleuth/")} == set(expected), "package file inventory differs"
        assert all(archive.read(name) == path.read_bytes() for name, path in expected.items()), "package file bytes differ"
        metadata_dirs = {name.split("/", 1)[0] for name in entries if not name.startswith("pipesleuth/")}
        prefix, = metadata_dirs
        assert re.fullmatch(r"pipesleuth-[A-Za-z0-9_.]+\.dist-info", prefix), "unexpected wheel metadata path"
        metadata_names = {name.removeprefix(prefix + "/") for name in entries if name.startswith(prefix + "/")}
        assert metadata_names == {"METADATA", "WHEEL", "entry_points.txt", "top_level.txt", "RECORD", "licenses/LICENSE"}
        assert archive.read(prefix + "/licenses/LICENSE") == (checkout / "LICENSE").read_bytes()
    return wheel, {"filename": wheel.name, "sha256": sha256(wheel), "bytes": wheel.stat().st_size,
                   "package_files_verified": len(expected), "browser_assets_verified": len(UI_ASSETS),
                   "mit_license_bytes_verified": True, "unexpected_wheel_entries": 0}


def install(root, wheel, dependencies, expected_versions, version, env):
    workspace = root / "workspace"
    workspace.mkdir()
    runtime = root / "runtime"
    command("fresh virtual environment", [sys.executable, "-I", "-m", "venv", runtime], cwd=workspace, env=env)
    python = runtime / "bin/python"
    pip = [python, "-I", "-m", "pip"]
    before = json.loads(command("fresh environment inventory", [*pip, "list", "--format=json"], cwd=workspace, env=env))
    assert before and all(item["name"] in {"pip", "setuptools"} for item in before)
    command("offline wheel install", [*pip, "install", "--no-index", "--no-deps", wheel, *dependencies], cwd=workspace, env=env)
    command("installed dependency check", [*pip, "check"], cwd=workspace, env=env)
    identity = """
from importlib import metadata
from pathlib import Path
import json
import re
import sys
import pipesleuth

print(json.dumps({
    'isolated_venv': sys.prefix != sys.base_prefix,
    'import_from_venv': Path(pipesleuth.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()),
    'module_version': pipesleuth.__version__,
    'packages': {re.sub(r'[-_.]+', '-', dist.metadata['Name']).lower(): dist.version
                 for dist in metadata.distributions()},
}))
"""
    installed = json.loads(command("installed package identity", [python, "-I", "-c", identity], cwd=workspace, env=env))
    assert installed["isolated_venv"] and installed["import_from_venv"]
    bootstrap = {item["name"]: item["version"] for item in before}
    assert installed["packages"] == {**bootstrap, **expected_versions, "pipesleuth": version}
    assert installed["module_version"] == version
    cli = runtime / "bin/pipesleuth"
    assert command("installed entry point", [cli, "--version"], cwd=workspace, env=env) == version
    installed["bootstrap_packages"] = bootstrap
    installed["pip_check_passed"] = True
    installed["installed_entry_point_version"] = version
    return workspace, cli, installed


def exercise(workspace, cli, env):
    failed = json.loads(command("installed incident CLI", [cli, "pipeline", "run", "--scenario", "schema_drift",
                        "--seed", "42", "--rows", "6"], cwd=workspace, env=env, expected=1))
    assert failed["status"] == "failed" and re.fullmatch(r"[0-9a-f]{32}", failed["run_id"])
    run_id = failed["run_id"]
    source_root = workspace / "var/sandboxes"
    source = json.loads((source_root / run_id / "source.json").read_text())
    original = _files(source_root)
    session_file = workspace / ".runtime/api-session.json"
    port = _port()
    process = subprocess.Popen([str(cli), "serve", "--port", str(port)], cwd=workspace, env=env,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        session, startup = _wait_session(process, session_file, port)
        public = audit_http(port)
        assert public["passed"], "installed public asset and boundary checks failed"
        audit = Audit(port, session)
        api_run = f"/v1/runs/{run_id}"
        caps = _expect(audit.call("installed_capabilities", "GET", "/v1/capabilities", role="read"), 200)
        assert caps["model_configured"] is False
        summary = _expect(audit.call("installed_run_summary", "GET", api_run, role="read"), 200)
        assert summary["status"] == "failed" and summary["rows_loaded"] == 0
        schema = _expect(audit.call("installed_schema", "GET", api_run + "/evidence/inspect_schema", role="read"), 200)
        assert schema["data"]["source_columns"] == source["schema"] and not schema["truncated"]
        diagnosis = _expect(audit.call("installed_checklist", "POST", api_run + "/diagnosis", payload={"method": "checklist"}), 200)
        assert diagnosis["method"] == "deterministic_checklist" and diagnosis["status"] == "diagnosed"
        assert [item["code"] for item in diagnosis["findings"]] == ["schema_mismatch"] and "model_calls" not in diagnosis
        disabled = _expect(audit.call("installed_model_disabled", "POST", api_run + "/diagnosis", payload={"method": "local"}), 409)
        assert disabled["error"]["code"] == "model_unconfigured"
        plan = _expect(audit.call("installed_repair_plan", "POST", "/v1/repairs",
                                 payload={"run_id": run_id, "action": "rename_amount_column"}), 200)
        assert plan["state"]["status"] == "planned"
        repair = f"/v1/repairs/{plan['investigation_id']}"
        hashes = {key: plan["state"][key] for key in ("plan_sha256", "input_fingerprint_sha256")}
        _expect(audit.call("installed_unapproved_execute", "POST", repair + "/execute", payload={}), 409)
        _expect(audit.call("installed_operation_cannot_approve", "POST", repair + "/approve", payload=hashes), 403)
        approved = _expect(audit.call("installed_scripted_approval", "POST", repair + "/approve", role="approval", payload=hashes), 200)
        assert approved["state"]["status"] == "approved"
        receipt = _expect(audit.call("installed_execute", "POST", repair + "/execute", payload={}), 200)
        assert receipt["state"]["status"] == "succeeded"
        target = receipt["state"]["plan"]["target_run_id"]
        assert target != run_id and re.fullmatch(r"[0-9a-f]{32}", target)
        actual = _database(workspace / "var/repaired" / target / "warehouse.sqlite3")
        expected_rows = sorted([[row["gross_total_minor" if column == "amount_minor" else column] for column in COLUMNS]
                                for row in source["rows"]])
        assert len(expected_rows) == 6 and actual["warehouse_orders"] == expected_rows
        assert actual["warehouse_value_types"] == [["text", "text", "integer", "text", "text"]] * 6
        assert actual["quarantined_records"] == []
        assert actual["pipeline_run"] == [[target, "succeeded", "reject", "reject", 0, 0]]
        assert _files(source_root) == original
    finally:
        cleanup = _stop(process, port, session_file)
    assert process.returncode == 0 and not cleanup["forced_shutdown_required"]
    assert cleanup["loopback_port_connection_refused"] and cleanup["private_session_file_removed"]
    return {"startup": startup, "public_http_checks": public, "api_requests": audit.requests,
            "cleanup": cleanup, "installed_cli_incident_status": failed["status"],
            "derived_warehouse_rows_independently_verified": len(expected_rows),
            "original_input_files_unchanged": True, "approval_kind": "scripted local acceptance check",
            "model_generation_requests": 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheelhouse", type=Path, default=ROOT / ".tools/api-wheels")
    parser.add_argument("--output", type=Path, default=ROOT / "reports/local/local-deployment.json")
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        parser.error("choose a fresh report path")
    if not __debug__:
        parser.error("audit requires enabled assertions")
    sources = package_sources()
    names = [*PROJECT_FILES, *sources, REQUIREMENTS, WHEEL_MANIFEST, *AUDIT_FILES]
    hashes = {name: sha256(ROOT / name) for name in names}
    dependencies, versions = reviewed_wheels(args.wheelhouse.resolve())
    version = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
    env = offline_environment()
    with tempfile.TemporaryDirectory(prefix="pipesleuth-install-") as temporary:
        root = Path(temporary).resolve()
        wheel, inventory = build_and_inspect(root, sources, env)
        workspace, cli, installed = install(root, wheel, dependencies, versions, version, env)
        http = exercise(workspace, cli, env)
    assert not root.exists() and hashes == {name: sha256(ROOT / name) for name in names}
    result = {"generated_at": datetime.now(timezone.utc).isoformat(), "project_version": version,
              "scope": "Offline wheel build and clean installed local prototype verification",
              "platform": {"system": platform.system(), "release": platform.release(), "machine": platform.machine(),
                           "python": platform.python_version(), "sqlite": sqlite3.sqlite_version},
              "builder": {"pip": metadata.version("pip"), "setuptools": metadata.version("setuptools"),
                          "separate_source_copy": True, "pip_index_disabled": True,
                          "build_isolation": False},
              "wheel": inventory, "reviewed_dependency_wheels": len(dependencies), "installed": installed,
              "loopback": http, "source_sha256": hashes, "temporary_workspace_removed": True, "passed": True}
    serialized = json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n"
    assert all(fragment not in serialized for fragment in ("/Users/", "/private/", "/tmp/", "Bearer "))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        stream.write(serialized)
    print(f"Verified PipeSleuth {version}: {len(UI_ASSETS)} assets, {len(dependencies)} reviewed wheels, "
          f"{len(http['public_http_checks']['responses']) + len(http['api_requests'])} HTTP checks, 6 exact derived rows.")


if __name__ == "__main__":
    main()
