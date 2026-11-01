"""Trusted approval workflow for fixed actions in new derived pipeline runs.

No method is exposed to the model. Approval is a local caller acknowledgement,
not authenticated proof of human review. Original incident files stay read-only.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import time
from uuid import uuid4

from pipesleuth.checkpoints import CheckpointStore
from pipesleuth.evidence import EvidenceError, EvidenceGateway, _check_database_header
from pipesleuth.input_binding import _file_hash, MAX_DATABASE_BYTES
from pipesleuth.pipeline import MAX_SOURCE_BYTES, SandboxPipeline
from pipesleuth.repair_plan import ACTIONS, prepare_repair


class RepairError(ValueError):
    """A repair request is unsupported, stale, unapproved, or uncertain."""


class _BodyFailure(Exception):
    """Carry caller failures through the evidence context's I/O error wrapper."""

    def __init__(self, original):
        self.original = original


_ID = re.compile(r"[0-9a-f]{32}")
_SHA = re.compile(r"[0-9a-f]{64}")
_KIND = "trusted-repair-v1"
_PLAN_KEYS = {"version", "action", "source_run_id", "target_run_id", "data_dir", "output_dir", "state_dir",
              "input", "policies", "effects", "warnings", "created_at"}
_STATE_KEYS = {"version", "status", "plan", "plan_sha256", "input_fingerprint_sha256", "approval", "execution"}


def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def _disjoint(left, right):
    return left != right and left not in right.parents and right not in left.parents


class RepairService:
    """Plan, explicitly acknowledge, then execute one approved derived repair."""

    def __init__(self, data_dir, output_dir, state_dir):
        self.data_dir = SandboxPipeline(data_dir).data_dir
        self.output_dir = SandboxPipeline(output_dir).data_dir
        self.store = CheckpointStore(state_dir)
        self._roots()

    def _roots(self):
        data = SandboxPipeline(self.data_dir).data_dir
        output = SandboxPipeline(self.output_dir).data_dir
        state = CheckpointStore(self.store.root).root
        if data != self.data_dir or output != self.output_dir:
            raise RepairError("repair roots no longer match their configured paths")
        if not all(_disjoint(a, b) for a, b in ((data, output), (data, state), (output, state))):
            raise RepairError("input, output and repair state directories must not overlap")
        return {"kind": _KIND, "data_dir": str(data), "output_dir": str(output), "state_dir": str(state)}

    @contextmanager
    def _locked_input(self, run_id):
        reader = EvidenceGateway(self.data_dir, timeout_seconds=5)
        deadline = time.monotonic() + 5
        try:
            reader._arguments("read_run", {"run_id": run_id})
            with reader._snapshot(run_id, deadline) as (snapshot, directory):
                metadata = reader._metadata(snapshot, run_id)
                if metadata["status"] != "failed":
                    raise RepairError("only a terminal failed incident can be repaired")
                payload = reader._source(directory, metadata, deadline)
                database = directory / "warehouse.sqlite3"
                database_sha, _ = _file_hash(database, MAX_DATABASE_BYTES, deadline)
                fingerprint = {"source_sha256": metadata["source_sha256"], "database_sha256": database_sha}

                def unchanged():
                    # Recheck just before output commit, while the original's
                    # shared SQLite lock still blocks ordinary writer commits.
                    _check_database_header(database)
                    source_sha, _ = _file_hash(directory / "source.json", MAX_SOURCE_BYTES, deadline)
                    current_database_sha, _ = _file_hash(database, MAX_DATABASE_BYTES, deadline)
                    if {"source_sha256": source_sha, "database_sha256": current_database_sha} != fingerprint:
                        raise RepairError("approved input has changed")

                unchanged()
                try:
                    yield payload, fingerprint, unchanged, deadline
                except Exception as error:
                    # The shared evidence reader redacts its own ValueErrors.
                    # Preserve workflow errors such as an uncertain commit.
                    raise _BodyFailure(error) from None
        except _BodyFailure as failure:
            raise failure.original from None
        except RepairError:
            raise
        except (ValueError, OSError, TypeError, KeyError):
            raise RepairError("repair input is unavailable, changed, or unsafe") from None

    def _record(self, identifier):
        record = self.store.load(identifier)
        try:
            if record["binding"] != self._roots():
                raise RepairError("repair root or policy binding does not match")
            state = record["state"]
            if (set(state) != _STATE_KEYS or type(state["version"]) is not int or state["version"] != 1
                    or state["status"] not in {"planned", "approved", "succeeded", "failed"}):
                raise RepairError("repair state is invalid")
            plan = state["plan"]
            if (set(plan) != _PLAN_KEYS or type(plan["version"]) is not int or plan["version"] != 1
                    or plan["action"] not in ACTIONS
                    or plan["data_dir"] != str(self.data_dir) or plan["output_dir"] != str(self.output_dir)
                    or plan["state_dir"] != str(self.store.root)
                    or not isinstance(plan["source_run_id"], str) or _ID.fullmatch(plan["source_run_id"]) is None
                    or not isinstance(plan["target_run_id"], str) or _ID.fullmatch(plan["target_run_id"]) is None
                    or plan["source_run_id"] == plan["target_run_id"]
                    or set(plan["input"]) != {"source_sha256", "database_sha256"}
                    or any(not isinstance(v, str) or _SHA.fullmatch(v) is None for v in plan["input"].values())
                    or set(plan["policies"]) != {"duplicate_policy", "invalid_policy"}
                    or plan["policies"]["duplicate_policy"] not in {"reject", "skip_identical"}
                    or plan["policies"]["invalid_policy"] not in {"reject", "quarantine"}
                    or set(plan["effects"]) != {"rows_loaded", "rows_quarantined", "duplicates_skipped"}
                    or any(type(v) is not int or not 0 <= v <= 10000 for v in plan["effects"].values())
                    or not isinstance(plan["warnings"], list) or len(plan["warnings"]) > 4
                    or any(not isinstance(v, str) or len(v) > 1024 for v in plan["warnings"])
                    or not isinstance(plan["created_at"], str) or len(plan["created_at"]) > 64
                    or state["plan_sha256"] != _digest(plan)
                    or state["input_fingerprint_sha256"] != _digest(plan["input"])):
                raise RepairError("repair plan is invalid or its fingerprint does not match")
            approval = state["approval"]
            if state["status"] == "planned":
                if approval is not None or state["execution"] is not None:
                    raise RepairError("unapproved repair state is invalid")
            else:
                if (not isinstance(approval, dict)
                        or set(approval) != {"plan_sha256", "input_fingerprint_sha256", "origin", "approved_at"}
                        or approval["plan_sha256"] != state["plan_sha256"]
                        or approval["input_fingerprint_sha256"] != state["input_fingerprint_sha256"]
                        or approval["origin"] not in {"trusted_cli", "trusted_api", "scripted_test"}
                        or not isinstance(approval["approved_at"], str)):
                    raise RepairError("approval does not identify this exact plan and input")
                if (state["status"] == "approved") != (state["execution"] is None):
                    raise RepairError("repair execution state is invalid")
            return record
        except RepairError:
            raise
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError):
            raise RepairError("repair record is invalid or incompatible") from None

    @staticmethod
    def _available(record):
        if record["pending"] is not None:
            raise RepairError("repair has an unresolved pending operation; outcome is uncertain")

    @staticmethod
    def _prepare(plan, payload, fingerprint):
        if fingerprint != plan["input"]:
            raise RepairError("approved input fingerprint is stale")
        prepared = prepare_repair(payload, plan["action"])
        if (plan["policies"] != {k: prepared[k] for k in ("duplicate_policy", "invalid_policy")}
                or plan["effects"] != prepared["effects"] or plan["warnings"] != prepared["warnings"]):
            raise RepairError("repair effects or preconditions no longer match the plan")
        return prepared

    def plan(self, run_id, action):
        binding = self._roots()
        with self._locked_input(run_id) as (payload, fingerprint, unchanged, _deadline):
            prepared = prepare_repair(payload, action)
            plan = {"version": 1, "action": action, "source_run_id": run_id, "target_run_id": uuid4().hex,
                    "data_dir": str(self.data_dir), "output_dir": str(self.output_dir),
                    "state_dir": str(self.store.root), "input": fingerprint,
                    "policies": {k: prepared[k] for k in ("duplicate_policy", "invalid_policy")},
                    "effects": prepared["effects"], "warnings": prepared["warnings"], "created_at": _now()}
            state = {"version": 1, "status": "planned", "plan": plan, "plan_sha256": _digest(plan),
                     "input_fingerprint_sha256": _digest(fingerprint), "approval": None, "execution": None}
            unchanged()
            return self.store.create(state, binding)

    def show(self, identifier):
        return self._record(identifier)

    def approve(self, identifier, plan_sha256, input_fingerprint_sha256, *, origin="trusted_cli"):
        record = self._record(identifier)
        self._available(record)
        state = record["state"]
        if (state["status"] != "planned" or not isinstance(origin, str)
                or origin not in {"trusted_cli", "trusted_api", "scripted_test"}
                or plan_sha256 != state["plan_sha256"]
                or input_fingerprint_sha256 != state["input_fingerprint_sha256"]):
            raise RepairError("approval requires the exact planned hashes and an explicit trusted caller")
        with self._locked_input(state["plan"]["source_run_id"]) as (payload, fingerprint, unchanged, _deadline):
            self._prepare(state["plan"], payload, fingerprint)
            claimed = self.store.claim(identifier, record["revision"], "approve_repair")
            unchanged()
            state["approval"] = {"plan_sha256": plan_sha256,
                                 "input_fingerprint_sha256": input_fingerprint_sha256,
                                 "origin": origin, "approved_at": _now()}
            state["status"] = "approved"
            return self.store.complete(identifier, claimed["pending"]["token"], state)

    def execute(self, identifier):
        from pipesleuth.repair_executor import RepairExecutionError, execute_repair
        record = self._record(identifier)
        self._available(record)
        state = record["state"]
        if state["status"] != "approved":
            raise RepairError("repair execution requires an unused exact-plan approval")
        with self._locked_input(state["plan"]["source_run_id"]) as (payload, fingerprint, unchanged, deadline):
            prepared = self._prepare(state["plan"], payload, fingerprint)
            if time.monotonic() >= deadline:
                raise RepairError("repair admission exceeded its time limit")
            claimed = self.store.claim(identifier, record["revision"], "execute_repair")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RepairError("repair admission expired; pending intent retained")
            try:
                receipt = execute_repair(self.output_dir, state["plan"]["target_run_id"], prepared,
                                        timeout_seconds=remaining, pre_commit=unchanged)
            except RepairExecutionError as error:
                receipt = error.receipt
            # Errors outside the observed executor contract leave the intent
            # unresolved. In particular, a failed audit save never repeats output.
            if not isinstance(receipt, dict) or receipt.get("target_run_id") != state["plan"]["target_run_id"]:
                raise RepairError("executor outcome is invalid; pending intent retained")
            if receipt.get("outcome_uncertain") is True:
                raise RepairError("repair commit outcome is uncertain; pending intent retained")
            if (type(receipt.get("committed")) is not bool or type(receipt.get("rolled_back")) is not bool
                    or receipt["committed"] and receipt["rolled_back"]):
                raise RepairError("executor outcome is invalid; pending intent retained")
            try:
                unchanged()
                receipt["post_input_verified"] = True
            except (ValueError, OSError):
                receipt["post_input_verified"] = False
                if receipt["committed"]:
                    # The output may already exist. Do not call this a rollback
                    # or replay the action when the source changes afterwards.
                    receipt["status"] = "failed"
                    receipt["failure_code"] = "input_changed_after_commit"
            verified = (receipt.get("status") == "verified" and receipt["committed"]
                        and not receipt["rolled_back"] and receipt["post_input_verified"]
                        and isinstance(receipt.get("verification"), dict)
                        and receipt.get("verification", {}).get("verified") is True)
            state["execution"], state["status"] = receipt, "succeeded" if verified else "failed"
            return self.store.complete(identifier, claimed["pending"]["token"], state)
