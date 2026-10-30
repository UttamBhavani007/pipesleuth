"""Trusted durable coordination around the read-only investigation engine.

Claims make a crash during an operation explicitly uncertain. They are never
automatically taken over or replayed. Fingerprints are checked around each
observed operation; this is not a lock against another local writer.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
import time

from pipesleuth.checkpoints import CheckpointError
from pipesleuth.evidence import EvidenceGateway
from pipesleuth.input_binding import InputBindingError, fingerprint_input
from pipesleuth.investigator import BoundedInvestigator, InvestigatorError
from pipesleuth.local_model import LocalModelConfig
from pipesleuth.pipeline import SandboxPipeline, PipelineError


POLICY_VERSION = "durable-agent-v1"
_SHA = re.compile(r"[0-9a-f]{64}")


class PersistentError(ValueError):
    """An unsafe, stale, mismatched or uncertain saved investigation."""


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


class PersistentInvestigator:
    """Save exact cursors, bind inputs/configuration, and admit one operation at a time."""

    def __init__(self, engine, store, *, retry_limit=1):
        if not isinstance(engine, BoundedInvestigator):
            raise PersistentError("engine must be a BoundedInvestigator")
        if type(retry_limit) is not int or not 0 <= retry_limit <= 1:
            raise PersistentError("retry_limit must be zero or one")
        self.engine, self.store, self.retry_limit = engine, store, retry_limit
        try:
            self.data_dir = SandboxPipeline(engine.gateway.data_dir).data_dir
            root = Path(store.root)
            if root == self.data_dir or root in self.data_dir.parents or self.data_dir in root.parents:
                raise PersistentError("checkpoint and pipeline roots must not overlap")
            self._config()
        except (AttributeError, TypeError, PipelineError, OSError, InvestigatorError):
            raise PersistentError("persistent investigation configuration is invalid") from None

    def _config(self):
        current_data_dir = SandboxPipeline(self.engine.gateway.data_dir).data_dir
        current_store_root = Path(self.store.root).resolve()
        if current_data_dir != self.data_dir:
            raise PersistentError("pipeline root no longer matches the saved configuration")
        if (current_store_root == current_data_dir or current_store_root in current_data_dir.parents
                or current_data_dir in current_store_root.parents):
            raise PersistentError("checkpoint and pipeline roots must not overlap")
        config = self.engine.client.config
        # Reconstruct validated trusted settings rather than accepting a mutated
        # dataclass or an unbounded component configuration.
        model = LocalModelConfig(endpoint=config.endpoint, model=config.model,
                                 expected_digest=config.expected_digest,
                                 timeout_seconds=config.timeout_seconds, num_ctx=config.num_ctx,
                                 max_output_tokens=config.max_output_tokens)
        if model.expected_digest is None:
            raise PersistentError("persistent investigations require an explicit model digest pin")
        checked_gateway = EvidenceGateway(self.data_dir,
            timeout_seconds=self.engine.gateway.timeout_seconds,
            max_response_bytes=self.engine.gateway.max_response_bytes)
        BoundedInvestigator(checked_gateway, self.engine.client, max_steps=self.engine.max_steps,
                            max_tool_calls=self.engine.max_tool_calls,
                            timeout_seconds=self.engine.timeout_seconds)
        return {"endpoint": model.endpoint, "model": model.model,
                "expected_digest": model.expected_digest, "timeout_seconds": float(model.timeout_seconds),
                "num_ctx": model.num_ctx, "max_output_tokens": model.max_output_tokens,
                "max_steps": self.engine.max_steps, "max_tool_calls": self.engine.max_tool_calls,
                "agent_timeout": float(self.engine.timeout_seconds), "retry_limit": self.retry_limit,
                "gateway_timeout_seconds": float(checked_gateway.timeout_seconds),
                "gateway_max_response_bytes": checked_gateway.max_response_bytes}

    def _fingerprint(self, run_id):
        try:
            value = fingerprint_input(self.engine.gateway, run_id)
            if (not isinstance(value, dict) or set(value) != {"source_sha256", "database_sha256"}
                    or any(not isinstance(item, str) or _SHA.fullmatch(item) is None for item in value.values())):
                raise InputBindingError("invalid input fingerprint")
            return value
        except (InputBindingError, OSError):
            raise PersistentError("input fingerprint is unavailable or invalid") from None

    def _binding(self, run_id):
        return {"version": 1, "policy": POLICY_VERSION, "data_dir": str(self.data_dir),
                "input": self._fingerprint(run_id), "config": self._config()}

    def _validate_record(self, record):
        try:
            binding = record["binding"]
            if (not isinstance(binding, dict) or set(binding) != {"version", "policy", "data_dir", "input", "config"}
                    or type(binding["version"]) is not int or binding["version"] != 1
                    or binding["policy"] != POLICY_VERSION or binding["data_dir"] != str(self.data_dir)
                    or _canonical(binding["config"]) != _canonical(self._config())
                    or not isinstance(binding["input"], dict)
                    or set(binding["input"]) != {"source_sha256", "database_sha256"}
                    or any(not isinstance(value, str) or _SHA.fullmatch(value) is None for value in binding["input"].values())):
                raise PersistentError("saved investigation policy, configuration or input binding does not match")
            state = self.engine.validate_state(record["state"])
            if record["pending"] is not None:
                raise PersistentError("investigation has an unresolved pending operation; outcome is uncertain")
            return state
        except PersistentError:
            raise
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError):
            raise PersistentError("saved investigation is invalid or incompatible") from None

    @staticmethod
    def _charge(state, elapsed):
        state["elapsed_seconds"] = elapsed
        state["result"]["elapsed_ms"] = round(elapsed * 1000, 3)

    def _expire(self, state):
        if state["elapsed_seconds"] >= self.engine.timeout_seconds:
            self.engine._fail(state["result"], "elapsed_budget_exceeded")
            state["done"], state["next"] = True, None

    def start(self, run_id):
        """Create a bound initial checkpoint; do not run an evidence/model operation."""
        started = time.monotonic()
        state = self.engine.initial_state(run_id)
        binding = self._binding(run_id)
        self._charge(state, time.monotonic() - started)
        self._expire(state)
        return self.store.create(state, binding)

    def show(self, investigation_id):
        """Read the saved record only, without evidence or model construction/calls."""
        return self.store.load(investigation_id)

    def resume(self, investigation_id, *, max_operations=None):
        """Claim, advance and commit each operation, or stop at an explicit pause."""
        if max_operations is not None and (type(max_operations) is not int or not 1 <= max_operations <= 16):
            raise PersistentError("max_operations must be an integer between 1 and 16")
        operation_started = time.monotonic()
        record = self.store.load(investigation_id)
        state = self._validate_record(record)
        if state["done"]:
            return record
        operations = 0
        while not state["done"] and (max_operations is None or operations < max_operations):
            prior_elapsed = state["elapsed_seconds"]
            # Pre-operation mismatch rejects the unchanged record before claim or
            # any tool/model admission. Rejected checks do not rewrite history.
            if self._fingerprint(state["result"]["run_id"]) != record["binding"]["input"]:
                raise PersistentError("saved investigation input has changed")
            cursor = state["next"]
            operation = "generate" if cursor["action"] == "generate" else "tool:" + cursor["tool"]
            claimed = self.store.claim(investigation_id, record["revision"], operation)
            token = claimed["pending"]["token"]
            self._charge(state, prior_elapsed + time.monotonic() - operation_started)
            # Unexpected errors from this point leave the pending intent intact.
            # They are not converted into a silent retry of an unknown outcome.
            advanced = self.engine.advance(state, retry_limit=self.retry_limit)
            try:
                current = self._fingerprint(advanced["result"]["run_id"])
                failure = "stale_input" if current != record["binding"]["input"] else None
            except PersistentError:
                failure = "input_check_unavailable"
            if failure:
                self.engine._fail(advanced["result"], failure)
                advanced["done"], advanced["next"] = True, None
            self._charge(advanced, prior_elapsed + time.monotonic() - operation_started)
            self._expire(advanced)
            self.engine._compact(advanced["result"])
            self._charge(advanced, prior_elapsed + time.monotonic() - operation_started)
            self._expire(advanced)
            self.engine.validate_state(advanced)
            checkpoint_started = time.monotonic()
            record = self.store.complete(investigation_id, token, advanced)
            operations += 1
            state = self._validate_record(record)
            # Charge completion latency in the next accepted operation. The last
            # checkpoint write cannot persist its own duration; it is excluded.
            operation_started = checkpoint_started
        return record
