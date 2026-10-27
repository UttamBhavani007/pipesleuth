"""Trusted host deadlines narrow real model HTTP and SQLite evidence reads."""

from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from pipesleuth.evidence import EvidenceError, EvidenceGateway
from pipesleuth.local_model import LocalModelClient, LocalModelConfig, LocalModelError
from pipesleuth.pipeline import SandboxPipeline
from test_local_model_integration import local_server, SCHEMA


class CallTimeoutTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.gateway = EvidenceGateway(self.root / "runs", timeout_seconds=2.0)
        self.client = LocalModelClient(LocalModelConfig(timeout_seconds=2.0))

    def create_run(self):
        pipeline = SandboxPipeline(self.gateway.data_dir)
        source = {"schema": ["order_id", "customer_id", "amount_minor", "currency", "created_at"],
                  "rows": [{"order_id": "O-1", "customer_id": "C-1", "amount_minor": 100,
                            "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"}]}
        run = pipeline.create(source)
        pipeline.run(run.run_id)
        return run.run_id, pipeline.database_path(run.run_id)

    def test_invalid_call_budgets_are_rejected_before_io(self):
        for value in (False, True, 0, -1, 2.001, float("nan"), float("inf"), "0.1", 10**10000):
            with self.subTest(value_type=type(value).__name__):
                with patch.object(self.client, "_health", side_effect=AssertionError("must not connect")):
                    with self.assertRaises(LocalModelError):
                        self.client.generate("system", "prompt", SCHEMA, timeout_seconds=value)
                with patch("pipesleuth.evidence.SandboxPipeline", side_effect=AssertionError("must not read paths")):
                    with self.assertRaises(EvidenceError):
                        self.gateway.call("read_run", {"run_id": "a" * 32}, timeout_seconds=value)
        self.assertFalse(self.gateway.data_dir.exists())
        self.assertEqual(self.client.generation_requests, 0)

    def test_model_override_shares_one_deadline_without_mutating_config(self):
        seen = []
        response = {"model": "qwen3:1.7b", "response": '{"status":"unknown"}',
                    "done": True, "done_reason": "stop", "prompt_eval_count": 7, "eval_count": 3,
                    "total_duration": 100, "load_duration": 10,
                    "prompt_eval_duration": 20, "eval_duration": 60}

        def health(deadline):
            seen.append(deadline)
            return {"digest": "a" * 64}

        def request(method, route, payload, deadline):
            seen.append(deadline)
            return response

        with patch("pipesleuth.local_model.time.monotonic", return_value=10.0), \
                patch.object(self.client, "_health", side_effect=health), \
                patch.object(self.client, "_request", side_effect=request):
            self.client.generate("system", "prompt", SCHEMA, timeout_seconds=0.05)
            self.assertEqual(seen, [10.05, 10.05])
            seen.clear()
            self.client.generate("system", "prompt", SCHEMA, timeout_seconds=None)
            self.assertEqual(seen, [12.0, 12.0])
        self.assertEqual(self.client.config.timeout_seconds, 2.0)
        self.assertEqual(self.client.generation_requests, 2)

    def test_short_model_override_interrupts_real_slow_headers_and_body(self):
        for stage in ("headers", "body"):
            with self.subTest(stage=stage), local_server({"drip": stage}) as (endpoint, state):
                client = LocalModelClient(LocalModelConfig(endpoint=endpoint, timeout_seconds=2.0))
                started = time.monotonic()
                with self.assertRaises(LocalModelError):
                    client.generate("system", "prompt", SCHEMA, timeout_seconds=0.06)
                self.assertLess(time.monotonic() - started, 0.35)
                self.assertEqual(client.config.timeout_seconds, 2.0)
                self.assertEqual(client.generation_requests, 1)
                self.assertEqual(sum(call["path"] == "/api/generate" for call in state["requests"]), 1)

    def test_gateway_override_limits_real_sqlite_busy_wait_and_preserves_files(self):
        run_id, database = self.create_run()
        before = {path.name: (path.read_bytes(), path.stat().st_mtime_ns)
                  for path in database.parent.iterdir()}
        actual_timeouts = []
        real_connect = sqlite3.connect

        def connect(*args, **kwargs):
            actual_timeouts.append(kwargs["timeout"])
            return real_connect(*args, **kwargs)

        with closing(real_connect(database)) as writer:
            writer.execute("BEGIN EXCLUSIVE")
            try:
                started = time.monotonic()
                with patch("pipesleuth.evidence.sqlite3.connect", side_effect=connect):
                    with self.assertRaises(EvidenceError):
                        self.gateway.call("read_run", {"run_id": run_id}, timeout_seconds=0.015)
                self.assertLess(time.monotonic() - started, 0.30)
            finally:
                writer.rollback()
        self.assertEqual(len(actual_timeouts), 1)
        self.assertGreater(actual_timeouts[0], 0)
        self.assertLessEqual(actual_timeouts[0], 0.015)
        self.assertEqual(self.gateway.timeout_seconds, 2.0)
        self.assertEqual({path.name: (path.read_bytes(), path.stat().st_mtime_ns)
                          for path in database.parent.iterdir()}, before)

    def test_timeout_keyword_is_not_a_model_visible_argument_or_fingerprint_field(self):
        run_id, _ = self.create_run()
        first = self.gateway.call("read_run", {"run_id": run_id})
        second = self.gateway.call("read_run", {"run_id": run_id}, timeout_seconds=0.5)
        self.assertEqual(first["data"], second["data"])
        self.assertEqual(first["content_sha256"], second["content_sha256"])
        self.assertEqual(second["arguments"], {"run_id": run_id})
        with self.assertRaises(EvidenceError):
            self.gateway.call("read_run", {"run_id": run_id, "timeout_seconds": 0.5})
        for tool in self.gateway.tools():
            self.assertNotIn("timeout_seconds", tool["parameters"]["properties"])


if __name__ == "__main__":
    unittest.main()
