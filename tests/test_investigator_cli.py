"""Real CLI and loopback HTTP fixtures; these fixtures are not AI model runs."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import SandboxPipeline


ROOT = Path(__file__).resolve().parents[1]


class InvestigatorCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.data_dir = Path(self.temporary.name).resolve() / "runs"
        self.pipeline = SandboxPipeline(self.data_dir)
        created = self.pipeline.create(generate_fixture("healthy", row_count=2).to_source_payload())
        self.run_id = self.pipeline.run(created.run_id).run_id

    def snapshot(self):
        return {str(path.relative_to(self.data_dir)): (path.read_bytes(), path.stat().st_mtime_ns)
                for path in self.data_dir.rglob("*") if path.is_file()}

    def server(self, malicious=False):
        state = {"calls": [], "generations": 0}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def send_json(self, value):
                body = json.dumps(value).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                state["calls"].append(self.path)
                if self.path == "/api/version":
                    self.send_json({"version": "0.0.1"})
                else:
                    self.send_json({"models": [{"name": "qwen3:1.7b", "digest": "a" * 64, "size": 1}]})

            def do_POST(self):
                state["calls"].append(self.path)
                self.rfile.read(int(self.headers["Content-Length"]))
                if self.path == "/api/show":
                    self.send_json({"model_info": {"general.architecture": "qwen3"},
                                    "capabilities": ["completion"]})
                    return
                state["generations"] += 1
                if malicious:
                    decision = {"action": "tool", "tool": "execute_sql", "approval": True}
                elif state["generations"] == 1:
                    decision = {"action": "tool", "tool": "read_logs"}
                else:
                    decision = {"action": "finish", "diagnosis": {"status": "no_incident", "findings": []}}
                self.send_json({"model": "qwen3:1.7b", "done": True, "done_reason": "stop",
                                "response": json.dumps(decision), "prompt_eval_count": 100, "eval_count": 30,
                                "total_duration": 4000, "load_duration": 1000,
                                "prompt_eval_duration": 1000, "eval_duration": 1000})

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()

        def cleanup():
            server.shutdown()
            server.server_close()
            thread.join(timeout=1)

        self.addCleanup(cleanup)
        return f"http://127.0.0.1:{server.server_port}", state

    def cli(self, run_id, endpoint, *extra):
        return subprocess.run(
            [sys.executable, "-m", "pipesleuth", "diagnose", run_id, "--method", "agent",
             "--endpoint", endpoint, "--data-dir", str(self.data_dir), *extra],
            cwd=ROOT, env=dict(os.environ, PYTHONPATH=str(ROOT / "src"), PYTHONDONTWRITEBYTECODE="1"),
            text=True, capture_output=True, timeout=10,
        )

    def test_adaptive_fixture_reads_then_finishes_with_a_bounded_trace(self):
        endpoint, state = self.server()
        before = self.snapshot()
        response = self.cli(self.run_id, endpoint)
        self.assertEqual(response.returncode, 0, response.stderr)
        value = json.loads(response.stdout)
        self.assertEqual((value["method"], value["status"]), ("bounded_local_agent", "no_incident"))
        self.assertEqual((value["tool_calls"], value["model_calls"], value["steps"]), (2, 2, 2))
        self.assertEqual(state["generations"], 2)
        self.assertTrue(value["trace"])
        self.assertFalse(value["semantic_support_verified"])
        self.assertLessEqual(len(response.stdout.rstrip("\n").encode()), 128 * 1024)
        self.assertEqual(self.snapshot(), before)

    def test_a_single_tool_budget_stops_without_contacting_the_model(self):
        endpoint, state = self.server()
        before = self.snapshot()
        response = self.cli(self.run_id, endpoint, "--max-tool-calls", "1")
        self.assertEqual(response.returncode, 1, response.stderr)
        value = json.loads(response.stdout)
        self.assertEqual((value["status"], value["tool_calls"], value["model_calls"]), ("unknown", 1, 0))
        self.assertEqual(state["calls"], [])
        self.assertEqual(self.snapshot(), before)

    def test_step_budget_keeps_observations_and_stops_after_one_generation(self):
        endpoint, state = self.server()
        before = self.snapshot()
        response = self.cli(self.run_id, endpoint, "--max-steps", "1")
        self.assertEqual(response.returncode, 1, response.stderr)
        value = json.loads(response.stdout)
        self.assertEqual((value["status"], value["model_calls"], value["steps"]), ("unknown", 1, 1))
        self.assertEqual(state["generations"], 1)
        self.assertTrue(value["trace"])
        self.assertEqual(self.snapshot(), before)

    def test_unknown_tool_and_approval_fields_never_change_the_database(self):
        endpoint, state = self.server(malicious=True)
        before = self.snapshot()
        response = self.cli(self.run_id, endpoint)
        self.assertEqual(response.returncode, 1, response.stderr)
        value = json.loads(response.stdout)
        self.assertEqual((value["status"], value["tool_calls"], value["model_calls"]), ("unknown", 1, 1))
        self.assertEqual(state["generations"], 1)
        self.assertEqual(value["findings"], [])
        self.assertEqual(self.snapshot(), before)

    def test_invalid_budgets_and_missing_run_do_not_contact_the_model(self):
        endpoint, state = self.server()
        before = self.snapshot()
        for option, argument in (("--max-steps", "0"), ("--max-steps", "9"),
                                 ("--max-tool-calls", "5"), ("--agent-timeout", "0"),
                                 ("--agent-timeout", "nan"), ("--agent-timeout", "121")):
            with self.subTest(option=option, argument=argument):
                response = self.cli(self.run_id, endpoint, option, argument)
                self.assertEqual(response.returncode, 2)
                self.assertEqual(response.stdout, "")
                self.assertNotIn("Traceback", response.stderr)
        missing = self.cli("0" * 32, endpoint)
        self.assertEqual(missing.returncode, 1, missing.stderr)
        self.assertEqual(json.loads(missing.stdout)["model_calls"], 0)
        self.assertEqual(state["calls"], [])
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
