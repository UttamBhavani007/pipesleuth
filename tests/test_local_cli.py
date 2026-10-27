"""Exercise CLI wiring with controlled local HTTP fixtures, not real model results."""

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


class LocalCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.data_dir = Path(self.temporary.name).resolve() / "runs"
        self.pipeline = SandboxPipeline(self.data_dir)
        created = self.pipeline.create(generate_fixture("healthy", row_count=3).to_source_payload())
        self.run_id = self.pipeline.run(created.run_id).run_id

    def snapshot(self):
        return {str(path.relative_to(self.data_dir)): (path.read_bytes(), path.stat().st_mtime_ns)
                for path in self.data_dir.rglob("*") if path.is_file()}

    def server(self, unavailable=False):
        calls = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def send_json(self, value, status=200):
                body = json.dumps(value).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                calls.append(self.path)
                if unavailable:
                    self.send_json({"error": "private server detail"}, 503)
                elif self.path == "/api/version":
                    self.send_json({"version": "0.0.1"})
                else:
                    self.send_json({"models": [{"name": "qwen3:1.7b", "digest": "a" * 64, "size": 1}]})

            def do_POST(self):
                calls.append(self.path)
                self.rfile.read(int(self.headers["Content-Length"]))
                if self.path == "/api/show":
                    self.send_json({"model_info": {"general.architecture": "qwen3"},
                                    "capabilities": ["completion"]})
                else:
                    self.send_json({"model": "qwen3:1.7b", "done": True, "done_reason": "stop",
                                    "response": '{"status":"no_incident","findings":[]}',
                                    "prompt_eval_count": 100, "eval_count": 20,
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
        return f"http://127.0.0.1:{server.server_port}", calls

    def cli(self, run_id, endpoint, *extra):
        return subprocess.run(
            [sys.executable, "-m", "pipesleuth", "diagnose", run_id, "--method", "local",
             "--endpoint", endpoint, "--model-timeout", "2", "--data-dir", str(self.data_dir), *extra],
            cwd=ROOT, env=dict(os.environ, PYTHONPATH=str(ROOT / "src"), PYTHONDONTWRITEBYTECODE="1"),
            text=True, capture_output=True, timeout=10,
        )

    def test_fixture_completion_uses_one_generation_and_preserves_files(self):
        endpoint, calls = self.server()
        before = self.snapshot()
        result = self.cli(self.run_id, endpoint)
        self.assertEqual(result.returncode, 0, result.stderr)
        value = json.loads(result.stdout)
        self.assertEqual(value["method"], "single_call_local_model")
        self.assertEqual((value["status"], value["tool_calls"], value["model_calls"]), ("no_incident", 4, 1))
        self.assertFalse(value["semantic_support_verified"])
        self.assertEqual(calls.count("/api/generate"), 1)
        self.assertEqual(self.snapshot(), before)

    def test_unavailable_runtime_is_unknown_without_inference_or_private_error_text(self):
        endpoint, calls = self.server(unavailable=True)
        before = self.snapshot()
        result = self.cli(self.run_id, endpoint)
        self.assertEqual(result.returncode, 1, result.stderr)
        value = json.loads(result.stdout)
        self.assertEqual((value["status"], value["model_calls"]), ("unknown", 0))
        self.assertNotIn("/api/generate", calls)
        self.assertNotIn("private server detail", result.stdout)
        self.assertEqual(self.snapshot(), before)

    def test_missing_evidence_does_not_contact_the_runtime(self):
        endpoint, calls = self.server()
        result = self.cli("0" * 32, endpoint)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(json.loads(result.stdout)["model_calls"], 0)
        self.assertEqual(calls, [])

    def test_remote_or_ambiguous_endpoints_are_rejected_before_network_access(self):
        before = self.snapshot()
        for endpoint in ("http://example.invalid:11436", "http://localhost:11436",
                         "http://127.0.0.1:11436/remote", "http://127.0.0.1:11436/?x=1"):
            with self.subTest(endpoint=endpoint):
                result = self.cli(self.run_id, endpoint)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertNotIn("Traceback", result.stderr)
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
