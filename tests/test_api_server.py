"""Owned foreground CLI, private sessions and configuration failure checks."""

from contextlib import redirect_stdout
import http.client
import io
import json
import os
from pathlib import Path
import socket
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

try:
    import fastapi
    import uvicorn
except ImportError:
    raise unittest.SkipTest('install requirements-api.txt for optional API checks')

from pipesleuth.api_server import ApiServerError, clean_environment, remove_owned_session, serve_api, write_session
from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import SandboxPipeline

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]


class ApiServerTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.data, self.output, self.invest, self.repairs = (self.root / n for n in ('data', 'out', 'invest', 'repairs'))
        self.session_path = self.root / 'private' / 'session.json'

    def command(self, port, *extra):
        return [sys.executable, '-m', 'pipesleuth', 'serve', '--port', str(port),
                '--data-dir', str(self.data), '--output-dir', str(self.output),
                '--investigation-state-dir', str(self.invest), '--repair-state-dir', str(self.repairs),
                '--session-file', str(self.session_path), *extra]

    def environment(self):
        return dict(os.environ, PYTHONPATH=str(ROOT / 'src'), PYTHONDONTWRITEBYTECODE='1',
                    OPENAI_API_KEY='controlled-provider-secret', HTTP_PROXY='http://invalid.test:9')

    def test_environment_preserves_platform_home_and_removes_provider_proxy_and_telemetry_settings(self):
        original = {'HOME': '/unchanged-user-home', 'PATH': '/usr/bin', 'HTTP_PROXY': 'private',
                    'OPENAI_API_KEY': 'private', 'OLLAMA_HOST': 'remote', 'OTEL_EXPORTER_OTLP_ENDPOINT': 'remote'}
        clean = clean_environment(original)
        self.assertEqual(clean['HOME'], original['HOME'])
        self.assertEqual(clean['PATH'], original['PATH'])
        self.assertEqual(clean['OTEL_SDK_DISABLED'], 'true')
        self.assertFalse(set(clean) & {'HTTP_PROXY', 'OPENAI_API_KEY', 'OLLAMA_HOST', 'OTEL_EXPORTER_OTLP_ENDPOINT'})

    def test_session_mode_tokens_exclusive_write_and_conditional_cleanup(self):
        path, value, identity = write_session(self.session_path, 8765)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        tokens = [value[key] for key in ('read_token', 'operation_token', 'approval_token')]
        self.assertEqual(len(set(tokens)), 3)
        self.assertTrue(all(len(token) >= 32 for token in tokens))
        before = path.read_bytes()
        with self.assertRaises(ApiServerError):
            write_session(path, 8765)
        self.assertEqual(path.read_bytes(), before)
        path.write_text('owner changed this artifact')
        self.assertFalse(remove_owned_session(path, identity))
        self.assertEqual(path.read_text(), 'owner changed this artifact')
        path.unlink()
        path, _, identity = write_session(path, 8765)
        self.assertTrue(remove_owned_session(path, identity))
        self.assertFalse(path.exists())

    def test_session_symlinks_are_not_written_or_removed(self):
        target = self.root / 'owner-file'
        target.write_text('preserve')
        self.session_path.parent.mkdir()
        self.session_path.symlink_to(target)
        with self.assertRaises(ApiServerError):
            write_session(self.session_path, 8765)
        self.assertFalse(remove_owned_session(self.session_path, (0, 0, 'invalid')))
        self.assertEqual(target.read_text(), 'preserve')

    def test_session_ancestor_and_invalid_configuration_fail_before_artifacts(self):
        ancestor = self.root / 'would-be-file'
        result = subprocess.run(self.command(free_port(), '--session-file', str(ancestor),
            '--data-dir', str(ancestor / 'data')), cwd=ROOT, env=self.environment(), capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 2)
        self.assertFalse(ancestor.exists())
        for option, value in (('--port', '1'), ('--port', '65536'), ('--model-endpoint', 'http://example.invalid:11436')):
            with self.subTest(option=option):
                result = subprocess.run(self.command(free_port(), option, value), cwd=ROOT,
                    env=self.environment(), capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 2)
                self.assertNotIn('Traceback', result.stderr)
                self.assertFalse(self.session_path.exists())

    def test_occupied_port_is_not_reused_or_given_a_session(self):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen()
            result = subprocess.run(self.command(listener.getsockname()[1]), cwd=ROOT,
                env=self.environment(), capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 2)
            self.assertFalse(self.session_path.exists())
            self.assertNotIn('controlled-provider-secret', result.stdout + result.stderr)

    def test_startup_failure_is_reported_and_owned_session_cleaned(self):
        class FailedServer:
            started = False
            def __init__(self, config): pass
            def run(self, **_options): pass
        with patch.dict(os.environ), patch.object(uvicorn, 'Server', FailedServer), redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ApiServerError, 'startup'):
                serve_api(data_dir=self.data, output_dir=self.output, investigation_state_dir=self.invest,
                          repair_state_dir=self.repairs, session_file=self.session_path, port=free_port())
        self.assertFalse(self.session_path.exists())

    def test_actual_foreground_server_has_private_authority_and_graceful_cleanup(self):
        pipeline = SandboxPipeline(self.data)
        created = pipeline.create(generate_fixture('healthy', row_count=2).to_source_payload())
        run_id = pipeline.run(created.run_id).run_id
        port = free_port()
        process = subprocess.Popen(self.command(port), cwd=ROOT, env=self.environment(),
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        session = None
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    self.fail('owned API exited during startup')
                try:
                    session = json.loads(self.session_path.read_text())
                    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=1)
                    try:
                        connection.request('GET', '/health')
                        response = connection.getresponse()
                        if response.status == 200:
                            response.read()
                            break
                    finally:
                        connection.close()
                except (FileNotFoundError, ConnectionError, OSError, ValueError):
                    time.sleep(0.02)
            else:
                self.fail('owned API startup deadline exceeded')
            connection = http.client.HTTPConnection('127.0.0.1', port, timeout=2)
            try:
                connection.request('GET', '/v1/runs/' + run_id,
                    headers={'Authorization': 'Bearer ' + session['read_token']})
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read())['rows_loaded'], 2)
            finally:
                connection.close()
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                output, errors = process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                output, errors = process.communicate(timeout=3)
                self.fail('owned server did not stop within deadline')
        self.assertEqual(process.returncode, 0, errors)
        self.assertFalse(self.session_path.exists())
        for token in (session['read_token'], session['operation_token'], session['approval_token'], 'controlled-provider-secret'):
            self.assertNotIn(token, output + errors)
        with socket.socket() as probe:
            self.assertNotEqual(probe.connect_ex(('127.0.0.1', port)), 0)


if __name__ == '__main__':
    unittest.main()
