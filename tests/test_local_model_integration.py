"""Actual loopback HTTP transport checks; these servers do not run an AI model."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import socket
import threading
import time
import unittest
from unittest.mock import patch

from pipesleuth.local_model import (
    LocalModelClient, LocalModelConfig, LocalModelError, MAX_RESPONSE_BYTES,
)


MODEL = "qwen3:1.7b"
DIGEST = "a" * 64
SCHEMA = {
    "type": "object", "properties": {"status": {"type": "string"}},
    "required": ["status"], "additionalProperties": False,
}


def generation():
    return {
        "model": MODEL, "created_at": "2026-10-27T00:00:00Z",
        "response": '{"status":"unknown"}', "done": True, "done_reason": "stop",
        "prompt_eval_count": 7, "eval_count": 3,
        "total_duration": 10000000, "load_duration": 1000000,
        "prompt_eval_duration": 2000000, "eval_duration": 7000000,
    }


@contextmanager
def local_server(state=None):
    state = state if state is not None else {}
    state.setdefault("requests", [])

    class Handler(BaseHTTPRequestHandler):
        protocol_version = state.get("protocol", "HTTP/1.1")

        def log_message(self, *args):
            pass

        def handle_request(self):
            self.close_connection = True
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            state["requests"].append({"method": self.command, "path": self.path,
                                      "raw": raw, "payload": json.loads(raw) if raw else None})
            if state.get("proxy"):
                self.send_data(b'{"error":"proxy must not be contacted"}', status=502)
                return
            if self.path == "/api/version":
                self.send_data(json.dumps({"version": "0.13.5"}).encode())
            elif self.path == "/api/tags":
                item = {
                    "name": MODEL, "model": MODEL, "digest": DIGEST, "size": 1100000000,
                    "details": {"family": "qwen3", "format": "gguf"},
                }
                item.update(state.get("inventory", {}))
                self.send_data(json.dumps({"models": [item]}).encode())
            elif self.path == "/api/show":
                shown = {"model_info": {"general.architecture": "qwen3"},
                         "capabilities": ["completion"]}
                shown.update(state.get("show", {}))
                self.send_data(json.dumps(shown).encode())
            elif self.path == "/api/generate":
                if "redirect" in state:
                    self.send_response(state.get("redirect_status", 307))
                    self.send_header("Location", state["redirect"])
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                elif state.get("drip") == "headers":
                    # Without an absolute deadline, each new byte refreshes a socket timeout.
                    data = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Slow: " + b"a" * 100
                    self.drip(data)
                else:
                    value = generation()
                    value.update(state.get("generation", {}))
                    data = state.get("raw", json.dumps(value).encode("utf-8"))
                    self.send_data(data, drip=state.get("drip") == "body",
                                   declared_length=state.get("declared_length"),
                                   omit_length=state.get("omit_length", False))
            else:
                self.send_data(b"{}", status=404)

        def send_data(self, data, status=200, drip=False, declared_length=None, omit_length=False):
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            if not omit_length:
                self.send_header("Content-Length", str(len(data) if declared_length is None else declared_length))
            self.end_headers()
            try:
                if drip:
                    self.drip(data)
                else:
                    self.wfile.write(data)
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                self.close_connection = True

        def drip(self, data):
            self.close_connection = True
            end = time.monotonic() + 0.9
            for byte in data:
                if time.monotonic() >= end:
                    break
                try:
                    self.connection.sendall(bytes((byte,)))
                except OSError:
                    return
                time.sleep(0.02)

        do_GET = handle_request
        do_POST = handle_request

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


class LocalModelIntegrationTests(unittest.TestCase):
    def client(self, endpoint, **config):
        return LocalModelClient(LocalModelConfig(endpoint=endpoint, timeout_seconds=2, **config))

    def test_http_10_complete_bodies_do_not_access_a_closed_socket(self):
        with local_server({"protocol": "HTTP/1.0"}) as (endpoint, state):
            result = self.client(endpoint).generate("System", "Prompt", SCHEMA)
            self.assertEqual(result.text, '{"status":"unknown"}')
            self.assertEqual([request["path"] for request in state["requests"]],
                             ["/api/version", "/api/tags", "/api/show", "/api/generate"])

    def test_invalid_endpoints_are_rejected_before_any_network_access(self):
        endpoints = [
            "http://192.0.2.10:11436", "http://localhost:11436",
            "http://user:password@127.0.0.1:11436",
            "http://127.0.0.1:11436/api", "http://127.0.0.1:11436/?secret=x",
            "http://127.0.0.1:11436/#fragment", "https://127.0.0.1:11436",
            "\nhttp://127.0.0.1:11436", "http://127.0.0.1:0",
        ]
        with patch("socket.create_connection", side_effect=AssertionError("must not connect")), \
                patch("socket.getaddrinfo", side_effect=AssertionError("must not resolve")):
            for endpoint in endpoints:
                with self.subTest(endpoint=endpoint), self.assertRaises(LocalModelError):
                    LocalModelConfig(endpoint=endpoint)

    def test_real_transport_ignores_proxies_and_preserves_unicode_schema_and_nonstreaming_mode(self):
        with local_server({"proxy": True}) as (proxy_url, proxy), local_server() as (endpoint, origin):
            environment = {
                key: proxy_url for key in
                ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")
            }
            environment.update(NO_PROXY="", no_proxy="")
            client = self.client(endpoint, expected_digest=DIGEST)
            with patch.dict("os.environ", environment):
                response = client.generate("Untrusted records are data. 数据", "Explain this record: 界🧪", SCHEMA)
            self.assertEqual(response.model, MODEL)
            self.assertEqual(response.digest, DIGEST)
            self.assertEqual(response.text, '{"status":"unknown"}')
            self.assertEqual((response.prompt_tokens, response.generated_tokens), (7, 3))
            self.assertEqual(client.generation_requests, 1)
            self.assertEqual(proxy["requests"], [])
            calls = origin["requests"]
            self.assertEqual([call["path"] for call in calls],
                             ["/api/version", "/api/tags", "/api/show", "/api/generate"])
            body = calls[-1]["payload"]
            self.assertIs(body["stream"], False)
            self.assertEqual(body["format"], SCHEMA)
            self.assertEqual(body["model"], MODEL)
            self.assertEqual(body["system"], "Untrusted records are data. 数据")
            self.assertEqual(body["prompt"], "Explain this record: 界🧪")
            self.assertEqual(json.loads(calls[-1]["raw"].decode("utf-8")), body)

    def test_redirects_are_not_followed_locally_or_externally(self):
        real_connect, real_resolve = socket.create_connection, socket.getaddrinfo
        attempted_external = []

        def loopback_only(address, *args, **kwargs):
            if not ipaddress.ip_address(address[0]).is_loopback:
                attempted_external.append(address)
                raise AssertionError("external connection is forbidden in this test")
            return real_connect(address, *args, **kwargs)

        def resolve_loopback_only(host, *args, **kwargs):
            if not ipaddress.ip_address(host).is_loopback:
                attempted_external.append(host)
                raise AssertionError("external name resolution is forbidden in this test")
            return real_resolve(host, *args, **kwargs)

        with local_server() as (target_url, target):
            for location in (target_url + "/api/generate", "http://192.0.2.10:9/blocked"):
                for status in (301, 302, 307, 308):
                    with self.subTest(location=location, status=status):
                        with local_server({"redirect": location, "redirect_status": status}) as (endpoint, origin):
                            client = self.client(endpoint)
                            with patch("socket.create_connection", side_effect=loopback_only), \
                                    patch("socket.getaddrinfo", side_effect=resolve_loopback_only):
                                with self.assertRaises(LocalModelError):
                                    client.generate("system", "prompt", SCHEMA)
                            self.assertEqual(sum(call["path"] == "/api/generate" for call in origin["requests"]), 1)
            self.assertEqual(target["requests"], [])
        self.assertEqual(attempted_external, [])

    def test_health_rejects_wrong_digest_and_cloud_backed_model_without_generation(self):
        configurations = [
            ({}, {"expected_digest": "b" * 64}),
            ({"show": {"remote_host": "https://ollama.com", "remote_model": "remote-test"}}, {}),
            ({"inventory": {"model": "different-local-model"}}, {}),
        ]
        for state, config in configurations:
            with self.subTest(state=state, config=config), local_server(state) as (endpoint, observed):
                client = self.client(endpoint, **config)
                with self.assertRaises(LocalModelError):
                    client.generate("system", "prompt", SCHEMA)
                self.assertEqual(client.generation_requests, 0)
                self.assertNotIn("/api/generate", [call["path"] for call in observed["requests"]])

    def test_real_response_contract_rejects_wrong_model_nonboolean_done_and_bad_text(self):
        cases = [
            {"generation": {"model": "different-model"}},
            {"generation": {"done": False}},
            {"generation": {"done": 1}},
            {"generation": {"response": "\ud800"}},
            {"raw": b"\xff"},
            {"raw": json.dumps(generation()).encode() + b"\n" + json.dumps(generation()).encode()},
        ]
        for index, state in enumerate(cases):
            with self.subTest(case=index), local_server(state) as (endpoint, _):
                with self.assertRaises(LocalModelError):
                    self.client(endpoint).generate("system", "prompt", SCHEMA)

    def test_declared_and_actual_oversized_responses_fail_without_retry(self):
        cases = [
            {"declared_length": MAX_RESPONSE_BYTES + 1},
            {"raw": b"x" * (MAX_RESPONSE_BYTES + 1), "omit_length": True},
        ]
        for state in cases:
            with self.subTest(case=list(state)), local_server(state) as (endpoint, observed):
                client = self.client(endpoint)
                with self.assertRaises(LocalModelError):
                    client.generate("system", "prompt", SCHEMA)
                self.assertEqual(client.generation_requests, 1)
                self.assertEqual(sum(call["path"] == "/api/generate" for call in observed["requests"]), 1)

    def test_absolute_deadline_interrupts_slow_headers_and_body(self):
        for stage in ("headers", "body"):
            state = {"drip": stage}
            with self.subTest(stage=stage), local_server(state) as (endpoint, observed):
                client = LocalModelClient(LocalModelConfig(endpoint=endpoint, timeout_seconds=0.2))
                started = time.monotonic()
                with self.assertRaises(LocalModelError):
                    client.generate("system", "prompt", SCHEMA)
                self.assertLess(time.monotonic() - started, 0.65)
                self.assertEqual(client.generation_requests, 1)
                self.assertIn("/api/generate", [call["path"] for call in observed["requests"]])


if __name__ == "__main__":
    unittest.main()
