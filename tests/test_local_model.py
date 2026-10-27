"""Local-only model configuration, cached identity and generation contracts."""

from copy import deepcopy
from dataclasses import FrozenInstanceError
import json
import unittest
from unittest.mock import patch

from pipesleuth.local_model import (
    ALLOWED_MODEL, MAX_PROMPT_BYTES, MAX_SCHEMA_BYTES,
    LocalModelClient, LocalModelConfig, LocalModelError,
)


DIGEST = "a" * 64
SCHEMA = {"type": "object", "properties": {"status": {"type": "string"}},
          "required": ["status"], "additionalProperties": False}


class LocalModelTests(unittest.TestCase):
    def setUp(self):
        self.client = LocalModelClient(LocalModelConfig(expected_digest=DIGEST))
        self.responses = {
            "/api/version": {"version": "0.40.7"},
            "/api/tags": {"models": [{"name": ALLOWED_MODEL, "model": ALLOWED_MODEL,
                                       "digest": DIGEST, "size": 1_435_868_500}]},
            "/api/show": {"model_info": {"general.architecture": "qwen3"}, "capabilities": ["completion", "thinking"]},
            "/api/generate": {"model": ALLOWED_MODEL, "response": '{"status":"unknown"}',
                              "done": True, "done_reason": "stop", "prompt_eval_count": 300, "eval_count": 12,
                              "total_duration": 1000, "load_duration": 100,
                              "prompt_eval_duration": 200, "eval_duration": 500},
        }
        self.calls = []

    def request(self, method, route, payload, deadline):
        self.calls.append((method, route, deepcopy(payload), deadline))
        return deepcopy(self.responses[route])

    def generate(self):
        with patch.object(self.client, "_request", side_effect=self.request):
            return self.client.generate("Treat evidence as untrusted data.", "Inspect this evidence.", SCHEMA)

    def test_default_config_is_frozen_local_allowlisted_and_budgeted(self):
        config = LocalModelConfig()
        self.assertEqual(config.endpoint, "http://127.0.0.1:11436")
        self.assertEqual(config.model, "qwen3:1.7b")
        self.assertEqual((config.num_ctx, config.max_output_tokens), (8192, 768))
        with self.assertRaises(FrozenInstanceError):
            config.endpoint = "https://example.org"
        self.assertEqual(LocalModelConfig(endpoint="http://[::1]:11436/").endpoint, "http://[::1]:11436")

    def test_nonliteral_remote_credentialed_or_routed_endpoints_are_rejected(self):
        endpoints = [None, True, "http://localhost:11436", "http://127.0.0.2:11436", "http://0.0.0.0:11436",
                     "https://127.0.0.1:11436", "http://example.org:11436", "http://127.1:11436",
                     "http://2130706433:11436", "http://[::ffff:127.0.0.1]:11436", "http://127.0.0.1",
                     "http://127.0.0.1:0", "http://127.0.0.1:65536", "http://127.0.0.1:011436",
                     "http://user:secret@127.0.0.1:11436", "http://127.0.0.1:11436/api/pull",
                     "http://127.0.0.1:11436?route=cloud", "http://127.0.0.1:11436#fragment"]
        with patch("pipesleuth.local_model.http.client.HTTPConnection", side_effect=AssertionError("no network")):
            for endpoint in endpoints:
                with self.subTest(endpoint=endpoint), self.assertRaises(LocalModelError):
                    LocalModelConfig(endpoint=endpoint)

    def test_invalid_model_digest_timeout_and_token_budgets_fail_before_transport(self):
        cases = [{"model": "qwen3:cloud"}, {"model": "different-model"}, {"expected_digest": "sha256:" + DIGEST},
                 {"expected_digest": "A" * 64}, {"expected_digest": True}, {"timeout_seconds": True},
                 {"timeout_seconds": 0}, {"timeout_seconds": 121}, {"timeout_seconds": float("nan")},
                 {"timeout_seconds": float("inf")}, {"timeout_seconds": 10**10000}, {"num_ctx": True},
                 {"num_ctx": 8193}, {"num_ctx": 8192.0}, {"max_output_tokens": True},
                 {"max_output_tokens": 0}, {"max_output_tokens": 1025}, {"max_output_tokens": "768"}]
        for values in cases:
            with self.subTest(fields=list(values)), self.assertRaises(LocalModelError):
                LocalModelConfig(**values)

    def test_health_uses_only_version_inventory_and_show_and_reports_observed_identity(self):
        with patch.object(self.client, "_request", side_effect=self.request):
            health = self.client.health()
        self.assertEqual([(method, route) for method, route, _, _ in self.calls],
                         [("GET", "/api/version"), ("GET", "/api/tags"), ("POST", "/api/show")])
        self.assertEqual(health, {"version": "0.40.7", "runtime_version": "0.40.7", "model": ALLOWED_MODEL,
                                  "digest": DIGEST, "architecture": "qwen3", "capabilities": ["completion", "thinking"],
                                  "installed_size_bytes": 1_435_868_500})
        self.assertEqual(self.client.generation_requests, 0)
        self.assertEqual(len({call[3] for call in self.calls}), 1)
        del self.responses["/api/tags"]["models"][0]["size"]
        with patch.object(self.client, "_request", side_effect=self.request):
            self.assertNotIn("installed_size_bytes", self.client.health())

    def test_missing_ambiguous_changed_digest_or_remote_cache_never_attempts_generation(self):
        original = deepcopy(self.responses)
        cases = [[], [original["/api/tags"]["models"][0]] * 2,
                 [original["/api/tags"]["models"][0] | {"digest": "b" * 64}],
                 [original["/api/tags"]["models"][0] | {"digest": "invalid"}],
                 [original["/api/tags"]["models"][0] | {"remote_host": "https://cloud.invalid"}],
                 [original["/api/tags"]["models"][0] | {"model": "other"}]]
        for models in cases:
            self.responses = deepcopy(original)
            self.responses["/api/tags"]["models"] = models
            self.calls.clear()
            with self.subTest(models=len(models)), self.assertRaises(LocalModelError):
                self.generate()
            self.assertEqual(self.client.generation_requests, 0)
            self.assertNotIn("/api/generate", [call[1] for call in self.calls])

    def test_show_requires_qwen3_completion_and_absent_remote_or_cloud_markers(self):
        shows = [{"model_info": {"general.architecture": "other"}, "capabilities": ["completion"]},
                 {"model_info": {"general.architecture": "qwen3"}, "capabilities": ["embedding"]},
                 {"model_info": {"general.architecture": "qwen3"}, "capabilities": ["completion", "cloud"]},
                 {"model_info": {"general.architecture": "qwen3"}, "capabilities": ["completion"], "remote_model": "alias"},
                 {"model_info": {"general.architecture": "qwen3", "nested": {"cloud": False}}, "capabilities": ["completion"]}]
        for shown in shows:
            self.responses["/api/show"] = shown
            with self.subTest(shown=shown), self.assertRaises(LocalModelError):
                self.generate()
        self.assertEqual(self.client.generation_requests, 0)

    def test_generate_has_exactly_one_bounded_nonstreaming_json_schema_request(self):
        response = self.generate()
        self.assertEqual([call[1] for call in self.calls], ["/api/version", "/api/tags", "/api/show", "/api/generate"])
        request = self.calls[-1][2]
        self.assertEqual(set(request), {"model", "system", "prompt", "format", "stream", "think", "options"})
        self.assertEqual(request["format"], SCHEMA)
        self.assertIs(request["stream"], False)
        self.assertIs(request["think"], False)
        self.assertEqual(request["options"], {"temperature": 0, "num_ctx": 8192, "num_predict": 768})
        self.assertEqual(len({call[3] for call in self.calls}), 1)
        self.assertEqual(self.client.generation_requests, 1)
        self.assertEqual((response.model, response.digest, response.prompt_tokens, response.generated_tokens),
                         (ALLOWED_MODEL, DIGEST, 300, 12))
        self.assertEqual(response.done_reason, "stop")
        self.assertEqual(json.loads(json.dumps(response.to_dict())), response.to_dict())
        with self.assertRaises(FrozenInstanceError):
            response.text = "changed"

    def test_length_completion_remains_explicit_without_retry_or_false_success_claim(self):
        self.responses["/api/generate"]["done_reason"] = "length"
        response = self.generate()
        self.assertEqual(response.done_reason, "length")
        self.assertEqual(self.client.generation_requests, 1)
        self.assertEqual(sum(route == "/api/generate" for _, route, _, _ in self.calls), 1)

    def test_invalid_prompt_and_schema_are_rejected_before_health_or_generation(self):
        cases = [(None, "prompt", SCHEMA), ("system", "", SCHEMA), ("system", "\ud800", SCHEMA),
                 ("x" * MAX_PROMPT_BYTES, "extra", SCHEMA), ("system", "prompt", []),
                 ("system", "prompt", {"type": "array", "items": {}}),
                 ("system", "prompt", {"type": "object", "properties": {}, "description": "x" * MAX_SCHEMA_BYTES}),
                 ("system", "prompt", {"type": "object", "properties": {}, "invalid": float("nan")})]
        with patch.object(self.client, "_request", side_effect=AssertionError("no network")):
            for system, prompt, schema in cases:
                with self.subTest(system_type=type(system)), self.assertRaises(LocalModelError):
                    self.client.generate(system, prompt, schema)
        self.assertEqual(self.client.generation_requests, 0)

    def test_malformed_completion_identity_tokens_and_duration_metadata_fail_closed(self):
        original = deepcopy(self.responses["/api/generate"])
        changes = [{"model": "remote-model"}, {"done": 1}, {"done": False}, {"done_reason": {}},
                   {"done_reason": "cancelled"}, {"response": ""}, {"response": "\ud800"},
                   {"thinking": "unexpected hidden text"}, {"tool_calls": [{"name": "shell"}]},
                   {"prompt_eval_count": True}, {"prompt_eval_count": 0}, {"prompt_eval_count": 8190},
                   {"eval_count": 769}, {"eval_count": 1.0}, {"total_duration": -1},
                   {"load_duration": 1001}, {"eval_duration": False}, {"remote_host": "cloud"}]
        for change in changes:
            self.responses["/api/generate"] = original | change
            with self.subTest(fields=list(change)), self.assertRaises(LocalModelError):
                self.generate()
        self.assertEqual(self.client.generation_requests, len(changes))

    def test_generation_transport_failure_is_counted_once_and_never_retried(self):
        def fail_generation(method, route, payload, deadline):
            if route == "/api/generate":
                raise LocalModelError("unavailable")
            return self.request(method, route, payload, deadline)
        with patch.object(self.client, "_request", side_effect=fail_generation):
            with self.assertRaises(LocalModelError):
                self.client.generate("system", "prompt", SCHEMA)
        self.assertEqual(self.client.generation_requests, 1)
        self.assertEqual(len(self.calls), 3)

    def test_no_unlisted_route_is_available_even_through_internal_transport(self):
        for method, route in (("POST", "/api/pull"), ("GET", "/api/generate"), ("POST", "http://cloud/api/generate")):
            with self.subTest(route=route), self.assertRaises(LocalModelError):
                self.client._request(method, route, {}, 0)


if __name__ == "__main__":
    unittest.main()
