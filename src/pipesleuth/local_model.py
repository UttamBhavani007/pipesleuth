"""A bounded, proxy-free client for one explicitly cached local Ollama model.

No discovery, downloads, cloud fallback, arbitrary routes, retries or tools are
available. The owner must launch and control the cloud-disabled local server.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import http.client
import json
import math
import re
import socket
import threading
import time


ALLOWED_MODEL = "qwen3:1.7b"
MAX_PROMPT_BYTES = 65_536
MAX_SCHEMA_BYTES = 16_384
MAX_RESPONSE_BYTES = 131_072
MAX_REQUEST_BYTES = 98_304
_ENDPOINT = re.compile(r"http://(127\.0\.0\.1|\[::1\]):([1-9][0-9]{0,4})/?")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_ROUTES = {("GET", "/api/version"), ("GET", "/api/tags"),
           ("POST", "/api/show"), ("POST", "/api/generate")}


class LocalModelError(ValueError):
    """Invalid configuration or unavailable/untrusted local-model response."""


@dataclass(frozen=True, slots=True)
class LocalModelConfig:
    endpoint: str = "http://127.0.0.1:11436"
    model: str = ALLOWED_MODEL
    expected_digest: str | None = None
    timeout_seconds: float = 120.0
    num_ctx: int = 8192
    max_output_tokens: int = 768

    def __post_init__(self):
        matched = _ENDPOINT.fullmatch(self.endpoint) if isinstance(self.endpoint, str) else None
        if matched is None or int(matched[2]) > 65_535:
            raise LocalModelError("endpoint must be literal loopback HTTP with an explicit valid port")
        object.__setattr__(self, "endpoint", self.endpoint.rstrip("/"))
        if self.model != ALLOWED_MODEL:
            raise LocalModelError("only the explicitly allowlisted local model is supported")
        if self.expected_digest is not None and (
            not isinstance(self.expected_digest, str) or _DIGEST.fullmatch(self.expected_digest) is None
        ):
            raise LocalModelError("expected_digest must be a lowercase SHA-256 digest")
        if (type(self.timeout_seconds) not in (int, float) or not 0 < self.timeout_seconds <= 120
                or not math.isfinite(self.timeout_seconds)):
            raise LocalModelError("timeout_seconds must be finite, positive and at most 120")
        if type(self.num_ctx) is not int or self.num_ctx != 8192:
            raise LocalModelError("num_ctx must be the fixed 8192-token context")
        if type(self.max_output_tokens) is not int or not 1 <= self.max_output_tokens <= 1024:
            raise LocalModelError("max_output_tokens must be an integer between 1 and 1024")


@dataclass(frozen=True, slots=True)
class ModelResponse:
    text: str
    model: str
    digest: str
    prompt_tokens: int
    generated_tokens: int
    total_duration_ns: int
    load_duration_ns: int
    prompt_eval_duration_ns: int
    eval_duration_ns: int
    done_reason: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _remaining(deadline: float) -> float:
    value = deadline - time.monotonic()
    if value <= 0:
        raise LocalModelError("local model operation exceeded its time limit")
    return value


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _nonfinite(_):
    raise ValueError("non-finite JSON value")


def _encoded(value) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise LocalModelError("local model request must contain bounded finite JSON") from None


def _cloud_markers(value, depth=0):
    if depth > 16:
        raise LocalModelError("local model metadata exceeds nesting limits")
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower() in {"cloud", "remote", "remote_host", "remote_model", "remote_url", "cloud_model"}:
                raise LocalModelError("remote or cloud model metadata is not permitted")
            _cloud_markers(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            _cloud_markers(child, depth + 1)


class LocalModelClient:
    """Health-check a cached local model and make one bounded generation call."""

    def __init__(self, config: LocalModelConfig | None = None):
        if config is not None and not isinstance(config, LocalModelConfig):
            raise LocalModelError("config must be a LocalModelConfig")
        self.config = config or LocalModelConfig()
        self._generation_requests = 0

    @property
    def generation_requests(self) -> int:
        return self._generation_requests

    def _request(self, method: str, route: str, payload, deadline: float) -> dict[str, object]:
        if (method, route) not in _ROUTES:
            raise LocalModelError("local model route is not allowed")
        body = _encoded(payload) if payload is not None else None
        if body is not None and len(body) > MAX_REQUEST_BYTES:
            raise LocalModelError("local model request exceeds its byte limit")
        matched = _ENDPOINT.fullmatch(self.config.endpoint)
        host, port = matched[1].strip("[]"), int(matched[2])
        connection = http.client.HTTPConnection(host, port, timeout=_remaining(deadline))
        timer, response = None, None
        try:
            connection.connect()
            connected_socket = connection.sock

            def abort_read():
                # close() alone can leave makefile readers blocked. Shutdown
                # interrupts hidden header/body loops in http.client as well.
                try:
                    connected_socket.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

            timer = threading.Timer(_remaining(deadline), abort_read)
            timer.daemon = True
            timer.start()
            connection.sock.settimeout(_remaining(deadline))
            connection.request(method, route, body=body,
                               headers={"Accept": "application/json", "Content-Type": "application/json"})
            response = connection.getresponse()
            _remaining(deadline)
            if response.status != 200:
                raise LocalModelError("local model endpoint returned an unsuccessful HTTP status")
            length = response.getheader("Content-Length")
            if length is not None and (not length.isascii() or not length.isdecimal()
                                       or len(length) > 8 or int(length) > MAX_RESPONSE_BYTES):
                raise LocalModelError("local model response exceeds its byte limit")
            if response.getheader("Content-Encoding", "identity").lower() != "identity":
                raise LocalModelError("encoded local model responses are not supported")
            chunks, received = [], 0
            for _ in range(512):
                _remaining(deadline)
                connected_socket.settimeout(_remaining(deadline))
                chunk = response.read1(min(16_384, MAX_RESPONSE_BYTES + 1 - received))
                if not chunk:
                    break
                chunks.append(chunk)
                received += len(chunk)
                if received > MAX_RESPONSE_BYTES:
                    raise LocalModelError("local model response exceeds its byte limit")
                # HTTP/1.0 may close the socket as soon as its declared body
                # is consumed; do not set another timeout on that closed FD.
                if length is not None and received == int(length):
                    break
            else:
                raise LocalModelError("local model response exceeded its read budget")
            _remaining(deadline)
            encoded = b"".join(chunks)
            if length is not None and len(encoded) != int(length):
                raise LocalModelError("local model response was incomplete")
            try:
                result = json.loads(encoded.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_nonfinite)
            except (ValueError, UnicodeError, RecursionError):
                raise LocalModelError("local model returned malformed JSON") from None
            if not isinstance(result, dict) or "error" in result:
                raise LocalModelError("local model returned an error or invalid response")
            _remaining(deadline)
            return result
        except LocalModelError:
            raise
        except (OSError, http.client.HTTPException, ValueError, OverflowError):
            raise LocalModelError("local model transport is unavailable or timed out") from None
        finally:
            if timer is not None:
                timer.cancel()
                timer.join(timeout=0.1)
            if response is not None:
                response.close()
            connection.close()

    def _health(self, deadline: float) -> dict[str, object]:
        version = self._request("GET", "/api/version", None, deadline).get("version")
        if not isinstance(version, str) or not re.fullmatch(r"[0-9][A-Za-z0-9.+_-]{0,63}", version):
            raise LocalModelError("local runtime version is invalid")
        inventory = self._request("GET", "/api/tags", None, deadline).get("models")
        if not isinstance(inventory, list) or len(inventory) > 128:
            raise LocalModelError("local model inventory is invalid")
        matches = [item for item in inventory if isinstance(item, dict) and item.get("name") == self.config.model]
        if len(matches) != 1:
            raise LocalModelError("the allowlisted model must already be cached locally")
        selected = matches[0]
        _cloud_markers(selected)
        if selected.get("model", self.config.model) != self.config.model:
            raise LocalModelError("cached local model identity is inconsistent")
        digest = selected.get("digest")
        if not isinstance(digest, str) or _DIGEST.fullmatch(digest) is None:
            raise LocalModelError("cached local model digest is invalid")
        if self.config.expected_digest is not None and digest != self.config.expected_digest:
            raise LocalModelError("cached local model digest does not match the configured pin")
        shown = self._request("POST", "/api/show", {"model": self.config.model, "verbose": False}, deadline)
        _cloud_markers(shown)
        information = shown.get("model_info")
        architecture = information.get("general.architecture") if isinstance(information, dict) else None
        capabilities = shown.get("capabilities")
        if (architecture != "qwen3" or not isinstance(capabilities, list) or not 1 <= len(capabilities) <= 16
                or any(not isinstance(item, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", item) is None for item in capabilities)
                or "completion" not in capabilities or any(item in {"cloud", "remote"} for item in capabilities)):
            raise LocalModelError("cached model lacks the required local completion capability")
        result = {"version": version, "runtime_version": version, "model": self.config.model,
                  "digest": digest, "architecture": architecture, "capabilities": list(capabilities)}
        if "size" in selected:
            size = selected["size"]
            if type(size) is not int or not 1 <= size <= 10**12:
                raise LocalModelError("cached model size metadata is invalid")
            result["installed_size_bytes"] = size
        _remaining(deadline)
        return result

    def health(self) -> dict[str, object]:
        return self._health(time.monotonic() + self.config.timeout_seconds)

    def generate(self, system: str, prompt: str, schema: dict[str, object]) -> ModelResponse:
        if not isinstance(system, str) or not isinstance(prompt, str) or not prompt:
            raise LocalModelError("system and non-empty prompt must be text")
        try:
            prompt_size = len(system.encode("utf-8")) + len(prompt.encode("utf-8"))
        except UnicodeError:
            raise LocalModelError("local model prompt must be valid UTF-8") from None
        if prompt_size > MAX_PROMPT_BYTES:
            raise LocalModelError("local model prompt exceeds its byte limit")
        if not isinstance(schema, dict) or schema.get("type") != "object" or not isinstance(schema.get("properties"), dict):
            raise LocalModelError("format must be an object JSON schema")
        if len(_encoded(schema)) > MAX_SCHEMA_BYTES:
            raise LocalModelError("format schema exceeds its byte limit")
        payload = {"model": self.config.model, "system": system, "prompt": prompt, "format": schema,
                   "stream": False, "think": False,
                   "options": {"temperature": 0, "num_ctx": self.config.num_ctx, "num_predict": self.config.max_output_tokens}}
        if len(_encoded(payload)) > MAX_REQUEST_BYTES:
            raise LocalModelError("local model request exceeds its byte limit")
        deadline = time.monotonic() + self.config.timeout_seconds
        health = self._health(deadline)
        _remaining(deadline)
        self._generation_requests += 1
        response = self._request("POST", "/api/generate", payload, deadline)
        _cloud_markers(response)
        reason = response.get("done_reason")
        if (response.get("model") != self.config.model or response.get("done") is not True
                or not isinstance(reason, str) or reason not in {"stop", "length"}
                or not isinstance(response.get("response"), str) or not response["response"]
                or response.get("thinking", "") not in (None, "") or response.get("tool_calls") not in (None, [])):
            raise LocalModelError("local model generation identity or completion metadata is invalid")
        try:
            if len(response["response"].encode("utf-8")) > MAX_RESPONSE_BYTES:
                raise LocalModelError("local model text exceeds its byte limit")
        except UnicodeError:
            raise LocalModelError("local model text must be valid UTF-8") from None
        counts = {}
        for name, maximum in (("prompt_eval_count", self.config.num_ctx), ("eval_count", self.config.max_output_tokens)):
            value = response.get(name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise LocalModelError("local model token metadata is invalid")
            counts[name] = value
        if counts["prompt_eval_count"] + counts["eval_count"] > self.config.num_ctx:
            raise LocalModelError("local model usage exceeded the context budget")
        durations = {}
        for name in ("total_duration", "load_duration", "prompt_eval_duration", "eval_duration"):
            value = response.get(name)
            if type(value) is not int or not 0 <= value <= 2**63 - 1:
                raise LocalModelError("local model duration metadata is invalid")
            durations[name] = value
        if any(value > durations["total_duration"] for value in durations.values()):
            raise LocalModelError("local model duration metadata is inconsistent")
        _remaining(deadline)
        return ModelResponse(text=response["response"], model=self.config.model, digest=health["digest"],
                             prompt_tokens=counts["prompt_eval_count"], generated_tokens=counts["eval_count"],
                             total_duration_ns=durations["total_duration"], load_duration_ns=durations["load_duration"],
                             prompt_eval_duration_ns=durations["prompt_eval_duration"], eval_duration_ns=durations["eval_duration"],
                             done_reason=response["done_reason"])
