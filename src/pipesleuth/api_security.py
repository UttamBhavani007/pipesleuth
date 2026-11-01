"""Small ASGI boundary for an explicitly local, single-process JSON API.

Tokens are host-owned bearer credentials. No cookie/session, proxy, browser
cross-origin access, logging, or automatic mutation retry is provided here.
"""

from __future__ import annotations

import asyncio
import hmac
import ipaddress
import json
import math
import re
import threading
from urllib.parse import parse_qsl


MAX_BODY_BYTES = 4096
BODY_TIMEOUT_SECONDS = 2.0
MAX_RESPONSE_BYTES = 512 * 1024
_APPROVE = re.compile(r"/v1/repairs/[0-9a-f]{32}/approve")
_EVIDENCE = re.compile(r"/v1/runs/[0-9a-f]{32}/evidence/(read_run|read_logs|inspect_schema|inspect_validation)")
_SECURITY_HEADERS = {
    b"host", b"authorization", b"origin", b"content-type", b"content-length",
    b"transfer-encoding", b"sec-fetch-site", b"sec-fetch-mode", b"sec-fetch-dest",
    b"cookie", b"x-real-ip", b"forwarded",
}
_RESPONSE_HEADERS = [(b"cache-control", b"no-store"), (b"pragma", b"no-cache"),
                     (b"x-content-type-options", b"nosniff"),
                     (b"referrer-policy", b"no-referrer")]


class _Denied(Exception):
    def __init__(self, status, code):
        self.status, self.code = status, code


def _pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate field")
        value[key] = item
    return value


def _constant(_value):
    raise ValueError("nonfinite number")


def _float(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("nonfinite number")
    return result


def _unicode(value):
    if isinstance(value, str):
        value.encode("utf-8")
    elif isinstance(value, dict):
        for key, item in value.items():
            key.encode("utf-8")
            _unicode(item)
    elif isinstance(value, list):
        for item in value:
            _unicode(item)


class ApiSecurity:
    """Enforce local authority, role tokens and bounded requests/responses."""

    def __init__(self, app, *, host="127.0.0.1", port, read_token,
                 operation_token, approval_token):
        if host != "127.0.0.1" or type(port) is not int or not 1 <= port <= 65535:
            raise ValueError("API requires literal 127.0.0.1 and a valid port")
        tokens = (read_token, operation_token, approval_token)
        if (any(not isinstance(token, str) or not 32 <= len(token) <= 256
                or any(not 33 <= ord(char) <= 126 for char in token) for token in tokens)
                or len(set(tokens)) != 3):
            raise ValueError("API requires three distinct strong printable tokens")
        self.app = app
        self.authority = f"127.0.0.1:{port}".encode("ascii")
        self.origin = b"http://" + self.authority
        self.tokens = tuple(token.encode("ascii") for token in tokens)
        self._mutation = threading.Lock()

    @staticmethod
    async def _error(send, status, code):
        body = json.dumps({"error": {"code": code}}, separators=(",", ":")).encode("ascii")
        await send({"type": "http.response.start", "status": status,
                    "headers": [(b"content-type", b"application/json"),
                                (b"content-length", str(len(body)).encode("ascii")), *_RESPONSE_HEADERS]})
        await send({"type": "http.response.body", "body": body, "more_body": False})

    def _headers(self, scope):
        client = scope.get("client")
        try:
            if not client or not ipaddress.ip_address(client[0]).is_loopback:
                raise ValueError()
        except (ValueError, TypeError, IndexError):
            raise _Denied(403, "forbidden") from None
        raw = scope.get("headers", [])
        if len(raw) > 64 or sum(len(key) + len(value) for key, value in raw) > 16384:
            raise _Denied(400, "invalid_request")
        headers = {}
        for key, value in raw:
            name = key.lower()
            if name.startswith(b"x-forwarded-") or name in {b"forwarded", b"x-real-ip"}:
                raise _Denied(403, "forbidden")
            if name in _SECURITY_HEADERS:
                if name in headers or b"\r" in value or b"\n" in value:
                    raise _Denied(400, "invalid_request")
                headers[name] = value
        if headers.get(b"host") != self.authority:
            raise _Denied(403, "forbidden")
        if b"origin" in headers and headers[b"origin"] != self.origin:
            raise _Denied(403, "forbidden")
        if headers.get(b"sec-fetch-site", b"none") not in {b"same-origin", b"none"}:
            raise _Denied(403, "forbidden")
        # Cookies cannot grant authority, and are not part of this API protocol.
        if b"cookie" in headers or b"transfer-encoding" in headers:
            raise _Denied(400, "invalid_request")
        return headers

    def _authorize(self, scope, headers):
        method, path = scope.get("method"), scope.get("path")
        if method not in {"GET", "HEAD", "POST"}:
            raise _Denied(405, "method_not_allowed")
        if method == "GET" and path == "/health":
            return
        authorization = headers.get(b"authorization", b"")
        if not authorization.startswith(b"Bearer "):
            raise _Denied(401, "unauthorized")
        token = authorization[7:]
        matches = [hmac.compare_digest(token, expected) for expected in self.tokens]
        allowed = any(matches) if method in {"GET", "HEAD"} else matches[2 if _APPROVE.fullmatch(path or "") else 1]
        if not allowed:
            raise _Denied(401, "unauthorized")

    @staticmethod
    def _query(scope):
        raw = scope.get("query_string", b"")
        if not raw:
            return
        if (scope["method"] != "GET" or not _EVIDENCE.fullmatch(scope.get("path", ""))
                or not isinstance(raw, bytes) or len(raw) > 512):
            raise _Denied(400, "invalid_query")
        try:
            encoded = raw.decode("ascii")
            if re.search(r"%(?![0-9a-fA-F]{2})", encoded):
                raise ValueError()
            pairs = parse_qsl(encoded, keep_blank_values=True, strict_parsing=True,
                              encoding="utf-8", errors="strict", max_num_fields=2)
            if len(pairs) != 1 or pairs[0][0] != "limit":
                raise ValueError()
        except (UnicodeError, ValueError):
            raise _Denied(400, "invalid_query") from None

    @staticmethod
    async def _body(scope, headers, receive):
        method = scope["method"]
        declared = headers.get(b"content-length")
        if declared is not None:
            if not declared or not declared.isdigit() or len(declared) > 10:
                raise _Denied(400, "invalid_request")
            declared = int(declared)
            if declared > MAX_BODY_BYTES:
                raise _Denied(413, "request_too_large")
        if method == "POST":
            media = headers.get(b"content-type", b"").lower().replace(b" ", b"")
            if media not in {b"application/json", b"application/json;charset=utf-8"}:
                raise _Denied(415, "unsupported_media_type")
        chunks, length = [], 0
        try:
            async with asyncio.timeout(BODY_TIMEOUT_SECONDS):
                while True:
                    message = await receive()
                    if message.get("type") != "http.request" or not isinstance(message.get("body", b""), bytes):
                        raise _Denied(400, "invalid_request")
                    body = message.get("body", b"")
                    length += len(body)
                    if length > MAX_BODY_BYTES:
                        raise _Denied(413, "request_too_large")
                    if method != "POST" and length:
                        raise _Denied(400, "invalid_request")
                    chunks.append(body)
                    if not message.get("more_body", False):
                        break
        except TimeoutError:
            raise _Denied(408, "request_timeout") from None
        if declared is not None and declared != length:
            raise _Denied(400, "invalid_request")
        body = b"".join(chunks)
        if method == "POST":
            try:
                value = json.loads(body.decode("utf-8"), object_pairs_hook=_pairs,
                                   parse_constant=_constant, parse_float=_float)
                if not isinstance(value, dict):
                    raise ValueError()
                _unicode(value)
            except (UnicodeError, ValueError, RecursionError, OverflowError):
                raise _Denied(400, "invalid_request") from None
        return body

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            if scope["type"] == "websocket":
                await send({"type": "websocket.close", "code": 1008})
            else:
                await self.app(scope, receive, send)
            return
        acquired = False
        try:
            headers = self._headers(scope)
            self._authorize(scope, headers)
            self._query(scope)
            if scope["method"] == "POST":
                acquired = self._mutation.acquire(blocking=False)
                if not acquired:
                    raise _Denied(503, "busy")
            body = await self._body(scope, headers, receive)
            consumed = False

            async def replay():
                nonlocal consumed
                if not consumed:
                    consumed = True
                    return {"type": "http.request", "body": body, "more_body": False}
                return await receive()

            start, chunks, length, finished = None, [], 0, False

            async def capture(message):
                nonlocal start, length, finished
                if message["type"] == "http.response.start":
                    if start is not None:
                        raise ValueError("duplicate response start")
                    response_headers = message.get("headers", [])
                    if (len(response_headers) > 64
                            or sum(len(key) + len(value) for key, value in response_headers) > 16384
                            or any(key.lower() == b"set-cookie" or key.lower().startswith(b"access-control-")
                                   for key, _ in response_headers)):
                        raise ValueError("unsafe response headers")
                    start = dict(message)
                elif message["type"] == "http.response.body":
                    if start is None or finished:
                        raise ValueError("invalid response order")
                    chunk = message.get("body", b"")
                    if not isinstance(chunk, bytes):
                        raise ValueError("invalid response body")
                    length += len(chunk)
                    if length > MAX_RESPONSE_BYTES:
                        raise _Denied(500, "response_too_large")
                    chunks.append(chunk)
                    finished = not message.get("more_body", False)
                else:
                    raise ValueError("unsupported response event")

            await self.app(scope, replay, capture)
            if start is None or not finished:
                raise ValueError("incomplete response")
            protected = {name for name, _ in _RESPONSE_HEADERS}
            start["headers"] = [(key, value) for key, value in start.get("headers", [])
                                if key.lower() not in protected] + _RESPONSE_HEADERS
            await send(start)
            await send({"type": "http.response.body", "body": b"".join(chunks), "more_body": False})
        except _Denied as exc:
            await self._error(send, exc.status, exc.code)
        except Exception:
            await self._error(send, 500, "internal_error")
        finally:
            if acquired:
                self._mutation.release()
