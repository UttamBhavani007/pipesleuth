"""Exercise the raw ASGI boundary, including streaming and competing requests."""

import asyncio
import json
import unittest

from pipesleuth.api_security import ApiSecurity, MAX_RESPONSE_BYTES


READ, OPERATE, APPROVE = "r" * 40, "o" * 40, "a" * 40
PORT = 8912


class ApiSecurityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.calls = []

        async def app(scope, receive, send):
            self.calls.append((scope, await receive()))
            await send({"type": "http.response.start", "status": 200,
                        "headers": [(b"content-type", b"application/json")]})
            await send({"type": "http.response.body", "body": b'{"ok":true}'})

        self.app = app
        self.secured = self.wrap(app)

    @staticmethod
    def wrap(app, **changes):
        return ApiSecurity(app, port=PORT, read_token=READ, operation_token=OPERATE,
                           approval_token=APPROVE, **changes)

    async def request(self, *, method="GET", path="/v1/runs", token=READ,
                      body=b"", headers=(), client=("127.0.0.1", 50000),
                      chunks=None, secured=None, query=b""):
        raw = [(b"host", f"127.0.0.1:{PORT}".encode())]
        if token is not None:
            raw.append((b"authorization", b"Bearer " + token.encode()))
        if method == "POST":
            raw.append((b"content-type", b"application/json"))
        raw.extend(headers)
        scope = {"type": "http", "http_version": "1.1", "method": method,
                 "path": path, "raw_path": path.encode(), "headers": raw,
                 "client": client, "server": ("127.0.0.1", PORT), "scheme": "http",
                 "query_string": query}
        messages = list(chunks or [{"type": "http.request", "body": body, "more_body": False}])

        async def receive():
            if messages:
                message = messages.pop(0)
                if isinstance(message, tuple):
                    delay, message = message
                    await asyncio.sleep(delay)
                return message
            return {"type": "http.disconnect"}

        sent = []
        await (secured or self.secured)(scope, receive, self.sender(sent))
        response = b"".join(item.get("body", b"") for item in sent)
        return sent[0]["status"], response, dict(sent[0].get("headers", []))

    @staticmethod
    def sender(sent):
        async def send(message):
            sent.append(message)
        return send

    def assert_code(self, response, status, code):
        self.assertEqual(response[0], status)
        self.assertEqual(json.loads(response[1]), {"error": {"code": code}})
        self.assertEqual(response[2][b"cache-control"], b"no-store")

    def test_configuration_rejects_weak_ambiguous_or_nonlocal_settings(self):
        for change in ({"host": "localhost"}, {"host": "0.0.0.0"}, {"port": True},
                       {"port": 65536}, {"read_token": "short"},
                       {"read_token": OPERATE}, {"read_token": "x" * 39 + "\n"}):
            config = dict(port=PORT, read_token=READ, operation_token=OPERATE, approval_token=APPROVE)
            config.update(change)
            with self.assertRaises(ValueError):
                ApiSecurity(self.app, **config)

    async def test_health_only_get_is_public_and_every_role_can_read(self):
        self.assertEqual((await self.request(path="/health", token=None))[0], 200)
        self.assert_code(await self.request(method="HEAD", path="/health", token=None), 401, "unauthorized")
        self.assert_code(await self.request(token=None), 401, "unauthorized")
        for token in (READ, OPERATE, APPROVE):
            response = await self.request(token=token)
            self.assertEqual(response[0], 200)
            self.assertEqual(response[2][b"x-content-type-options"], b"nosniff")

    async def test_operation_and_approval_authorities_cannot_substitute(self):
        approve_path = "/v1/repairs/" + "a" * 32 + "/approve"
        for token in (READ, APPROVE):
            self.assert_code(await self.request(method="POST", token=token, body=b"{}"), 401, "unauthorized")
        self.assertEqual((await self.request(method="POST", token=OPERATE, body=b"{}"))[0], 200)
        for token in (READ, OPERATE):
            self.assert_code(await self.request(method="POST", path=approve_path, token=token, body=b"{}"), 401, "unauthorized")
        self.assertEqual((await self.request(method="POST", path=approve_path, token=APPROVE, body=b"{}"))[0], 200)
        self.assert_code(await self.request(method="POST", path=approve_path + "/", token=APPROVE, body=b"{}"), 401, "unauthorized")

    async def test_nonloopback_origin_proxy_cookie_and_crosssite_are_refused(self):
        for client in (("192.0.2.1", 1), ("localhost", 1), None):
            self.assert_code(await self.request(client=client), 403, "forbidden")
        for header in ((b"origin", b"http://evil.example"), (b"origin", b"null"),
                       (b"sec-fetch-site", b"cross-site"), (b"sec-fetch-site", b"same-site"),
                       (b"forwarded", b"for=127.0.0.1"), (b"x-forwarded-host", b"127.0.0.1")):
            self.assert_code(await self.request(headers=[header]), 403, "forbidden")
        self.assert_code(await self.request(headers=[(b"cookie", b"session=anything")]), 400, "invalid_request")
        self.assertEqual((await self.request(headers=[(b"origin", f"http://127.0.0.1:{PORT}".encode()),
                                                       (b"sec-fetch-site", b"same-origin")]))[0], 200)

    async def test_duplicate_security_headers_are_refused_before_app(self):
        for header in ((b"Host", b"127.0.0.1:8912"),
                       (b"authorization", b"Bearer " + READ.encode())):
            self.assert_code(await self.request(headers=[header]), 400, "invalid_request")
        for name, value in ((b"origin", b"http://127.0.0.1:8912"),
                            (b"content-length", b"0"), (b"sec-fetch-site", b"same-origin")):
            self.assert_code(await self.request(headers=[(name, value), (name, value)]), 400, "invalid_request")
        self.assertEqual(self.calls, [])

    async def test_host_must_be_exact_literal_authority(self):
        async def receive():
            return {"type": "http.request", "body": b""}
        for host in (b"localhost:8912", b"127.0.0.1", b"127.0.0.1:8913", b"127.0.0.1:8912 "):
            sent = []
            scope = {"type": "http", "method": "GET", "path": "/health",
                     "client": ("127.0.0.1", 1), "headers": [(b"host", host)]}
            await self.secured(scope, receive, self.sender(sent))
            self.assertEqual(sent[0]["status"], 403)

    async def test_strict_json_rejects_duplicates_nonfinite_unicode_and_trailing_values(self):
        for body in (b'{"a":1,"a":2}', b'{"a":{"b":1,"b":2}}', b'{"x":NaN}',
                     b'{"x":Infinity}', b'{"x":1e999}', b'{"x":"\\ud800"}',
                     b'{"x":"\xff"}', b'{} {}', b'[]', b'', b'{"sql":"secret",'):
            self.assert_code(await self.request(method="POST", token=OPERATE, body=body), 400, "invalid_request")
        self.assertEqual(self.calls, [])
        self.assertEqual((await self.request(method="POST", token=OPERATE, body=b'{"ok":[1,true,null]}'))[0], 200)
        self.assertEqual(self.calls[0][1]["body"], b'{"ok":[1,true,null]}')

    async def test_media_length_methods_and_get_bodies(self):
        self.assert_code(await self.request(method="DELETE"), 405, "method_not_allowed")
        self.assert_code(await self.request(body=b"{}"), 400, "invalid_request")
        self.assert_code(await self.request(headers=[(b"content-length", b"1")]), 400, "invalid_request")
        self.assert_code(await self.request(method="POST", token=OPERATE, body=b"{}",
                                            headers=[(b"content-type", b"text/plain")]), 400, "invalid_request")
        # Use a separate scope to test missing/unsupported type rather than a duplicate.
        for media in (None, b"text/plain", b"application/json; charset=latin1"):
            raw = [(b"host", b"127.0.0.1:8912"), (b"authorization", b"Bearer " + OPERATE.encode())]
            if media:
                raw.append((b"content-type", media))
            scope = {"type": "http", "method": "POST", "path": "/v1/run", "client": ("127.0.0.1", 1), "headers": raw}
            sent = []
            async def receive():
                return {"type": "http.request", "body": b"{}"}
            await self.secured(scope, receive, self.sender(sent))
            self.assertEqual(sent[0]["status"], 415)

    async def test_streaming_body_cap_and_total_read_deadline(self):
        self.assert_code(await self.request(method="POST", token=OPERATE,
            headers=[(b"content-length", b"4097")]), 413, "request_too_large")
        chunks = [{"type": "http.request", "body": b"x" * 2048, "more_body": True},
                  {"type": "http.request", "body": b"x" * 2049, "more_body": False}]
        self.assert_code(await self.request(method="POST", token=OPERATE, chunks=chunks), 413, "request_too_large")
        slow = [(0.8, {"type": "http.request", "body": b" ", "more_body": True})] * 3
        started = asyncio.get_running_loop().time()
        self.assert_code(await self.request(method="POST", token=OPERATE, chunks=slow), 408, "request_timeout")
        elapsed = asyncio.get_running_loop().time() - started
        self.assertGreaterEqual(elapsed, 1.9)
        self.assertLess(elapsed, 2.3)
        self.assertEqual((await self.request(method="POST", token=OPERATE, body=b"{}"))[0], 200)

    async def test_only_one_post_admitted_but_reads_remain_available(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def blocking(scope, receive, send):
            if scope["method"] == "POST":
                entered.set()
                await release.wait()
            await self.app(scope, receive, send)
        secured = self.wrap(blocking)
        first = asyncio.create_task(self.request(method="POST", token=OPERATE, body=b"{}", secured=secured))
        await entered.wait()
        self.assert_code(await self.request(method="POST", token=OPERATE, body=b"{}", secured=secured), 503, "busy")
        self.assertEqual((await self.request(secured=secured))[0], 200)
        release.set()
        self.assertEqual((await first)[0], 200)
        self.assertEqual((await self.request(method="POST", token=OPERATE, body=b"{}", secured=secured))[0], 200)

    async def test_response_buffer_bounds_and_exception_redaction(self):
        async def huge(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"secret" * (MAX_RESPONSE_BYTES // 6 + 1)})
        self.assert_code(await self.request(secured=self.wrap(huge)), 500, "response_too_large")
        async def failing(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": READ.encode(), "more_body": True})
            raise RuntimeError("/private/path " + READ)
        response = await self.request(secured=self.wrap(failing))
        self.assert_code(response, 500, "internal_error")
        self.assertNotIn(READ.encode(), response[1])
        self.assertNotIn(b"private", response[1])

    async def test_response_cookies_cors_and_websockets_are_not_enabled(self):
        for header in ((b"set-cookie", b"session=x"), (b"access-control-allow-origin", b"*")):
            async def app(scope, receive, send):
                await send({"type": "http.response.start", "status": 200, "headers": [header]})
                await send({"type": "http.response.body", "body": b"{}"})
            self.assert_code(await self.request(secured=self.wrap(app)), 500, "internal_error")
        sent = []
        async def receive():
            raise AssertionError("no websocket receive")
        await self.secured({"type": "websocket"}, receive, self.sender(sent))
        self.assertEqual(sent, [{"type": "websocket.close", "code": 1008}])

    async def test_query_is_closed_with_only_one_evidence_limit(self):
        path = "/v1/runs/" + "a" * 32 + "/evidence/read_logs"
        self.assertEqual((await self.request(path=path, query=b"limit=20"))[0], 200)
        self.assertEqual((await self.request(path=path, query=b"%6cimit=2"))[0], 200)
        for query in (b"limit=1&limit=2", b"limit=1&%6cimit=2", b"sql=SELECT", b"limit=1&path=x",
                      b"limit", b"limit=%ff", b"limit=%xx", b"limit=1&", b"\xff", b"x" * 513):
            self.assert_code(await self.request(path=path, query=query), 400, "invalid_query")
        for route in ("/health", "/v1/runs", path.replace("read_logs", "execute_sql")):
            self.assert_code(await self.request(path=route, query=b"limit=2"), 400, "invalid_query")
        self.assert_code(await self.request(method="POST", path=path, token=OPERATE,
                                            body=b"{}", query=b"limit=2"), 400, "invalid_query")


if __name__ == "__main__":
    unittest.main()
