"""Closed local UI files, HTTP headers and unchanged API authority boundaries."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

try:
    from fastapi.testclient import TestClient
except ImportError as error:
    raise unittest.SkipTest("optional API dependencies are not installed") from error

from pipesleuth.api import ApiSettings, create_app
from pipesleuth.web_assets import CSP, UI_ASSETS


class UiServingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.web = self.root / "web"
        self.web.mkdir()
        for filename, _media in UI_ASSETS.values():
            (self.web / filename).write_bytes(("synthetic public asset " + filename).encode())
        self.settings = ApiSettings(*(self.root / name for name in ("input", "output", "investigations", "repairs")))
        self.read, self.operate, self.approve = "r" * 40, "o" * 40, "a" * 40
        self.app = create_app(self.settings, read_token=self.read, operation_token=self.operate,
                              approval_token=self.approve)
        self.client = self.make_client()
        self.addCleanup(self.client.close)
        self.asset_root = patch("pipesleuth.api.__file__", str(self.root / "api.py"))
        self.asset_root.start()
        self.addCleanup(self.asset_root.stop)

    def make_client(self, peer="127.0.0.1"):
        return TestClient(self.app, base_url="http://127.0.0.1:8765",
                          client=(peer, 50000), raise_server_exceptions=False)

    def test_exact_public_files_support_get_and_head_without_capabilities(self):
        for url, (filename, media) in UI_ASSETS.items():
            with self.subTest(url=url):
                response = self.client.get(url)
                expected = (self.web / filename).read_bytes()
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.content, expected)
                self.assertEqual(response.headers["content-type"], media + "; charset=utf-8")
                head = self.client.head(url)
                self.assertEqual(head.status_code, 200)
                self.assertEqual(head.content, b"")
                self.assertEqual(head.headers["content-length"], str(len(expected)))
        self.assertFalse(self.settings.investigation_state_dir.exists())
        self.assertFalse(self.settings.repair_state_dir.exists())

    def test_all_response_classes_have_local_only_browser_policy(self):
        for response in (self.client.get("/"), self.client.get("/health"),
                         self.client.get("/v1/capabilities"), self.client.get("/?x=1")):
            self.assertEqual(response.headers["content-security-policy"], CSP)
            self.assertEqual(response.headers["cache-control"], "no-store")
            self.assertEqual(response.headers["referrer-policy"], "no-referrer")
            self.assertEqual(response.headers["x-content-type-options"], "nosniff")
            self.assertEqual(response.headers["x-frame-options"], "DENY")
            self.assertNotIn("set-cookie", response.headers)
            self.assertFalse(any(name.startswith("access-control-") for name in response.headers))

    def test_static_reads_preserve_host_origin_peer_proxy_and_cookie_refusal(self):
        for headers, status in (({"Host": "localhost:8765"}, 400),
                                ({"Origin": "https://foreign.example"}, 403),
                                ({"Origin": "null"}, 403),
                                ({"Sec-Fetch-Site": "cross-site"}, 403),
                                ({"X-Forwarded-Host": "127.0.0.1:8765"}, 400),
                                ({"Cookie": "local=anything"}, 400)):
            with self.subTest(headers=headers):
                response = self.client.get("/ui/app.mjs", headers=headers)
                self.assertEqual(response.status_code, status)
                self.assertNotIn("synthetic public asset", response.text)
        with self.make_client(peer="192.0.2.8") as client:
            self.assertEqual(client.get("/").status_code, 403)
        self.assertEqual(self.client.get("/", headers={"Origin": "http://127.0.0.1:8765",
                                                       "Sec-Fetch-Site": "same-origin"}).status_code, 200)

    def test_unknown_encoded_or_traversal_paths_are_never_public_file_readers(self):
        paths = ("/ui/", "/ui/index.html", "/ui/missing.mjs", "/ui/app.mjs/", "/index.html",
                 "/ui/%61pp.mjs", "/ui/%2e%2e/api.py", "/.runtime/api-session.json", "/session")
        for path in paths:
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, 401)
                response = self.client.get(path, headers={"Authorization": "Bearer " + self.read})
                self.assertEqual(response.status_code, 404)
                self.assertEqual(response.json(), {"error": {"code": "not_found"}})
                self.assertNotIn(str(self.root), response.text)

    def test_public_ui_has_closed_queries_and_rejects_request_bodies(self):
        for path in ("/?read_token=anything", "/ui/app.mjs?x=1", "/ui/style.css?limit=1"):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 400)
            self.assertEqual(response.json(), {"error": {"code": "invalid_query"}})
        response = self.client.request("GET", "/", content=b"secret")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"error": {"code": "invalid_request"}})

    def test_post_ui_cannot_become_an_operation_or_bypass_existing_api_roles(self):
        self.assertEqual(self.client.post("/", json={}).status_code, 401)
        self.assertEqual(self.client.post("/", json={}, headers={"Authorization": "Bearer " + self.read}).status_code, 403)
        self.assertEqual(self.client.post("/", json={}, headers={"Authorization": "Bearer " + self.operate}).status_code, 405)
        self.assertEqual(self.client.get("/v1/capabilities").status_code, 401)
        self.assertEqual(self.client.get("/openapi.json").status_code, 401)
        self.assertEqual(self.client.get("/v1/capabilities", headers={"Authorization": "Bearer " + self.read}).status_code, 200)
        self.assertEqual(self.client.post("/v1/repairs/" + "f" * 32 + "/approve", json={},
            headers={"Authorization": "Bearer " + self.operate}).status_code, 403)
        self.assertEqual(self.client.post("/v1/repairs/" + "f" * 32 + "/execute", json={},
            headers={"Authorization": "Bearer " + self.approve}).status_code, 403)

    def test_missing_large_and_symlink_assets_fail_closed_without_local_file_leaks(self):
        path = self.web / "app.mjs"
        path.unlink()
        response = self.client.get("/ui/app.mjs")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"error": {"code": "ui_unavailable"}})
        path.write_bytes(b"x" * (256 * 1024 + 1))
        self.assertEqual(self.client.get("/ui/app.mjs").status_code, 404)
        path.unlink()
        private = self.root / "private.json"
        private.write_text("unrelated private content")
        path.symlink_to(private)
        response = self.client.get("/ui/app.mjs")
        self.assertEqual(response.status_code, 404)
        self.assertNotIn("unrelated private content", response.text)
        self.assertNotIn(str(private), response.text)

    def test_asset_root_symlink_is_refused_and_unrelated_files_preserved(self):
        alternate = self.root / "alternate"
        self.web.rename(alternate)
        self.web.symlink_to(alternate, target_is_directory=True)
        response = self.client.get("/")
        self.assertEqual(response.status_code, 404)
        self.assertEqual((alternate / "index.html").read_text(), "synthetic public asset index.html")

    def test_assets_are_not_api_schema_operations(self):
        schema = self.client.get("/openapi.json", headers={"Authorization": "Bearer " + self.read}).json()
        self.assertTrue(all(path not in schema["paths"] for path in UI_ASSETS))
