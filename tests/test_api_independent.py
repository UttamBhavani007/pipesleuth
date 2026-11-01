"""Independent local API checks, with a controlled HTTP fixture, not an LLM."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import secrets
import sqlite3
import tempfile
import unittest

try:
    from fastapi.testclient import TestClient
    import httpx  # TestClient's optional transport dependency.
except ImportError:
    raise unittest.SkipTest("optional API test dependencies are not installed")

from pipesleuth.api import ApiSettings, create_app
from pipesleuth.pipeline import SandboxPipeline
from test_local_model_integration import DIGEST, local_server


COLUMNS = ["order_id", "customer_id", "amount_minor", "currency", "created_at"]
ROWS = [
    {"order_id": "A-1", "customer_id": "C-A", "amount_minor": 125,
     "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"},
    {"order_id": "A-2", "customer_id": "C-B", "amount_minor": 375,
     "currency": "USD", "created_at": "2026-01-02T09:00:00Z"},
]
EXPECTED = [("A-1", "C-A", 125, "EUR", "2026-01-01T09:00:00Z"),
            ("A-2", "C-B", 375, "USD", "2026-01-02T09:00:00Z")]


def files(root):
    return {str(path.relative_to(root)): (path.read_bytes(), path.stat().st_mtime_ns)
            for path in root.rglob("*") if path.is_file()} if root.exists() else {}


class IndependentApiTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.settings = ApiSettings(*(self.root / name for name in
            ("incidents", "derived", "investigations", "repairs")), port=8765)
        self.tokens = {name: secrets.token_urlsafe(32) for name in ("read", "operation", "approval")}
        self.pipeline = SandboxPipeline(self.settings.data_dir)
        healthy = self.pipeline.create({"schema": COLUMNS[:], "rows": deepcopy(ROWS)})
        self.pipeline.run(healthy.run_id)
        self.healthy_id = healthy.run_id
        broken = {"schema": ["gross_total_minor" if name == "amount_minor" else name for name in COLUMNS],
                  "rows": deepcopy(ROWS)}
        for row in broken["rows"]:
            row["gross_total_minor"] = row.pop("amount_minor")
        failed = self.pipeline.create(broken)
        self.assertEqual(self.pipeline.run(failed.run_id).status, "failed")
        self.failed_id = failed.run_id
        self.client = self.enterContext(self.client_for(self.settings))

    def client_for(self, settings):
        app = create_app(settings, read_token=self.tokens["read"],
                         operation_token=self.tokens["operation"], approval_token=self.tokens["approval"])
        return TestClient(app, base_url="http://127.0.0.1:8765", client=("127.0.0.1", 50000))

    def request(self, method, path, *, role="operation", client=None, **kwargs):
        headers = dict(kwargs.pop("headers", {}))
        if role is not None:
            headers["Authorization"] = "Bearer " + self.tokens[role]
        return (client or self.client).request(method, path, headers=headers, **kwargs)

    def error(self, response, expected_status):
        self.assertEqual(response.status_code, expected_status)
        body = response.json()
        self.assertEqual(set(body), {"error"})
        self.assertEqual(set(body["error"]), {"code"})
        self.assertRegex(body["error"]["code"], r"^[a-z][a-z0-9_]{0,63}$")
        self.assertFalse(str(self.root) in response.text, "an error echoed a private local path")
        self.assertFalse(any(token in response.text for token in self.tokens.values()), "an error leaked a capability")

    def plan(self):
        response = self.request("POST", "/v1/repairs", json={"run_id": self.failed_id, "action": "rename_amount_column"})
        self.assertEqual(response.status_code, 200)
        return response.json()

    def approve(self, record):
        state = record["state"]
        return self.request("POST", f"/v1/repairs/{record['investigation_id']}/approve", role="approval",
                            json={"plan_sha256": state["plan_sha256"],
                                  "input_fingerprint_sha256": state["input_fingerprint_sha256"]})

    def test_read_capabilities_and_noncreating_failures_cannot_mutate(self):
        before = files(self.root)
        self.assertEqual(self.request("GET", "/health", role=None).json(), {"status": "ok", "api_version": 1})
        self.error(self.request("GET", "/v1/capabilities", role=None), 401)
        for role in self.tokens:
            self.assertEqual(self.request("GET", f"/v1/runs/{self.healthy_id}", role=role).status_code, 200)
        for resource in ("runs", "investigations", "repairs"):
            self.error(self.request("GET", f"/v1/{resource}/{'0' * 32}", role="read"), 404)
        self.error(self.request("GET", f"/v1/runs/{self.healthy_id}?data_dir={self.root}", role="read"), 400)
        self.error(self.request("POST", f"/v1/runs/{self.healthy_id}/diagnosis", role="read", json={"method": "checklist"}), 403)
        self.error(self.request("POST", f"/v1/runs/{self.healthy_id}/diagnosis", role="approval", json={"method": "checklist"}), 403)
        self.assertEqual(files(self.root), before)

    def test_strict_json_and_security_errors_do_not_echo_inputs(self):
        route = f"/v1/runs/{self.healthy_id}/diagnosis"
        samples = [(b'{"method":"checklist","method":"local"}', "application/json", 400),
                   (b'{"method":NaN}', "application/json", 400),
                   (b" " * 4097, "application/json", 413),
                   (b"hello", "text/plain", 415)]
        before = files(self.root)
        for body, media, status in samples:
            with self.subTest(status=status, media=media):
                self.error(self.request("POST", route, content=body, headers={"Content-Type": media}), status)
        self.error(self.request("POST", route, json={"method": "checklist", "data_dir": str(self.root)}), 422)
        self.error(self.request("GET", "/v1/capabilities", headers={"Origin": "https://example.invalid"}), 403)
        self.error(self.request("GET", "/v1/capabilities", headers={"Host": "example.invalid"}), 400)
        self.error(self.request("GET", "/v1/capabilities", headers={"X-Forwarded-For": "127.0.0.1"}), 400)
        self.assertEqual(files(self.root), before)

    def test_operator_cannot_approve_and_exact_approved_repair_is_verified(self):
        original = files(self.pipeline.data_dir)
        record = self.plan()
        identifier, state = record["investigation_id"], record["state"]
        approval = {"plan_sha256": state["plan_sha256"], "input_fingerprint_sha256": state["input_fingerprint_sha256"]}
        self.error(self.request("POST", f"/v1/repairs/{identifier}/approve", json=approval), 403)
        self.error(self.request("POST", f"/v1/repairs/{identifier}/execute", role="approval", json={}), 403)
        self.error(self.request("POST", f"/v1/repairs/{identifier}/approve", role="approval",
                                json={**approval, "plan_sha256": "0" * 64}), 409)
        self.assertEqual(self.approve(record).status_code, 200)
        result = self.request("POST", f"/v1/repairs/{identifier}/execute", json={})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json()["state"]["status"], "succeeded")
        database = self.settings.output_dir / state["plan"]["target_run_id"] / "warehouse.sqlite3"
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
            self.assertEqual(connection.execute("SELECT order_id,customer_id,amount_minor,currency,created_at FROM warehouse_orders ORDER BY order_id").fetchall(), EXPECTED)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM quarantined_records").fetchone(), (0,))
        self.error(self.request("POST", f"/v1/repairs/{identifier}/execute", json={}), 409)
        self.assertEqual(files(self.pipeline.data_dir), original)

    def test_stale_approval_is_refused_without_target_or_record_change(self):
        record = self.plan()
        approved = self.approve(record).json()
        source = self.pipeline.data_dir / self.failed_id / "source.json"
        source.write_bytes(source.read_bytes() + b"\n")
        before = files(self.root)
        self.error(self.request("POST", f"/v1/repairs/{record['investigation_id']}/execute", json={}), 409)
        shown = self.request("GET", f"/v1/repairs/{record['investigation_id']}", role="read")
        self.assertEqual(shown.json(), approved)
        self.assertEqual(files(self.root), before)

    def test_concurrent_execution_admits_one_target_and_preserves_incident(self):
        original = files(self.pipeline.data_dir)
        record = self.plan()
        self.assertEqual(self.approve(record).status_code, 200)
        path = f"/v1/repairs/{record['investigation_id']}/execute"
        with ThreadPoolExecutor(max_workers=2) as executor:
            responses = list(executor.map(lambda _: self.request("POST", path, json={}), range(2)))
        statuses = [response.status_code for response in responses]
        self.assertEqual(statuses.count(200), 1)
        self.assertIn(next(status for status in statuses if status != 200), (409, 503))
        self.assertEqual([path.name for path in self.settings.output_dir.iterdir()], [record["state"]["plan"]["target_run_id"]])
        self.assertEqual(files(self.pipeline.data_dir), original)

    def test_controlled_http_model_fixture_resumes_without_real_inference_or_replay(self):
        # The server below serializes fixed responses; it never runs an AI model.
        state = {"generation": {"response": '{"status":"unknown","findings":[]}'}}
        with local_server(state) as (endpoint, observed):
            settings = replace(self.settings, endpoint=endpoint, model_digest=DIGEST,
                               timeout_seconds=2, agent_timeout=10)
            with self.client_for(settings) as client:
                route = f"/v1/runs/{self.healthy_id}/diagnosis"
                baseline = self.request("POST", route, client=client, json={"method": "local"})
                self.assertEqual(baseline.status_code, 200)
                self.assertEqual(baseline.json()["status"], "unknown")
                def generations():
                    return sum(request["path"] == "/api/generate" for request in observed["requests"])
                self.assertEqual(generations(), 1)
                state["generation"]["response"] = '{"action":"tool","tool":"read_logs"}'
                started = self.request("POST", "/v1/investigations", client=client, json={"run_id": self.healthy_id})
                self.assertEqual(started.status_code, 200)
                identifier = started.json()["investigation_id"]
                self.assertEqual(generations(), 1, "creating state must not generate")
                path = f"/v1/investigations/{identifier}/resume"
                paused = self.request("POST", path, client=client, json={"max_operations": 2})
                self.assertEqual(paused.status_code, 200)
                self.assertEqual(paused.json()["state"]["next"]["tool"], "read_logs")
                self.assertEqual(generations(), 2)
                state["generation"]["response"] = '{"action":"finish","diagnosis":{"status":"unknown","findings":[]}}'
                completed = self.request("POST", path, client=client, json={"max_operations": 2})
                self.assertEqual(completed.status_code, 200)
                result = completed.json()
                self.assertIs(result["state"]["done"], True)
                self.assertEqual(result["state"]["result"]["model_calls"], 2)
                self.assertEqual(generations(), 3)
                self.assertEqual(self.request("POST", path, client=client, json={"max_operations": 2}).json(), result)
                self.assertEqual(self.request("GET", f"/v1/investigations/{identifier}", client=client, role="read").json(), result)
                self.assertEqual(generations(), 3, "terminal resume/show must not repeat generation")
        self.error(self.request("POST", f"/v1/runs/{self.healthy_id}/diagnosis", json={"method": "local"}), 409)


if __name__ == "__main__":
    unittest.main()
