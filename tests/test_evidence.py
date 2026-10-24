"""Evidence contracts, provenance, bounded data and safe malformed-input handling."""

from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from pipesleuth.domain import ORDER_COLUMNS
from pipesleuth.evidence import EvidenceError, EvidenceGateway, MAX_TEXT_BYTES
from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import SandboxPipeline


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.pipeline = SandboxPipeline(self.root / "runs")
        self.gateway = EvidenceGateway(self.pipeline.data_dir)
        self.source = generate_fixture("healthy", row_count=3).to_source_payload()

    def create(self, source=None, **policies):
        summary = self.pipeline.create(source or self.source, **policies)
        self.pipeline.run(summary.run_id)
        return summary.run_id

    def call(self, name, run_id, **arguments):
        return self.gateway.call(name, {"run_id": run_id, **arguments})

    def mutate(self, run_id, sql, parameters=()):
        with closing(sqlite3.connect(self.pipeline.database_path(run_id))) as connection, connection:
            connection.execute(sql, parameters)

    def snapshot(self, run_id):
        directory = self.pipeline.database_path(run_id).parent
        return {path.name: (path.read_bytes(), path.stat().st_mtime_ns) for path in directory.iterdir()}

    def assert_digest(self, response):
        binding = {key: response[key] for key in ("run_id", "tool", "arguments", "data", "truncated")}
        encoded = json.dumps(binding, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
        self.assertEqual(response["content_sha256"], hashlib.sha256(encoded).hexdigest())
        self.assertEqual(response["evidence_id"], "ev_" + response["content_sha256"][:32])

    def test_catalog_is_fresh_json_safe_and_has_exact_closed_tool_schemas(self):
        catalog = EvidenceGateway.tools()
        self.assertEqual([tool["name"] for tool in catalog], ["read_run", "read_logs", "inspect_schema", "inspect_validation"])
        self.assertEqual(json.loads(json.dumps(catalog)), catalog)
        for tool in catalog:
            self.assertFalse(tool["parameters"]["additionalProperties"])
            self.assertEqual(tool["parameters"]["required"], ["run_id"])
            expected = {"run_id", "limit"} if tool["name"] in {"read_logs", "inspect_validation"} else {"run_id"}
            self.assertEqual(set(tool["parameters"]["properties"]), expected)
            self.assertIn("untrusted data", tool["description"])
        catalog[0]["parameters"]["properties"]["run_id"]["pattern"] = "changed"
        self.assertEqual(EvidenceGateway.tools()[0]["parameters"]["properties"]["run_id"]["pattern"], "^[0-9a-f]{32}$")

    def test_invalid_config_is_evidence_error_without_filesystem_access(self):
        for timeout in (True, None, 0, -1, 5.1, float("nan"), float("inf"), "2", 10**10000):
            with self.subTest(timeout_type=type(timeout)), self.assertRaises(EvidenceError):
                EvidenceGateway(self.root / "absent", timeout_seconds=timeout)
        for cap in (True, None, 1023, 16385, 1024.0, "16384"):
            with self.subTest(cap=cap), self.assertRaises(EvidenceError):
                EvidenceGateway(self.root / "absent", max_response_bytes=cap)
        self.assertFalse((self.root / "absent").exists())
        EvidenceGateway(self.root / "absent", timeout_seconds=5, max_response_bytes=1024)

    def test_argument_allowlist_rejects_injection_before_path_or_sql_access(self):
        run_id = "a" * 32
        invalid = [
            ("query", {"run_id": run_id}), ("read_run", {}), ("read_logs", []),
            ("read_run", {"run_id": "../outside"}), ("read_run", {"run_id": "A" * 32}),
            ("read_run", {"run_id": run_id, "limit": 1}),
            ("read_run", {"run_id": run_id, "sql": "DROP TABLE warehouse_orders"}),
            ("read_logs", {"run_id": run_id, "path": "/etc/passwd"}),
            ("inspect_validation", {"run_id": run_id, "shell": "touch sentinel"}),
        ]
        for limit in (0, 21, True, "10", None, 1.0):
            invalid.append(("read_logs", {"run_id": run_id, "limit": limit}))
        with patch("pipesleuth.evidence.SandboxPipeline", side_effect=AssertionError("filesystem must not be reached")):
            for name, arguments in invalid:
                with self.subTest(name=name, arguments=arguments), self.assertRaises(EvidenceError):
                    self.gateway.call(name, arguments)

    def test_provenance_normalizes_default_limit_and_excludes_observation_clock(self):
        run_id = self.create()
        first = self.call("read_logs", run_id)
        second = self.call("read_logs", run_id, limit=10)
        self.assertEqual(first["arguments"], {"run_id": run_id, "limit": 10})
        self.assertEqual(first["content_sha256"], second["content_sha256"])
        self.assertEqual(first["evidence_id"], second["evidence_id"])
        self.assert_digest(first)
        self.assertTrue(first["observed_at"].endswith("Z"))
        changed_limit = self.call("read_logs", run_id, limit=1)
        self.assertNotEqual(first["content_sha256"], changed_limit["content_sha256"])
        self.assertTrue(changed_limit["truncated"])
        self.assertEqual(first["data"]["events_total"], 3)

    def test_summary_uses_actual_counts_and_schema_observations(self):
        run_id = self.create()
        self.mutate(run_id, "UPDATE pipeline_run SET rows_quarantined = 999")
        summary = self.call("read_run", run_id)["data"]
        self.assertEqual((summary["status"], summary["rows_loaded"], summary["rows_quarantined"]), ("succeeded", 3, 0))
        self.assertEqual(summary["totals_minor"], {"EUR": 8207})
        self.assertEqual(summary["duplicates_skipped"], 0)
        schema = self.call("inspect_schema", run_id)["data"]
        self.assertEqual(schema["required_columns"], list(ORDER_COLUMNS))
        self.assertEqual(schema["source_columns"], list(ORDER_COLUMNS))
        self.assertEqual(schema["warehouse_columns"][2], {"name": "amount_minor", "type": "INTEGER", "not_null": True, "primary_key": False})
        self.assertEqual(schema["warehouse_columns_omitted"], 0)
        fault_id = self.create(generate_fixture("schema_drift", row_count=3).to_source_payload())
        self.assertIn("gross_total_minor", self.call("inspect_schema", fault_id)["data"]["source_columns"])
        failure = self.call("inspect_validation", fault_id)["data"]["failure"]
        self.assertEqual(failure["code"], "schema_mismatch")
        self.assertEqual(failure["missing_columns"], ["amount_minor"])
        self.assertNotIn("message", failure)

    def test_validation_reports_actual_accepted_and_quarantined_rows_with_bounded_raw_evidence(self):
        source = generate_fixture("invalid_records").to_source_payload()
        run_id = self.create(source, invalid_policy="quarantine")
        response = self.call("inspect_validation", run_id)
        data = response["data"]
        self.assertTrue(data["available"])
        self.assertTrue(data["warehouse_available"])
        self.assertEqual((data["rows_loaded"], data["rows_quarantined"]), (12, 4))
        self.assertEqual([row["row_number"] for row in data["records"]], [13, 14, 15, 16])
        self.assertEqual([row["raw_record"] for row in data["records"]], source["rows"][12:])
        self.assertEqual([row["reason_code"] for row in data["records"]], ["invalid_row"] * 4)
        self.assertEqual(data["records"][1]["reason"], "amount_minor must be a non-negative integer")
        limited = self.call("inspect_validation", run_id, limit=2)
        self.assertEqual(len(limited["data"]["records"]), 2)
        self.assertTrue(limited["truncated"])
        self.assert_digest(response)

    def test_source_change_blocks_schema_but_keeps_run_and_logs_readable(self):
        run_id = self.create()
        source_path = self.pipeline.database_path(run_id).parent / "source.json"
        source_path.write_bytes(source_path.read_bytes() + b"\n")
        with self.assertRaises(EvidenceError) as captured:
            self.call("inspect_schema", run_id)
        self.assertNotIn(str(self.root), str(captured.exception))
        self.assertEqual(self.call("read_run", run_id)["data"]["rows_loaded"], 3)
        self.assertEqual(len(self.call("read_logs", run_id)["data"]["events"]), 3)
        # Matching a checksum never authorizes an authoring envelope as source.
        authored = json.dumps(generate_fixture("healthy").to_authoring_dict()).encode()
        source_path.write_bytes(authored)
        self.mutate(run_id, "UPDATE pipeline_run SET source_sha256 = ?", (hashlib.sha256(authored).hexdigest(),))
        with self.assertRaises(EvidenceError):
            self.call("inspect_schema", run_id)

    def test_nested_known_fields_cannot_smuggle_private_objects(self):
        for details in ({"reason": {"oracle": "private-answer"}},
                        {"expected_sha256": {"golden": "private-answer"}},
                        {"observed_schema": [{"oracle": "private-answer"}]}):
            run_id = self.create(generate_fixture("schema_drift").to_source_payload())
            self.mutate(run_id, "UPDATE pipeline_events SET details_json = ? WHERE event_type = 'load_failed'", (json.dumps(details),))
            for tool in ("read_logs", "inspect_validation"):
                with self.subTest(details=details, tool=tool), self.assertRaises(EvidenceError) as captured:
                    self.call(tool, run_id)
                self.assertNotIn("private-answer", str(captured.exception))
        run_id = self.create(generate_fixture("invalid_records").to_source_payload(), invalid_policy="quarantine")
        self.mutate(run_id, "UPDATE quarantined_records SET raw_record_json = ? WHERE row_number = 13",
                    ('{"customer_id":{"oracle":"private-answer"}}',))
        with self.assertRaises(EvidenceError):
            self.call("inspect_validation", run_id)

    def test_utf8_pruning_obeys_small_cap_and_marks_all_transformations(self):
        source = {"schema": list(ORDER_COLUMNS), "rows": []}
        for index in range(4):
            row = dict(self.source["rows"][0])
            row.update(order_id="\U0001f4da" * 500 + str(index), currency="ZZZ")
            source["rows"].append(row)
        run_id = self.create(source, invalid_policy="quarantine")
        self.gateway = EvidenceGateway(self.pipeline.data_dir, max_response_bytes=1024)
        response = self.call("inspect_validation", run_id)
        encoded = json.dumps(response, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        self.assertLessEqual(len(encoded), 1024)
        self.assertTrue(response["truncated"])
        self.assert_digest(response)
        self.assertEqual(self.call("inspect_validation", run_id)["content_sha256"], response["content_sha256"])

        def strings(value):
            if isinstance(value, str):
                yield value
            elif isinstance(value, list):
                for item in value:
                    yield from strings(item)
            elif isinstance(value, dict):
                for item in value.values():
                    yield from strings(item)
        self.assertTrue(all(len(value.encode()) <= MAX_TEXT_BYTES for value in strings(response["data"])))

    def test_malformed_and_deep_persisted_json_fail_with_safe_errors(self):
        malformed = [b"\xff", "not-json", '{"observed_schema":' + "[" * 1100 + "0" + "]" * 1100 + "}"]
        for value in malformed:
            run_id = self.create()
            self.mutate(run_id, "UPDATE pipeline_events SET details_json = ? WHERE event_id = 1", (value,))
            before = self.snapshot(run_id)
            with self.subTest(value_type=type(value)), self.assertRaises(EvidenceError) as captured:
                self.call("read_logs", run_id)
            self.assertNotIn(str(self.root), str(captured.exception))
            self.assertEqual(self.snapshot(run_id), before)

    def test_absent_legacy_tables_are_distinct_from_genuine_empty_tables(self):
        run_id = self.create()
        self.assertTrue(self.call("inspect_validation", run_id)["data"]["available"])
        with closing(sqlite3.connect(self.pipeline.database_path(run_id))) as connection, connection:
            connection.execute("ALTER TABLE pipeline_run RENAME TO modern_run")
            connection.execute("CREATE TABLE pipeline_run (run_id TEXT PRIMARY KEY, status TEXT, source_sha256 TEXT, "
                               "created_at TEXT, started_at TEXT, finished_at TEXT, error TEXT)")
            connection.execute("INSERT INTO pipeline_run SELECT run_id,status,source_sha256,created_at,started_at,finished_at,error FROM modern_run")
            connection.execute("DROP TABLE modern_run")
            connection.execute("DROP TABLE quarantined_records")
            connection.execute("DROP TABLE pipeline_events")
        before = self.snapshot(run_id)
        validation = self.call("inspect_validation", run_id)["data"]
        self.assertEqual((validation["available"], validation["rows_quarantined"], validation["records"]), (False, 0, []))
        self.assertEqual(validation["rows_loaded"], 3)
        self.assertFalse(self.call("read_logs", run_id)["data"]["available"])
        run = self.call("read_run", run_id)["data"]
        self.assertEqual((run["duplicate_policy"], run["invalid_policy"]), ("reject", "reject"))
        self.assertEqual(self.snapshot(run_id), before)

    def test_missing_symlink_and_expired_deadline_fail_without_writes(self):
        missing = EvidenceGateway(self.root / "absent")
        with self.assertRaises(EvidenceError):
            missing.call("read_run", {"run_id": "0" * 32})
        self.assertFalse((self.root / "absent").exists())
        run_id = self.create()
        before = self.snapshot(run_id)
        with patch("pipesleuth.evidence.time.monotonic", side_effect=[10.0, 20.0]):
            with self.assertRaises(EvidenceError):
                self.call("read_run", run_id)
        self.assertEqual(self.snapshot(run_id), before)
        target = self.root / "target.txt"
        target.write_text("untouched")
        source = self.pipeline.database_path(run_id).parent / "source.json"
        source.unlink()
        source.symlink_to(target)
        with self.assertRaises(EvidenceError):
            self.call("inspect_schema", run_id)
        self.assertEqual(target.read_text(), "untouched")

    def test_wal_mode_without_sidecars_is_rejected_before_sqlite_can_create_them(self):
        run_id = self.create()
        with closing(sqlite3.connect(self.pipeline.database_path(run_id))) as connection:
            self.assertEqual(connection.execute("PRAGMA journal_mode = WAL").fetchone()[0], "wal")
        directory = self.pipeline.database_path(run_id).parent
        self.assertEqual({path.name for path in directory.iterdir()}, {"source.json", "warehouse.sqlite3"})
        before = self.snapshot(run_id)
        with patch("pipesleuth.evidence.sqlite3.connect", side_effect=AssertionError("WAL must be rejected before SQLite opens")):
            for tool in EvidenceGateway.tools():
                with self.subTest(tool=tool["name"]), self.assertRaises(EvidenceError):
                    self.call(tool["name"], run_id)
        self.assertEqual(self.snapshot(run_id), before)

    def test_existing_regular_journal_sidecars_are_rejected_without_modifying_them(self):
        run_id = self.create()
        database = self.pipeline.database_path(run_id)
        for suffix in ("-wal", "-shm", "-journal"):
            sidecar = database.with_name(database.name + suffix)
            sidecar.write_bytes(b"retained sidecar evidence")
            before = self.snapshot(run_id)
            try:
                with patch("pipesleuth.evidence.sqlite3.connect", side_effect=AssertionError("sidecar must be rejected before opening")):
                    with self.assertRaises(EvidenceError):
                        self.call("read_run", run_id)
                self.assertEqual(self.snapshot(run_id), before)
            finally:
                sidecar.unlink()

    def test_malformed_and_unsupported_database_headers_fail_before_sqlite_open(self):
        run_id = self.create()
        database = self.pipeline.database_path(run_id)
        original = database.read_bytes()
        invalid = [b"short", b"not a sqlite db!\x00" + original[16:], original[:18] + b"\x03\x01" + original[20:]]
        for encoded in invalid:
            database.write_bytes(encoded)
            before = self.snapshot(run_id)
            with patch("pipesleuth.evidence.sqlite3.connect", side_effect=AssertionError("invalid header must not be opened")):
                with self.assertRaises(EvidenceError) as captured:
                    self.call("read_logs", run_id)
            self.assertNotIn(str(self.root), str(captured.exception))
            self.assertEqual(self.snapshot(run_id), before)
        database.write_bytes(original)

    def test_unknown_quarantine_diagnostics_are_rejected_instead_of_invented(self):
        source = generate_fixture("invalid_records").to_source_payload()
        for query, value in (("UPDATE quarantined_records SET reason_code = ? WHERE row_number = 13", "unsupported"),
                             ("UPDATE quarantined_records SET row_number = ? WHERE row_number = 13", 0),
                             ("UPDATE quarantined_records SET reason = ? WHERE row_number = 13", b"not text")):
            run_id = self.create(source, invalid_policy="quarantine")
            self.mutate(run_id, query, (value,))
            before = self.snapshot(run_id)
            with self.assertRaises(EvidenceError):
                self.call("inspect_validation", run_id)
            self.assertEqual(self.snapshot(run_id), before)

    @unittest.skipUnless(hasattr(os, "mkfifo"), "requires POSIX named pipes")
    def test_named_pipe_source_and_database_are_rejected_without_blocking_or_writes(self):
        run_id = self.create()
        directory = self.pipeline.database_path(run_id).parent

        def snapshot_without_reading_special_files():
            return {path.name: (path.lstat().st_mode, path.lstat().st_ino, path.lstat().st_size,
                                path.lstat().st_mtime_ns, path.read_bytes() if path.is_file() else None)
                    for path in directory.iterdir()}

        script = (
            "import sys; from pipesleuth.evidence import EvidenceGateway, EvidenceError\n"
            "gateway = EvidenceGateway(sys.argv[1], timeout_seconds=.05)\n"
            "try: gateway.call(sys.argv[3], {'run_id': sys.argv[2]})\n"
            "except EvidenceError: print('rejected')\n"
            "else: raise AssertionError('named pipe must be rejected')\n"
        )
        for filename, tool in (("source.json", "inspect_schema"), ("warehouse.sqlite3", "read_run")):
            with self.subTest(filename=filename):
                path = directory / filename
                original = path.read_bytes()
                path.unlink()
                os.mkfifo(path)
                before = snapshot_without_reading_special_files()
                try:
                    completed = subprocess.run(
                        [sys.executable, "-c", script, str(self.pipeline.data_dir), run_id, tool],
                        capture_output=True, text=True, timeout=1, check=False,
                    )
                    self.assertEqual(completed.returncode, 0, completed.stderr)
                    self.assertEqual(completed.stdout.strip(), "rejected")
                    self.assertEqual(snapshot_without_reading_special_files(), before)
                finally:
                    path.unlink()
                    path.write_bytes(original)


if __name__ == "__main__":
    unittest.main()
