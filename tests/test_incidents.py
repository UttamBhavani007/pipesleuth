"""Observable incidents, explicit retry policy and verified no-op replay."""

from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

from pipesleuth.domain import ORDER_COLUMNS
from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import MAX_EVENT_LIMIT, PipelineError, PipelineStateError, SandboxPipeline


class IncidentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.pipeline = SandboxPipeline(Path(temporary.name) / "runs")
        self.clean = generate_fixture("healthy", row_count=3).to_source_payload()

    def rows(self, run_id):
        with closing(sqlite3.connect(self.pipeline.database_path(run_id))) as connection:
            return connection.execute("SELECT * FROM warehouse_orders ORDER BY order_id").fetchall()

    def snapshot(self, run_id):
        directory = self.pipeline.database_path(run_id).parent
        return {path.name: (path.read_bytes(), path.stat().st_mtime_ns) for path in directory.iterdir()}

    def test_schema_fault_and_clean_control_record_real_observations(self):
        clean = self.pipeline.create(self.clean)
        clean_result = self.pipeline.run(clean.run_id)
        self.assertEqual((clean_result.rows_loaded, clean_result.totals_minor), (3, {"EUR": 8207}))
        fault = self.pipeline.create(generate_fixture("schema_drift", row_count=3).to_source_payload())
        failed = self.pipeline.run(fault.run_id)
        self.assertEqual((failed.status, failed.rows_loaded, failed.totals_minor), ("failed", 0, {}))
        self.assertEqual(self.rows(fault.run_id), [])
        events = self.pipeline.events(fault.run_id)
        self.assertEqual([event["event_type"] for event in events], ["source_received", "load_started", "load_failed"])
        self.assertEqual(events[0]["details"], {
            "observed_schema": ["order_id", "customer_id", "gross_total_minor", "currency", "created_at"],
            "required_schema": list(ORDER_COLUMNS), "raw_row_count": 3,
        })
        self.assertEqual(events[-1]["details"]["code"], "schema_mismatch")
        self.assertEqual(events[-1]["details"]["missing_columns"], ["amount_minor"])
        self.assertEqual(events[-1]["details"]["extra_columns"], ["gross_total_minor"])
        serialized = json.dumps(events)
        for label in ("scenario", "oracle", "expected_orders", "seed", "schema_drift"):
            self.assertNotIn(label, serialized)
        self.assertEqual(self.pipeline.inspect(clean.run_id), clean_result)

    def test_retries_reject_by_default_and_exact_duplicates_skip_only_when_requested(self):
        source = generate_fixture("duplicate_ingestion", row_count=3).to_source_payload()
        rejected = self.pipeline.create(source)
        failed = self.pipeline.run(rejected.run_id)
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.duplicate_policy, "reject")
        self.assertEqual(failed.duplicates_skipped, 0)
        self.assertEqual(self.rows(rejected.run_id), [])
        diagnostic = self.pipeline.events(rejected.run_id)[-1]["details"]
        self.assertEqual(diagnostic["code"], "duplicate_key")
        self.assertEqual((diagnostic["order_id"], diagnostic["row_number"]), ("ORD-0001", 4))
        accepted = self.pipeline.create(source, duplicate_policy="skip_identical")
        succeeded = self.pipeline.run(accepted.run_id)
        self.assertEqual(succeeded.status, "succeeded")
        self.assertEqual(succeeded.duplicates_skipped, 3)
        self.assertEqual(succeeded.rows_loaded, 3)
        self.assertEqual(succeeded.totals_minor, {"EUR": 8207})
        self.assertEqual(self.pipeline.events(accepted.run_id)[-1]["details"], {
            "rows_loaded": 3, "duplicates_skipped": 3,
        })
        self.assertEqual(self.pipeline.run(accepted.run_id), succeeded)
        self.assertEqual(len(self.rows(accepted.run_id)), 3)

    def test_canonical_equal_timestamp_spellings_can_be_identical_retries(self):
        retry = dict(self.clean["rows"][0])
        retry["created_at"] = "2026-01-01T09:00:00+00:00"
        self.clean["rows"].extend([retry, dict(retry)])
        created = self.pipeline.create(self.clean, duplicate_policy="skip_identical")
        summary = self.pipeline.run(created.run_id)
        self.assertEqual((summary.rows_loaded, summary.duplicates_skipped), (3, 2))
        self.assertEqual(summary.totals_minor, {"EUR": 8207})

    def test_conflicting_same_key_never_silently_selects_a_record(self):
        for policy in ("reject", "skip_identical"):
            for column, changed in (("customer_id", "C-OTHER"), ("amount_minor", 910),
                                    ("currency", "USD"), ("created_at", "2026-01-01T09:00:01Z")):
                with self.subTest(policy=policy, column=column):
                    source = json.loads(json.dumps(self.clean))
                    retry = dict(source["rows"][0])
                    retry[column] = changed
                    source["rows"].append(retry)
                    created = self.pipeline.create(source, duplicate_policy=policy)
                    failed = self.pipeline.run(created.run_id)
                    self.assertEqual(failed.status, "failed")
                    self.assertEqual((failed.rows_loaded, failed.duplicates_skipped), (0, 0))
                    self.assertEqual(self.rows(created.run_id), [])
                    self.assertEqual(self.pipeline.events(created.run_id)[-1]["details"]["code"], "conflicting_duplicate")

    def test_policy_is_creation_only_and_invalid_policy_creates_nothing(self):
        for policy in ("silent", "skip", "", None, True, {}):
            with self.subTest(policy=policy), self.assertRaises(PipelineError):
                self.pipeline.create(self.clean, duplicate_policy=policy)
        self.assertFalse(self.pipeline.data_dir.exists())
        created = self.pipeline.create(self.clean)
        with self.assertRaises(TypeError):
            self.pipeline.run(created.run_id, duplicate_policy="skip_identical")
        self.assertEqual(self.pipeline.inspect(created.run_id).duplicate_policy, "reject")

    def test_events_are_bounded_read_only_and_separate_from_fixture_truth(self):
        created = self.pipeline.create(self.clean)
        self.pipeline.run(created.run_id)
        before = self.snapshot(created.run_id)
        self.assertEqual(len(self.pipeline.events(created.run_id, limit=1)), 1)
        self.assertEqual([event["event_id"] for event in self.pipeline.events(created.run_id)], [1, 2, 3])
        self.assertEqual(self.snapshot(created.run_id), before)
        for limit in (0, -1, MAX_EVENT_LIMIT + 1, True, 1.5, "10"):
            with self.subTest(limit=limit), self.assertRaises(PipelineError):
                self.pipeline.events(created.run_id, limit=limit)
        missing = SandboxPipeline(self.pipeline.data_dir / "absent")
        with self.assertRaises(PipelineError):
            missing.events("0" * 32)
        self.assertFalse(missing.data_dir.exists())

    def test_successful_replay_does_not_write_files_events_or_clock(self):
        created = self.pipeline.create(self.clean)
        finished = self.pipeline.run(created.run_id)
        before = self.snapshot(created.run_id)
        history = self.pipeline.events(created.run_id)
        with patch("pipesleuth.pipeline._now", side_effect=AssertionError("replay must not read the clock")):
            self.assertEqual(self.pipeline.run(created.run_id), finished)
        self.assertEqual(self.pipeline.events(created.run_id), history)
        self.assertEqual(self.snapshot(created.run_id), before)

    def test_every_persisted_order_field_is_checked_before_replay(self):
        changes = {
            "order_id": "ORD-DIFFERENT", "customer_id": "CUST-DIFFERENT", "amount_minor": 910,
            "currency": "USD", "created_at": "2026-01-01T09:00:01Z",
        }
        for column, value in changes.items():
            with self.subTest(column=column):
                created = self.pipeline.create(self.clean)
                self.pipeline.run(created.run_id)
                with closing(sqlite3.connect(self.pipeline.database_path(created.run_id))) as connection, connection:
                    # Column names come exclusively from this fixed test allowlist.
                    connection.execute(f"UPDATE warehouse_orders SET {column} = ? WHERE order_id = ?", (value, "ORD-0001"))
                before = self.snapshot(created.run_id)
                with self.assertRaises(PipelineStateError):
                    self.pipeline.run(created.run_id)
                self.assertEqual(self.snapshot(created.run_id), before)
                self.assertEqual(self.pipeline.inspect(created.run_id).status, "succeeded")

    def test_missing_extra_orders_or_changed_skip_count_block_replay(self):
        for change in ("missing", "extra", "skip_count"):
            with self.subTest(change=change):
                created = self.pipeline.create(self.clean)
                self.pipeline.run(created.run_id)
                with closing(sqlite3.connect(self.pipeline.database_path(created.run_id))) as connection, connection:
                    if change == "missing":
                        connection.execute("DELETE FROM warehouse_orders WHERE order_id = ?", ("ORD-0001",))
                    elif change == "extra":
                        connection.execute("INSERT INTO warehouse_orders VALUES (?, ?, ?, ?, ?)",
                                           ("EXTRA", "C", 0, "EUR", "2026-01-01T00:00:00Z"))
                    else:
                        connection.execute("UPDATE pipeline_run SET duplicates_skipped = 1")
                before = self.snapshot(created.run_id)
                with self.assertRaises(PipelineStateError):
                    self.pipeline.run(created.run_id)
                self.assertEqual(self.snapshot(created.run_id), before)

    def test_failed_and_interrupted_runs_do_not_restart_or_gain_events(self):
        failed = self.pipeline.create(generate_fixture("schema_drift").to_source_payload())
        self.pipeline.run(failed.run_id)
        running = self.pipeline.create(self.clean)
        with closing(sqlite3.connect(self.pipeline.database_path(running.run_id))) as connection, connection:
            connection.execute("UPDATE pipeline_run SET status = 'running'")
        for summary in (failed, running):
            before = self.snapshot(summary.run_id)
            with self.assertRaises(PipelineStateError):
                self.pipeline.run(summary.run_id)
            self.assertEqual(self.snapshot(summary.run_id), before)

    def test_source_and_record_failures_have_observed_diagnostic_codes(self):
        invalid = json.loads(json.dumps(self.clean))
        invalid["rows"][-1]["amount_minor"] = "2786"
        created = self.pipeline.create(invalid)
        self.pipeline.run(created.run_id)
        details = self.pipeline.events(created.run_id)[-1]["details"]
        self.assertEqual((details["code"], details["row_number"]), ("invalid_row", 3))
        modified = self.pipeline.create(self.clean)
        source = self.pipeline.database_path(modified.run_id).parent / "source.json"
        source.write_bytes(source.read_bytes() + b"\n")
        self.pipeline.run(modified.run_id)
        details = self.pipeline.events(modified.run_id)[-1]["details"]
        self.assertEqual(details["code"], "source_changed")
        self.assertEqual(details["observed_sha256"], hashlib.sha256(source.read_bytes()).hexdigest())
        self.assertEqual(details["expected_sha256"], modified.source_sha256)

    def test_database_failure_after_two_inserts_rolls_back_the_entire_load(self):
        created = self.pipeline.create(self.clean)
        with closing(sqlite3.connect(self.pipeline.database_path(created.run_id))) as connection, connection:
            connection.execute(
                "CREATE TRIGGER reject_last BEFORE INSERT ON warehouse_orders "
                "WHEN NEW.order_id = 'ORD-0003' "
                "BEGIN SELECT RAISE(ABORT, 'test constraint failure'); END"
            )
        failed = self.pipeline.run(created.run_id)
        self.assertEqual(failed.status, "failed")
        self.assertEqual(self.rows(created.run_id), [])
        self.assertEqual(self.pipeline.events(created.run_id)[-1]["details"]["code"], "load_error")
        self.assertIn("test constraint failure", failed.error)

    def test_legacy_success_inspection_events_and_replay_make_no_schema_changes(self):
        run_id = uuid4().hex
        directory = self.pipeline.data_dir / run_id
        directory.mkdir(parents=True)
        source = json.dumps(self.clean, separators=(",", ":")).encode()
        (directory / "source.json").write_bytes(source)
        timestamp = "2026-10-21T10:00:00Z"
        with closing(sqlite3.connect(directory / "warehouse.sqlite3")) as connection, connection:
            connection.execute("CREATE TABLE pipeline_run (run_id TEXT PRIMARY KEY, status TEXT NOT NULL, "
                               "source_sha256 TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT, "
                               "finished_at TEXT, error TEXT)")
            connection.execute("CREATE TABLE warehouse_orders (order_id TEXT PRIMARY KEY NOT NULL, "
                               "customer_id TEXT NOT NULL, amount_minor INTEGER NOT NULL CHECK(amount_minor >= 0), "
                               "currency TEXT NOT NULL, created_at TEXT NOT NULL)")
            connection.execute("INSERT INTO pipeline_run VALUES (?, ?, ?, ?, ?, ?, ?)",
                               (run_id, "succeeded", hashlib.sha256(source).hexdigest(), timestamp, timestamp, timestamp, None))
            connection.executemany("INSERT INTO warehouse_orders VALUES (?, ?, ?, ?, ?)",
                                   [tuple(row[column] for column in ORDER_COLUMNS) for row in self.clean["rows"]])
        before = self.snapshot(run_id)
        summary = self.pipeline.inspect(run_id)
        self.assertEqual((summary.duplicate_policy, summary.duplicates_skipped), ("reject", 0))
        self.assertEqual(self.pipeline.events(run_id), [])
        with patch("pipesleuth.pipeline._now", side_effect=AssertionError("legacy replay must not write clocks")):
            self.assertEqual(self.pipeline.run(run_id), summary)
        self.assertEqual(self.snapshot(run_id), before)


if __name__ == "__main__":
    unittest.main()
