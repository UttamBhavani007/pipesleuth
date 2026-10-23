"""Retained invalid records, atomic loading and guarded quarantine replay."""

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
from pipesleuth.pipeline import (
    MAX_QUARANTINE_LIMIT, MAX_SOURCE_BYTES, PipelineError, PipelineStateError,
    SQLITE_MAX_INTEGER, SandboxPipeline,
)


class QuarantineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.pipeline = SandboxPipeline(Path(temporary.name) / "runs")
        self.source = generate_fixture("invalid_records").to_source_payload()
        self.clean = generate_fixture("healthy", row_count=3).to_source_payload()

    def sql_rows(self, run_id, query, parameters=()):
        with closing(sqlite3.connect(self.pipeline.database_path(run_id))) as connection:
            return connection.execute(query, parameters).fetchall()

    def snapshot(self, run_id):
        directory = self.pipeline.database_path(run_id).parent
        return {path.name: (path.read_bytes(), path.stat().st_mtime_ns) for path in directory.iterdir()}

    def test_fixed_fixture_keeps_twelve_orders_and_four_exact_invalid_records(self):
        created = self.pipeline.create(self.source, invalid_policy="quarantine")
        before_source = (self.pipeline.database_path(created.run_id).parent / "source.json").read_bytes()
        summary = self.pipeline.run(created.run_id)
        self.assertEqual(summary.status, "succeeded")
        self.assertEqual((summary.rows_loaded, summary.rows_quarantined), (12, 4))
        self.assertEqual(summary.totals_minor, {"EUR": 73525})
        self.assertEqual(summary.invalid_policy, "quarantine")
        self.assertEqual(summary.duplicates_skipped, 0)
        self.assertNotIn("error", summary.to_dict())
        records = self.pipeline.quarantine(created.run_id)
        self.assertEqual([row["row_number"] for row in records], [13, 14, 15, 16])
        self.assertEqual([row["raw_record"] for row in records], self.source["rows"][12:])
        self.assertEqual([row["reason_code"] for row in records], ["invalid_row"] * 4)
        self.assertEqual([row["reason"] for row in records], [
            "order columns do not match contract (missing=['amount_minor'], extra=[])",
            "amount_minor must be a non-negative integer",
            "currency must be one of EUR, GBP, USD",
            "created_at must be timezone-aware and in UTC",
        ])
        persisted = self.sql_rows(created.run_id, "SELECT row_number, reason_code, reason, raw_record_json "
                                  "FROM quarantined_records ORDER BY row_number")
        self.assertEqual([json.loads(row[3]) for row in persisted], self.source["rows"][12:])
        loaded = self.sql_rows(created.run_id, "SELECT * FROM warehouse_orders ORDER BY order_id")
        self.assertEqual(loaded, [tuple(row[column] for column in ORDER_COLUMNS) for row in self.source["rows"][:12]])
        source_path = self.pipeline.database_path(created.run_id).parent / "source.json"
        self.assertEqual(source_path.read_bytes(), before_source)
        self.assertEqual(hashlib.sha256(before_source).hexdigest(), summary.source_sha256)
        self.assertEqual(self.pipeline.events(created.run_id)[-1]["details"], {
            "rows_loaded": 12, "duplicates_skipped": 0, "rows_quarantined": 4,
        })
        metadata = self.sql_rows(created.run_id, "SELECT invalid_policy, rows_quarantined FROM pipeline_run")
        self.assertEqual(metadata, [("quarantine", 4)])
        serialized = json.dumps(summary.to_dict()) + json.dumps(self.pipeline.events(created.run_id))
        for label in ("oracle", "expected_orders", "scenario", "invalid_records", "seed"):
            self.assertNotIn(label, serialized)

    def test_default_reject_is_atomic_and_does_not_implicitly_quarantine(self):
        created = self.pipeline.create(self.source)
        failed = self.pipeline.run(created.run_id)
        self.assertEqual((failed.status, failed.invalid_policy), ("failed", "reject"))
        self.assertEqual((failed.rows_loaded, failed.rows_quarantined), (0, 0))
        self.assertEqual(self.pipeline.quarantine(created.run_id), [])
        self.assertEqual(self.sql_rows(created.run_id, "SELECT * FROM warehouse_orders"), [])
        self.assertIn("row 13", failed.error)
        self.assertEqual(self.pipeline.events(created.run_id)[-1]["details"]["code"], "invalid_row")

    def test_money_types_are_preserved_without_float_string_or_boolean_coercion(self):
        amounts = [True, 12.5, "1250", None, SQLITE_MAX_INTEGER + 1]
        for index, amount in enumerate(amounts, start=4):
            row = dict(self.clean["rows"][0])
            row.update(order_id=f"ORDER-{index}", amount_minor=amount)
            self.clean["rows"].append(row)
        created = self.pipeline.create(self.clean, invalid_policy="quarantine")
        summary = self.pipeline.run(created.run_id)
        self.assertEqual((summary.rows_loaded, summary.rows_quarantined), (3, 5))
        self.assertEqual(summary.totals_minor, {"EUR": 8207})
        quarantined = self.pipeline.quarantine(created.run_id)
        self.assertEqual([row["row_number"] for row in quarantined], [4, 5, 6, 7, 8])
        self.assertEqual([row["raw_record"]["amount_minor"] for row in quarantined], amounts)
        self.assertEqual([type(row["raw_record"]["amount_minor"]) for row in quarantined],
                         [bool, float, str, type(None), int])
        self.assertEqual([row["reason_code"] for row in quarantined], ["invalid_row"] * 4 + ["integer_overflow"])
        self.assertEqual(quarantined[-1]["reason"], "amount_minor exceeds SQLite signed 64-bit integer range")

    def test_all_invalid_is_visibly_distinct_from_an_empty_valid_source(self):
        source = {"schema": list(ORDER_COLUMNS), "rows": self.source["rows"][12:]}
        created = self.pipeline.create(source, invalid_policy="quarantine")
        summary = self.pipeline.run(created.run_id)
        self.assertEqual((summary.status, summary.rows_loaded, summary.rows_quarantined), ("succeeded", 0, 4))
        self.assertEqual(summary.totals_minor, {})
        self.assertEqual([row["row_number"] for row in self.pipeline.quarantine(created.run_id)], [1, 2, 3, 4])
        self.assertEqual(self.pipeline.run(created.run_id), summary)
        empty = self.pipeline.create({"schema": list(ORDER_COLUMNS), "rows": []}, invalid_policy="quarantine")
        empty_summary = self.pipeline.run(empty.run_id)
        self.assertEqual((empty_summary.status, empty_summary.rows_loaded, empty_summary.rows_quarantined),
                         ("succeeded", 0, 0))

    def test_exact_retry_deduplication_can_combine_with_quarantine(self):
        invalid = dict(self.clean["rows"][0])
        invalid["amount_minor"] = True
        self.clean["rows"].extend([invalid, dict(self.clean["rows"][0]), dict(invalid), dict(self.clean["rows"][2])])
        created = self.pipeline.create(self.clean, duplicate_policy="skip_identical", invalid_policy="quarantine")
        summary = self.pipeline.run(created.run_id)
        self.assertEqual((summary.rows_loaded, summary.duplicates_skipped, summary.rows_quarantined), (3, 2, 2))
        self.assertEqual(summary.totals_minor, {"EUR": 8207})
        self.assertEqual([row["row_number"] for row in self.pipeline.quarantine(created.run_id)], [4, 6])
        self.assertEqual(self.pipeline.run(created.run_id), summary)

    def test_quarantine_does_not_absorb_global_schema_checksum_or_size_failures(self):
        schema = self.pipeline.create(generate_fixture("schema_drift").to_source_payload(), invalid_policy="quarantine")
        self.assertEqual(self.pipeline.run(schema.run_id).status, "failed")
        self.assertEqual(self.pipeline.quarantine(schema.run_id), [])
        for changed in (b"\n", b"x" * (MAX_SOURCE_BYTES + 1)):
            created = self.pipeline.create(self.source, invalid_policy="quarantine")
            source = self.pipeline.database_path(created.run_id).parent / "source.json"
            source.write_bytes(changed)
            failed = self.pipeline.run(created.run_id)
            self.assertEqual((failed.status, failed.rows_loaded, failed.rows_quarantined), ("failed", 0, 0))
            self.assertEqual(self.pipeline.quarantine(created.run_id), [])

    def test_policy_is_validated_before_artifacts_and_cannot_change_at_run_time(self):
        for policy in (None, True, 1, {}, "", "skip", "QUARANTINE"):
            with self.subTest(policy=policy), self.assertRaises(PipelineError):
                self.pipeline.create(self.source, invalid_policy=policy)
        self.assertFalse(self.pipeline.data_dir.exists())
        created = self.pipeline.create(self.source)
        with self.assertRaises(TypeError):
            self.pipeline.run(created.run_id, invalid_policy="quarantine")
        self.assertEqual(self.pipeline.inspect(created.run_id).invalid_policy, "reject")

    def test_quarantine_inspection_is_bounded_read_only_and_uses_all_not_null_columns(self):
        created = self.pipeline.create(self.source, invalid_policy="quarantine")
        self.pipeline.run(created.run_id)
        before = self.snapshot(created.run_id)
        with patch("pipesleuth.pipeline._now", side_effect=AssertionError("read must not use a clock")):
            self.assertEqual(len(self.pipeline.quarantine(created.run_id, limit=2)), 2)
            self.assertEqual(len(self.pipeline.quarantine(created.run_id, limit=MAX_QUARANTINE_LIMIT)), 4)
        self.assertEqual(self.snapshot(created.run_id), before)
        columns = self.sql_rows(created.run_id, "PRAGMA table_info(quarantined_records)")
        self.assertEqual([row[1] for row in columns], ["row_number", "reason_code", "reason", "raw_record_json"])
        self.assertTrue(all(row[3] == 1 for row in columns))
        for limit in (0, -1, MAX_QUARANTINE_LIMIT + 1, True, 1.5, "1"):
            with self.subTest(limit=limit), self.assertRaises(PipelineError):
                self.pipeline.quarantine(created.run_id, limit=limit)
        missing = SandboxPipeline(self.pipeline.data_dir / "absent")
        with self.assertRaises(PipelineError):
            missing.quarantine("0" * 32)
        self.assertFalse(missing.data_dir.exists())

    def test_replay_verifies_every_retained_field_and_count_without_rewriting_evidence(self):
        mutations = [
            ("UPDATE quarantined_records SET row_number = 99 WHERE row_number = 13", ()),
            ("UPDATE quarantined_records SET reason_code = ? WHERE row_number = 13", ("changed",)),
            ("UPDATE quarantined_records SET reason = ? WHERE row_number = 13", ("changed",)),
            ("UPDATE quarantined_records SET raw_record_json = ? WHERE row_number = 13", ('{"changed":true}',)),
            ("DELETE FROM quarantined_records WHERE row_number = 13", ()),
            ("UPDATE pipeline_run SET rows_quarantined = 3", ()),
        ]
        for query, parameters in mutations:
            with self.subTest(query=query):
                created = self.pipeline.create(self.source, invalid_policy="quarantine")
                self.pipeline.run(created.run_id)
                with closing(sqlite3.connect(self.pipeline.database_path(created.run_id))) as connection, connection:
                    connection.execute(query, parameters)
                before = self.snapshot(created.run_id)
                with self.assertRaises(PipelineStateError):
                    self.pipeline.run(created.run_id)
                self.assertEqual(self.snapshot(created.run_id), before)
                self.assertEqual(self.pipeline.inspect(created.run_id).status, "succeeded")

    def test_successful_quarantine_replay_uses_no_clock_or_file_writes_and_checks_source_bytes(self):
        created = self.pipeline.create(self.source, invalid_policy="quarantine")
        summary = self.pipeline.run(created.run_id)
        before = self.snapshot(created.run_id)
        with patch("pipesleuth.pipeline._now", side_effect=AssertionError("replay must not use a clock")):
            self.assertEqual(self.pipeline.run(created.run_id), summary)
        self.assertEqual(self.snapshot(created.run_id), before)
        source = self.pipeline.database_path(created.run_id).parent / "source.json"
        source.write_bytes(source.read_bytes() + b"\n")
        changed = self.snapshot(created.run_id)
        with self.assertRaises(PipelineStateError):
            self.pipeline.run(created.run_id)
        self.assertEqual(self.snapshot(created.run_id), changed)

    def test_storage_failure_after_warehouse_and_first_quarantine_insert_rolls_back_both(self):
        created = self.pipeline.create(self.source, invalid_policy="quarantine")
        with closing(sqlite3.connect(self.pipeline.database_path(created.run_id))) as connection, connection:
            connection.execute("CREATE TRIGGER reject_second BEFORE INSERT ON quarantined_records "
                               "WHEN NEW.row_number = 14 BEGIN SELECT RAISE(ABORT, 'test quarantine failure'); END")
        failed = self.pipeline.run(created.run_id)
        self.assertEqual((failed.status, failed.rows_loaded, failed.rows_quarantined), ("failed", 0, 0))
        self.assertEqual(self.sql_rows(created.run_id, "SELECT * FROM warehouse_orders"), [])
        self.assertEqual(self.sql_rows(created.run_id, "SELECT * FROM quarantined_records"), [])
        self.assertIn("test quarantine failure", failed.error)
        self.assertEqual([event["event_type"] for event in self.pipeline.events(created.run_id)],
                         ["source_received", "load_started", "load_failed"])
        self.assertEqual(self.pipeline.events(created.run_id)[-1]["details"]["code"], "load_error")

    def make_legacy(self, day, status):
        run_id = uuid4().hex
        directory = self.pipeline.data_dir / run_id
        directory.mkdir(parents=True)
        encoded = json.dumps(self.clean, separators=(",", ":")).encode()
        (directory / "source.json").write_bytes(encoded)
        stamp = "2026-10-21T10:00:00Z"
        with closing(sqlite3.connect(directory / "warehouse.sqlite3")) as connection, connection:
            extras = ", duplicate_policy TEXT NOT NULL, duplicates_skipped INTEGER NOT NULL" if day == 3 else ""
            connection.execute("CREATE TABLE pipeline_run (run_id TEXT PRIMARY KEY, status TEXT NOT NULL, "
                               "source_sha256 TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT, "
                               "finished_at TEXT, error TEXT" + extras + ")")
            connection.execute("CREATE TABLE warehouse_orders (order_id TEXT PRIMARY KEY NOT NULL, "
                               "customer_id TEXT NOT NULL, amount_minor INTEGER NOT NULL CHECK(amount_minor >= 0), "
                               "currency TEXT NOT NULL, created_at TEXT NOT NULL)")
            values = (run_id, status, hashlib.sha256(encoded).hexdigest(), stamp,
                      stamp if status == "succeeded" else None, stamp if status == "succeeded" else None, None)
            if day == 3:
                connection.execute("INSERT INTO pipeline_run VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", values + ("reject", 0))
                connection.execute("CREATE TABLE pipeline_events (event_id INTEGER PRIMARY KEY, event_type TEXT NOT NULL, "
                                   "created_at TEXT NOT NULL, details_json TEXT NOT NULL)")
            else:
                connection.execute("INSERT INTO pipeline_run VALUES (?, ?, ?, ?, ?, ?, ?)", values)
            if status == "succeeded":
                connection.executemany("INSERT INTO warehouse_orders VALUES (?, ?, ?, ?, ?)",
                                       [tuple(row[column] for column in ORDER_COLUMNS) for row in self.clean["rows"]])
        return run_id

    def test_legacy_day_two_and_three_read_replay_and_clean_new_loading_need_no_migrations(self):
        for day in (2, 3):
            for status in ("new", "succeeded"):
                with self.subTest(day=day, status=status):
                    run_id = self.make_legacy(day, status)
                    before = self.snapshot(run_id)
                    summary = self.pipeline.inspect(run_id)
                    self.assertEqual((summary.invalid_policy, summary.rows_quarantined), ("reject", 0))
                    self.assertEqual(self.pipeline.quarantine(run_id), [])
                    self.assertEqual(self.snapshot(run_id), before)
                    schema = self.sql_rows(run_id, "SELECT name, sql FROM sqlite_master WHERE type = 'table' ORDER BY name")
                    completed = self.pipeline.run(run_id)
                    self.assertEqual((completed.status, completed.rows_loaded), ("succeeded", 3))
                    self.assertEqual(self.sql_rows(run_id, "SELECT name, sql FROM sqlite_master WHERE type = 'table' ORDER BY name"), schema)
                    after = self.snapshot(run_id)
                    self.assertEqual(self.pipeline.run(run_id), completed)
                    self.assertEqual(self.snapshot(run_id), after)
                    if status == "succeeded":
                        self.assertEqual(after, before)


if __name__ == "__main__":
    unittest.main()
