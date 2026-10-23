"""Independent quarantine checks against literal inputs and actual SQLite state."""

from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from pipesleuth.pipeline import PipelineStateError, SandboxPipeline


class QuarantineIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.pipeline = SandboxPipeline(Path(self.temporary.name).resolve() / "runs")
        # Expectations below are literals, independent of the fixture generator.
        self.source = {
            "schema": ["order_id", "customer_id", "amount_minor", "currency", "created_at"],
            "rows": [
                {"order_id": "O-1", "customer_id": "C-1", "amount_minor": 100,
                 "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"},
                {"order_id": "O-2", "customer_id": "C-2", "amount_minor": 300,
                 "currency": "EUR", "created_at": "2026-01-02T09:00:00Z"},
                {"order_id": "O-3", "customer_id": "C-3", "amount_minor": 0,
                 "currency": "USD", "created_at": "2026-01-03T09:00:00Z"},
                {"order_id": "O-4", "customer_id": "C-4", "amount_minor": True,
                 "currency": "EUR", "created_at": "2026-01-04T09:00:00Z"},
                {"order_id": "O-5", "customer_id": "C-5", "amount_minor": 9223372036854775808,
                 "currency": "EUR", "created_at": "2026-01-05T09:00:00Z"},
            ],
        }

    def database(self, run_id):
        return self.pipeline.data_dir / run_id / "warehouse.sqlite3"

    def read_tables(self, run_id):
        with closing(sqlite3.connect(self.database(run_id).as_uri() + "?mode=ro", uri=True)) as connection:
            warehouse = connection.execute(
                "SELECT order_id, customer_id, amount_minor, currency, created_at "
                "FROM warehouse_orders ORDER BY order_id"
            ).fetchall()
            quarantine = connection.execute(
                "SELECT row_number, reason_code, reason, raw_record_json "
                "FROM quarantined_records ORDER BY row_number"
            ).fetchall()
        return warehouse, quarantine

    def test_literal_valid_orders_and_raw_rejections_are_preserved_exactly(self):
        created = self.pipeline.create(self.source, invalid_policy="quarantine")
        completed = self.pipeline.run(created.run_id)
        self.assertEqual((completed.status, completed.rows_loaded, completed.rows_quarantined),
                         ("succeeded", 3, 2))
        self.assertEqual(completed.totals_minor, {"EUR": 400, "USD": 0})
        warehouse, quarantine = self.read_tables(created.run_id)
        self.assertEqual(warehouse, [
            ("O-1", "C-1", 100, "EUR", "2026-01-01T09:00:00Z"),
            ("O-2", "C-2", 300, "EUR", "2026-01-02T09:00:00Z"),
            ("O-3", "C-3", 0, "USD", "2026-01-03T09:00:00Z"),
        ])
        self.assertEqual([row[:3] for row in quarantine], [
            (4, "invalid_row", "amount_minor must be a non-negative integer"),
            (5, "integer_overflow", "amount_minor exceeds SQLite signed 64-bit integer range"),
        ])
        self.assertEqual(json.loads(quarantine[0][3]), self.source["rows"][3])
        self.assertIs(type(json.loads(quarantine[0][3])["amount_minor"]), bool)
        self.assertEqual(json.loads(quarantine[1][3]), self.source["rows"][4])
        self.assertIs(type(json.loads(quarantine[1][3])["amount_minor"]), int)

    def test_same_count_quarantine_corruption_blocks_replay_without_mutation(self):
        for change in ("reason", "reason_code", "row_number", "raw_json_type"):
            with self.subTest(change=change):
                created = self.pipeline.create(self.source, invalid_policy="quarantine")
                self.pipeline.run(created.run_id)
                database = self.database(created.run_id)
                with closing(sqlite3.connect(database)) as connection, connection:
                    if change == "reason":
                        connection.execute("UPDATE quarantined_records SET reason = ? WHERE row_number = 4",
                                           ("invented reason",))
                    elif change == "reason_code":
                        connection.execute("UPDATE quarantined_records SET reason_code = ? WHERE row_number = 4",
                                           ("integer_overflow",))
                    elif change == "row_number":
                        connection.execute("UPDATE quarantined_records SET row_number = 40 WHERE row_number = 4")
                    else:
                        changed = dict(self.source["rows"][3], amount_minor=1)
                        connection.execute(
                            "UPDATE quarantined_records SET raw_record_json = ? WHERE row_number = 4",
                            (json.dumps(changed, separators=(",", ":")),),
                        )
                before_bytes = database.read_bytes()
                before_tables = self.read_tables(created.run_id)
                before_events = self.pipeline.events(created.run_id)
                before_summary = self.pipeline.inspect(created.run_id)
                self.assertEqual(len(before_tables[1]), 2)
                with self.assertRaises(PipelineStateError):
                    self.pipeline.run(created.run_id)
                self.assertEqual(database.read_bytes(), before_bytes)
                self.assertEqual(self.read_tables(created.run_id), before_tables)
                self.assertEqual(self.pipeline.events(created.run_id), before_events)
                self.assertEqual(self.pipeline.inspect(created.run_id), before_summary)

    def test_sql_error_after_partial_quarantine_insertion_rolls_back_both_tables(self):
        created = self.pipeline.create(self.source, invalid_policy="quarantine")
        with closing(sqlite3.connect(self.database(created.run_id))) as connection, connection:
            connection.execute(
                "CREATE TRIGGER fail_second_rejection BEFORE INSERT ON quarantined_records "
                "WHEN NEW.row_number = 5 BEGIN "
                "SELECT CASE WHEN (SELECT COUNT(*) FROM quarantined_records) = 1 "
                "AND (SELECT COUNT(*) FROM warehouse_orders) = 3 "
                "THEN RAISE(ABORT, 'storage failure after partial insertion') "
                "ELSE RAISE(ABORT, 'test insertion precondition failed') END; END"
            )
        failed = self.pipeline.run(created.run_id)
        self.assertEqual(failed.status, "failed")
        self.assertIn("storage failure after partial insertion", failed.error)
        self.assertEqual((failed.rows_loaded, failed.rows_quarantined, failed.duplicates_skipped), (0, 0, 0))
        self.assertEqual(self.read_tables(created.run_id), ([], []))
        failure = self.pipeline.events(created.run_id)[-1]
        self.assertEqual(failure["event_type"], "load_failed")
        self.assertEqual(failure["details"]["code"], "load_error")

    def test_valid_conflicting_retry_aborts_even_after_invalid_and_identical_rows(self):
        source = deepcopy(self.source)
        source["rows"].append(dict(source["rows"][0]))
        source["rows"].append(dict(source["rows"][0], customer_id="C-OTHER"))
        created = self.pipeline.create(source, invalid_policy="quarantine", duplicate_policy="skip_identical")
        failed = self.pipeline.run(created.run_id)
        self.assertEqual(failed.status, "failed")
        self.assertEqual((failed.rows_loaded, failed.rows_quarantined, failed.duplicates_skipped), (0, 0, 0))
        self.assertEqual(self.read_tables(created.run_id), ([], []))
        details = self.pipeline.events(created.run_id)[-1]["details"]
        self.assertEqual((details["code"], details["row_number"], details["order_id"]),
                         ("conflicting_duplicate", 7, "O-1"))


if __name__ == "__main__":
    unittest.main()
