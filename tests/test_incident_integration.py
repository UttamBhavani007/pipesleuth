"""Independent incident checks against actual SQLite rows and persisted bytes."""

from contextlib import closing
from copy import deepcopy
from pathlib import Path
import sqlite3
import tempfile
import unittest

from pipesleuth.pipeline import PipelineStateError, SandboxPipeline


class IncidentIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.pipeline = SandboxPipeline(Path(self.temporary.name).resolve() / "runs")
        self.source = {
            "schema": ["order_id", "customer_id", "amount_minor", "currency", "created_at"],
            "rows": [
                {"order_id": "O-1", "customer_id": "C-1", "amount_minor": 100,
                 "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"},
                {"order_id": "O-2", "customer_id": "C-2", "amount_minor": 300,
                 "currency": "EUR", "created_at": "2026-01-02T09:00:00Z"},
                {"order_id": "O-3", "customer_id": "C-3", "amount_minor": 500,
                 "currency": "EUR", "created_at": "2026-01-03T09:00:00Z"},
            ],
        }

    def database(self, run_id):
        return self.pipeline.data_dir / run_id / "warehouse.sqlite3"

    def rows(self, run_id):
        database = self.database(run_id)
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
            return connection.execute(
                "SELECT order_id, customer_id, amount_minor, currency, created_at "
                "FROM warehouse_orders ORDER BY order_id"
            ).fetchall()

    def assert_replay_rejects_without_writing(self, run_id):
        database = self.database(run_id)
        before = database.read_bytes()
        before_rows = self.rows(run_id)
        before_events = self.pipeline.events(run_id)
        before_summary = self.pipeline.inspect(run_id)
        with self.assertRaises(PipelineStateError):
            self.pipeline.run(run_id)
        self.assertEqual(database.read_bytes(), before)
        self.assertEqual(self.rows(run_id), before_rows)
        self.assertEqual(self.pipeline.events(run_id), before_events)
        self.assertEqual(self.pipeline.inspect(run_id), before_summary)

    def test_same_count_and_total_cannot_hide_swapped_order_amounts_on_replay(self):
        created = self.pipeline.create(self.source)
        completed = self.pipeline.run(created.run_id)
        with closing(sqlite3.connect(self.database(created.run_id))) as connection, connection:
            connection.execute(
                "UPDATE warehouse_orders SET amount_minor = CASE order_id "
                "WHEN 'O-1' THEN 300 WHEN 'O-2' THEN 100 ELSE amount_minor END"
            )
        # Both summary aggregates remain correct, despite two incorrect orders.
        corrupted = self.pipeline.inspect(created.run_id)
        self.assertEqual(corrupted.rows_loaded, completed.rows_loaded)
        self.assertEqual(corrupted.totals_minor, completed.totals_minor)
        self.assertEqual([row[2] for row in self.rows(created.run_id)], [300, 100, 500])
        self.assert_replay_rejects_without_writing(created.run_id)

    def test_late_retry_with_changed_customer_is_not_silently_deduplicated(self):
        source = deepcopy(self.source)
        # An identical retry precedes a later conflicting retry of another order.
        source["rows"].append(dict(source["rows"][2]))
        conflict = dict(source["rows"][0])
        conflict["customer_id"] = "C-OTHER"
        source["rows"].append(conflict)
        created = self.pipeline.create(source, duplicate_policy="skip_identical")
        failed = self.pipeline.run(created.run_id)
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.rows_loaded, 0)
        self.assertEqual(failed.duplicates_skipped, 0)
        self.assertEqual(self.rows(created.run_id), [])
        failure = self.pipeline.events(created.run_id)[-1]
        self.assertEqual(failure["event_type"], "load_failed")
        self.assertEqual(failure["details"]["code"], "conflicting_duplicate")
        self.assertEqual(failure["details"]["row_number"], 5)
        self.assertEqual(failure["details"]["order_id"], "O-1")

    def test_changed_source_bytes_after_success_block_replay_even_if_json_is_equivalent(self):
        created = self.pipeline.create(self.source)
        self.pipeline.run(created.run_id)
        source_path = self.database(created.run_id).parent / "source.json"
        source_path.write_bytes(source_path.read_bytes() + b"\n")
        changed_source = source_path.read_bytes()
        self.assert_replay_rejects_without_writing(created.run_id)
        self.assertEqual(source_path.read_bytes(), changed_source)


if __name__ == "__main__":
    unittest.main()
