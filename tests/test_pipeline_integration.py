"""Independent SQLite checks for currency and persisted-source boundaries."""

from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest

from pipesleuth.pipeline import MAX_SOURCE_BYTES, SandboxPipeline


class PipelineIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.pipeline = SandboxPipeline(Path(self.temporary.name).resolve() / "local runs #1")
        # Literal input avoids deriving expected answers from fixture-generation code.
        self.source = {
            "schema": ["order_id", "customer_id", "amount_minor", "currency", "created_at"],
            "rows": [
                {"order_id": "O-3", "customer_id": "C-2", "amount_minor": 9001,
                 "currency": "GBP", "created_at": "2026-01-03T09:00:00+00:00"},
                {"order_id": "O-1", "customer_id": "C-1", "amount_minor": 303,
                 "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"},
                {"order_id": "O-2", "customer_id": "C-1", "amount_minor": 0,
                 "currency": "USD", "created_at": "2026-01-02T09:00:00Z"},
            ],
        }

    def read_rows_independently(self, run_id):
        database = self.pipeline.data_dir / run_id / "warehouse.sqlite3"
        with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
            return connection.execute(
                "SELECT order_id, customer_id, amount_minor, currency, created_at "
                "FROM warehouse_orders ORDER BY order_id"
            ).fetchall()

    def test_exact_multicurrency_rows_and_zero_amount_survive_loading(self):
        created = self.pipeline.create(self.source)
        summary = self.pipeline.run(created.run_id)
        self.assertEqual(summary.status, "succeeded")
        self.assertEqual(summary.to_dict()["totals_minor"], {"EUR": 303, "GBP": 9001, "USD": 0})
        self.assertEqual(summary.rows_loaded, 3)
        self.assertEqual(self.read_rows_independently(created.run_id), [
            ("O-1", "C-1", 303, "EUR", "2026-01-01T09:00:00Z"),
            ("O-2", "C-1", 0, "USD", "2026-01-02T09:00:00Z"),
            ("O-3", "C-2", 9001, "GBP", "2026-01-03T09:00:00Z"),
        ])

    def test_currency_total_may_exceed_one_sqlite_integer_without_losing_rows(self):
        self.source["rows"][0].update(amount_minor=9223372036854775807, currency="EUR")
        self.source["rows"][1]["amount_minor"] = 1
        created = self.pipeline.create(self.source)
        summary = self.pipeline.run(created.run_id)
        self.assertEqual(summary.status, "succeeded")
        self.assertEqual(summary.totals_minor, {"EUR": 9223372036854775808, "USD": 0})
        rows = self.read_rows_independently(created.run_id)
        self.assertEqual([row[2] for row in rows], [1, 0, 9223372036854775807])
        self.assertEqual(self.pipeline.inspect(created.run_id), summary)

    def test_oversized_persisted_source_fails_without_loading_any_orders(self):
        created = self.pipeline.create(self.source)
        source_path = self.pipeline.data_dir / created.run_id / "source.json"
        source_path.write_bytes(b"x" * (MAX_SOURCE_BYTES + 1))
        summary = self.pipeline.run(created.run_id)
        self.assertEqual(summary.status, "failed")
        self.assertEqual(summary.rows_loaded, 0)
        self.assertEqual(summary.totals_minor, {})
        self.assertIn("source must be a regular file at most", summary.error)
        self.assertEqual(self.read_rows_independently(created.run_id), [])


if __name__ == "__main__":
    unittest.main()
