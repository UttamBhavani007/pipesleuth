"""Independent repair verification against actual SQLite contents and corruption."""

from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import SandboxPipeline
from pipesleuth.repair_verifier import RepairVerificationError, verify_repair


class RepairVerifierTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.pipeline = SandboxPipeline(Path(self.temporary.name).resolve() / "runs")

    def create(self, scenario="healthy", **policies):
        source = generate_fixture(scenario, row_count=3).to_source_payload()
        created = self.pipeline.create(source, **policies)
        completed = self.pipeline.run(created.run_id)
        self.assertEqual(completed.status, "succeeded")
        return source, completed

    def verify(self, source, completed, connection=None):
        kwargs = {"run_id": completed.run_id, "duplicate_policy": completed.duplicate_policy,
                  "invalid_policy": completed.invalid_policy, "source_sha256": completed.source_sha256}
        if connection is not None:
            return verify_repair(connection, source, **kwargs)
        path = self.pipeline.database_path(completed.run_id)
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
            return verify_repair(connection, source, **kwargs)

    def mutate(self, completed, sql, parameters=()):
        with closing(sqlite3.connect(self.pipeline.database_path(completed.run_id))) as connection, connection:
            connection.execute(sql, parameters)

    def test_literal_rows_counts_digest_and_read_only_independence(self):
        source, completed = self.create()
        path = self.pipeline.database_path(completed.run_id)
        before = path.read_bytes(), path.stat().st_mtime_ns
        with patch.object(SandboxPipeline, "_read_orders", side_effect=AssertionError("verifier must be independent")):
            first = self.verify(source, completed)
            second = self.verify(source, completed)
        self.assertTrue(first["verified"])
        self.assertEqual((first["rows_loaded"], first["rows_quarantined"], first["duplicates_skipped"]), (3, 0, 0))
        self.assertEqual(first["totals_minor"], {"EUR": 8207})
        self.assertEqual(first["content_sha256"], second["content_sha256"])
        self.assertEqual(len(first["content_sha256"]), 64)
        self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), before)
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
            self.assertEqual(connection.execute("SELECT order_id,amount_minor FROM warehouse_orders ORDER BY order_id").fetchall(),
                             [("ORD-0001", 909), ("ORD-0002", 4512), ("ORD-0003", 2786)])

    def test_identical_retries_and_quarantine_are_derived_without_executor_helpers(self):
        for scenario, policies, expected in (
            ("duplicate_ingestion", {"duplicate_policy": "skip_identical"}, (3, 0, 3)),
            ("invalid_records", {"invalid_policy": "quarantine"}, (3, 4, 0)),
        ):
            with self.subTest(scenario=scenario):
                source, completed = self.create(scenario, **policies)
                result = self.verify(source, completed)
                self.assertEqual((result["rows_loaded"], result["rows_quarantined"], result["duplicates_skipped"]), expected)

    def test_exact_warehouse_fields_are_checked_even_with_same_count_and_total(self):
        source, completed = self.create()
        self.mutate(completed, "UPDATE warehouse_orders SET amount_minor = CASE order_id WHEN 'ORD-0001' THEN 4512 WHEN 'ORD-0002' THEN 909 ELSE amount_minor END")
        with self.assertRaisesRegex(RepairVerificationError, "warehouse"):
            self.verify(source, completed)

    def test_quarantine_checks_positions_reasons_codes_and_exact_json_types(self):
        mutations = [
            ("UPDATE quarantined_records SET row_number=99 WHERE row_number=4", ()),
            ("UPDATE quarantined_records SET reason_code=? WHERE row_number=4", ("integer_overflow",)),
            ("UPDATE quarantined_records SET reason=? WHERE row_number=4", ("invented reason",)),
            ("UPDATE quarantined_records SET raw_record_json=? WHERE row_number=4", ('{"amount_minor":true}',)),
            ("DELETE FROM quarantined_records WHERE row_number=4", ()),
        ]
        for sql, parameters in mutations:
            with self.subTest(sql=sql):
                source, completed = self.create("invalid_records", invalid_policy="quarantine")
                self.mutate(completed, sql, parameters)
                with self.assertRaisesRegex(RepairVerificationError, "quarantine"):
                    self.verify(source, completed)

    def test_schema_metadata_and_event_facts_are_checked(self):
        for sql in (
            "UPDATE pipeline_run SET duplicates_skipped=1",
            "UPDATE pipeline_run SET source_sha256='" + "a" * 64 + "'",
            "CREATE INDEX unexpected_index ON warehouse_orders(customer_id)",
            "UPDATE pipeline_events SET details_json='{\"rows_loaded\":999,\"rows_loaded\":3,\"duplicates_skipped\":0,\"rows_quarantined\":0}' WHERE event_type='load_succeeded'",
        ):
            with self.subTest(sql=sql):
                source, completed = self.create()
                self.mutate(completed, sql)
                with self.assertRaises(RepairVerificationError):
                    self.verify(source, completed)

    def test_caller_transaction_is_never_committed_or_rolled_back(self):
        source, completed = self.create()
        with closing(sqlite3.connect(self.pipeline.database_path(completed.run_id))) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self.assertTrue(self.verify(source, completed, connection)["verified"])
            self.assertTrue(connection.in_transaction)
            connection.execute("UPDATE warehouse_orders SET customer_id='changed' WHERE order_id='ORD-0001'")
            with self.assertRaises(RepairVerificationError):
                self.verify(source, completed, connection)
            self.assertTrue(connection.in_transaction)
            self.assertEqual(connection.execute("SELECT customer_id FROM warehouse_orders WHERE order_id='ORD-0001'").fetchone()[0], "changed")
            connection.rollback()
        self.assertTrue(self.verify(source, completed)["verified"])

    def test_wrong_source_hash_and_noncanonical_source_fail_before_acceptance(self):
        source, completed = self.create()
        source["rows"][0]["amount_minor"] += 1
        with self.assertRaisesRegex(RepairVerificationError, "fingerprint"):
            self.verify(source, completed)
        source["schema"][2] = "gross_total_minor"
        with self.assertRaises(RepairVerificationError):
            self.verify(source, completed)


if __name__ == "__main__":
    unittest.main()
