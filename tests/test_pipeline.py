"""SQLite ETL contracts, source separation, and failure boundaries."""

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import tempfile
import unittest

from pipesleuth.domain import ORDER_COLUMNS
from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import (
    MAX_SOURCE_BYTES,
    MAX_SOURCE_ROWS,
    SQLITE_MAX_INTEGER,
    PipelineError,
    PipelineStateError,
    SandboxPipeline,
)


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.pipeline = SandboxPipeline(self.root / "runs")
        self.payload = generate_fixture("healthy", row_count=3).to_source_payload()

    def read_orders(self, run_id):
        with sqlite3.connect(self.pipeline.database_path(run_id)) as connection:
            return connection.execute("SELECT * FROM warehouse_orders ORDER BY order_id").fetchall()

    def test_healthy_run_loads_exact_known_rows_and_summary(self) -> None:
        created = self.pipeline.create(self.payload)
        self.assertRegex(created.run_id, r"^[0-9a-f]{32}$")
        self.assertEqual(created.status, "new")
        self.assertEqual(created.rows_loaded, 0)
        self.assertEqual(created.totals_minor, {})
        completed = self.pipeline.run(created.run_id)
        self.assertEqual(completed.status, "succeeded")
        self.assertEqual(completed.rows_loaded, 3)
        self.assertEqual(completed.totals_minor, {"EUR": 8207})
        self.assertEqual(
            self.read_orders(created.run_id),
            [
                ("ORD-0001", "CUST-002", 909, "EUR", "2026-01-01T09:00:00Z"),
                ("ORD-0002", "CUST-005", 4512, "EUR", "2026-01-01T09:07:00Z"),
                ("ORD-0003", "CUST-004", 2786, "EUR", "2026-01-01T09:14:00Z"),
            ],
        )
        self.assertTrue(completed.created_at.endswith("Z"))
        self.assertTrue(completed.started_at.endswith("Z"))
        self.assertTrue(completed.finished_at.endswith("Z"))
        self.assertEqual(self.pipeline.inspect(created.run_id), completed)
        self.assertNotIn("error", completed.to_dict())
        self.assertEqual(json.loads(json.dumps(completed.to_dict())), completed.to_dict())
        with sqlite3.connect(self.pipeline.database_path(created.run_id)) as connection:
            columns = connection.execute("PRAGMA table_info(warehouse_orders)").fetchall()
        self.assertEqual(tuple(row[1] for row in columns), ORDER_COLUMNS)

    def test_source_and_runtime_metadata_do_not_store_authoring_truth(self) -> None:
        created = self.pipeline.create(self.payload)
        directory = self.pipeline.database_path(created.run_id).parent
        source = (directory / "source.json").read_bytes()
        self.assertEqual(json.loads(source), self.payload)
        self.assertEqual(hashlib.sha256(source).hexdigest(), created.source_sha256)
        self.assertEqual({path.name for path in directory.iterdir()}, {"source.json", "warehouse.sqlite3"})
        with sqlite3.connect(self.pipeline.database_path(created.run_id)) as connection:
            metadata = connection.execute("SELECT * FROM pipeline_run").fetchone()
        persisted = source.decode() + repr(metadata) + json.dumps(created.to_dict())
        for label in ("healthy", "scenario", "expected_orders", "oracle", "seed"):
            self.assertNotIn(label, persisted)
        with self.assertRaises(PipelineError):
            self.pipeline.create(generate_fixture("healthy").to_authoring_dict())

    def test_runs_have_independent_sources_databases_and_status(self) -> None:
        first = self.pipeline.create(self.payload)
        changed = generate_fixture("healthy", seed=5, row_count=1).to_source_payload()
        second = self.pipeline.create(changed)
        self.assertNotEqual(first.run_id, second.run_id)
        self.pipeline.run(first.run_id)
        self.assertEqual(self.pipeline.inspect(second.run_id).status, "new")
        self.assertEqual(self.read_orders(second.run_id), [])
        self.pipeline.run(second.run_id)
        self.assertEqual(self.pipeline.inspect(first.run_id).totals_minor, {"EUR": 8207})
        self.assertEqual(self.pipeline.inspect(second.run_id).rows_loaded, 1)

    def test_repeated_run_is_rejected_without_adding_rows(self) -> None:
        created = self.pipeline.create(self.payload)
        completed = self.pipeline.run(created.run_id)
        with self.assertRaises(PipelineStateError):
            self.pipeline.run(created.run_id)
        self.assertEqual(self.pipeline.inspect(created.run_id), completed)
        self.assertEqual(len(self.read_orders(created.run_id)), 3)

    def test_concurrent_starts_allow_exactly_one_load(self) -> None:
        created = self.pipeline.create(self.payload)

        def attempt():
            try:
                return self.pipeline.run(created.run_id).status
            except PipelineStateError:
                return "rejected"

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: attempt(), range(2)))
        self.assertCountEqual(results, ["succeeded", "rejected"])
        self.assertEqual(len(self.read_orders(created.run_id)), 3)

    def test_sql_like_identifiers_are_stored_as_parameter_data(self) -> None:
        attack = "x'); DROP TABLE warehouse_orders; --"
        self.payload["rows"][0]["customer_id"] = attack
        self.payload["rows"][0]["order_id"] = attack
        created = self.pipeline.create(self.payload)
        self.assertEqual(self.pipeline.run(created.run_id).status, "succeeded")
        self.assertIn((attack, attack, 909, "EUR", "2026-01-01T09:00:00Z"), self.read_orders(created.run_id))

    def test_late_invalid_order_fails_without_partial_load(self) -> None:
        self.payload["rows"][-1]["amount_minor"] = True
        created = self.pipeline.create(self.payload)
        failed = self.pipeline.run(created.run_id)
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.rows_loaded, 0)
        self.assertEqual(failed.totals_minor, {})
        self.assertIn("row 3", failed.error)
        self.assertIn("amount_minor", failed.to_dict()["error"])
        self.assertEqual(self.read_orders(created.run_id), [])
        self.assertEqual(self.pipeline.inspect(created.run_id), failed)
        with self.assertRaises(PipelineStateError):
            self.pipeline.run(created.run_id)

    def test_duplicate_failure_rolls_back_already_inserted_rows(self) -> None:
        self.payload["rows"].append(dict(self.payload["rows"][0]))
        created = self.pipeline.create(self.payload)
        failed = self.pipeline.run(created.run_id)
        self.assertEqual(failed.status, "failed")
        self.assertEqual(self.read_orders(created.run_id), [])

    def test_faulty_scalar_records_can_be_created_for_later_investigation(self) -> None:
        for kind in ("schema_drift", "invalid_records"):
            with self.subTest(kind=kind):
                source = generate_fixture(kind, row_count=3).to_source_payload()
                created = self.pipeline.create(source)
                self.assertEqual(created.status, "new")
                failed = self.pipeline.run(created.run_id)
                self.assertEqual(failed.status, "failed")
                self.assertEqual(failed.rows_loaded, 0)

    def test_checksum_change_is_reported_before_loading(self) -> None:
        created = self.pipeline.create(self.payload)
        source_path = self.pipeline.database_path(created.run_id).parent / "source.json"
        source_path.write_bytes(source_path.read_bytes() + b"\n")
        failed = self.pipeline.run(created.run_id)
        self.assertEqual(failed.status, "failed")
        self.assertIn("checksum mismatch", failed.error)
        self.assertEqual(self.read_orders(created.run_id), [])

    def test_integer_overflow_is_a_clear_failed_run(self) -> None:
        self.payload["rows"][-1]["amount_minor"] = SQLITE_MAX_INTEGER + 1
        created = self.pipeline.create(self.payload)
        failed = self.pipeline.run(created.run_id)
        self.assertEqual(failed.status, "failed")
        self.assertIn("SQLite signed 64-bit", failed.error)
        self.assertEqual(self.read_orders(created.run_id), [])

    def test_missing_inspection_and_bad_ids_never_create_files(self) -> None:
        missing = self.root / "missing" / "nested"
        pipeline = SandboxPipeline(missing)
        self.assertFalse(missing.exists())
        for method in (pipeline.inspect, pipeline.run, pipeline.database_path):
            for run_id in ("0" * 32, "../outside", "A" * 32, "a" * 31, None):
                with self.subTest(method=method.__name__, run_id=run_id), self.assertRaises(PipelineError):
                    method(run_id)
        self.assertFalse((self.root / "missing").exists())

    def test_missing_database_is_not_recreated_by_inspection(self) -> None:
        created = self.pipeline.create(self.payload)
        database = self.pipeline.database_path(created.run_id)
        database.unlink()
        with self.assertRaises(PipelineError):
            self.pipeline.inspect(created.run_id)
        self.assertFalse(database.exists())

    def test_symlink_run_root_source_database_and_sidecars_are_rejected(self) -> None:
        target = self.root / "target.txt"
        target.write_text("untouched")
        created = self.pipeline.create(self.payload)
        directory = self.pipeline.database_path(created.run_id).parent
        for filename in ("source.json", "warehouse.sqlite3", "warehouse.sqlite3-journal"):
            with self.subTest(filename=filename):
                candidate = directory / filename
                backup = candidate.read_bytes() if candidate.exists() else None
                if candidate.exists():
                    candidate.unlink()
                candidate.symlink_to(target)
                try:
                    for method in (self.pipeline.inspect, self.pipeline.run, self.pipeline.database_path):
                        with self.assertRaises(PipelineError):
                            method(created.run_id)
                    self.assertEqual(target.read_text(), "untouched")
                finally:
                    candidate.unlink()
                    if backup is not None:
                        candidate.write_bytes(backup)
        alias = self.root / "alias"
        alias.symlink_to(self.pipeline.data_dir, target_is_directory=True)
        with self.assertRaises(PipelineError):
            SandboxPipeline(alias)
        fake_id = "a" * 32
        (self.pipeline.data_dir / fake_id).symlink_to(directory, target_is_directory=True)
        with self.assertRaises(PipelineError):
            self.pipeline.inspect(fake_id)

    def test_input_envelope_limits_and_nonfinite_nested_values_are_rejected(self) -> None:
        invalid_payloads = [
            {}, self.payload | {"oracle": {}},
            {"schema": ["amount_minor", "amount_minor"], "rows": []},
            {"schema": ["a"], "rows": [{"a": {"hidden": 1}}]},
            {"schema": ["a"], "rows": [{"a": [1]}]},
            {"schema": ["a"], "rows": [{"a": float("nan")}]},
            {"schema": ["a"], "rows": [{"a": float("inf")}]},
            {"schema": ["a"], "rows": [{}] * (MAX_SOURCE_ROWS + 1)},
            {"schema": ["a"], "rows": [{"a": "x" * MAX_SOURCE_BYTES}]},
        ]
        for payload in invalid_payloads:
            with self.subTest(payload_type=str(type(payload))), self.assertRaises(PipelineError):
                self.pipeline.create(payload)
        self.assertFalse(self.pipeline.data_dir.exists())


if __name__ == "__main__":
    unittest.main()
