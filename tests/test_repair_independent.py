"""Public repair-service checks against literal inputs and separate SQLite reads."""

from contextlib import closing
from copy import deepcopy
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from pipesleuth.checkpoints import CheckpointStore
from pipesleuth.pipeline import SandboxPipeline
from pipesleuth.repairs import RepairError, RepairService


COLUMNS = ["order_id", "customer_id", "amount_minor", "currency", "created_at"]
ROWS = [
    {"order_id": "L-1", "customer_id": "C-A", "amount_minor": 125,
     "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"},
    {"order_id": "L-2", "customer_id": "C-B", "amount_minor": 375,
     "currency": "EUR", "created_at": "2026-01-02T09:00:00Z"},
    {"order_id": "L-3", "customer_id": "C-C", "amount_minor": 0,
     "currency": "USD", "created_at": "2026-01-03T09:00:00Z"},
]
EXPECTED = [tuple(row[name] for name in COLUMNS) for row in ROWS]


def source(action):
    payload = {"schema": COLUMNS[:], "rows": deepcopy(ROWS)}
    if action == "rename_amount_column":
        payload["schema"][2] = "gross_total_minor"
        for row in payload["rows"]:
            row["gross_total_minor"] = row.pop("amount_minor")
    elif action == "skip_identical_duplicates":
        payload["rows"] += [deepcopy(ROWS[0]), deepcopy(ROWS[2])]
    elif action == "quarantine_invalid_rows":
        payload["rows"].append(dict(ROWS[0], order_id="L-X", amount_minor=-7))
    return payload


def snapshot(root):
    return {str(path.relative_to(root)): (path.read_bytes(), path.stat().st_mtime_ns)
            for path in root.rglob("*") if path.is_file()} if root.exists() else {}


def rows(database):
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
        orders = connection.execute(
            "SELECT order_id, customer_id, amount_minor, currency, created_at "
            "FROM warehouse_orders ORDER BY order_id").fetchall()
        rejected = connection.execute(
            "SELECT row_number, reason_code, reason, raw_record_json "
            "FROM quarantined_records ORDER BY row_number").fetchall()
    return orders, rejected


class IndependentRepairTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.data, self.output, self.state = (self.root / name for name in ("incidents", "derived", "state"))
        self.pipeline = SandboxPipeline(self.data)
        self.service = RepairService(self.data, self.output, self.state)

    def planned(self, action="rename_amount_column"):
        run = self.pipeline.create(source(action))
        self.assertEqual(self.pipeline.run(run.run_id).status, "failed")
        return run.run_id, self.service.plan(run.run_id, action)

    def approve(self, record):
        state = record["state"]
        return self.service.approve(record["investigation_id"], state["plan_sha256"],
                                    state["input_fingerprint_sha256"], origin="scripted_test")

    def test_three_literal_repairs_preserve_original_and_unrelated_files(self):
        unrelated = self.pipeline.create({"schema": COLUMNS[:], "rows": deepcopy(ROWS)})
        self.pipeline.run(unrelated.run_id)
        for action in ("rename_amount_column", "skip_identical_duplicates", "quarantine_invalid_rows"):
            with self.subTest(action=action):
                original, planned = self.planned(action)
                before = snapshot(self.data)
                plan = planned["state"]["plan"]
                self.assertNotEqual(plan["target_run_id"], original)
                self.assertEqual(plan["source_run_id"], original)
                self.assertEqual((plan["data_dir"], plan["output_dir"], plan["state_dir"]),
                                 (str(self.data), str(self.output), str(self.state)))
                approved = self.approve(planned)
                self.assertEqual(approved["state"]["approval"]["origin"], "scripted_test")
                completed = self.service.execute(planned["investigation_id"])
                self.assertEqual(completed["state"]["status"], "succeeded")
                self.assertIs(completed["state"]["execution"]["post_input_verified"], True)
                self.assertEqual(snapshot(self.data), before)
                orders, rejected = rows(self.output / plan["target_run_id"] / "warehouse.sqlite3")
                self.assertEqual(orders, EXPECTED)
                if action == "quarantine_invalid_rows":
                    import json
                    self.assertEqual([row[:3] for row in rejected],
                                     [(4, "invalid_row", "amount_minor must be a non-negative integer")])
                    self.assertEqual(json.loads(rejected[0][3]), dict(ROWS[0], order_id="L-X", amount_minor=-7))
                else:
                    self.assertEqual(rejected, [])

    def test_unapproved_mismatched_and_duplicate_execution_do_not_mutate_outputs(self):
        _, planned = self.planned()
        identifier = planned["investigation_id"]
        with self.assertRaises(RepairError):
            self.service.execute(identifier)
        self.assertEqual(self.service.show(identifier), planned)
        for plan_hash, input_hash in (("0" * 64, planned["state"]["input_fingerprint_sha256"]),
                                     (planned["state"]["plan_sha256"], "0" * 64)):
            with self.subTest(plan_hash=plan_hash):
                with self.assertRaises(RepairError):
                    self.service.approve(identifier, plan_hash, input_hash, origin="scripted_test")
                self.assertEqual(self.service.show(identifier), planned)
        completed = self.service.execute(self.approve(planned)["investigation_id"])
        before = snapshot(self.output)
        with self.assertRaises(RepairError):
            self.service.execute(identifier)
        self.assertEqual(self.service.show(identifier), completed)
        self.assertEqual(snapshot(self.output), before)

    def test_source_whitespace_or_same_count_database_change_invalidates_approval(self):
        for mutation in ("source_whitespace", "same_count_database_metadata"):
            with self.subTest(mutation=mutation):
                original, planned = self.planned()
                approved = self.approve(planned)
                if mutation == "source_whitespace":
                    path = self.data / original / "source.json"
                    path.write_bytes(path.read_bytes() + b"\n")
                else:
                    with closing(sqlite3.connect(self.data / original / "warehouse.sqlite3")) as connection, connection:
                        self.assertEqual(connection.execute("SELECT COUNT(*) FROM warehouse_orders").fetchone(), (0,))
                        connection.execute("UPDATE pipeline_run SET error = 'changed metadata, same warehouse count'")
                before = snapshot(self.output)
                with self.assertRaises(RepairError):
                    self.service.execute(planned["investigation_id"])
                self.assertEqual(self.service.show(planned["investigation_id"]), approved)
                self.assertEqual(snapshot(self.output), before)

    def test_missing_show_and_overlapping_roots_create_nothing(self):
        before = snapshot(self.root)
        with self.assertRaises((RepairError, ValueError)):
            self.service.show("a" * 32)
        self.assertEqual(snapshot(self.root), before)
        for output, state in ((self.data / "nested", self.state), (self.output, self.data),
                              (self.output, self.output / "nested")):
            with self.subTest(output=output, state=state):
                with self.assertRaises(RepairError):
                    RepairService(self.data, output, state)
                self.assertEqual(snapshot(self.root), before)

    def test_copying_store_to_another_state_root_does_not_move_approval(self):
        _, planned = self.planned()
        approved = self.approve(planned)
        copied_state = self.root / "copied-state"
        copied_state.mkdir()
        (copied_state / "checkpoints.sqlite3").write_bytes((self.state / "checkpoints.sqlite3").read_bytes())
        moved = RepairService(self.data, self.output, copied_state)
        before = snapshot(self.output)
        for method in (moved.show, moved.execute):
            with self.assertRaises(RepairError):
                method(planned["investigation_id"])
        self.assertEqual(self.service.show(planned["investigation_id"]), approved)
        self.assertEqual(snapshot(self.output), before)

    def test_save_failure_after_commit_stays_pending_without_execution_replay(self):
        _, planned = self.planned()
        approved = self.approve(planned)
        identifier = planned["investigation_id"]
        with patch.object(CheckpointStore, "complete", side_effect=RuntimeError("injected save failure")):
            with self.assertRaises(RuntimeError):
                self.service.execute(identifier)
        pending = self.service.show(identifier)
        self.assertIsNotNone(pending["pending"])
        self.assertEqual(pending["state"], approved["state"])
        target = self.output / planned["state"]["plan"]["target_run_id"] / "warehouse.sqlite3"
        self.assertEqual(rows(target), (EXPECTED, []))
        before = snapshot(self.output)
        fresh = RepairService(self.data, self.output, self.state)
        with self.assertRaises(RepairError):
            fresh.execute(identifier)
        self.assertEqual(fresh.show(identifier), pending)
        self.assertEqual(snapshot(self.output), before)


if __name__ == "__main__":
    unittest.main()
