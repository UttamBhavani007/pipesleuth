"""Deterministic repair preconditions and projections; no repair is executed."""

from copy import deepcopy
import unittest
from unittest.mock import patch

from pipesleuth.domain import ORDER_COLUMNS
from pipesleuth.pipeline import MAX_SOURCE_BYTES, MAX_SOURCE_ROWS, SQLITE_MAX_INTEGER
from pipesleuth.repair_plan import ACTIONS, RepairPlanError, action_descriptions, prepare_repair


class RepairPlanTests(unittest.TestCase):
    def setUp(self):
        self.source = {"schema": list(ORDER_COLUMNS), "rows": [
            {"order_id": "O-1", "customer_id": "C-1", "amount_minor": 120,
             "currency": "EUR", "created_at": "2026-01-01T09:00:00Z"},
            {"order_id": "O-2", "customer_id": "C-2", "amount_minor": 0,
             "currency": "USD", "created_at": "2026-01-02T09:00:00+00:00"},
        ]}

    def renamed(self):
        source = deepcopy(self.source)
        source["schema"][2] = "gross_total_minor"
        for row in source["rows"]:
            row["gross_total_minor"] = row.pop("amount_minor")
        return source

    def test_rename_changes_only_column_mapping_with_explicit_unit_assumption(self):
        source = self.renamed()
        before = deepcopy(source)
        result = prepare_repair(source, "rename_amount_column")
        self.assertEqual(set(result), {"source", "duplicate_policy", "invalid_policy", "effects", "warnings"})
        self.assertEqual(result["source"], self.source)
        self.assertEqual(result["effects"], {"rows_loaded": 2, "rows_quarantined": 0, "duplicates_skipped": 0})
        self.assertEqual((result["duplicate_policy"], result["invalid_policy"]), ("reject", "reject"))
        self.assertEqual(len(result["warnings"]), 1)
        self.assertIn("same minor currency units", result["warnings"][0])
        self.assertEqual(source, before)
        result["source"]["rows"][0]["amount_minor"] = 999
        self.assertEqual(source, before)

    def test_skip_preserves_raw_rows_and_counts_identical_canonical_orders(self):
        source = deepcopy(self.source)
        source["rows"].extend([dict(source["rows"][0]),
                               dict(source["rows"][0], created_at="2026-01-01T09:00:00+00:00")])
        before = deepcopy(source)
        result = prepare_repair(source, "skip_identical_duplicates")
        self.assertEqual(result["source"], before)
        self.assertEqual(result["effects"], {"rows_loaded": 2, "rows_quarantined": 0, "duplicates_skipped": 2})
        self.assertEqual((result["duplicate_policy"], result["invalid_policy"]), ("skip_identical", "reject"))
        self.assertEqual(result["warnings"], [])
        result["source"]["rows"].pop()
        self.assertEqual(source, before)

    def test_quarantine_retains_invalid_types_and_every_raw_record(self):
        source = deepcopy(self.source)
        amounts = [True, 1.5, "120", None, -1, SQLITE_MAX_INTEGER + 1]
        source["rows"].extend(dict(source["rows"][0], order_id=f"R-{index}", amount_minor=amount)
                              for index, amount in enumerate(amounts))
        before = deepcopy(source)
        result = prepare_repair(source, "quarantine_invalid_rows")
        self.assertEqual(result["source"], before)
        self.assertEqual([type(row["amount_minor"]) for row in result["source"]["rows"][2:]],
                         [bool, float, str, type(None), int, int])
        self.assertEqual(result["effects"], {"rows_loaded": 2, "rows_quarantined": 6, "duplicates_skipped": 0})
        self.assertEqual((result["duplicate_policy"], result["invalid_policy"]), ("reject", "quarantine"))
        self.assertEqual(source, before)

    def test_healthy_noop_and_all_invalid_actions_are_rejected(self):
        empty = {"schema": list(ORDER_COLUMNS), "rows": []}
        for source in (self.source, empty):
            for action in ACTIONS:
                with self.subTest(action=action, empty=not source["rows"]), self.assertRaises(RepairPlanError):
                    prepare_repair(source, action)
        invalid = deepcopy(self.source)
        for row in invalid["rows"]:
            row["amount_minor"] = True
        with self.assertRaises(RepairPlanError):
            prepare_repair(invalid, "quarantine_invalid_rows")

    def test_valid_duplicate_conflicts_and_compound_faults_are_rejected(self):
        conflict = deepcopy(self.source)
        conflict["rows"].append(dict(conflict["rows"][0], customer_id="C-OTHER"))
        with self.assertRaises(RepairPlanError):
            prepare_repair(conflict, "skip_identical_duplicates")
        mixed = deepcopy(self.source)
        mixed["rows"].extend([dict(mixed["rows"][0]), dict(mixed["rows"][1], amount_minor=True)])
        for action in ("skip_identical_duplicates", "quarantine_invalid_rows"):
            with self.subTest(action=action), self.assertRaises(RepairPlanError):
                prepare_repair(mixed, action)
        renamed = self.renamed()
        renamed["rows"].append(dict(renamed["rows"][0]))
        with self.assertRaises(RepairPlanError):
            prepare_repair(renamed, "rename_amount_column")
        for action in ("skip_identical_duplicates", "quarantine_invalid_rows"):
            with self.assertRaises(RepairPlanError):
                prepare_repair(self.renamed(), action)

    def test_rename_requires_exact_schema_row_keys_and_valid_unique_orders(self):
        for field, value in (("amount_minor", 120), ("unknown", "x"), ("currency", "ZZZ"),
                             ("gross_total_minor", SQLITE_MAX_INTEGER + 1)):
            source = self.renamed()
            source["rows"][0][field] = value
            with self.subTest(field=field), self.assertRaises(RepairPlanError):
                prepare_repair(source, "rename_amount_column")
        missing = self.renamed()
        missing["rows"][0].pop("gross_total_minor")
        with self.assertRaises(RepairPlanError):
            prepare_repair(missing, "rename_amount_column")
        reordered = self.renamed()
        reordered["schema"].reverse()
        with self.assertRaises(RepairPlanError):
            prepare_repair(reordered, "rename_amount_column")

    def test_sqlite_maximum_is_valid_but_overflow_is_only_quarantinable(self):
        source = deepcopy(self.source)
        source["rows"][0]["amount_minor"] = SQLITE_MAX_INTEGER
        source["rows"] = [source["rows"][0], dict(source["rows"][0])]
        self.assertEqual(prepare_repair(source, "skip_identical_duplicates")["effects"]["rows_loaded"], 1)
        source["rows"][1]["amount_minor"] = SQLITE_MAX_INTEGER + 1
        with self.assertRaises(RepairPlanError):
            prepare_repair(source, "skip_identical_duplicates")
        result = prepare_repair(source, "quarantine_invalid_rows")
        self.assertEqual(result["effects"], {"rows_loaded": 1, "rows_quarantined": 1, "duplicates_skipped": 0})
        self.assertEqual(result["source"]["rows"][0]["amount_minor"], SQLITE_MAX_INTEGER)

    def test_unknown_actions_and_unbounded_or_authored_source_are_rejected(self):
        for action in (None, True, {}, [], "", "execute_sql", "rename_amount_column; shell"):
            with self.subTest(action_type=type(action).__name__), self.assertRaises(RepairPlanError):
                prepare_repair(None, action)
        cases = [self.source | {"oracle": {}}, {"schema": list(ORDER_COLUMNS), "rows": [self.source["rows"][0]] * (MAX_SOURCE_ROWS + 1)}]
        huge = deepcopy(self.source)
        huge["rows"][0]["customer_id"] = "x" * MAX_SOURCE_BYTES
        cases.append(huge)
        nested = deepcopy(self.source)
        nested["rows"][0]["customer_id"] = {"private": "nested"}
        cases.append(nested)
        for index, source in enumerate(cases):
            with self.subTest(case=index), self.assertRaises(RepairPlanError):
                prepare_repair(source, "quarantine_invalid_rows")

    def test_planner_has_no_io_and_descriptions_are_fresh(self):
        descriptions = action_descriptions()
        self.assertEqual(tuple(descriptions), ACTIONS)
        descriptions["rename_amount_column"] = "changed"
        self.assertNotEqual(action_descriptions()["rename_amount_column"], "changed")
        with patch("builtins.open", side_effect=AssertionError("planner must not read files")), \
                patch("socket.create_connection", side_effect=AssertionError("planner must not connect")), \
                patch("sqlite3.connect", side_effect=AssertionError("planner must not execute SQL")):
            self.assertEqual(prepare_repair(self.renamed(), "rename_amount_column")["source"], self.source)


if __name__ == "__main__":
    unittest.main()
