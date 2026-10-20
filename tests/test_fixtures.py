"""Known-answer fixture checks, including evidence/oracle separation."""

from collections import Counter
from dataclasses import FrozenInstanceError
import json
import random
import unittest

from pipesleuth.domain import ORDER_COLUMNS, IncidentKind, Order, OrderValidationError
from pipesleuth.fixtures import MAX_ROW_COUNT, MAX_SEED, generate_fixture


class FixtureTests(unittest.TestCase):
    def test_seed_42_has_fixed_known_records_and_totals(self) -> None:
        fixture = generate_fixture("healthy", row_count=3)
        self.assertEqual(
            fixture.to_source_payload()["rows"],
            [
                {
                    "order_id": "ORD-0001",
                    "customer_id": "CUST-002",
                    "amount_minor": 909,
                    "currency": "EUR",
                    "created_at": "2026-01-01T09:00:00Z",
                },
                {
                    "order_id": "ORD-0002",
                    "customer_id": "CUST-005",
                    "amount_minor": 4512,
                    "currency": "EUR",
                    "created_at": "2026-01-01T09:07:00Z",
                },
                {
                    "order_id": "ORD-0003",
                    "customer_id": "CUST-004",
                    "amount_minor": 2786,
                    "currency": "EUR",
                    "created_at": "2026-01-01T09:14:00Z",
                },
            ],
        )
        self.assertEqual(fixture.oracle.expected_totals_minor, {"EUR": 8207})
        self.assertEqual(generate_fixture("healthy").oracle.expected_totals_minor, {"EUR": 73525})

    def test_scenarios_share_exact_clean_truth_and_are_reproducible(self) -> None:
        reference = generate_fixture("healthy")
        for kind in IncidentKind:
            with self.subTest(kind=kind):
                fixture = generate_fixture(kind)
                self.assertEqual(fixture.to_authoring_dict(), generate_fixture(kind).to_authoring_dict())
                self.assertEqual(fixture.oracle.expected_orders, reference.oracle.expected_orders)
                self.assertEqual(fixture.oracle.expected_order_count, 12)

    def test_healthy_rows_satisfy_contract_and_oracle(self) -> None:
        fixture = generate_fixture("healthy", seed=42, row_count=3)
        observed = tuple(Order.from_record(row) for row in fixture.source_rows)
        self.assertEqual(observed, fixture.oracle.expected_orders)
        self.assertEqual(fixture.source_columns, ORDER_COLUMNS)
        self.assertEqual(fixture.oracle.duplicate_order_ids, ())
        self.assertEqual(fixture.oracle.invalid_row_numbers, ())
        self.assertEqual(fixture.oracle.column_renames, ())

    def test_schema_drift_really_renames_money_column(self) -> None:
        fixture = generate_fixture("schema_drift", row_count=3)
        self.assertNotIn("amount_minor", fixture.source_columns)
        self.assertIn("gross_total_minor", fixture.source_columns)
        self.assertEqual(fixture.oracle.column_renames, (("gross_total_minor", "amount_minor"),))
        normalized = []
        for row in fixture.source_rows:
            self.assertNotIn("amount_minor", row)
            with self.assertRaises(OrderValidationError):
                Order.from_record(row)
            canonical = dict(row)
            canonical["amount_minor"] = canonical.pop("gross_total_minor")
            normalized.append(Order.from_record(canonical))
        self.assertEqual(tuple(normalized), fixture.oracle.expected_orders)

    def test_retry_rows_are_identical_and_have_expected_unique_orders(self) -> None:
        fixture = generate_fixture("duplicate_ingestion")
        self.assertEqual(len(fixture.source_rows), 15)
        self.assertEqual(fixture.oracle.duplicate_order_ids, ("ORD-0001", "ORD-0007", "ORD-0012"))
        counts = Counter(row["order_id"] for row in fixture.source_rows)
        self.assertEqual({key for key, count in counts.items() if count == 2}, set(fixture.oracle.duplicate_order_ids))
        originals = {row["order_id"]: row for row in fixture.source_rows[:12]}
        for retry in fixture.source_rows[12:]:
            self.assertEqual(retry, originals[retry["order_id"]])
        self.assertEqual(len(counts), fixture.oracle.expected_order_count)
        self.assertGreater(
            sum(row["amount_minor"] for row in fixture.source_rows),
            fixture.oracle.expected_totals_minor["EUR"],
        )

    def test_small_duplicate_fixtures_do_not_duplicate_selection_positions(self) -> None:
        for size, expected in ((1, 2), (2, 4), (3, 6)):
            with self.subTest(size=size):
                fixture = generate_fixture("duplicate_ingestion", row_count=size)
                self.assertEqual(len(fixture.source_rows), expected)
                self.assertEqual(len(fixture.oracle.duplicate_order_ids), size)

    def test_invalid_rows_are_appended_and_original_valid_orders_are_preserved(self) -> None:
        fixture = generate_fixture("invalid_records", row_count=3)
        self.assertEqual(len(fixture.source_rows), 7)
        self.assertEqual(fixture.oracle.invalid_row_numbers, (4, 5, 6, 7))
        valid_orders = []
        rejected_row_numbers = []
        for row_number, row in enumerate(fixture.source_rows, start=1):
            try:
                valid_orders.append(Order.from_record(row))
            except OrderValidationError:
                rejected_row_numbers.append(row_number)
        self.assertEqual(tuple(valid_orders), fixture.oracle.expected_orders)
        self.assertEqual(tuple(rejected_row_numbers), fixture.oracle.invalid_row_numbers)
        self.assertNotIn("amount_minor", fixture.source_rows[3])
        self.assertEqual(fixture.source_rows[4]["amount_minor"], -250)
        self.assertEqual(fixture.source_rows[5]["currency"], "ZZZ")
        self.assertEqual(fixture.source_rows[6]["created_at"], "2026-01-01T09:00:00")

    def test_evidence_contains_observations_without_authoring_answers(self) -> None:
        forbidden = {"scenario", "kind", "seed", "metadata", "oracle", "expected_orders", "expected_order_count", "expected_totals_minor", "duplicate_order_ids", "invalid_row_numbers", "column_renames"}

        def all_keys(value):
            if isinstance(value, dict):
                for key, child in value.items():
                    yield key
                    yield from all_keys(child)
            elif isinstance(value, list):
                for child in value:
                    yield from all_keys(child)

        for kind in IncidentKind:
            with self.subTest(kind=kind):
                fixture = generate_fixture(kind)
                evidence = fixture.to_evidence_dict()
                self.assertEqual(set(evidence), {"schema", "rows"})
                self.assertEqual(evidence, fixture.to_source_payload())
                self.assertFalse(forbidden.intersection(all_keys(evidence)))
                self.assertEqual(json.loads(json.dumps(evidence)), evidence)
                authored = fixture.to_authoring_dict()
                self.assertEqual(authored["metadata"]["scenario"], kind.value)
                self.assertEqual(authored["oracle"]["expected_order_count"], 12)

    def test_fixture_rows_are_immutable_and_exports_are_independent(self) -> None:
        fixture = generate_fixture("healthy")
        with self.assertRaises(FrozenInstanceError):
            fixture.seed = 4
        with self.assertRaises(TypeError):
            fixture.source_rows[0]["amount_minor"] = 1
        exported = fixture.to_source_payload()
        exported["rows"][0]["amount_minor"] = 1
        exported["schema"].append("new_column")
        self.assertNotEqual(fixture.source_rows[0]["amount_minor"], 1)
        self.assertNotIn("new_column", fixture.source_columns)

    def test_seed_changes_source_and_does_not_touch_global_rng(self) -> None:
        self.assertNotEqual(generate_fixture("healthy", seed=1).to_source_payload(), generate_fixture("healthy", seed=2).to_source_payload())
        state = random.getstate()
        generate_fixture("healthy")
        self.assertEqual(random.getstate(), state)

    def test_generation_rejects_invalid_seed_count_and_kind(self) -> None:
        for seed in (True, False, -1, MAX_SEED + 1, 1.5, "42", None):
            with self.subTest(seed=seed), self.assertRaises(ValueError):
                generate_fixture("healthy", seed=seed)
        for size in (True, False, 0, -1, MAX_ROW_COUNT + 1, 1.5, "12", None):
            with self.subTest(size=size), self.assertRaises(ValueError):
                generate_fixture("healthy", row_count=size)
        for kind in ("unknown", "HEALTHY", None, 4):
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                generate_fixture(kind)

    def test_generation_accepts_seed_and_row_count_boundaries(self) -> None:
        for seed in (0, MAX_SEED):
            with self.subTest(seed=seed):
                self.assertEqual(generate_fixture("healthy", seed=seed, row_count=1).oracle.expected_order_count, 1)
        fixture = generate_fixture("healthy", row_count=MAX_ROW_COUNT)
        self.assertEqual(fixture.oracle.expected_order_count, MAX_ROW_COUNT)
        self.assertEqual(fixture.source_rows[-1]["order_id"], "ORD-10000")


if __name__ == "__main__":
    unittest.main()
