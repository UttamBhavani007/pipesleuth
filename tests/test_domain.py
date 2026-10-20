"""Contract boundary checks for typed orders and source records."""

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import unittest

from pipesleuth.domain import IncidentKind, Order, OrderValidationError


class OrderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fields = {
            "order_id": "ORD-0001",
            "customer_id": "CUST-001",
            "amount_minor": 1250,
            "currency": "EUR",
            "created_at": datetime(2026, 1, 1, 9, tzinfo=timezone.utc),
        }

    def test_valid_order_round_trips_without_floating_money(self) -> None:
        order = Order(**self.fields)
        self.assertEqual(
            order.to_record(),
            {
                "order_id": "ORD-0001",
                "customer_id": "CUST-001",
                "amount_minor": 1250,
                "currency": "EUR",
                "created_at": "2026-01-01T09:00:00Z",
            },
        )
        self.assertEqual(Order.from_record(order.to_record()), order)
        self.assertIs(type(order.to_record()["amount_minor"]), int)

    def test_order_is_immutable_and_records_are_independent_copies(self) -> None:
        order = Order(**self.fields)
        with self.assertRaises(FrozenInstanceError):
            order.amount_minor = 0
        record = order.to_record()
        record["amount_minor"] = 0
        self.assertEqual(order.amount_minor, 1250)

    def test_money_rejects_bool_float_string_negative_and_none(self) -> None:
        for amount in (True, False, 12.5, "1250", -1, None):
            with self.subTest(amount=amount), self.assertRaises(OrderValidationError):
                Order(**(self.fields | {"amount_minor": amount}))
        self.assertEqual(Order(**(self.fields | {"amount_minor": 0})).amount_minor, 0)

    def test_identifiers_must_be_non_empty_trimmed_strings(self) -> None:
        for name in ("order_id", "customer_id"):
            for value in ("", "   ", " ID", "ID ", 7, None):
                with self.subTest(name=name, value=value), self.assertRaises(OrderValidationError):
                    Order(**(self.fields | {name: value}))

    def test_currency_has_an_explicit_supported_scope(self) -> None:
        for currency in ("EUR", "USD", "GBP"):
            with self.subTest(currency=currency):
                self.assertEqual(Order(**(self.fields | {"currency": currency})).currency, currency)
        for currency in ("eur", "ZZZ", "EU", "EURO", " EUR", True, None):
            with self.subTest(currency=currency), self.assertRaises(OrderValidationError):
                Order(**(self.fields | {"currency": currency}))

    def test_timestamp_must_be_aware_utc_datetime(self) -> None:
        invalid = (
            datetime(2026, 1, 1, 9),
            datetime(2026, 1, 1, 9, tzinfo=timezone(timedelta(hours=1))),
            "2026-01-01T09:00:00Z",
            None,
        )
        for timestamp in invalid:
            with self.subTest(timestamp=timestamp), self.assertRaises(OrderValidationError):
                Order(**(self.fields | {"created_at": timestamp}))

    def test_record_reader_rejects_missing_and_extra_columns(self) -> None:
        record = Order(**self.fields).to_record()
        missing = dict(record)
        del missing["amount_minor"]
        drifted = missing | {"gross_total_minor": 1250}
        for bad_record in (missing, drifted, record | {"debug": "unexpected"}, None, []):
            with self.subTest(record=bad_record), self.assertRaises(OrderValidationError):
                Order.from_record(bad_record)

    def test_record_reader_does_not_coerce_money_or_timestamp(self) -> None:
        record = Order(**self.fields).to_record()
        for amount in (True, 1250.0, "1250"):
            with self.subTest(amount=amount), self.assertRaises(OrderValidationError):
                Order.from_record(record | {"amount_minor": amount})
        for timestamp in ("not-a-date", "2026-01-01", "2026-01-01T10:00:00+01:00", 42):
            with self.subTest(timestamp=timestamp), self.assertRaises(OrderValidationError):
                Order.from_record(record | {"created_at": timestamp})

    def test_explicit_zero_offset_is_accepted_and_serialized_as_z(self) -> None:
        record = Order(**self.fields).to_record()
        order = Order.from_record(record | {"created_at": "2026-01-01T09:00:00+00:00"})
        self.assertEqual(order.to_record()["created_at"], "2026-01-01T09:00:00Z")

    def test_incident_names_are_stable(self) -> None:
        self.assertEqual(
            [kind.value for kind in IncidentKind],
            ["healthy", "schema_drift", "duplicate_ingestion", "invalid_records"],
        )


if __name__ == "__main__":
    unittest.main()
