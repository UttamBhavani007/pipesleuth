"""Validated order contracts for the synthetic order-import demonstration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum


ORDER_COLUMNS = (
    "order_id",
    "customer_id",
    "amount_minor",
    "currency",
    "created_at",
)
# Deliberately small, documented currency scope for this demonstration.
SUPPORTED_CURRENCIES = frozenset({"EUR", "USD", "GBP"})


class IncidentKind(str, Enum):
    """Synthetic scenarios; these labels belong to fixture authoring only."""

    HEALTHY = "healthy"
    SCHEMA_DRIFT = "schema_drift"
    DUPLICATE_INGESTION = "duplicate_ingestion"
    INVALID_RECORDS = "invalid_records"


class OrderValidationError(ValueError):
    """An incoming record does not satisfy the order contract."""


def _validate_identifier(name: str, value: object) -> None:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise OrderValidationError(f"{name} must be a non-empty, trimmed string")


@dataclass(frozen=True, slots=True)
class Order:
    """An immutable order with integer minor units and an aware UTC timestamp.

    EUR, USD, and GBP use two decimal places, so ``amount_minor=1250`` means
    12.50 in the stated currency. Zero-value orders are valid; refunds are not
    part of this contract. Datetimes must already be expressed in UTC.
    """

    order_id: str
    customer_id: str
    amount_minor: int
    currency: str
    created_at: datetime

    def __post_init__(self) -> None:
        _validate_identifier("order_id", self.order_id)
        _validate_identifier("customer_id", self.customer_id)
        if type(self.amount_minor) is not int or self.amount_minor < 0:
            raise OrderValidationError("amount_minor must be a non-negative integer")
        if not isinstance(self.currency, str) or self.currency not in SUPPORTED_CURRENCIES:
            raise OrderValidationError("currency must be one of EUR, GBP, USD")
        if not isinstance(self.created_at, datetime):
            raise OrderValidationError("created_at must be a datetime")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() != timedelta(0):
            raise OrderValidationError("created_at must be timezone-aware and in UTC")

    def to_record(self) -> dict[str, str | int]:
        """Return a fresh, JSON-compatible source record."""

        return {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "amount_minor": self.amount_minor,
            "currency": self.currency,
            "created_at": self.created_at.isoformat().removesuffix("+00:00") + "Z",
        }

    @classmethod
    def from_record(cls, record: Mapping[str, object]) -> Order:
        """Read exactly the canonical schema without coercing money or IDs.

        Extra columns are rejected as well as missing ones. A later ingestion
        boundary can decide how to handle schema changes explicitly.
        """

        if not isinstance(record, Mapping):
            raise OrderValidationError("order record must be a mapping")
        expected_columns = set(ORDER_COLUMNS)
        actual_columns = set(record)
        if actual_columns != expected_columns:
            missing = sorted(expected_columns - actual_columns)
            extra = sorted(str(key) for key in actual_columns - expected_columns)
            raise OrderValidationError(
                f"order columns do not match contract (missing={missing}, extra={extra})"
            )
        timestamp = record["created_at"]
        if not isinstance(timestamp, str):
            raise OrderValidationError("created_at must be an ISO 8601 UTC string")
        try:
            created_at = datetime.fromisoformat(timestamp)
        except ValueError as exc:
            raise OrderValidationError("created_at must be an ISO 8601 UTC string") from exc
        return cls(
            order_id=record["order_id"],  # type: ignore[arg-type]
            customer_id=record["customer_id"],  # type: ignore[arg-type]
            amount_minor=record["amount_minor"],  # type: ignore[arg-type]
            currency=record["currency"],  # type: ignore[arg-type]
            created_at=created_at,
        )
