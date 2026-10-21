"""Deterministic synthetic inputs, with authoring truth kept out of evidence.

These fixtures do not execute the pipeline or investigator. The oracle
describes canonical orders independently of each corrupted representation.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from random import Random
from types import MappingProxyType

from pipesleuth.domain import ORDER_COLUMNS, IncidentKind, Order


MAX_ROW_COUNT = 10_000
MAX_SEED = 2**32 - 1
SourceValue = str | int | None


@dataclass(frozen=True, slots=True)
class FixtureOracle:
    """Trusted evaluation truth, never an input to a future investigator.

    Row numbers are one-based. Column renames map an observed source name to
    its canonical name. Totals are always separated by currency.
    """

    expected_orders: tuple[Order, ...]
    duplicate_order_ids: tuple[str, ...] = ()
    invalid_row_numbers: tuple[int, ...] = ()
    column_renames: tuple[tuple[str, str], ...] = ()

    @property
    def expected_order_count(self) -> int:
        return len(self.expected_orders)

    @property
    def expected_totals_minor(self) -> dict[str, int]:
        totals: dict[str, int] = {}
        for order in self.expected_orders:
            totals[order.currency] = totals.get(order.currency, 0) + order.amount_minor
        return totals

    def to_dict(self) -> dict[str, object]:
        """Serialize trusted evaluation data for fixture authors and tests."""

        return {
            "expected_order_count": self.expected_order_count,
            "expected_totals_minor": self.expected_totals_minor,
            "expected_orders": [order.to_record() for order in self.expected_orders],
            "duplicate_order_ids": list(self.duplicate_order_ids),
            "invalid_row_numbers": list(self.invalid_row_numbers),
            "column_renames": dict(self.column_renames),
        }


@dataclass(frozen=True, slots=True)
class IncidentFixture:
    """A source observation alongside separately accessible authoring truth."""

    kind: IncidentKind
    seed: int
    source_columns: tuple[str, ...]
    source_rows: tuple[Mapping[str, SourceValue], ...]
    oracle: FixtureOracle

    def __post_init__(self) -> None:
        # Copy before freezing so callers cannot mutate fixture rows indirectly.
        object.__setattr__(
            self,
            "source_rows",
            tuple(MappingProxyType(dict(row)) for row in self.source_rows),
        )

    def to_source_payload(self) -> dict[str, object]:
        """Return observable source data only, without labels or expected answers."""

        return {
            "schema": list(self.source_columns),
            "rows": [dict(row) for row in self.source_rows],
        }

    def to_evidence_dict(self) -> dict[str, object]:
        """An explicit evidence-only alias for future investigator input."""

        return self.to_source_payload()

    def to_authoring_dict(self) -> dict[str, object]:
        """Return the complete trusted fixture; never feed this to an agent."""

        return {
            "metadata": {
                "scenario": self.kind.value,
                "seed": self.seed,
                "clean_row_count": self.oracle.expected_order_count,
            },
            "source": self.to_source_payload(),
            "oracle": self.oracle.to_dict(),
        }


def _validate_generation_args(seed: int, row_count: int) -> None:
    if type(seed) is not int or not 0 <= seed <= MAX_SEED:
        raise ValueError(f"seed must be an integer between 0 and {MAX_SEED}")
    if type(row_count) is not int or not 1 <= row_count <= MAX_ROW_COUNT:
        raise ValueError(f"row_count must be an integer between 1 and {MAX_ROW_COUNT}")


def _clean_orders(seed: int, row_count: int) -> tuple[Order, ...]:
    random = Random(seed)
    start = datetime(2026, 1, 1, 9, tzinfo=timezone.utc)
    return tuple(
        Order(
            order_id=f"ORD-{index + 1:04d}",
            customer_id=f"CUST-{random.randint(1, 8):03d}",
            amount_minor=random.randint(500, 15_000),
            currency="EUR",
            created_at=start + timedelta(minutes=7 * index),
        )
        for index in range(row_count)
    )


def generate_fixture(
    kind: IncidentKind | str,
    seed: int = 42,
    row_count: int = 12,
) -> IncidentFixture:
    """Build a reproducible observation and a separately derived clean oracle.

    ``row_count`` is the clean order count (1..10,000). Duplicate ingestion
    appends identical retries of the first, middle and last orders (unique
    positions for small inputs). Invalid-record fixtures append four malformed
    rows while preserving every original valid order. No global RNG is used.
    """

    _validate_generation_args(seed, row_count)
    try:
        incident_kind = IncidentKind(kind)
    except (TypeError, ValueError) as exc:
        choices = ", ".join(member.value for member in IncidentKind)
        raise ValueError(f"kind must be one of: {choices}") from exc

    # Establish truth before modifying a separate source representation.
    expected_orders = _clean_orders(seed, row_count)
    rows: list[dict[str, SourceValue]] = [order.to_record() for order in expected_orders]
    columns = ORDER_COLUMNS
    duplicate_ids: tuple[str, ...] = ()
    invalid_rows: tuple[int, ...] = ()
    renames: tuple[tuple[str, str], ...] = ()

    if incident_kind is IncidentKind.SCHEMA_DRIFT:
        columns = tuple(
            "gross_total_minor" if column == "amount_minor" else column
            for column in ORDER_COLUMNS
        )
        for row in rows:
            row["gross_total_minor"] = row.pop("amount_minor")
        renames = (("gross_total_minor", "amount_minor"),)
    elif incident_kind is IncidentKind.DUPLICATE_INGESTION:
        retry_indices = sorted({0, row_count // 2, row_count - 1})
        duplicate_ids = tuple(expected_orders[index].order_id for index in retry_indices)
        rows.extend(dict(rows[index]) for index in retry_indices)
    elif incident_kind is IncidentKind.INVALID_RECORDS:
        malformed = [dict(rows[0]) for _ in range(4)]
        for index, row in enumerate(malformed, start=1):
            row["order_id"] = f"ORD-{row_count + index:04d}"
        malformed[0].pop("amount_minor")
        malformed[1]["amount_minor"] = -250
        malformed[2]["currency"] = "ZZZ"
        malformed[3]["created_at"] = "2026-01-01T09:00:00"
        rows.extend(malformed)
        invalid_rows = tuple(range(row_count + 1, row_count + 5))

    return IncidentFixture(
        kind=incident_kind,
        seed=seed,
        source_columns=columns,
        source_rows=tuple(rows),
        oracle=FixtureOracle(
            expected_orders=expected_orders,
            duplicate_order_ids=duplicate_ids,
            invalid_row_numbers=invalid_rows,
            column_renames=renames,
        ),
    )
