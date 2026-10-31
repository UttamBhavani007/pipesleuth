"""Pure preconditions and transformations for three trusted repair actions.

This module does not execute a plan or grant approval. It uses observed source
rows and the order contract, without model output or evaluator expectations.
"""

from __future__ import annotations

import json

from pipesleuth.domain import ORDER_COLUMNS, Order, OrderValidationError
from pipesleuth.pipeline import SQLITE_MAX_INTEGER, PipelineError, _encode_source


ACTIONS = ("rename_amount_column", "skip_identical_duplicates", "quarantine_invalid_rows")
_RENAMED_COLUMNS = ["gross_total_minor" if name == "amount_minor" else name for name in ORDER_COLUMNS]


class RepairPlanError(ValueError):
    """The source or action does not meet one supported repair's preconditions."""


def action_descriptions():
    """Return fresh descriptions for a trusted caller's action selector."""

    return {
        "rename_amount_column": "Rename gross_total_minor only when it already uses minor currency units.",
        "skip_identical_duplicates": "Skip equal canonical retry records while preserving their raw source.",
        "quarantine_invalid_rows": "Retain invalid rows separately and load unique valid records.",
    }


def _order(row):
    try:
        order = Order.from_record(row)
    except OrderValidationError:
        return None
    return order if order.amount_minor <= SQLITE_MAX_INTEGER else None


def _unique_orders(rows):
    seen = {}
    duplicates = 0
    for row in rows:
        order = _order(row)
        if order is None:
            raise RepairPlanError("this action requires every transformed record to be valid")
        previous = seen.get(order.order_id)
        if previous is not None:
            if previous != order:
                raise RepairPlanError("conflicting valid records cannot be repaired by this action")
            duplicates += 1
        else:
            seen[order.order_id] = order
    return len(seen), duplicates


def prepare_repair(payload, action):
    """Validate one allowlisted action and return a detached proposed source/policy."""

    if not isinstance(action, str) or action not in ACTIONS:
        raise RepairPlanError("repair action is not allowlisted")
    try:
        source = json.loads(_encode_source(payload))
    except (PipelineError, ValueError, TypeError, UnicodeError, RecursionError):
        raise RepairPlanError("repair source must satisfy the bounded source JSON contract") from None
    duplicate_policy, invalid_policy = "reject", "reject"
    loaded, quarantined, skipped = 0, 0, 0
    warnings = []
    if action == "rename_amount_column":
        if source["schema"] != _RENAMED_COLUMNS:
            raise RepairPlanError("rename requires the exact gross_total_minor source schema")
        expected = set(_RENAMED_COLUMNS)
        for row in source["rows"]:
            if set(row) != expected:
                raise RepairPlanError("rename requires exact row columns without ambiguous amount fields")
            row["amount_minor"] = row.pop("gross_total_minor")
        source["schema"] = list(ORDER_COLUMNS)
        loaded, duplicates = _unique_orders(source["rows"])
        if duplicates:
            raise RepairPlanError("rename does not also repair duplicate records")
        warnings = ["Assumes gross_total_minor already uses the same minor currency units as amount_minor; no unit conversion is performed."]
    else:
        if source["schema"] != list(ORDER_COLUMNS):
            raise RepairPlanError("this action requires the canonical order schema")
        if action == "skip_identical_duplicates":
            loaded, skipped = _unique_orders(source["rows"])
            if skipped == 0:
                raise RepairPlanError("duplicate repair requires at least one identical repeat")
            duplicate_policy = "skip_identical"
        else:
            seen = set()
            for row in source["rows"]:
                order = _order(row)
                if order is None:
                    quarantined += 1
                else:
                    if order.order_id in seen:
                        raise RepairPlanError("quarantine does not also repair repeated valid identifiers")
                    seen.add(order.order_id)
                    loaded += 1
            if loaded == 0 or quarantined == 0:
                raise RepairPlanError("quarantine requires both valid and invalid records")
            invalid_policy = "quarantine"
    # Revalidate the transformed envelope; no action may expand beyond source bounds.
    try:
        source = json.loads(_encode_source(source))
    except (PipelineError, ValueError, TypeError, UnicodeError, RecursionError):
        raise RepairPlanError("transformed repair source exceeds the supported source contract") from None
    return {"source": source, "duplicate_policy": duplicate_policy, "invalid_policy": invalid_policy,
            "effects": {"rows_loaded": loaded, "rows_quarantined": quarantined, "duplicates_skipped": skipped},
            "warnings": warnings}
