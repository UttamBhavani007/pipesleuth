"""Pure, closed decisions for the bounded read-only investigator.

The host chooses the run, arguments and budgets. A model can select an offered
tool name or return a diagnosis, but cannot add an execution or approval action.
"""

from __future__ import annotations

import json
import re

from pipesleuth.single_call import _schema, _validate


MAX_DECISION_BYTES = 8192
TOOL_NAMES = ("read_run", "read_logs", "inspect_schema", "inspect_validation")
_REFERENCE = re.compile(r"ev_[0-9a-f]{32}")


class AgentProtocolError(ValueError):
    """A trusted offer or untrusted model decision violates the protocol."""


# Keep callers free to use the shorter protocol-specific spelling.
ProtocolError = AgentProtocolError


def _configuration(available_tools, references, allow_finish):
    if (not isinstance(available_tools, (list, tuple)) or len(available_tools) > 4
            or any(not isinstance(name, str) or name not in TOOL_NAMES for name in available_tools)
            or len(set(available_tools)) != len(available_tools)):
        raise AgentProtocolError("available tools must be unique allowlisted names")
    if (not isinstance(references, (list, tuple)) or len(references) > 4
            or any(not isinstance(ref, str) or _REFERENCE.fullmatch(ref) is None for ref in references)
            or len(set(references)) != len(references)):
        raise AgentProtocolError("references must be unique evidence identifiers")
    if type(allow_finish) is not bool or allow_finish and not references:
        raise AgentProtocolError("finish permission must be explicit and have evidence")
    if not available_tools and not allow_finish:
        raise AgentProtocolError("no decision is available")
    return list(available_tools), list(references)


def decision_schema(available_tools, references, allow_finish):
    """Describe only the actions offered by the trusted host for this turn."""

    available, references = _configuration(available_tools, references, allow_finish)
    branches = []
    if available:
        branches.append({"type": "object", "additionalProperties": False,
                         "properties": {"action": {"type": "string", "enum": ["tool"]},
                                        "tool": {"type": "string", "enum": available}},
                         "required": ["action", "tool"]})
    if allow_finish:
        branches.append({"type": "object", "additionalProperties": False,
                         "properties": {"action": {"type": "string", "enum": ["finish"]},
                                        "diagnosis": _schema(references)},
                         "required": ["action", "diagnosis"]})
    # The top-level object/properties also satisfies the existing model client's
    # format contract; each union branch closes its own permitted properties.
    return {"type": "object", "properties": {}, "anyOf": branches}


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _constant(_value):
    raise ValueError("nonfinite JSON")


def parse_decision(text, available_tools, references, allow_finish):
    """Parse one bounded decision; valid references do not prove claim support."""

    available, references = _configuration(available_tools, references, allow_finish)
    try:
        if not isinstance(text, str) or len(text.encode("utf-8")) > MAX_DECISION_BYTES:
            raise ValueError("invalid decision text")
        value = json.loads(text, object_pairs_hook=_object, parse_constant=_constant)
        if not isinstance(value, dict):
            raise ValueError("decision must be an object")
        if value.get("action") == "tool":
            if set(value) != {"action", "tool"} or value["tool"] not in available:
                raise ValueError("tool was not offered")
            return value
        if value.get("action") == "finish":
            if not allow_finish or set(value) != {"action", "diagnosis"}:
                raise ValueError("finish was not offered")
            diagnosis = json.dumps(value["diagnosis"], ensure_ascii=False,
                                   separators=(",", ":"), allow_nan=False)
            return {"action": "finish", "diagnosis": _validate(diagnosis, references)}
        raise ValueError("unknown decision action")
    except (ValueError, TypeError, KeyError, UnicodeError, OverflowError, RecursionError):
        raise AgentProtocolError("model decision violates the allowed protocol") from None
