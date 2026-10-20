"""Source-only commands for authoring reproducible synthetic incidents."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from pipesleuth import __version__
from pipesleuth.domain import IncidentKind
from pipesleuth.fixtures import generate_fixture


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipesleuth",
        description="PipeSleuth day-1 fixture tools. No pipeline or AI model runs yet.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("scenarios", help="List the supported synthetic incident families")
    fixture = commands.add_parser("fixture", help="Print deterministic synthetic source JSON")
    fixture.add_argument("--scenario", choices=[kind.value for kind in IncidentKind], required=True)
    fixture.add_argument("--seed", type=int, default=42, help="Unsigned 32-bit seed (default: 42)")
    fixture.add_argument("--rows", type=int, default=12, help="Clean order count, 1–10000 (default: 12)")
    fixture.add_argument(
        "--include-oracle",
        action="store_true",
        help="Trusted fixture authoring only: include labels and expected results",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Emit JSON on stdout and actionable errors on stderr."""
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command == "scenarios":
        payload = {
            "synthetic": True,
            "stage": "fixture_foundation",
            "scenarios": [kind.value for kind in IncidentKind],
        }
    else:
        try:
            fixture = generate_fixture(args.scenario, seed=args.seed, row_count=args.rows)
        except (ValueError, TypeError) as exc:
            parser.error(str(exc))
        if args.include_oracle:
            print(
                "Authoring output includes ground truth. Do not expose it to an investigator.",
                file=sys.stderr,
            )
            payload = fixture.to_authoring_dict()
        else:
            payload = fixture.to_source_payload()
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False))
    return 0
