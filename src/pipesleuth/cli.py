"""Generate synthetic inputs and run or inspect the local order pipeline."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from pipesleuth import __version__
from pipesleuth.checklist import ChecklistBaseline
from pipesleuth.domain import IncidentKind
from pipesleuth.evidence import EvidenceGateway
from pipesleuth.fixtures import generate_fixture
from pipesleuth.pipeline import SandboxPipeline


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipesleuth",
        description="Generate synthetic orders and run a local SQLite pipeline.",
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
    pipeline = commands.add_parser("pipeline", help="Run or inspect an isolated local pipeline")
    operations = pipeline.add_subparsers(dest="operation", required=True)
    run = operations.add_parser("run", help="Run a synthetic order batch in a new sandbox")
    run.add_argument("--seed", type=int, default=42, help="Unsigned 32-bit seed (default: 42)")
    run.add_argument("--rows", type=int, default=12, help="Clean order count, 1–10000 (default: 12)")
    run.add_argument(
        "--scenario", choices=[kind.value for kind in IncidentKind],
        default="healthy", help="Synthetic input to execute (default: healthy)",
    )
    run.add_argument(
        "--deduplicate", action="store_true",
        help="Skip identical repeated orders; conflicting records still fail",
    )
    run.add_argument(
        "--quarantine", action="store_true",
        help="Keep invalid rows with rejection reasons and load the valid rows",
    )
    show = operations.add_parser("show", help="Read an existing pipeline run without changing it")
    show.add_argument("run_id", help="Run identifier returned by pipeline run")
    rerun = operations.add_parser("rerun", help="Verify a successful run without inserting rows again")
    rerun.add_argument("run_id", help="Previously successful run to verify")
    events = operations.add_parser("events", help="Read the observed events for a pipeline run")
    events.add_argument("run_id", help="Run identifier returned by pipeline run")
    events.add_argument("--limit", type=int, default=100, help="Maximum events, 1–1000 (default: 100)")
    quarantine = operations.add_parser("quarantine", help="Read rejected records and their reasons")
    quarantine.add_argument("run_id", help="Run identifier returned by pipeline run")
    quarantine.add_argument("--limit", type=int, default=100, help="Maximum records, 1–1000 (default: 100)")
    for operation in (run, show, rerun, events, quarantine):
        operation.add_argument(
            "--data-dir", type=Path, default=Path("var/sandboxes"),
            help="Local sandbox directory (default: var/sandboxes)",
        )
    evidence = commands.add_parser("evidence", help="Read bounded investigation evidence")
    evidence_tools = evidence.add_subparsers(dest="tool", required=True)
    evidence_tools.add_parser("tools", help="List the available read-only evidence tools")
    for name, description in (
        ("read_run", "Read run status and counts"),
        ("read_logs", "Read observed pipeline events"),
        ("inspect_schema", "Inspect source and warehouse columns"),
        ("inspect_validation", "Inspect rejected records and validation results"),
    ):
        tool = evidence_tools.add_parser(name, help=description)
        tool.add_argument("run_id", help="Run identifier returned by pipeline run")
        tool.add_argument(
            "--data-dir", type=Path, default=Path("var/sandboxes"),
            help="Local sandbox directory (default: var/sandboxes)",
        )
        if name in ("read_logs", "inspect_validation"):
            tool.add_argument("--limit", type=int, default=10, help="Maximum records, 1–20 (default: 10)")
    diagnose = commands.add_parser("diagnose", help="Run the deterministic troubleshooting checklist")
    diagnose.add_argument("run_id", help="Run identifier returned by pipeline run")
    diagnose.add_argument(
        "--data-dir", type=Path, default=Path("var/sandboxes"),
        help="Local sandbox directory (default: var/sandboxes)",
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
    elif args.command == "fixture":
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
    elif args.command == "diagnose":
        try:
            payload = ChecklistBaseline(EvidenceGateway(args.data_dir)).diagnose(args.run_id)
        except (ValueError, OSError) as exc:
            parser.error(str(exc))
    elif args.command == "evidence":
        if args.tool == "tools":
            payload = {"tools": EvidenceGateway.tools()}
        else:
            arguments = {"run_id": args.run_id}
            if args.tool in ("read_logs", "inspect_validation"):
                arguments["limit"] = args.limit
            try:
                payload = EvidenceGateway(args.data_dir).call(args.tool, arguments)
            except (ValueError, OSError) as exc:
                parser.error(str(exc))
    else:
        try:
            pipeline = SandboxPipeline(args.data_dir)
            if args.operation == "run":
                source = generate_fixture(args.scenario, seed=args.seed, row_count=args.rows)
                run = pipeline.create(
                    source.to_source_payload(),
                    duplicate_policy="skip_identical" if args.deduplicate else "reject",
                    invalid_policy="quarantine" if args.quarantine else "reject",
                )
                payload = pipeline.run(run.run_id).to_dict()
            elif args.operation == "rerun":
                payload = pipeline.run(args.run_id).to_dict()
            elif args.operation == "events":
                payload = {"run_id": args.run_id, "events": pipeline.events(args.run_id, limit=args.limit)}
            elif args.operation == "quarantine":
                payload = {"run_id": args.run_id, "records": pipeline.quarantine(args.run_id, limit=args.limit)}
            else:
                payload = pipeline.inspect(args.run_id).to_dict()
        except (ValueError, OSError) as exc:
            parser.error(str(exc))
    if args.command in {"evidence", "diagnose"}:
        print(json.dumps(payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False, allow_nan=False))
    else:
        print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False))
    return 1 if payload.get("status") in {"failed", "unknown"} else 0
