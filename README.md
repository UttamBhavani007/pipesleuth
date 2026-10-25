# PipeSleuth

A local tool for investigating data pipeline failures.

**In development:** synthetic fixtures and a local SQLite pipeline are available.
The AI investigator and repair workflow are planned.

## Why this project?

A renamed column can break an import. A retry can load the same orders twice.
Bad records can leave incorrect data in a database. Finding the cause often
means checking logs, schemas, and validation results by hand.

PipeSleuth is being built to bring that evidence together, explain the likely
cause, and suggest a repair. You review the repair before it runs, and the tool
checks the database afterward to confirm the result.

## Current features

- Reproducible order data with healthy, schema-change, duplicate, and invalid-record scenarios.
- Isolated SQLite runs with exact integer amounts, row counts, and currency totals.
- Logged schema-change and duplicate-record failures.
- Optional skipping of identical retry records, with safe verification of completed runs.
- Optional quarantine of invalid rows, preserving their input and validation reasons.
- Read-only evidence tools for run status, logs, schema, and validation results.
- A deterministic troubleshooting checklist with evidence references and uncertainty.

## Getting started

Requires Python **3.11+**. No third-party dependencies or accounts are needed
for the current version.

```sh
git clone https://github.com/UttamBhavani007/pipesleuth.git
cd pipesleuth

# Load clean orders into a new local database
PYTHONPATH=src python3 -m pipesleuth pipeline run --seed 42 --rows 12

# Inspect a run using the run_id printed by the command above
PYTHONPATH=src python3 -m pipesleuth pipeline show RUN_ID

# Read its events, or verify it again without inserting rows
PYTHONPATH=src python3 -m pipesleuth pipeline events RUN_ID
PYTHONPATH=src python3 -m pipesleuth pipeline rerun RUN_ID

# Diagnose observed faults using the deterministic checklist
PYTHONPATH=src python3 -m pipesleuth diagnose RUN_ID
```

To try incident inputs:

```sh
# Deliberately fail on a renamed amount column; inspect the returned run_id
PYTHONPATH=src python3 -m pipesleuth pipeline run --scenario schema_drift

# Load a retry batch while skipping identical repeated orders
PYTHONPATH=src python3 -m pipesleuth pipeline run --scenario duplicate_ingestion --deduplicate

# Load valid rows and retain invalid records separately, then inspect them
PYTHONPATH=src python3 -m pipesleuth pipeline run --scenario invalid_records --quarantine
PYTHONPATH=src python3 -m pipesleuth pipeline quarantine RUN_ID
```

Without `--deduplicate`, duplicate IDs fail the load. Conflicting valid orders
with the same ID always fail. Without `--quarantine`, an invalid row fails the
whole load. Quarantine retains each rejected input and its first validation
reason; the result reports `rows_loaded` and `rows_quarantined` separately.

Replaying a completed run checks the source, accepted rows, and quarantined
records first. Changed data is rejected without reloading it.

Pipeline files are saved under `var/sandboxes/RUN_ID/`. Each run has its own
`source.json` and `warehouse.sqlite3`. Use `--data-dir PATH` with any pipeline
command to choose another local directory.

To inspect investigation evidence:

```sh
PYTHONPATH=src python3 -m pipesleuth evidence tools
PYTHONPATH=src python3 -m pipesleuth evidence inspect_schema RUN_ID
PYTHONPATH=src python3 -m pipesleuth evidence inspect_validation RUN_ID --limit 5
```

The tools return bounded JSON with a content fingerprint. They cannot change
data or run caller-supplied SQL or shell commands. See the
[tool reference](docs/EVIDENCE_TOOLS.md).

The checklist reports supported faults or `unknown` when evidence is incomplete
or inconsistent. It leaves the data unchanged. See the
[checklist reference](docs/CHECKLIST.md) for its rules and limitations.

AI investigation, automatic schema repair, and approved repairs are planned.
No AI model runs in the current version.

## Tests

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## Roadmap

- A local AI investigator with saved progress and limited tool access.
- Repair approval, database verification, and a simple web interface.
- Evaluation against a checklist and a single model call.

Built with synthetic data and local infrastructure. No paid services or cloud
model APIs are required. See the [delivery plan](docs/DELIVERY_PLAN.md),
[development log](docs/DEVELOPMENT_LOG.md), and [cost policy](docs/COST_POLICY.md)
for details.

## License

[MIT](LICENSE).
