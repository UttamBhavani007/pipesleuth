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
```

To try the two supported incident inputs:

```sh
# Deliberately fail on a renamed amount column; inspect the returned run_id
PYTHONPATH=src python3 -m pipesleuth pipeline run --scenario schema_drift

# Load a retry batch while skipping identical repeated orders
PYTHONPATH=src python3 -m pipesleuth pipeline run --scenario duplicate_ingestion --deduplicate
```

Without `--deduplicate`, duplicate IDs fail the load. Conflicting records with
the same ID always fail. Replaying a completed run checks the source and stored
rows first; it rejects changed data instead of silently reloading it.

Pipeline files are saved under `var/sandboxes/RUN_ID/`. Each run has its own
`source.json` and `warehouse.sqlite3`. Use `--data-dir PATH` with any pipeline
command to choose another local directory.

Automatic schema repair, invalid-record quarantine, and AI investigation are
planned. No AI model runs in the current version.

## Tests

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## Roadmap

- Invalid-record quarantine and read-only evidence tools.
- A local AI investigator with saved progress and limited tool access.
- Repair approval, database verification, and a simple web interface.
- Evaluation against a checklist and a single model call.

Built with synthetic data and local infrastructure. No paid services or cloud
model APIs are required. See the [delivery plan](docs/DELIVERY_PLAN.md),
[development log](docs/DEVELOPMENT_LOG.md), and [cost policy](docs/COST_POLICY.md)
for details.

## License

[MIT](LICENSE).
