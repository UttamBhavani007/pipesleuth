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
- Validated order records with integer amounts and UTC timestamps.
- A command-line tool for generating incident inputs.
- Isolated SQLite runs that load clean orders and report row counts and totals.

## Getting started

Requires Python **3.11+**. No third-party dependencies or accounts are needed
for the current version.

```sh
git clone https://github.com/UttamBhavani007/pipesleuth.git
cd pipesleuth

# List the available scenarios
PYTHONPATH=src python3 -m pipesleuth scenarios

# Load clean orders into a new local database
PYTHONPATH=src python3 -m pipesleuth pipeline run --seed 42 --rows 12

# Inspect a run using the run_id printed by the command above
PYTHONPATH=src python3 -m pipesleuth pipeline show RUN_ID

# Generate a small input with a renamed amount column
PYTHONPATH=src python3 -m pipesleuth fixture --scenario schema_drift --seed 42 --rows 3
```

The fixture command prints JSON. Use the same scenario and seed to reproduce
an input. `--include-oracle` adds expected results for fixture authors; keep
that output separate from investigator inputs.

Pipeline files are saved under `var/sandboxes/RUN_ID/`. Each run has its own
`source.json` and `warehouse.sqlite3`. Use `--data-dir PATH` with either pipeline
command to choose another local directory.

The pipeline currently loads clean data only. Fault handling is the next stage;
the other scenarios are available as generated inputs.

## Tests

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## Roadmap

- Fault handling and read-only tools for inspecting failures.
- A local AI investigator with saved progress and limited tool access.
- Repair approval, database verification, and a simple web interface.
- Evaluation against a checklist and a single model call.

Built with synthetic data and local infrastructure. No paid services or cloud
model APIs are required. See the [delivery plan](docs/DELIVERY_PLAN.md),
[development log](docs/DEVELOPMENT_LOG.md), and [cost policy](docs/COST_POLICY.md)
for details.

## License

[MIT](LICENSE).
