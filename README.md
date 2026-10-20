# PipeSleuth

A local tool for investigating data pipeline failures.

**In development:** synthetic incident generation is available. The pipeline,
AI investigator, and repair workflow are coming next.

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

## Getting started

Requires Python **3.11+**. No third-party dependencies or accounts are needed
for the current version.

```sh
git clone https://github.com/UttamBhavani007/pipesleuth.git
cd pipesleuth

# List the available scenarios
PYTHONPATH=src python3 -m pipesleuth scenarios

# Generate a small input with a renamed amount column
PYTHONPATH=src python3 -m pipesleuth fixture --scenario schema_drift --seed 42 --rows 3
```

The fixture command prints JSON. Use the same scenario and seed to reproduce
an input. `--include-oracle` adds expected results for fixture authors; keep
that output separate from investigator inputs.

## Tests

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## Roadmap

- A SQLite pipeline with tools for inspecting failures.
- A local AI investigator with saved progress and limited tool access.
- Repair approval, database verification, and a simple web interface.
- Evaluation against a checklist and a single model call.

Built with synthetic data and local infrastructure. No paid services or cloud
model APIs are required. See the [delivery plan](docs/DELIVERY_PLAN.md),
[development log](docs/DEVELOPMENT_LOG.md), and [cost policy](docs/COST_POLICY.md)
for details.

## License

[MIT](LICENSE).
