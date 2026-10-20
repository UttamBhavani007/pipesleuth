# PipeSleuth

A local tool for investigating data pipeline failures.

**In development.** This first commit sets up the Python package and project
plan. Synthetic incident fixtures are the next increment.

## Why this project?

A renamed column can break an import. A retry can load the same orders twice.
Bad records can leave incorrect data in a database. Finding the cause often
means checking logs, schemas, and data validation results by hand.

PipeSleuth is being built to bring that evidence together, explain the likely
cause, and suggest a repair. You review the repair before it runs, and the tool
checks the database afterward to confirm the result.

## Getting started

Requires Python **3.11+**. No third-party runtime dependencies are needed yet.

```sh
git clone https://github.com/UttamBhavani007/pipesleuth.git
cd pipesleuth
PYTHONPATH=src python3 -c 'import pipesleuth; print(pipesleuth.__version__)'
```

## Roadmap

- Reproducible incidents: schema changes, duplicates, and invalid records.
- A small SQLite pipeline with tools for inspecting failures.
- A local AI investigator with saved progress and limited tool access.
- Repair approval, database verification, and a simple web interface.
- A comparison against a checklist and a single model call.

The project uses synthetic data and local infrastructure. No paid services or
cloud model APIs are required. See the [delivery plan](docs/DELIVERY_PLAN.md),
[development log](docs/DEVELOPMENT_LOG.md), and [cost policy](docs/COST_POLICY.md)
for details.

## License

[MIT](LICENSE). Built with AI assistance; changes are tested and reviewed.
