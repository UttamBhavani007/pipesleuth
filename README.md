# PipeSleuth

A local workspace for investigating data pipeline failures and reviewing fixed repairs.

## Why this project?

A renamed column can break an import. Repeated batches can duplicate orders, and
invalid records can leave a load incomplete. Finding the cause usually means
checking logs, schemas and validation results separately.

PipeSleuth brings those facts together. Inspect an incident, run a checklist or
an experimental local model, and review a supported repair before it creates a
new database. The original incident stays available for comparison.

**Status: local prototype.** The pipeline, browser interface and repair workflow
are implemented. Model diagnosis accuracy has not been established; earlier
local-model smoke runs include false findings and rejected responses.

## Features

- Synthetic healthy, schema-change, duplicate and invalid-record inputs.
- Isolated SQLite runs, validation, quarantine and guarded replay.
- Read-only evidence tools with content fingerprints.
- A deterministic checklist, single-call local model and bounded tool agent.
- Saved investigations with explicit resume and uncertain-outcome limits.
- Three fixed repairs with exact-plan approval and independent database checks.
- A local API and browser workspace with separate operation and approval access.

The model cannot run arbitrary SQL or shell commands, or approve a repair.
These are development demonstrations, not production recovery results.

## Getting started

The pipeline uses Python **3.11+** and the standard library. The browser interface
uses the optional API dependencies; that environment was checked with Python
3.13.7 on macOS arm64.

```sh
git clone https://github.com/UttamBhavani007/pipesleuth.git
cd pipesleuth
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-api.txt

# Create a run and keep the printed run_id
PYTHONPATH=src .venv/bin/python -m pipesleuth pipeline run --seed 42 --rows 12

# Start the local API and browser workspace
PYTHONPATH=src .venv/bin/python -m pipesleuth serve
```

Open **http://127.0.0.1:8765**. Choose the private session file printed by the
server, then enter the run ID. Inspect its events, schema and validation, or run
the deterministic checklist. Stop the server with Ctrl-C.

To create an incident, run this in another terminal. A failed load is expected;
the command prints the ID to inspect:

```sh
PYTHONPATH=src .venv/bin/python -m pipesleuth pipeline run --scenario schema_drift
```

For a supported incident, choose a repair action and review its plan and warnings.
Load approval access separately using the session file, acknowledge the exact
plan, approve it, then execute it. Repairs produce a new run under `var/repaired/`.
See the [UI guide](docs/UI.md) for the workflow and interruption limits.

The checklist needs no model. To enable the experimental model methods, follow
[local model setup](docs/LOCAL_MODEL.md) and the digest-pinning instructions in
[the API guide](docs/API.md). There is no remote fallback.

For command-line use:

```sh
PYTHONPATH=src python3 -m pipesleuth diagnose RUN_ID
PYTHONPATH=src python3 -m pipesleuth evidence inspect_schema RUN_ID
```

[Evidence tools](docs/EVIDENCE_TOOLS.md) · [Investigator](docs/INVESTIGATOR.md) ·
[Saved progress](docs/CHECKPOINTS.md) · [Repairs](docs/REPAIRS.md)

## Tests

```sh
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
# Optional JavaScript development checks, with an existing Node installation
node --test tests/ui_*.mjs
```

Without API dependencies, standard-library discovery skips the optional API
checks. Software tests and browser checks do not establish model accuracy.

## Remaining work

- Matched evaluation of the checklist, single-call model and tool agent.
- Clean-environment packaging and broader browser/accessibility checks.

The scheduled build ends on **2 November 2026** with that scope unfinished.
[Development log](docs/DEVELOPMENT_LOG.md) · [Delivery plan](docs/DELIVERY_PLAN.md)

All execution is local. No paid services, cloud inference or provider API keys
are used. [Cost policy](docs/COST_POLICY.md).

## License

[MIT](LICENSE).
