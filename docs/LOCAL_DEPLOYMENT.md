# Local installation

PipeSleuth is a local prototype. Start with a synthetic incident, inspect its
evidence and run the checklist. The browser and repair workflow use local SQLite
files and the same loopback server. The model is optional.

## Run from the source checkout

Use Python 3.11 or newer. Run these commands from the project directory.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-api.txt
PYTHONPATH=src .venv/bin/python -m pipesleuth pipeline run \
  --scenario schema_drift --seed 42 --rows 6
PYTHONPATH=src .venv/bin/python -m pipesleuth serve
```

The synthetic schema incident deliberately exits with status 1 and prints a
`run_id`. Copy that ID, start the server and open **http://127.0.0.1:8765/**.
Choose the private session file printed by `serve`, then open the run in the
browser. The event timeline and schema show the renamed column. The checklist
runs with the default server configuration. Stop the server with Ctrl-C.
See the [browser guide](UI.md) and [demo walkthrough](DEMO.md).

By default, `var/sandboxes` holds inputs and their pipeline databases,
`var/repaired` holds derived repairs, and `var/investigations` and `var/repairs`
hold saved state. Keep the session file outside those directories and private.
The foreground server removes its unchanged session file on normal shutdown.
If startup refuses an occupied port or an existing session file, use `--port`
or `--session-file` to choose a free port or a fresh filename.
See the [API guide](API.md).

## Install the CLI entry point

From the checkout, use the same virtual environment and its pinned API
requirements. The build declares setuptools as a build dependency.

```sh
.venv/bin/python -m pip install --no-deps .
.venv/bin/pipesleuth --version
.venv/bin/pipesleuth pipeline run --scenario schema_drift --seed 42 --rows 6
.venv/bin/pipesleuth serve
```

The installed wheel includes the six browser assets. The installed CLI can run
from a separate working directory, where it creates that workspace's own `var`
and `.runtime` directories. Choose an explicit workspace directory before
running a demo so the paths stay easy to find. The core pipeline and checklist
use only the standard library. The API requirements add the local server and
its test client.

## Reproduce the offline installation check

The verifier uses Python with pip and the declared setuptools build backend.
Provide a local wheel directory with the 17 exact artifacts in the
[reviewed wheel manifest](../reports/api-dependencies.json). Those hashes are
for the reviewed macOS arm64 and CPython 3.13 wheel set. The project keeps wheel
binaries outside Git.

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 scripts/smoke_install.py \
  --wheelhouse .tools/api-wheels --output reports/local/install-check.json
```

The verifier checks each dependency wheel's recorded hash and size. It builds
PipeSleuth in a temporary source copy with package-index access disabled and the
existing build backend, then installs the wheel and reviewed dependencies in a
separate fresh virtual environment. It checks the installed versions and assets,
launches the installed CLI from an empty workspace, and exercises actual
loopback HTTP. The repair probe uses a scripted exact-plan approval and reads the
derived SQLite database independently. The owned server, private session and
temporary installation are cleaned up afterward. Reports use an exclusive output
file so earlier observations stay available.

The [installation report](../reports/local-deployment.json) records the observed
wheel SHA-256, package inventory, platform, Python and dependency versions,
HTTP checks, database verification and cleanup. The verification scope is the
recorded host and a model-free install. The browser interaction and local model
measurements have separate evidence. See the [UI guide](UI.md) and
[evaluation report](EVALUATION.md).
