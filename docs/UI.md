# Local web interface

PipeSleuth's browser workspace brings a run's events, schema and validation
results together. You can run a checklist, inspect experimental model output,
continue a saved investigation and review a fixed repair before executing it.
It uses the same local API, evidence gateway, approval checks and independent
repair verifier as the command-line workflow.

## Start the workspace

Run from the repository root. The optional API dependencies also serve the UI;
there is no frontend build step or additional JavaScript dependency.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-api.txt

# Create a deliberately failed input and copy its printed run_id.
PYTHONPATH=src .venv/bin/python -m pipesleuth pipeline run \
  --scenario schema_drift --seed 42 --rows 6

# Start the foreground API and browser interface.
PYTHONPATH=src .venv/bin/python -m pipesleuth serve --port 8765 \
  --session-file .runtime/api-session.json
```

The schema-change command intentionally exits with a failed pipeline status.
That is the incident to inspect. A healthy control is available with
`pipeline run --seed 42 --rows 6` instead.

Open **http://127.0.0.1:8765/** in a browser. Keep the foreground server running;
stop it with Ctrl-C when finished. The server prints the path to its private
session file, never the tokens inside it. Custom pipeline roots must match the
server's `--data-dir`; the browser cannot choose arbitrary local directories.
See [API setup](API.md) for the other fixed roots and launcher options.

## Connect and inspect a run

1. Choose the server's private session JSON file using **Choose session file**.
   Hidden directories such as `.runtime` may require the file dialog's
   “Go to folder” or “show hidden files” control.
2. Paste the run ID from the pipeline command and select **Open run**.
3. Inspect the event timeline, schema, validation or run facts. Open the details
   to see the bounded evidence envelope and its content fingerprint.
4. Select **Deterministic checklist** and request a diagnosis. It uses fixed
   rules and read-only evidence; it does not call an AI model.

The file is read locally by the browser. The file itself and its filesystem path
are not uploaded to the API. Connecting retains read and operation capabilities
in the tab's memory. Each permitted API request sends the appropriate bearer
capability over the same loopback origin. Capabilities are not placed in URLs,
cookies, browser storage or displayed records.

The interface loads no remote scripts, fonts, styles or telemetry. Its server
uses a restricted content security policy and accepts only the configured
loopback host and same-origin browser requests. These are local workspace
protections, not an identity system or isolation from other programs that can
read the session file. Disconnecting clears retained access and displayed
records. Reloading requires choosing the file again.

## Diagnosis and saved investigations

The two model methods are experimental. Their output includes observed tool and
model request counts, limitations, evidence references and a trace. Structurally
valid references do not establish that a finding is correct. **Unknown** means
the method did not establish a diagnosis; inspect its limitations rather than
treating it as a healthy result.

By default the server has no model digest pin. Model controls remain disabled;
evidence and the checklist still work. For model operations, follow
[LOCAL_MODEL.md](LOCAL_MODEL.md), start the controlled local runtime and pass its
verified cached digest with `serve --model-digest SHA256`. There is no automatic
model startup, weight download or remote fallback. A configured digest alone
does not make a stopped runtime available. An unavailable runtime can produce an
unknown/error result without any actual generation request; that is failure
handling, not a model-quality measurement.

In **Saved investigation**, starting saves an initial cursor without contacting
the model. Inspect the returned ID, then explicitly resume a bounded number of
operations. One operation may be a read or a model request; it is not necessarily
one complete diagnosis. Copy the saved ID before disconnecting so you can inspect
it later. Finished unknown results remain terminal.

A **pending** record has an admitted operation whose completion was not saved.
Its confirmed counts may be lower bounds. Resume is blocked, and the interface
never automatically replays the operation. A lost HTTP response does not prove
that work was cancelled. Inspect the saved record before deciding what to do
next. Changing the displayed run or disconnecting does not undo a submitted
operation. See [CHECKPOINTS.md](CHECKPOINTS.md).

## Review and execute a fixed repair

Repair actions are selected by the operator. The model cannot approve a repair,
choose arbitrary SQL or run commands. The supported actions are:

| Action | Intended input |
|---|---|
| Rename amount column | Exact `gross_total_minor` → `amount_minor` schema change, with the same-unit assumption reviewed |
| Skip identical duplicates | Canonical valid rows containing identical repeated orders |
| Quarantine invalid rows | A canonical batch containing both valid and invalid rows |

Unsupported or compound faults are refused. See [REPAIRS.md](REPAIRS.md) for exact
preconditions and assumptions.

1. Open a failed run, choose a fixed action and create its repair plan.
2. Review the source, new derived run ID, expected row effects, warnings, plan
   SHA-256 and input fingerprint. The full saved record remains inspectable.
3. Expand **Separate approval access** and choose the same private session file
   again. This loads the approval capability separately from normal operations.
4. Acknowledge the displayed plan, then select **Approve this exact plan**.
5. Review the updated approved record and separately select
   **Execute approved repair**.

The second file selection adds an explicit interface step; it does not prove a
person's identity or establish human review. The API sees the approval bearer
capability and the exact plan/input hashes, not the chosen file or its path.
Approval records identify the trusted API caller channel.

Editing the saved repair ID clears the displayed plan and acknowledgement.
Inspect that ID before taking another action. Changing a record or reconnecting
also clears the acknowledgement. Approval cannot silently authorize an updated
plan or changed input: the server checks the exact saved plan and current source
and database fingerprints again before admission.

Execution creates a new derived run in the server's output directory. A verified
receipt reports the actual database checks and rechecked input. A committed
failure, a rollback and an uncertain outcome are displayed separately. Do not
assume an error means nothing was committed, and do not repeat a pending action.
After a rejected or uncertain mutation, explicitly inspect the saved repair.
The interface sends no automatic retry.

The output root is separate from the input root. To inspect a derived run with
the default directories, use:

```sh
PYTHONPATH=src .venv/bin/python -m pipesleuth pipeline show DERIVED_RUN_ID \
  --data-dir var/repaired
```

## Checks and delivery limits

The UI uses labels, native controls, visible focus styles and responsive CSS.
These design choices do not establish complete keyboard, screen-reader or mobile
accessibility. Actual browser observations and software checks are recorded in
the [development log](DEVELOPMENT_LOG.md); they must not be confused with model
accuracy or production recovery evidence. Screenshots must come from the actual
running interface.

Optional JavaScript controller/transport checks use Node's built-in test runner;
there are no npm packages to install:

```sh
node --test tests/ui_*.mjs
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python \
  -W error::ResourceWarning -m unittest discover -s tests -q
```

The authorized progressive build ends on **2 November 2026**. This UI stage does
not complete the planned matched three-approach evaluation, frozen synthetic
holdout or final clean-environment packaging. Those remain incomplete at the
cutoff. Earlier real local-model diagnosis failures are not resolved by adding a
browser interface. No production deployment, diagnosis accuracy or human review
is claimed.

The [2 November integration report](../reports/ui-smoke.json) separates observed
native-browser interactions, independent database reads and the exact-client
HTTP probe. Actual UI images were inspected in the tool; screenshot-file export
was blocked by that tool's filesystem/network permissions, so no screenshot
artifact is included. Native snapshots sometimes lagged, and browser response
time was not measured. The owned server and private session were cleaned up.
