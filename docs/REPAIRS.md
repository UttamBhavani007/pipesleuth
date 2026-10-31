# Approved repairs

Repairs create a new derived pipeline run. The original incident source and
warehouse remain read-only. A trusted operator selects an allowlisted action;
a model cannot approve it or supply SQL, commands, paths or replacement code.

## Available actions

| Action | Intended input | Effect |
|---|---|---|
| `rename_amount_column` | The exact supported schema with `gross_total_minor` | Rename that field to `amount_minor`, then validate and load. |
| `skip_identical_duplicates` | Repeated identical orders | Keep one copy; conflicting values still fail. |
| `quarantine_invalid_rows` | Invalid order records | Keep valid orders and preserve rejected rows with reasons. |

The column rename is an operator assumption about this synthetic input contract.
A matching column name alone does not establish equivalent business meaning.
These actions are fixed application operations, not general data cleaning.

## Usage

List supported actions, create a plan, inspect it, then explicitly acknowledge
its displayed hashes before execution:

```sh
PYTHONPATH=src python3 -m pipesleuth repair actions
PYTHONPATH=src python3 -m pipesleuth repair plan RUN_ID --action ACTION
PYTHONPATH=src python3 -m pipesleuth repair show REPAIR_ID
PYTHONPATH=src python3 -m pipesleuth repair approve REPAIR_ID \
  --plan-sha256 PLAN_HASH --input-fingerprint-sha256 INPUT_HASH
PYTHONPATH=src python3 -m pipesleuth repair execute REPAIR_ID
```

`plan` accepts `--data-dir`, `--output-dir` and `--state-dir`. The later commands
accept `--state-dir` and restore the bound original/output settings. Inspect the
plan's action, paths, proposed effects and warnings before acknowledging it.
Use the printed `investigation_id` value as `REPAIR_ID`. The default roots are
`var/sandboxes`, `var/repaired` and `var/repairs`. Combined faults and conflicting
valid duplicates are outside these individual repair actions.

## Approval and execution

A plan binds its action, original run, exact source and raw database bytes,
separate output/state roots and derived run ID. Approval must name that exact
plan hash and input fingerprint. Changed inputs, a different plan or missing
approval prevent execution. The trusted CLI approval represents the local
operator; `scripted_test` is reserved for clearly labelled software fixtures.
Neither label proves an external human review occurred.

Execution records a durable pending intent before creating the derived run.
Trusted code loads it in one transaction and checks actual order/quarantine
contents independently before commit, then checks again through a fresh
connection. The original input is checked again after the executor returns. A
change observed after commit is a failed result with committed output retained;
it is not reported as rollback. A precommit failure rolls back; an unresolved
pending operation refuses automatic replay. A lost commit acknowledgement also
retains an uncertain intent rather than asserting success or rollback. Preserve
uncertain artifacts instead of clearing a marker or claiming exactly-once recovery.

The local path and input guards protect a trusted workspace. They do not
isolate the application from an owner who can rewrite files or bypass its API.
The original incident is not overwritten, and a successful derived run does
not show that the model diagnosed the incident correctly.

## Verification

The repair smoke uses literal synthetic sources, fixed expected database rows
and explicitly scripted approvals. It records refusal paths, original and
unrelated file preservation, and exact derived warehouse/quarantine contents.
It performs no model inference and is not a model-quality benchmark or a
production-safety claim.

Run a fresh deterministic audit with:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 scripts/smoke_repair.py \
  --output reports/local/repair-smoke.json
```

Existing report paths are refused so an earlier outcome is not overwritten.

## Observed on 31 October 2026

The [saved repair audit](../reports/repair-smoke.json) records one run against
frozen source: **3/3 derived runs** matched the exact literal warehouse,
quarantine and run-metadata expectations. Each contains three valid orders.
The duplicate case skipped two identical repeats; the quarantine case preserved
one rejected negative-amount row with its exact reason and raw record.

All **10 refusal checks** passed: unapproved execution, a mismatched plan hash
and repeated execution for each action, plus a separate source-whitespace
change after approval. Those refusals preserved the saved record and output
files. Original incident database contents, original/unrelated file bytes and
mtimes, and source hashes remained unchanged during successful repairs.

The report has zero case/driver errors and **zero model requests**. Every
approval is explicitly `scripted_test`; this demonstrates deterministic repair
integration, not human approval, model diagnosis accuracy or production safety.
The original failed incidents remain separate from their successful derived
runs. Fault-injection rollback and uncertain-save cases are software tests,
not claims that the audit exercised every possible crash point.
