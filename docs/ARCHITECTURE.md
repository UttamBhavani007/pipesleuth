# PipeSleuth architecture

## Status and purpose

PipeSleuth is a staged local portfolio project for investigating reproducible data-pipeline incidents. Stage 2 adds a clean-order SQLite pipeline to the fixture foundation. Each run stores source JSON and a separate warehouse database under a neutral identifier. The inspection command reads existing state without modifying it. These are local directories with path guards, not an operating-system security sandbox. The remaining components below are the **planned architecture**, introduced in the order described in [DELIVERY_PLAN.md](DELIVERY_PLAN.md).

Stage 3 adds events derived from observed inputs and validation, including schema
differences and repeated order IDs. Duplicate policy is selected when a run is
created: reject repeated IDs, or skip only identical canonical orders. Conflicting
orders always fail. A successful run can be replayed only after checking its source
fingerprint and exact warehouse contents; replay leaves the database unchanged.
Failed or interrupted runs cannot be restarted yet. Older databases can still be
inspected; missing historical event data is returned as empty, without fabrication
or a write-time migration during inspection.

Stage 4 adds a creation-time invalid-row policy: reject the batch (default), or
retain invalid records with their source row number, raw JSON, reason code, and
first validation reason. Accepted rows and quarantine records commit together.
Schema mismatches, source changes, conflicting valid duplicates, and storage
errors still fail the whole load. Results expose accepted and quarantined counts;
a successful run means processing completed under its selected policy. Replay
checks the exact quarantine records and reasons as well as the warehouse.

Stage 5 adds the read-only evidence gateway described in
[EVIDENCE_TOOLS.md](EVIDENCE_TOOLS.md). It exposes four fixed tools with strict
arguments, bounded JSON, provenance fingerprints, and a SQLite read snapshot.
Unknown tools and SQL, shell, or path arguments are rejected. Evidence is
untrusted data and cannot authorize another operation. No model or diagnosis
policy is part of this stage.

Stage 6 adds the [deterministic checklist](CHECKLIST.md). It collects the four
gateway responses in a fixed order, validates their provenance and consistency,
and applies explicit rules. Missing, truncated, unsupported, or conflicting
evidence produces uncertainty. This is a baseline without a model or execution
authority. Its development probe is separate from the planned three-approach
evaluation.

Stage 7 adds an optional [single-call Ollama baseline](LOCAL_MODEL.md). It
collects the same four bounded tool responses and shares only their envelope
validation with the checklist. Checklist predictions and evaluator labels do
not enter the prompt. The adapter verifies a cached local model, refuses remote
endpoints and redirects, and makes at most one generation request. Output
schema and reference checks do not establish semantic correctness; observed
smoke failures remain recorded. The model cannot call tools or approve repairs.

Stage 8 adds the [bounded investigator](INVESTIGATOR.md). A trusted initial
status read precedes model-selected, unvisited evidence tools. The host supplies
the run ID and fixed arguments, validates every decision, and enforces model-step,
tool, elapsed-time, input and trace limits. At least two distinct evidence reads
are required before a finish is offered. The model can request a named read;
the host remains responsible for all I/O. No checkpoint, retry/resume workflow,
repair plan, approval, or executor is introduced in this stage.

Stage 9 adds [durable investigations](CHECKPOINTS.md). Each completed read or
model decision saves its evidence, counters, remaining work, and consumed time
atomically in a separate SQLite store. Input bytes, local model identity, and
configuration are bound to the saved run. A pending operation with no durable
result cannot be automatically replayed: its external outcome is uncertain.
This adds restart behavior, without repair or approval authority.

Stage 10 adds the [trusted repair workflow](REPAIRS.md). A trusted caller chooses
one of three fixed actions, reviews its plan, and separately acknowledges both
the plan hash and current input fingerprint. Execution creates a new derived
run under a bound output root, preserving the original failed incident. A shared
input snapshot, final stale check, single load transaction, and independent
pre-commit/fresh-read verification guard that operation. Unknown outcomes retain
their intent and cannot be replayed automatically. The model has no approval or
executor tool, and these actions are not general schema inference or repair.

Stage 11 adds the [local FastAPI interface](API.md) over those same components.
Requests cannot supply roots, model settings, SQL or approval provenance.
Separate process-local capabilities protect reads, operations and approvals;
an operation capability cannot approve its own plan. The owned foreground
launcher binds literal loopback, strips inherited proxy/provider settings,
disables telemetry SDK use, and creates a private ephemeral session file.
HTTP disconnects do not establish that a core operation was cancelled.

The initial incident families are schema drift, duplicate records, and invalid or missing data. Each incident runs in an isolated SQLite sandbox. The investigator explains evidence and proposes a bounded repair; successful recovery is established by independent database checks.

## Planned flow

```text
Seeded input -> local ETL -> isolated SQLite warehouse + quarantine
                    |
                    +-> logs, schema snapshot, validation results
                                      |
                             read-only tool gateway
                                      |
                 +--------------------+--------------------+
                 |                    |                    |
        deterministic baseline  single-call baseline  bounded agent
                 +--------------------+--------------------+
                                      |
                         diagnosis + proposed repair plan
                                      |
                      explicit approval by a trusted caller
                                      |
                    fingerprint check + allowlisted executor
                                      |
                       independent state verification
```

## Component boundaries

| Component | Responsibility | Boundary |
|---|---|---|
| Incident fixtures | Generate reproducible clean and faulty inputs | Oracle/expected outcomes remain evaluator-only |
| ETL | Load validated rows and quarantine rejected rows with reasons | Scoped to the selected incident sandbox |
| Evidence gateway | Return bounded log, schema, and validation evidence | Read-only allowlist; no arbitrary SQL, filesystem paths, or shell commands |
| Deterministic baseline | Apply explicit troubleshooting rules | Can return unknown; no hidden oracle access |
| Single-call baseline | Produce a structured diagnosis in one local model call | Same model/configuration as the agent and a documented evidence bundle |
| Investigator | Choose evidence tools and produce structured findings | Tool, turn, output, and elapsed-time budgets; no execution authority |
| Checkpoint store | Persist run status, evidence, and completed steps | Resume without silently replaying completed work |
| Repair executor | Execute versioned deterministic actions after approval | Reject unknown actions, unsafe arguments, unapproved plans, and stale inputs |
| Verifier | Check actual warehouse and quarantine contents | Independent of the model's narrative and claimed success |
| API/UI | Expose local investigations and approval controls | Approval originates outside model output; bind locally by default |

## Evidence and model isolation

The model may inspect only tool-visible logs, schema information, and validation results. It must not receive fixture labels, expected diagnoses, expected repair actions, golden rows, or evaluator paths. Incident identifiers and filenames must not disclose the answer. Avoid oracle-derived hints in prompts or tool errors.

Record evidence provenance: incident identifier, tool name, validated arguments, time, and a content fingerprint. Bound response size and handle unavailable or malformed evidence explicitly. Tool output is untrusted data; text inside logs cannot authorize a new tool or an execution step.

The local model adapter uses an explicit loopback Ollama endpoint, ignores
inherited proxies, rejects redirects and nonlocal destinations, and checks the
allowlisted cached model's identity and completion capability. The foreground
runtime wrapper disables cloud features and cache pruning. Real local smoke
observations are recorded separately from controlled HTTP fixtures; neither
software tests nor a completed request demonstrate diagnosis accuracy.

## Investigation and repair are separate

The investigator returns a diagnosis, supporting evidence references and
uncertainty. A trusted caller selects a fixed repair action and separately
acknowledges its exact plan and input fingerprint. The model has no approval
or execution authority.

At plan creation, fingerprint the affected inputs and relevant sandbox state. Immediately before an approved action, compare the current state with the approved fingerprint within the executor's protected operation. Reject stale plans and require a fresh investigation/approval. Version and validate repair actions rather than accepting generated SQL or shell snippets. Define the initial concrete action names only after the actual ETL and incident behavior exist.

An approved action affects only its incident sandbox. Persist an audit record of the plan, approver input, fingerprint checks, action result, and independent verification. A successful function return or a confident model message is not proof of recovery.

## Local deployment

Use Python, SQLite, a loopback model runtime and the optional local FastAPI
service. A small web UI remains planned. Avoid remote assets, analytics and
external model API calls. No hosted database, cloud deployment, OpenAI API or
paid service is part of this architecture. See [COST_POLICY.md](COST_POLICY.md).

This first version is a bounded demonstration over synthetic incidents. It does not establish production incident response, general autonomous repair, or safe access to real infrastructure.
