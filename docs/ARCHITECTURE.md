# PipeSleuth architecture

## Status and purpose

PipeSleuth is a staged local portfolio project for investigating reproducible data-pipeline incidents. Stage 2 adds a clean-order SQLite pipeline to the fixture foundation. Each run stores source JSON and a separate warehouse database under a neutral identifier. The inspection command reads existing state without modifying it. These are local directories with path guards, not an operating-system security sandbox. The remaining components below are the **planned architecture**, introduced in the order described in [DELIVERY_PLAN.md](DELIVERY_PLAN.md).

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

The local model integration is scheduled for Stage 7. It must use an explicit loopback Ollama endpoint, disable inherited proxies and redirects, and reject nonlocal destinations. Disable any cloud mode or fallback. A working integration requires an actual local model smoke test; a mock test alone does not establish it works.

## Investigation and repair are separate

The agent returns a diagnosis, supporting evidence references, uncertainty, and an optional structured repair proposal. It cannot approve its own proposal. Approval must be supplied through a trusted programmatic or user interface and must identify the exact plan and incident.

At plan creation, fingerprint the affected inputs and relevant sandbox state. Immediately before an approved action, compare the current state with the approved fingerprint within the executor's protected operation. Reject stale plans and require a fresh investigation/approval. Version and validate repair actions rather than accepting generated SQL or shell snippets. Define the initial concrete action names only after the actual ETL and incident behavior exist.

An approved action affects only its incident sandbox. Persist an audit record of the plan, approver input, fingerprint checks, action result, and independent verification. A successful function return or a confident model message is not proof of recovery.

## Local deployment

Use Python, SQLite, and a loopback model runtime. Add FastAPI and a small local web UI in later stages; avoid remote assets, analytics, and external API calls. No hosted database, cloud deployment, OpenAI API, or other paid service is part of this architecture. See [COST_POLICY.md](COST_POLICY.md) for the distinction between service fees and local resource use.

This first version is a bounded demonstration over synthetic incidents. It does not establish production incident response, general autonomous repair, or safe access to real infrastructure.
