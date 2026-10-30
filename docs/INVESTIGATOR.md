# Bounded local investigator

The investigator lets the local model choose which read-only evidence to request
before returning a structured conclusion. It is an experimental comparison with
the deterministic checklist and the single-call baseline. It cannot execute a
repair, approve a plan, run SQL or a shell, or read arbitrary files.

With the explicitly controlled [foreground runtime](LOCAL_MODEL.md) running:

```sh
PYTHONPATH=src python3 -m pipesleuth diagnose RUN_ID --method agent
```

`--max-steps` accepts 1–8 model steps, `--max-tool-calls` accepts 1–4 total reads,
and `--agent-timeout` accepts a finite positive budget up to 120 seconds. The
default limits below are conservative. A budget too small to read a second tool
and finish returns uncertainty. The endpoint, model timeout and optional digest
pin use the same trusted settings as the [local baseline](LOCAL_MODEL.md).

## Host-controlled decisions

The host first reads `read_run`. On each model step, the only decisions are:

```json
{"action":"tool","tool":"inspect_schema"}
```

or, after at least two distinct evidence tools have been read:

```json
{"action":"finish","diagnosis":{"status":"unknown","findings":[]}}
```

Available tools are unvisited members of `read_logs`, `inspect_schema`, and
`inspect_validation`. The host supplies the original run ID and a fixed limit
of 20 for log/validation reads. Model-supplied arguments, other run IDs, paths,
SQL, approvals, unknown tools, repeated reads, and malformed finishes are denied.

The model receives only returned evidence, available tool descriptions and
allowed actions. Case labels and evaluator expectations stay outside its input.
Text in records and logs remains untrusted data and cannot authorize an action.

## Budgets and results

Defaults are four model steps, four total tool calls including the initial run
read, and 60 seconds across the operation. The host passes the remaining elapsed
budget to each trusted tool/model call. These controls use the existing local
SQLite and loopback HTTP deadlines; they are not an operating-system sandbox.

Input is bounded for the same 8,192-token model context and 768-token output cap
used by the single-call baseline. Model, digest, temperature and thinking settings
remain unchanged. Each step adds model overhead, so a tool loop is not assumed to
be faster or more accurate. An exhausted time, step, tool, input or result budget
returns `unknown` with a limitation instead of inventing a completed diagnosis.

The compact result is capped at 128 KiB. Its bounded trace records admitted tool
choices, generation requests, usage, raw model text, timing, failures and evidence
identifiers. A truncated trace is marked explicitly. The `diagnose` command
remains an unsaved run. The separate [checkpoint commands](CHECKPOINTS.md)
persist the same investigator between operations. Repairs remain a later stage.

`diagnosed`, `no_incident`, and `unknown` have the same meanings as the
[single-call baseline](LOCAL_MODEL.md). JSON/schema and reference checks only
establish a permitted shape and existing references. They do not prove that a
claim is semantically supported. `semantic_support_verified` remains false.

## Actual local smoke

Start the explicitly controlled foreground runtime using the
[local-model setup](LOCAL_MODEL.md), then run:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 scripts/smoke_investigator.py \
  --output reports/local/investigator-smoke.json
```

The driver uses healthy and renamed-column synthetic inputs matching the prior
smoke fixtures. It preserves every recorded case, error, budget stop and raw
trace, checks exact database state and files before/after, and records source
hashes. Existing output paths are rejected to preserve prior outcomes.

The published report is a small development integration observation, not a
frozen holdout, human review, diagnostic accuracy estimate, or recovery result.
The local model's prior [single-call false positives](LOCAL_MODEL.md#recorded-observations-and-limitations)
remain an observed limitation; introducing a loop does not establish that they
are solved. Actual agent outcomes must be read from the recorded report.

## Recorded 28 October observations

The [actual local smoke](../reports/investigator-smoke.json) made **four real
generation requests** across two cases. In both cases, the model chose
`read_logs`; the host performed that read with the original UUID and limit 20.
Together with the initial trusted `read_run`, this demonstrates actual adaptive
tool use rather than a precomputed checklist result. The runtime returned model
responses for the JSON Schema `anyOf` requests; the local parser independently
enforced the permitted decisions.

**Neither case produced an accepted finish.** Both second model responses used
`status: unknown` with nonempty findings, an inconsistent payload. The host
rejected them and returned `unknown` with `invalid_decision` and no accepted
findings. The healthy-input raw response also claimed unsupported schema/row
faults. The raw responses and traces remain in the report; no retry, prompt
tuning or model substitution followed these results.

| Input | Model-chosen read after `read_run` | Accepted finish | Exposed result |
|---|---|---|---|
| Healthy | `read_logs` | No | `unknown`, `invalid_decision` |
| Renamed amount column | `read_logs` | No | `unknown`, `invalid_decision` |

There were zero driver/case exceptions, and both recorded cases preserved exact
database contents, run-file bytes/mtimes and cache size/mtime metadata. Counter
and fixed-argument checks passed; implementation hashes stayed unchanged during
the run. No time, step or tool limit was exhausted in these two actual cases.
Separate software tests exercise budget failures and hostile decisions.

The model and requested generation settings were unchanged from Stage 7:
`qwen3:1.7b`, digest
`8f68893c685c3ddff2aa3fffce2aa60a30bb2da65ca488b61fff134a4d1730e7`,
8,192-token context, 768-token output cap, temperature 0 and thinking disabled.
Startup logs confirmed loopback binding, cloud disabling, no request-body debug
logging, no cache pruning and an effective 8,192-token context. The owned
foreground server was stopped afterward; port 11436 then refused connections.

These observations verify bounded integration and fail-closed behavior, not a
correct diagnosis or improvement over the single-call method. The two literal
development expectations were not met. The investigator remains experimental;
semantic support, general reliability and recovery are unvalidated.
