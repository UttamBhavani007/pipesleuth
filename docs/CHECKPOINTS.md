# Saved investigations

Investigations can save each completed operation in a separate local SQLite
store, then continue in another process. The pipeline source and warehouse are
still read-only to the investigator. This stage adds persistence, not repair
approval or execution.

## Workflow

With the explicitly controlled [local model runtime](LOCAL_MODEL.md) available:

```sh
PYTHONPATH=src python3 -m pipesleuth investigation start RUN_ID \
  --model-digest SHA256 --max-operations 2
PYTHONPATH=src python3 -m pipesleuth investigation show INVESTIGATION_ID
PYTHONPATH=src python3 -m pipesleuth investigation resume INVESTIGATION_ID
```

Replace `SHA256` with the exact cached model digest. `start` creates a neutral
investigation ID; its normal CLI path runs operations unless explicitly paused.
`--max-operations` pauses only between completed operations, with a limit of
1–16 operations per invocation. Without it, resume continues within the saved
model, tool, input and elapsed budgets. Use `--data-dir PATH` on `start` for the
trusted pipeline root. Resume restores the original saved path and settings;
it does not accept replacement pipeline/model configuration. Use `--state-dir
PATH` for separate local checkpoint storage on all three commands.
The defaults keep them in `var/sandboxes` and `var/investigations` respectively.

`show` reads the saved record without creating a missing store or calling the
model. Re-reading or resuming an already terminal investigation returns the
saved result without generating it again. A stored `unknown` remains uncertain;
saved output does not establish diagnosis accuracy or semantic support.

## Durable operations and binding

Before an operation, a compare-and-swap claim records a pending intent and
token in SQLite. Only its owner can atomically save the completed engine state
and next-operation cursor while clearing that intent. A competing or stale
caller cannot perform the same operation through another claim.

The saved binding fixes the pipeline run, exact source and warehouse contents,
model digest, configuration and state policy. Changed inputs or settings are
rejected before more work. Evidence, model decisions, counters and raw traces
that reached a completed checkpoint are retained. The store uses bounded JSON
and an integrity checksum to detect accidental corruption. That checksum is
not authentication against an owner who can rewrite local files.

The warehouse fingerprint binds exact raw database bytes, conservatively. An
equivalent rewrite such as `VACUUM` can invalidate an investigation even when
its rows and totals are unchanged. Do not bypass that refusal based on counts.

There is no claim of exactly-once external inference. A process can disappear
after a model request was sent but before its result was committed. A durable
pending operation therefore **always refuses automatic resume**. An unknown
request outcome cannot be inferred from a timeout, absent output or an expired
process. Confirmed counters are lower bounds when such an outcome is unresolved.
Preserve the old investigation for inspection; use a new investigation rather
than clearing the pending marker or blindly repeating the request.

## Storage and failure limits

The checkpoint directory must remain separate from pipeline files. Path and
symlink guards protect a trusted local workspace; they are not operating-system
isolation. Unknown database formats, malformed/checksum-invalid state, WAL or
journal sidecars, and hot-journal interruption artifacts are refused. Inspection
does not migrate, recover, repair or delete such artifacts implicitly.

A fresh checkpoint directory may be needed after a store-level interruption.
Preserve the old files; do not delete journals or claim that uncertain work was
recovered. Power-loss recovery and arbitrary external concurrent modification
are not established by a clean process-restart smoke.

The saved global retry limit is 0–1. It applies only to a failed read without a
completed evidence envelope or a model-health failure before any generation
request was attempted. Attempted generations, invalid model output and uncertain
pending requests are never eligible. Model, tool, step and active elapsed limits
remain cumulative across saved operations; restarting is not a new unlimited run.

Active elapsed time excludes the user's downtime between resume invocations.
Continuing checkpoint-write overhead is charged to later work where measurable.
The final SQLite commit cannot record its own completed duration in that commit;
therefore this is not a strict end-to-end wall-clock guarantee including every
final storage write. The store also has its own bounded SQLite wait budget.

## Interruption smoke

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 scripts/smoke_resume.py \
  --output reports/local/resume-smoke.json
```

The driver starts owned child processes. For the real local-model case, it waits
for a completed model-selection checkpoint, reloads it after commit, writes a
marker and blocks. The parent sends SIGTERM only to that owned child. A fresh
process resumes from the saved cursor; another fresh inspection proves that
terminal output makes no further calls. If the model terminates before choosing
evidence, the report records that limitation instead of inventing the checkpoint.

A separate deterministic model fixture blocks after a durable pending claim.
After that owned child is stopped, fresh resume must refuse it without any more
tool or model requests. This fixture performs no real model inference. The
report separates it from the real local-model case and records exact unchanged
pipeline files/database contents, request counters, source hashes and raw results.
Existing output paths are refused so earlier failures are retained.

These are local persistence observations, not a model-quality benchmark, human
review, successful repair, or proof of recovery from every crash point.

## Observed on 30 October 2026

The [saved smoke report](../reports/resume-smoke.json) records one actual local
restart attempt with the same pinned Qwen model and settings as the earlier
investigator smoke. It made two generation requests in total, without tuning
or rerunning to select a better result.

After the model selected `read_logs`, the worker saved revision 5 with that
next-tool cursor and no pending operation. SIGTERM stopped the owned worker.
A fresh process read `read_logs` first, preserving the evidence and trace prefix
without repeating `read_run` or the saved model selection. Its next model answer
failed protocol validation (`invalid_decision`), so revision 9 saved a terminal
`unknown` result. This shows continuation of the saved decision; it does not
establish a correct diagnosis.

A further fresh terminal inspection made zero tool or generation calls and
preserved the store bytes. The separate deterministic in-flight fixture also
refused resume with zero calls after its pending claim was interrupted. All
recorded restart checks passed; exact pipeline files/database state, cache
metadata and source hashes were unchanged during the run. The owned model
runtime was stopped afterwards, and its loopback port was confirmed closed.
