# Development log

## 2026-10-20 — day 1

The owner requested a data-pipeline incident investigator, then specified the
same progressive approach as VersionLens: approximately 15 days with
**1–2 meaningful commits daily**. The owner then set **2 November 2026** as
the cutoff, giving a 14-day target window of **20 October–2 November 2026**.
The existing free-only requirement applies throughout. Relevant skills and
plugins may be used where useful, within that cost boundary.
The owner subsequently requested a higher pace once the RAG project finishes:
target 1–2 commits daily through 26 October and 5–6 substantial commits daily
from 27 October through 2 November. Verify that completion before assuming
the extra capacity. Keep the public README simple and focused on the tool's
purpose and usage; scheduling details belong in this log and the delivery plan.

### First increment

Added a separate Python package, MIT license, repository instructions, status
README, architecture proposal, evaluation protocol, cost policy, and dated
delivery plan. No paid service or cloud API is configured. Day 1 is restricted
to project foundations and synthetic fixture generation.

### Second increment

Added immutable validated order records, seeded fixtures for all four incident
families, separate authoring oracles, source-only JSON export, a CLI and tests.
The normal fixture output contains only schema and source rows; oracle export
requires the explicit `--include-oracle` flag. Money uses integer minor units.
The default seed-42 clean fixture has 12 orders totaling 73,525 EUR cents.

### Verification

- `PYTHONPATH=src python3 -m unittest discover -s tests -v`: 28 checks pass on
  Python 3.13.7. A separate reviewing agent also ran the suite successfully.
- `PYTHONPATH=src python3 -m pipesleuth fixture --scenario schema_drift --seed
  42 --rows 3`: emits the three expected records with `gross_total_minor`
  replacing `amount_minor` and no labels or expected answers.
- Package metadata and version match; runtime dependency list is empty.
- The CLI checks cover deterministic replay, explicit oracle export, and
  clean rejection of invalid scenario/seed/count arguments.
- No local model is installed or downloaded by this stage. No pipeline
  execution, AI diagnosis, repair success, UI, deployment, or benchmark
  performance is claimed.

### Repository and schedule

The public GitHub repository was created as
`UttamBhavani007/pipesleuth`. Commit `e66fd9e` establishes the project; the
second day-1 commit adds the tested fixture tools. Publication uses the
owner's existing authenticated CLI and verified Git identity.
The initial public upload was blocked pending explicit approval. The owner
then approved the code/tests/docs/license payload and future scheduled public
pushes in this chat. Both day-1 commits were published; GitHub confirmed main
at `a22aa1e11a33ce9031e2a7877b998cce29a5c211` with the owner's attribution.
The earlier publication blocker is resolved.

The `build-pipesleuth-daily-milestones` local heartbeat is active for
**19:30 Europe/Berlin on 21 October–2 November 2026**. The 13 future dates were
checked using timezone-aware recurrence expansion, including the 25 October
DST transition. The laptop must be awake, online and running the desktop app.
The schedule stops at the deadline; no run while the laptop is unavailable is
claimed. See the [official scheduling guidance](https://learn.chatgpt.com/docs/automations).

The README follows the owner's requested short GitHub format: purpose, actual
features, setup and usage, tests, roadmap, and license. Detailed plans and
evaluation methods remain in docs/.

### Next stage

Day 2 implements an isolated local SQLite ETL sandbox and healthy pipeline
execution. Incident execution and the investigator remain later milestones.

## 2026-10-21 — day 2

Implemented the next planned stage only: a clean-order SQLite ETL with isolated
run directories. No model, paid service, new dependency, or external runtime
service was introduced.

### Pipeline core

- Each run has a neutral UUID, its own `source.json`, and `warehouse.sqlite3`.
  Only schema and observed rows enter the source file; authoring oracles and
  scenario labels are rejected from the input envelope.
- Run state is stored in SQLite. Starting a new run claims it atomically;
  validated orders and the successful result commit together. Invalid input
  or a failed insert leaves the warehouse empty and records a failed result.
- Source bytes are bounded and checked against their saved SHA-256 before
  loading. File and identifier guards reject traversal and symlinks; these
  local directories are not an operating-system security sandbox.
- Currency totals use exact Python integers, including when the combined
  total exceeds one SQLite signed integer. Individual stored amounts must fit
  SQLite's signed 64-bit range.

### CLI and documentation

Added `pipeline run --seed 42 --rows 12` and `pipeline show RUN_ID`, with an
optional `--data-dir PATH`. The README keeps setup and usage short and clearly
distinguishes clean loading from future fault handling. Inspection uses a
read-only connection and never creates a missing database or run.

### Verification

- `PYTHONPATH=src python3 -m unittest discover -s tests -v`: **50 tests pass**
  on Python 3.13.7. A separate reviewing agent ran the full suite and added
  independent integration checks.
- Exact three-order checks compare all five warehouse fields with literal
  expected records. Additional independent checks cover multi-currency rows,
  zero amounts, large integer totals, and oversized persisted input.
- A separate SQLite connection inspected a live seed-42, 12-order run:
  12 rows, 12 unique order IDs, and **73,525 EUR cents**. The first record also
  matched its known customer, amount, currency, and UTC timestamp.
- Failure checks cover late invalid records, duplicate-key rollback, source
  tampering, rejected paths/symlinks, concurrent starts, and repeated-run
  rejection. A repeated run is rejected rather than silently loading twice.
- The CLI was exercised through subprocesses, including read-only inspection,
  separate runs, invalid arguments, and a data directory containing spaces.

These are software and database checks, not AI diagnosis or recovery results.
Process-interruption recovery, idempotent retries, fault remediation, and the
AI investigator are not implemented in this stage.

### Next stage

Day 3 (22 October): execute reproducible schema-drift and duplicate incidents,
preserve useful failure evidence, and introduce verified idempotent loading.
Keep the target of 1–2 meaningful commits for that day.

### Commit boundaries

`03ecb24` adds the pipeline core and its direct/independent database checks.
The second day-2 commit adds the CLI, subprocess checks, and usage/status
documentation. Both changes were made on 21 October; no future stage was
implemented or committed in advance.

## 2026-10-22 — day 3

Completed reproducible schema-change and duplicate incidents, observed event
logs, and guarded successful replay. Today's work began after the owner asked
for status; no scheduled day-3 completion was assumed from yesterday's output.
No model, new dependency, paid service, quarantine, or repair executor was added.

### Incident behavior

- `pipeline run --scenario schema_drift` records a failed load with the observed
  and required schema, missing `amount_minor`, and extra `gross_total_minor`.
  Raw source remains available and no warehouse rows are loaded.
- `pipeline run --scenario duplicate_ingestion` rejects repeated order IDs by
  default. `--deduplicate` selects a creation-time `skip_identical` policy;
  it skips only equal canonical orders. A repeated ID with any conflicting
  field fails the whole load rather than selecting a record.
- New runs persist source-received, load-started, and load-result events in
  SQLite. Diagnostics come from observed inputs and validation, with no
  fixture labels, seed, expected orders, or evaluator oracle in runtime events.
- `pipeline events RUN_ID` reads a bounded chronological event list. Legacy
  day-2 databases return an empty history without inventing events or writing
  a migration during inspection.

### Replay and integrity

`pipeline rerun RUN_ID` verifies an already successful run against its source
SHA-256, all five warehouse fields, the exact row set, and its duplicate count
before returning the existing result. It does not insert orders or update the
database, events, timestamps, or clock. Changed source or warehouse data is
rejected while preserving the previous state. Failed/interrupted runs still
cannot be restarted; process recovery and approved repairs remain later work.

### Verification

- `PYTHONPATH=src python3 -W error::ResourceWarning -m unittest discover -s
  tests -q`: **71 tests pass** on Python 3.13.7 with no resource warnings.
  Direct, subprocess, and independent SQLite checks cover the changed behavior.
- Live seed-42 smoke: healthy input loads 12 unique orders totaling 73,525 EUR
  cents; schema and default duplicate failures load zero orders. The explicit
  identical-retry policy skips 3 repeated rows and loads the same 12 exact
  canonical orders and currency total as the healthy control.
- Replaying successful smoke runs returns the same summary, database bytes,
  and event list. A same-count/same-total amount swap is rejected by independent
  integration tests, as are source-byte changes and conflicting retry fields.
- A database trigger rejecting the third insert proves that already inserted
  rows roll back, while an observed failure event remains persisted.
- Older database inspection/replay, missing/extra order detection, bounded
  event reads, and failed/interrupted run rejection are covered. The README
  retains simple purpose, setup, usage, tests, and roadmap sections.

These are implementation and data-integrity results, not AI diagnosis accuracy
or autonomous recovery results. No later stage was implemented in advance.

### Next stage

Day 4 (23 October): validate invalid records, quarantine rejected rows with
reasons, and independently inspect exact warehouse and quarantine contents.
Today's two commits exhaust the 22 October target; a later duplicate wake-up
must not advance to day 4 on the same date.

### Commit boundaries

`8210a28` adds the incident events, duplicate policies, guarded replay, and
direct/independent database checks. The second day-3 commit exposes those
features through the CLI and updates usage documentation with subprocess checks.

## 2026-10-23 — day 4

Completed invalid-record validation and opt-in quarantine. Today's work began
after the owner asked for status; the latest completed stage was still day 3.
No later-stage tools, model, new dependency, paid service, or repair executor
was added. Publication continues under the owner's existing public-upload and
scheduled-push approval.

### Validation and quarantine

- The creation-time `invalid_policy` defaults to `reject`: one invalid row
  still fails the entire load. `pipeline run --scenario invalid_records
  --quarantine` explicitly selects quarantine and loads the valid rows.
- Each rejected record retains its one-based source position, original JSON
  values, stable reason code, and first validation reason. Missing fields,
  invalid currency/timestamps, invalid money types, and signed-integer overflow
  are handled without silently coercing the input.
- Accepted orders, rejected records, counts, and the successful event commit
  together. A schema mismatch, changed source, conflicting valid duplicate,
  or storage failure still fails the batch. Exact-retry deduplication can be
  combined with quarantine.
- Results expose accepted and quarantined counts separately. An all-invalid
  batch can complete with zero accepted rows and a nonzero quarantine count;
  success means processing completed under the selected policy.
- `pipeline quarantine RUN_ID --limit 100` reads retained records without
  writing. Reads accept limits of 1–1,000. Successful replay also verifies every
  quarantine row, raw JSON, reason, and count before returning the old result.
  Legacy day-2/day-3 databases remain readable without implicit migrations.

### Verification

- `PYTHONPATH=src python3 -W error::ResourceWarning -m unittest discover -s
  tests -q`: **91 tests pass** on Python 3.13.7. A separate reviewing agent
  also ran the full suite and added independent SQLite integration checks.
- Live CLI smoke with seed 42 and 12 clean orders: healthy input loads all
  12; invalid input without quarantine fails with both stores empty; explicit
  quarantine loads the same 12 exact orders totaling **73,525 EUR cents** and
  retains the four original invalid records at positions 13–16 with reasons.
  A schema-change batch still fails with both stores empty under quarantine.
- A separate read-only SQLite connection compared all five accepted fields
  against the separately held fixture oracle and inspected the rejected raw
  records directly. CLI quarantine output matched those original records.
  Successful smoke replay preserved summaries, database bytes, and events.
- Integration checks compare literal multi-currency rows and raw boolean and
  oversized-integer rejections. Same-count corruption of any quarantine field
  blocks replay without writes, including a boolean changed to an integer.
- An injected SQL failure after accepted rows and the first rejected row were
  inserted rolls both stores back. Tests also cover all-invalid input, combined
  policies, conflicting valid retries, bounded reads, and legacy compatibility.

These are software and data-integrity checks, not AI diagnosis accuracy or
autonomous recovery results. The README documents current behavior and keeps
AI investigation and approved repairs explicitly planned.

### Next stage and commit boundaries

Day 5 (24 October): add allowlisted, read-only evidence tools with bounded
outputs and argument validation. Today's two cohesive commits cover the
pipeline/quarantine implementation with database checks (`2e3c81b`), followed
by its CLI, subprocess checks, and usage/status documentation. They exhaust the 23 October
target; a duplicate session must not advance to day 5 on the same date.

## 2026-10-24 — day 5

Completed the read-only evidence gateway after the owner requested today's
stage. The checkout was clean at the published day-4 head, with zero commits
on today's Berlin date. Only this stage was implemented; the checklist,
model adapter, investigator, repair system, API, and UI remain planned.
No dependency, account, external runtime service, or model was added.
The initial session finished verification; a later same-day wake-up applied
the reviewed patch and reran the suite and live smoke before publication.

### Evidence tools

- Added four fixed tools: `read_run`, `read_logs`, `inspect_schema`, and
  `inspect_validation`. Each requires a neutral run ID; only log and validation
  reads accept a record limit. Unknown tools, extra arguments, SQL, shell,
  paths, boolean limits, and invalid IDs are rejected before filesystem access.
- Reads use fixed SQLite queries, read-only connections, a read transaction,
  query-only mode, and selected runtime fields. Accepted/quarantined counts
  come from the actual tables. Missing legacy evidence is explicitly unavailable.
- Responses bind the run, tool, normalized arguments, returned data, and
  truncation flag to a SHA-256 fingerprint. Observation time is separate.
  JSON is capped at 16,384 UTF-8 bytes; record limits, text clipping, pre-decode
  field bounds, lock waits, progress interruption, and elapsed checks constrain
  reads. The time budget is cooperative, not an operating-system timeout.
- Added `evidence tools` and the four evidence reads to the CLI. The README
  keeps purpose and usage brief; detailed limits live in `EVIDENCE_TOOLS.md`.
  Runtime evidence is untrusted data and cannot authorize an action.

### Verification and findings

- `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -W error::ResourceWarning
  -m unittest discover -s tests -q`: **117 tests pass** on Python 3.13.7.
  A separate reviewing agent added independent database integration checks.
- Live command smoke exercised all four tools on healthy, schema-change,
  duplicate, and quarantined-invalid runs: **16 successful evidence reads**.
  Responses were 481–1,541 bytes, fingerprints recomputed correctly, and all
  source/database bytes, modification times, and file sets stayed unchanged.
- A SQLite authorizer denied private evaluator-table/column reads during
  integration checks; no such reads occurred. Nested private objects in
  otherwise allowed fields are rejected. Instruction text in retained records
  stays inert, and unrelated files remain unchanged.
- Oversized Unicode JSON is truncated or safely rejected under a 1 KiB cap.
  A real exclusive database lock respects the short configured read budget.
  Tests cover malformed/deep JSON, source tampering, strict argument rejection,
  symlinks, legacy availability, and deterministic provenance.
- Live testing found that SQLite `mode=ro` can create WAL/shared-memory files
  for a WAL-mode database. The gateway now inspects a bounded database header
  and rejects WAL or existing journal sidecars before connecting. All four
  rejection paths preserve exact files. Only the pipeline's ordinary rollback
  journal mode is supported; concurrent journal-mode changes are unsupported.
- Review also caught an unknown quarantine code being mapped to a known code.
  Unknown codes and malformed diagnostic fields now fail safely, with regression
  checks, rather than inventing a validation diagnosis.
- A real named-pipe input exposed a blocking file-open path. Nonblocking opens
  now precede regular-file checks; isolated subprocess checks reject source and
  database FIFOs promptly without changing the files.

These are software and evidence-boundary checks, not AI accuracy or repair
results. No later stage was implemented early.

### Next stage and commit boundaries

Day 6 (25 October): implement the deterministic troubleshooting checklist with
uncertainty for insufficient evidence. Today's two cohesive commits cover the
gateway with direct/independent checks, followed by its CLI, subprocess checks,
and documentation. The gateway commit is `3c08908`. They exhaust the 24 October
target; a duplicate run must not advance to day 6 on this date. Public pushes
use the existing approval.

## 2026-10-25 — day 6

Completed the deterministic troubleshooting baseline from the published day-5
head, with a clean checkout and zero commits on today's Berlin date. No model,
new dependency, paid service, agent loop, repair, API, or UI was introduced.
Repository writes and temporary test runs used scoped sandbox escalation; the
existing public-push approval remains in effect.

### Checklist behavior

- `diagnose RUN_ID` calls the existing run, log, schema, and validation tools
  once each in a fixed order. It receives no fixture labels, evaluator data,
  raw file access, arbitrary SQL, or execution capability.
- Supported schema, duplicate, invalid-row, and integer-overflow observations
  produce static explanations, validated facts, and actual evidence references.
  Quarantine findings describe retained rejections; they do not execute a repair.
  Successful identical-retry deduplication is a handled control with a note.
- Missing, malformed, truncated, unsupported, unfinished, or conflicting
  evidence produces `unknown` without confident findings. Invalid IDs fail
  before tool access. The compact result contains the bounded evidence bundle,
  real call count, and measured elapsed time; its limit is 80 KiB.
- The baseline recomputes provenance hashes and checks strict tool shapes,
  normalized arguments, source/schema observations, actual counts, lifecycle
  events, failure details, and chronological timestamps. Hashes bind content;
  they do not authenticate its truth. Separate tool snapshots are not atomic.
- CLI exit codes distinguish supported conclusions (0), uncertainty (1), and
  invalid arguments (2). README usage stays short; `CHECKLIST.md` records rules,
  limits, development evidence, and what remains unimplemented.

### Verification and development probe

- `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -W error::ResourceWarning
  -m unittest discover -s tests -q`: **140 tests pass** on Python 3.13.7.
  A separate reviewing agent also ran the suite and added independent checks.
- Real subprocess checks cover clean runs, handled retries, supported failed
  and quarantined runs, missing evidence, invalid arguments, actual evidence
  references, and unchanged source/database bytes and modification times.
- Independent tests deny private evaluator-table/column reads and keep
  instruction text inert. Even correctly rehashed but conflicting affected-row,
  schema, or count claims produce `unknown`. Additional direct checks reject
  impossible totals, first-row duplicates, untrimmed IDs, and reversed clocks.
- `scripts/checklist_development.py` executed ten AI-authored, source-checked
  synthetic development cases. All **10/10** matched their declared expected
  behaviors: **4 diagnosed, 2 no_incident, 4 unknown**, with **40 actual tool
  calls**, zero exceptions, ten unchanged file snapshots, and ten matching,
  unchanged independent database counts. Actual probe time was about 0.060 s.
- `reports/checklist-development.json` records inputs' expected behaviors,
  observed bounded evidence, per-case timing/errors, and denominator counts.
  Its checklist, gateway, and probe source hashes match the final files.
  Documentation links and the unchanged empty runtime dependency list pass.

The development cases were used during implementation. These results are not
held-out accuracy, human review, local-model results, agent evaluation, or
verified recovery. The full comparison remains scheduled for stage 13.

### Next stage and commit boundaries

Day 7 (26 October): add the loopback-only local Ollama adapter and single-call
baseline, with real model smoke evidence and timeout/unavailable handling.
Verify licenses and capabilities before any weights download. Today's two
cohesive commits cover the checklist with direct/independent checks, followed
by the CLI, development probe/results, and documentation. The checklist commit
is `9e860d8`. They exhaust the
25 October target; a duplicate run must not implement day 7 on this date.

## 2026-10-27 — stage 7, delayed from 26 October

The 26 October wake-up reached execution after midnight in Berlin. This stage
was implemented and verified on 27 October, with an interruption before the
owner requested continuation. No 26 October commits or completed work are
invented. Work resumed from the published stage-6 head and saved adapter files;
there were zero commits on today's Berlin date before this delivery.

### Local adapter and baseline

- Added a standard-library Ollama client for one explicitly cached allowlisted
  local model. Literal loopback endpoints, fixed routes, no proxy inheritance,
  no redirects, bounded bodies, and one shared request deadline constrain access.
  Inventory, digest, architecture, cloud markers, completion, UTF-8, and usage
  metadata are checked. There is no automatic pull, account, key, paid endpoint,
  retry, or cloud fallback.
- The foreground runtime wrapper disables cloud features, inherited provider
  settings, request-body debug logging, and cache pruning. It uses an 8192-token
  context and one parallel request. Existing verified local runtime/model
  artifacts were reused read-only; nothing was downloaded or added to Git.
- `diagnose RUN_ID --method local` collects the same four bounded evidence
  responses, sharing only envelope validation with the checklist. Checklist
  predictions and evaluator labels never enter the prompt. At most one actual
  generation request follows; incomplete evidence or input-budget rejection
  causes zero model requests.
- The response contract validates JSON fields, statuses, code allowlists, and
  references to existing evidence. Malformed output becomes `unknown`, preserving
  bounded raw output and real metrics. Reference existence is not semantic
  support: `semantic_support_verified` remains false. The model cannot invoke
  tools, write data, approve a plan, or execute a repair.

### Verification and actual observations

- `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -W error::ResourceWarning
  -m unittest discover -s tests -q`: **177 tests pass** on Python 3.13.7.
  Independent checks cover real loopback HTTP transport, slow-header/body
  deadlines, redirects/proxies, private-data isolation, unchanged files,
  strict model output, unavailable evidence, and single-generation limits.
- The new positive CLI fixture found an HTTP/1.0 completion bug: reading again
  after consuming the declared body could touch a closed socket. The adapter
  now stops at the complete body length; the regression and full suite pass.
- Four actual local generations are retained across two smoke attempts with
  unchanged model, prompt, and synthetic fixtures. The initial attempt's
  recorded client hash already matches the final source, but its execution
  overlapped the transport edit, so a fresh-process attempt was recorded rather
  than assuming which in-memory code version had been loaded.
- Initial attempt: a healthy control falsely produced three invalid-row claims
  with invented IDs; the schema failure produced a schema-mismatch finding.
  Literal declared expectations matched **1/2** cases.
- Fresh attempt: the healthy control falsely produced invalid-row, duplicate,
  and schema claims. The schema case returned `unknown` with findings, violating
  the output contract, and was rejected as `invalid_generation`. Literal declared
  expectations matched **0/2** cases. Both raw responses and failures remain in
  the reports; no prompt or model tuning followed these observations.
- Both attempts completed two real model responses without transport exceptions
  and preserved all run files and cached-model size/modification-time metadata.
  Actual Ollama missing-model HTTP 404 checks and separately labelled controlled
  missing-inventory/timeout checks passed. Software fixtures are not model runs.
- Runtime v0.40.7 served digest
  `8f68893c685c3ddff2aa3fffce2aa60a30bb2da65ca488b61fff134a4d1730e7`.
  Cloud-off, loopback, no-prune, debug-off, and effective 8192 context settings
  were observed. The served license was verified separately from publisher
  descriptions. Runtime-reported parameter/context metadata differences are
  recorded without guessing model identity from a mutable tag.
- The fresh report's adapter, baseline, gateway, and smoke source hashes match
  the final files. The owned foreground server was stopped and port 11436 was
  confirmed closed. No VersionLens source, schedule, model cache, or service
  configuration was changed.

The local transport and experimental single-call baseline are implemented;
diagnosis quality is not established and the observed healthy false positives
are a material limitation. These four calls are development smoke observations,
not an accuracy benchmark, human review, or verified recovery result.

### Capacity conflict and next stage

Read-only inspection still found VersionLens's recorded question/answer reviews
pending and no final completion entry. Its higher-capacity assumption is not
treated as satisfied, and only two cohesive stage-7 commits are made today.
The first contains the adapter/baseline and their direct/independent checks;
its commit is `d34b004`. The second contains CLI/runtime integration, actual
smoke reports, and docs.

Stage 8, the bounded investigator, remains the next unfinished stage for the
next development date. Do not advance it during another 27 October wake-up or
pad today's count. Recheck actual VersionLens completion before ramping. Keep
the 2 November cutoff; report any scope still incomplete at that date rather
than extending the schedule or claiming an unrun stage.

## 2026-10-28 — stage 8, bounded investigator

The 27 October scheduled wake-up reached execution after midnight in Berlin.
Stage 7 was already published, so only the next unfinished stage was built on
the actual 28 October date. The checkout was clean at `d38f951` and no commits
existed on today's Berlin date when this stage began.

Read-only inspection verified VersionLens's completed prototype software build
and release at `2c69be6`. Its human question/answer reviews and reviewed holdout
remain unfinished external requirements, not completed evaluation. With the
software-build workload finished, the focused-build pace is now enabled.

### Implemented boundaries

- Evidence and model calls now accept a trusted per-call timeout that can only
  narrow their configured budget. Model health and generation share the budget;
  SQLite lock waits also use the remaining time. This keyword is not a model
  tool argument and does not change existing default calls or model settings.
- A pure, closed decision protocol offers only unvisited evidence tools or a
  structured finish. Unknown actions, extra arguments, run IDs, SQL, paths,
  approvals, repeated tools, duplicate JSON keys, and fabricated references are
  rejected before any corresponding operation. References prove existence only.
- The investigator first performs a trusted `read_run`, then lets the model
  select additional allowlisted evidence. The host supplies the original UUID
  and limit 20 for log/validation reads. Two distinct reads are required before
  a finish is offered. Checklist predictions and evaluator labels are absent.
- Defaults are four model steps, four total tool reads including the initial
  read, and 60 seconds. Hard configuration ceilings are eight steps, four reads,
  and 120 seconds. Remaining time is passed through to every component, with
  deadline checks after tool/decision validation and before final conclusions.
- Input is bounded for the unchanged 8192-token context/768-token output setup;
  output is capped at 128 KiB with explicit trace truncation. Actual model
  request attempts, tool attempts, raw responses, usage, elapsed time, evidence
  references, and failure codes remain inspectable. No checkpoint, retry/resume,
  repair, approval, API, or UI is introduced by this stage.
- `diagnose RUN_ID --method agent` exposes the investigator with explicit step,
  tool, and whole-investigation budget flags. The default checklist and optional
  single-call method remain available.

### Verification and actual smoke

- `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -W error::ResourceWarning
  -m unittest discover -s tests -q`: **213 tests pass** on Python 3.13.7.
  Independent integration checks and real CLI/HTTP fixtures cover the new work.
- Five new shared-deadline checks include slow HTTP headers/body and a real
  exclusive SQLite lock. Ten protocol checks cover closed decisions. Eleven
  direct loop checks cover adaptive reads, counts, limits, errors, truncation,
  and expiry during finish validation. Five independent checks deny private
  evaluator reads, preserve exact files, and reject injected/cross-run actions.
  Five CLI checks exercise actual subprocesses and controlled loopback fixtures.
- `scripts/smoke_investigator.py` ran the healthy and renamed-column development
  cases once each. There were **four real local generations** and **two admitted
  model-selected `read_logs` calls**, in addition to the two trusted initial
  status reads. Actual argument and request counters matched the trace.
- **Neither case produced an accepted finish.** Both second responses contained
  `status: unknown` with nonempty findings. The parser rejected these inconsistent
  decisions and returned `unknown`/`invalid_decision` with no accepted findings.
  Raw healthy-input text also invented fault claims. Literal declared development
  expectations matched **0/2** cases. All responses remain in the report; there
  was no retry, prompt tuning, model substitution, or discarded failed case.
- The report records zero case/driver exceptions, unchanged exact warehouse and
  quarantine contents, unchanged run-file bytes/mtimes, and unchanged model-cache
  size/mtime metadata. Local generation used the same cached Qwen digest,
  temperature 0, thinking disabled, and context/output settings as stage 7.
  Runtime logs confirmed loopback, cloud-off, debug-off, no-prune, and effective
  context 8192. The owned foreground server was stopped; port 11436 was closed.
- Recorded implementation hashes match the final files. Documentation links,
  the unchanged empty runtime dependency list, and actual-date commit counts
  were checked. No model weights, database, credential, or unrelated files are
  included, and VersionLens was inspected without edits.

This stage establishes bounded integration and strict rejection of unsupported
model output. It does not establish a correct diagnosis, improved accuracy,
human review, semantic support, production safety, or successful recovery.
The model remains experimental and its failures are material limitations.

### Commit boundaries and next stage

Five substantive increments deliver only stage 8:

1. `58fdeca`: shared per-call deadlines and real lock/HTTP checks.
2. `4eff984`: the closed decision protocol and its tests.
3. `debffad`: the bounded loop with direct and independent checks.
4. `413f794`: CLI budgets/trace integration with subprocess checks.
5. The actual smoke driver/results, user guide, and this evidence/log update.

Stage 9, durable checkpoints and resume, is the next unfinished stage for the
next actual development date. Do not advance it during another 28 October
wake-up or add commits to pad the count. The 2 November cutoff is unchanged;
report any remaining scope honestly rather than extending the schedule.
