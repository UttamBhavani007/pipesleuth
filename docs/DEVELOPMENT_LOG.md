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
