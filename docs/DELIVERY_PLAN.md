# PipeSleuth delivery plan

Build a local data-pipeline incident investigator over **14 actual days, 20 October–2 November 2026**. Target **1–2 meaningful commits daily on 20–26 October**, then **5–6 substantial commits daily on 27 October–2 November**, conditional on the RAG project actually finishing by 26 October. The aim is a working, explainable portfolio project whose Git history reflects the work actually performed.

| Phase | Dates | Daily target | Capacity condition |
|---|---|---|---|
| Foundation | 20–26 Oct | 1–2 meaningful commits | RAG work continues alongside PipeSleuth |
| Focused build | 27 Oct–2 Nov | 5–6 substantial commits | Begin the ramp only after actual RAG completion; flag a capacity conflict if it remains unfinished |

## Delivery rules

- Deliver the next unfinished stage in each work session. Do not build later stages early to fill the calendar.
- Follow the phase targets above. Day 1 remains limited to two commits and its current scaffold/fixture scope. A commit must deliver a cohesive useful change with its relevant checks; do not split implementation from matching tests or pad history to reach a count.
- The later daily targets are plans, not guaranteed counts. Record fewer commits when justified by the actual work or a blocker; never invent changes or completion. If RAG is unfinished on 27 October, flag the capacity conflict before assuming the focused-build workload is feasible.
- Use real commit dates. If a session is missed or a stage is blocked, record the delay and continue from it. Do not backdate, fabricate progress, or silently compress unfinished work into later days.
- Each stage must pass its acceptance check before being marked complete. Record commands and observed results; distinguish a skipped check from a passed check.
- Use local execution and free software only. Follow [COST_POLICY.md](COST_POLICY.md); never add a paid or cloud fallback.
- Stage 7 is the current delivery boundary: the SQLite pipeline, bounded evidence tools, deterministic checklist, and experimental single-call local model baseline. The bounded investigator, repair engine, API, and UI belong to later stages.

The 26 October wake-up reached execution after midnight in Berlin. Stage 7
therefore continued on 27 October with real dates; no 26 October work or commits
are invented. The higher commit pace remains conditional on verified VersionLens
completion. Finish only this delayed stage on 27 October; a duplicate session
must not advance to stage 8 that day. Continue the next unfinished stage on the
next development date and keep the 2 November cutoff, reporting unfinished scope.

## Stages and acceptance checks

| Day | Date | Deliverable | Acceptance check | Suggested commit boundary |
|---|---|---|---|---|
| 1 | 20 Oct | Scope, scaffold, validated incident definitions, seeded fixtures | Validate fixture structure, deterministic generation, and incident expectations without pretending an ETL or agent exists | Scope/scaffold; fixtures and validation |
| 2 | 21 Oct | Local SQLite ETL and isolated incident databases | Run clean input through the ETL and independently inspect exact warehouse rows | Working ETL; integration validation if separate |
| 3 | 22 Oct | Reproducible schema drift, duplicates, and idempotent loading | Show schema failure and a clean control; demonstrate duplicate input; verify repeated clean loading does not duplicate warehouse rows | Schema incident and evidence; duplicate/idempotency work as an optional second meaningful commit |
| 4 | 23 Oct | Data validation and quarantine | Invalid rows are separated with reasons; verify exact warehouse and quarantine contents | Validation/quarantine; boundary checks |
| 5 | 24 Oct | Read-only evidence tools | Exercise allowlisted log, schema, and validation inspection; reject unknown tools and unsafe arguments | Evidence gateway; rejection checks |
| 6 | 25 Oct | Deterministic troubleshooting baseline | Diagnose known development cases without a model; report uncertainty for insufficient evidence | Checklist baseline and results |
| 7 | 26 Oct | Local Ollama adapter and single-call LLM baseline | Perform and record a real loopback model smoke test, structured response validation, timeout, and unavailable-model handling | Local adapter; baseline and smoke evidence |
| 8 | 27 Oct | Bounded tool-calling investigator | Investigate development cases through the same gateway within explicit tool/time limits | 5–6 cohesive increments below |
| 9 | 28 Oct | Durable checkpoints, retry, and resume | Interrupt and resume a run without losing evidence or replaying completed actions | 5–6 cohesive increments below |
| 10 | 29 Oct | Trusted approved repairs with stale-input guards | Reject unapproved or stale plans; execute an approved allowlisted action in its sandbox; independently verify exact database state | 5–6 cohesive increments below |
| 11 | 30 Oct | Local FastAPI interface | Validate requests and responses; demonstrate approval boundaries and error handling through the API | 5–6 cohesive increments below |
| 12 | 31 Oct | Small local web UI | Run the UI against the live API and inspect investigation, evidence, approval, and failure paths | 5–6 cohesive increments below |
| 13 | 1 Nov | Evaluation of all three approaches | Run development and frozen synthetic holdout cases, clean controls, negative controls, and injected failures; publish measured results and limitations | 5–6 cohesive increments below |
| 14 | 2 Nov | Reproducible portfolio packaging | Follow setup from a clean environment, capture actual screenshots, and verify documented commands; validate Docker only if included | 5–6 cohesive increments below |

Dates are the intended progression, not evidence of completion. If a required local model or acceptance check is unavailable, keep that stage incomplete and record the blocker.

## Focused-build increments

These are useful candidate commit scopes, each including its relevant validation. Adjust boundaries to the implementation; a heading is not a reason to manufacture a commit. Actual smoke runs must produce inspectable evidence and address integration findings.

- **Day 8:** structured decisions and evidence-reference validation; bounded tool loop; abstention and budget handling; real local development smoke integration; investigation CLI with trace metrics; an optional useful improvement justified by observed failures.
- **Day 9:** durable SQLite run state; atomic checkpoints; restart/resume CLI; bounded retries and fault recovery; actual interruption integration; optional usable trace export.
- **Day 10:** concrete repair plans and preconditions; approval bound to plan/input fingerprints; transactional allowlisted executors; independent verification and rollback on failure; concurrency and stale-state guards; repair CLI with an actual audit demonstration.
- **Day 11:** typed read API; investigation/resume operations; approval endpoints; loopback, origin, and session protections; concurrency and error behavior; live API integration checks and any resulting fixes.
- **Day 12:** incident case UI; evidence timeline; diagnosis and uncertainty presentation; explicit approval screen; resume/error flows and keyboard access; actual UI smoke, integration fixes, and screenshots.
- **Day 13:** frozen synthetic case manifest; matched baseline harness; actual local model evaluation; recovery verification integration; measured result artifacts; report and failure analysis.
- **Day 14:** clean setup and packaging; local deployment configuration; offline runtime wrapper; concise README and actual screenshots; optional free CI only after current terms/configuration are verified; actual release/demo evidence and resulting corrections. If free CI is not verified, omit it and retain five substantive scopes.

## Session handoff

Keep a short dated log with:

1. Stage attempted and what changed.
2. Checks actually run and their results.
3. Commit hashes, if commits were made.
4. Remaining work and the next unfinished stage.

Do not claim production readiness, learning gains, human review, measured reliability, or successful repairs without corresponding evidence.

## Work time

The local heartbeat `build-pipesleuth-daily-milestones` is active for **19:30 Europe/Berlin**, daily from **21 October through 2 November 2026**. Its saved rule covers 13 scheduled sessions and preserves the local time across the October DST change. Local execution requires the desktop app and host to be available and awake. A missed run follows the delivery rules above; it does not justify fabricated commits. Stop at the 2 November cutoff and report any unfinished scope accurately.
