# PipeSleuth working instructions

Build this project progressively through **2026-11-02**, starting 2026-10-20
(14 planned days). The owner requested **1–2 meaningful commits per development
day through 26 October**, increasing to **5–6 substantial commits daily from
27 October through 2 November**, after the RAG project's planned completion.
Follow `docs/DELIVERY_PLAN.md` and the latest entry in
`docs/DEVELOPMENT_LOG.md`. Finish the next unfinished stage, verify it, and
record actual evidence. Do not implement all remaining stages at once.
Do not continue the schedule beyond 2 November without a new user request.

- Inspect Git status/history before work. Preserve user changes and published
  history. Never force-push, backdate commits, or pad the commit count.
- Treat commit counts as targets for useful verified work, not a reason to
  create empty changes or split one change into fragments. Confirm the RAG
  project has actually finished before assuming the additional capacity.
- Keep work in this repository. The parent `sources/` is read-only reference
  material. Do not modify the separate VersionLens project or its schedule.
- Every project component must be free: no paid API, cloud inference fallback,
  rented compute, hosted database, card-backed trial, subscription, billing
  account, or secret provisioning. Read `docs/COST_POLICY.md` before adding a
  dependency or external service. Local model weights are not source files.
- Use synthetic records only. Keep scenario labels and evaluation oracles out
  of model-visible evidence, prompts, filenames, and tool responses.
- A model must never execute arbitrary SQL, shell commands, or file paths.
  Repairs require an explicit approval for the exact plan and current input
  fingerprint; verify database contents independently after execution.
- Do not call a checklist an AI agent, report a mock as a real model run,
  fabricate evaluation results, or call a personal demo production experience.
- Current source-only checks use Python 3.11+ and the standard library:
  `PYTHONPATH=src python3 -m unittest discover -s tests -v`.
- Keep the README conventional and short: purpose/problem, current features,
  setup and usage, tests, brief roadmap, and license. Put scheduling, research
  design and internal build details in docs/ rather than the README.
- Capture screenshots only once the actual application runs. Record untested
  integrations and missing dependencies plainly. Fixtures and a clean SQLite
  pipeline are available; the AI investigation system remains a later stage.
