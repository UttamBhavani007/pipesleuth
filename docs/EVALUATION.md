# PipeSleuth evaluation plan

**Status: planned. No evaluation results or human review are claimed.** Introduce the harness as the implementation progresses and run the full comparison in Stage 13.

## Question

For a bounded set of synthetic ETL incidents, does adaptive local tool use improve correct diagnosis and verified recovery compared with a deterministic checklist and a single local LLM call? Measure both benefits and failure costs.

## Three approaches

| Approach | Information and behavior |
|---|---|
| Deterministic checklist | Explicit rules over a documented read-only evidence bundle; may return unknown |
| Single-call LLM | One local generation over the same complete evidence bundle; structured diagnosis and optional repair proposal |
| Tool-calling agent | Same local model, generation configuration, output schema, and tool-visible information; adaptively requests evidence through the allowlisted gateway within fixed budgets |

Construct the complete evidence bundle with the same deterministic tool outputs available to the agent. Document truncation and ordering. The one-call method receives that bundle up front; the agent may request a subset. This compares complete upfront evidence with adaptive acquisition and is **not a perfectly isolated test of reasoning quality**. Report that information-delivery difference, tool overhead, and model-call counts. Hold incident inputs, repair executor, approval policy, and independent verifier constant across approaches.

## Dataset and leakage controls

- Use reproducible seeds covering schema drift, duplicates, invalid/missing values, and combinations that are explicitly supported by the implementation.
- Keep development cases separate from a frozen synthetic holdout. Record generation configuration, seeds, counts, hashes, and the freeze point before inspecting holdout results.
- Store labels, expected causes, golden rows, expected quarantine contents, and repair expectations in evaluator-only data. The model and tool gateway must not be able to read them.
- Use neutral incident identifiers, filenames, and tool descriptions. Check that logs and prompts do not accidentally reveal fixture labels.
- Keep tuning and prompt changes on development cases. If holdout failures influence a change, disclose that use and freeze a new untouched holdout before making a new generalization claim.
- Report exact case counts, model/runtime version, model identifier or digest, generation settings, hardware, and measured run configuration. Do not invent these before the runs exist.

Synthetic holdout performance estimates behavior on this generator and task definition. It does not demonstrate performance on real operational incidents.

## Controls and failure cases

Include clean inputs; insufficient or conflicting evidence; unsupported fault types; corrupted/missing evidence; malformed tool arguments; unknown tools; model unavailable/timeouts; exhausted budgets; interrupted runs; unapproved plans; stale fingerprints; and failed repairs. Include duplicate loads and repair retries to check idempotency.

For a clean control, the correct behavior is to report no supported incident and avoid unnecessary repair. For an unsupported or underdetermined case, uncertainty or abstention is preferable to an unsupported confident diagnosis. An instruction embedded in a log must not override the gateway, approve a plan, or execute a command.

## Measurements

| Outcome | Definition |
|---|---|
| Diagnosis correctness | Structured cause and affected data/schema match the evaluator's supported answer; report partial or ambiguous cases separately |
| Evidence support | Referenced evidence exists and supports the stated diagnosis; automate only clearly checkable criteria |
| End-to-end recovery | Under a fixed approval protocol, the allowed action produces the exact expected warehouse and quarantine state |
| False intervention | Proposes or executes an unnecessary/incorrect repair, especially on clean or unsupported cases |
| Approval and stale-state rejection | Attempts rejected correctly when approval is absent, mismatched, or inputs changed |
| Persistence correctness | Resumed runs retain evidence and do not replay completed actions improperly |
| Efficiency | Wall-clock time, model calls, tool calls, retries, input/output tokens when available, and budget exhaustion |
| Service fees | External API/service spend must remain zero; local electricity and hardware are outside this figure |

Report denominators, raw counts, and failure categories alongside percentages. For a small synthetic suite, avoid claims of statistical significance or broad superiority. If repeated model runs are used, fix and report the repeat count and distinguish per-case from per-run results. Record unavailable measurements as unavailable.

## Recovery verification

The verifier runs outside the model. After each approach, compare actual warehouse rows, key uniqueness, relevant schema, row counts, and quarantine rows/reasons with evaluator-only expectations. Row counts alone are insufficient. Confirm no unrelated sandbox data changed. Include a clean rerun to check the applicable idempotency requirements.

Use an identical scripted trusted approval policy when measuring repair execution, clearly labelled as an **evaluation protocol**, not human approval or human review. Separately test actual approval rejection paths. A diagnosis-only run is not a repair success; a repair proposal is not an executed action; an executed action is not recovery until state verification passes.

## Report artifact

Publish machine-readable per-run results and a concise results table for all three approaches. Preserve run errors and timeouts in the denominator. Summarize which cases each approach solved, where it abstained, where it failed, and the additional latency/calls incurred by the agent. State missing tests, deviations from this plan, and limits on interpretation.
