# Matched synthetic evaluation

The deterministic checklist matched **6 of 6** expected holdout outcomes. The
single-call model matched **2 of 6**, and the bounded tool agent matched **1 of
6**. All three stopped when event evidence was missing. Among the model methods,
only the single-call method produced a correct model-generated finding, for an
identical duplicate. It also reported faults in a healthy run.

## Results

On 3 November 2026, all 27 case-method trials finished with 24 actual local
generations. Recovery uses the three holdout cases eligible for repair.

| Method | Development matches | Holdout matches | Holdout verified repairs | Holdout model requests | Median holdout diagnosis |
| --- | ---: | ---: | ---: | ---: | ---: |
| Deterministic checklist | 3/3 | 6/6 | 3/3 | 0 | 2.7 ms |
| Single-call local model | 0/3 | 2/6 | 1/3 | 5 | 5.98 s |
| Bounded tool agent | 0/3 | 1/6 | 0/3 | 10 | 5.49 s |

- The checklist found all six actionable faults across both splits. All six
  derived repairs passed independent database checks.
- The single-call model found the holdout duplicate and cited matching
  evidence. The fixed policy produced a verified repair. It emitted multiple
  findings for the healthy and injected storage cases. The single-finding
  policy blocked repair selection in both.
- The agent chose the log tool in all eight trials that reached the model.
  Each subsequent model decision failed validation. The ninth trial stopped
  because event evidence was missing. The storage case also ended in a
  literal unknown, but the scored match excludes its invalid decision.
- No model request timed out. Two single-call responses failed output
  validation. All original sources and databases retained their exact
  hashes. The policy selected no incorrect repair. That measure counts
  selected actions separately from unsupported findings.

The [machine-readable results](../reports/matched-evaluation.json) include each
trial, tool call, model trace, token count, timing, repair receipt and input
preservation check. The [development preflight](../reports/local-model-smoke-nov03.json)
records two additional generations used to choose a time budget. Both missed
their literal expectations.

## Cases and settings

The [literal synthetic manifest](../evaluation/matched-v1.json) and scorer were
committed at `37791c6` before the matched run. Three development cases cover an
amount-column rename, an identical duplicate and an invalid value. Six separate
holdout inputs repeat those fault families and add a healthy control, an
injected SQLite write failure and a case with the event table removed. These
are author-defined cases using the same pipeline contracts as development.

All methods inspected the same prepared run for each case. Inputs contained
only schema and rows, and run paths used opaque identifiers. The evaluator kept
labels and golden rows outside the gateway root. Before sending a prompt, its
observer checked that the included evidence came from the actual gateway.
The checklist and single-call model requested four reads up front. The agent
used a trusted initial read and selected further reads. The prompts, evidence
volume and decision schemas differ by design.

Both methods used cached `qwen3:1.7b` on Ollama 0.40.7, pinned to
`8f68893c685c3ddff2aa3fffce2aa60a30bb2da65ca488b61fff134a4d1730e7`. Runs used
macOS arm64 and Python 3.13.7 with cloud features disabled. Context was 8192
tokens and output was capped at 768. Temperature was zero and thinking was
off. Each call had 25 seconds. The agent had 45 seconds overall, four model
steps and four reads. A gateway read had a two-second deadline and 16 KiB
response cap. Each pair ran once without retries. Method order rotated and
each model trial used a fresh client. Timings include local load and tool time.

## Scoring and recovery

A match required the expected status and finding codes, valid run-scoped
references and no listed technical failure. Actionable cases also required a
cited envelope containing the expected observable fact. The report measures
that mechanical check, and the model's explanatory prose has no human score.
An expected unknown from missing evidence counts as a correct system outcome.
No trial exhausted its step or tool cap. The frozen scorer can count such an
abort on a future expected-unknown case, so examine those limits separately
when reusing it.

The scripted policy maps exactly one accepted, referenced actionable finding
to a fixed repair. The service checks preconditions. The evaluator then
acknowledges the exact plan and input hashes through the `scripted_test`
channel. Every method uses the same executor and verifier. Separate SQL reads
compare exact derived warehouse rows and types, quarantine records and
metadata. The audit also checks clean replay and refuses repeated execution.
The oracle scores the result after action selection.

The [repair audit](../reports/repair-smoke.json) and
[checkpoint audit](../reports/resume-smoke.json) separately cover unapproved
and stale requests, rollback, restart and uncertain operations. The
[local model smoke](LOCAL_MODEL.md) records unavailable-model and controlled
timeout checks. These tests have separate denominators.

## Reproduce locally

Start the pinned, cloud-disabled runtime using the [local model guide](LOCAL_MODEL.md).
From the repository root, choose a fresh output file.

```sh
PYTHONPATH=src python3 scripts/evaluate_matched.py \
  --model-digest 8f68893c685c3ddff2aa3fffce2aa60a30bb2da65ca488b61fff134a4d1730e7 \
  --model-timeout 25 --agent-timeout 45 \
  --report reports/local/matched-rerun.json
```

The measurements apply to this small synthetic suite and recorded runtime.
Treat the model methods as experimental. Production reliability and human
review of finding explanations remain unmeasured.
