# Local model setup

PipeSleuth's Stage 7 adds a single-call local model baseline. It is not the
later tool-calling investigator. Local transport and schema checks do not
establish that a diagnosis is correct. No account, API key, paid service, or
cloud fallback is used.

## Reviewed model and runtime

Primary sources checked on **27 October 2026**:

- [Qwen's model card](https://huggingface.co/Qwen/Qwen3-1.7B) describes a 1.7B
  causal language model with thinking/non-thinking modes and a 32,768-token
  context. Its [publisher license](https://huggingface.co/Qwen/Qwen3-1.7B/blob/main/LICENSE)
  is Apache 2.0. These are publisher descriptions, not PipeSleuth measurements.
- The [Ollama qwen3:1.7b tag](https://ollama.com/library/qwen3:1.7b) supplies
  quantized weights. A mutable tag is not an immutable model identity; record
  the actual local inventory digest in each smoke report.
- The [Ollama generate API](https://docs.ollama.com/api/generate) accepts a JSON
  schema in `format`, `stream: false`, `think: false`, bounded generation
  options, and usage/duration fields. Structured output still needs local
  validation and does not prove semantic correctness.
- The [Ollama FAQ](https://docs.ollama.com/faq#how-do-i-disable-ollamas-cloud-features)
  documents `OLLAMA_NO_CLOUD=1` to disable cloud models and web search. The
  [v0.40.7 configuration source](https://github.com/ollama/ollama/blob/v0.40.7/envconfig/config.go)
  documents cloud disabling, request-body debug logging, history, parallelism,
  context limits, and `OLLAMA_NOPRUNE` for disabling startup cache pruning.
- [Ollama v0.40.7's runtime license](https://github.com/ollama/ollama/blob/v0.40.7/LICENSE)
  is MIT. Runtime and model licenses are distinct from this repository's license.

## Start a foreground server

Obtain a trusted free Ollama installation and already downloaded `qwen3:1.7b`
weights separately. This repository neither installs nor downloads them.
From the repository root, supply the two existing local paths:

```sh
python3 scripts/local_runtime.py --binary /absolute/path/to/ollama --models /absolute/path/to/models
```

The wrapper checks for the cached manifest and layer files, binds only
`127.0.0.1:11436`, and runs in the foreground. Stop it with **Ctrl-C**. It does
not register a background service or reuse another application's server.
Trusted binary/model paths remain caller configuration; the wrapper does not
independently authenticate a downloaded release or hash every model blob.

With that foreground server running, use a second terminal for the explicitly
experimental local method:

```sh
PYTHONPATH=src python3 -m pipesleuth diagnose RUN_ID --method local
```

The default endpoint is `http://127.0.0.1:11436`. `--endpoint` accepts an explicit
literal loopback HTTP address and port, `--model-timeout` sets a finite positive
budget up to 120 seconds, and `--model-digest SHA256` optionally pins the exact
local inventory digest. No other model tag or cloud fallback is selected.
Without `--method local`, diagnosis still uses the deterministic checklist.
The single-call baseline is for comparing one generation over a complete
evidence bundle with that checklist and the later investigator, not for granting
model output execution authority.

The server receives cloud-off settings, an 8192-token default context, one
parallel request, one loaded model, no persistent readline history, no debug
request-body recording, and no startup cache pruning. Inherited proxies,
provider tokens, and Ollama overrides are excluded. Essential platform values,
including the user's home directory, pass through unchanged; the wrapper does
not read identity files or change user-level settings. Ollama itself may access
its ordinary user settings. Ordinary startup and HTTP access logs still appear.
These are application/runtime controls, not an operating-system network sandbox.
Do not send private data to another process listening on the loopback endpoint.

The available development host has an Apple M4 and **16 GiB RAM**. The cached
model's main weight blob is **1,359,279,776 bytes**. Runtime memory includes
weights, context caches, and overhead; file size is not a RAM or speed guarantee.
The 8192-token setting is a bounded starting configuration. Record the effective
runtime context and memory observed during a real smoke instead of assuming the
publisher context is active. No larger model or cloud substitution is automatic.

## Smoke evidence

Run the smoke command only with the explicitly controlled foreground server:

```sh
PYTHONPATH=src python3 scripts/smoke_local.py --output reports/local-model-smoke.json
```

The smoke creates two temporary synthetic pipeline runs and passes neutral run
identifiers to the single-call baseline. Case labels stay in the smoke driver.
It records actual completions, failures, usage, timing, and unchanged run files.
No repairs run. Unavailable-model and timeout checks, when recorded, distinguish
actual Ollama responses from controlled local test-server behavior. A missing
runtime or model is a failure, not permission to download or call a cloud API.

Only an actually generated report establishes that a smoke ran. Two synthetic
cases cannot estimate diagnostic accuracy, grounding quality, robustness, or
production readiness. The later matched evaluation remains separate.

## Recorded observations and limitations

The [fresh 27 October smoke](../reports/local-model-smoke.json) and the
[initial attempt](../reports/local-model-smoke-initial.json) preserve four real
generation requests in total. The fresh process was requested after a transport
completion fix; the prompt template, model configuration and literal fixture
inputs were unchanged. New run timestamps and identifiers changed the evidence
identifiers naturally. No prompt or model tuning followed these results.
The initial report's recorded client source hash already matches the final
client, so the archive does not assert that older code was definitely loaded.

**The model produced unsupported faults for the healthy control.** Passing
transport, JSON and evidence-reference checks does not establish a correct
diagnosis. The observed outcomes are retained rather than replaced with the
declared fixture expectations:

| Attempt | Healthy input | Renamed amount column | Literal expectations matched |
|---|---|---|---|
| Initial | Invented invalid rows and order identifiers | Returned `schema_mismatch` | 1 of 2 |
| Fresh | Claimed invalid rows, duplicates and schema mismatch | Returned `unknown` with findings; local validation rejected the inconsistent payload | 0 of 2 |

The fresh schema result is exposed as `unknown` with `invalid_generation`,
without accepted findings. Raw model output remains in the report for inspection.
The valid-shaped healthy false positive shows that reference existence alone
does not establish semantic support. Neither run approved or executed a repair.
These are integration/development observations, not a held-out accuracy score
or human semantic review. The single-call model baseline remains experimental.

Observed runtime metadata was Ollama **0.40.7**, tag `qwen3:1.7b`, Q4_K_M,
installed size **1,359,293,444 bytes**, and inventory digest
`8f68893c685c3ddff2aa3fffce2aa60a30bb2da65ca488b61fff134a4d1730e7`.
The served Apache 2.0 license was checked separately from the publisher page.
The local API reported **2,031,739,904 parameters** and **40,960 context tokens**;
these differ from the publisher-card descriptions above and are recorded as
observed metadata, without inferring checkpoint equivalence from a mutable tag.
Actual backend logs showed the configured **8,192-token** context.

Both attempts completed two model responses with unchanged run-file snapshots
and unchanged model-cache size/mtime metadata. Startup logs confirmed loopback
binding, cloud disabling, no request-body debug logging and no cache pruning.
An actual Ollama missing-model request returned 404 without inference. Separate
controlled local HTTP checks rejected missing inventory before generation and
interrupted a delayed generation at the short configured timeout; those checks
are explicitly not Ollama/model outputs. The owned foreground server was stopped
after verification, and port 11436 then refused connections.
