# Local API

The API exposes the existing pipeline reads, deterministic checklist, saved
investigations and approved repair workflow over loopback HTTP. Its roots and
model settings are fixed when the server starts; a request cannot choose file
paths, SQL, commands or a remote model endpoint. Model diagnosis remains
experimental, including the previously recorded failures.

## Start locally

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-api.txt
PYTHONPATH=src .venv/bin/python -m pipesleuth serve --port 8765 \
  --session-file .runtime/api-session.json
```

The API dependencies are optional; the existing command-line core remains
usable without them. Choose fixed roots with `--data-dir`, `--output-dir`,
`--investigation-state-dir` and `--repair-state-dir`. Stop the foreground server
with Ctrl-C. Its own unchanged session file is removed on shutdown; replacements
are preserved. An existing session file is never overwritten.

By default, model operations return `model_unconfigured`. To enable them, first
start the explicitly controlled [local runtime](LOCAL_MODEL.md), then supply the
exact cached `--model-digest`. `--model-endpoint` accepts a literal loopback
endpoint only. There is no automatic model startup, download or cloud fallback.

## Access and local session

The server listens on literal `127.0.0.1` and one configured port. The launcher
creates a private session file containing the local URL and three random
capabilities. Keep that file local; do not commit it or print its tokens.

| Capability | Allowed operations |
|---|---|
| Read | Authenticated reads |
| Operation | Reads, diagnosis, investigation progress and repair planning/execution |
| Approval | Reads and approval of an exact repair plan/input fingerprint |

Operation capability cannot approve its own repair. Approval capability is a
separate acknowledgement step; possession does not authenticate a person's
identity or prove human review. The model receives none of these capabilities.
`GET /health` returns minimal public liveness information.

The API rejects unexpected Host values, nonlocal clients, foreign origins,
forwarded headers and ambiguous security headers. It does not enable cross-origin
browser access. This protects a trusted local workspace; it does not isolate
it from other programs or an owner who can read the session file or rewrite data.

## Routes

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Minimal liveness |
| GET | `/v1/capabilities` | Available methods, tools and model configuration flag |
| GET | `/v1/runs/{id}` | Pipeline summary |
| GET | `/v1/runs/{id}/evidence/{tool}` | Bounded read-only evidence |
| POST | `/v1/runs/{id}/diagnosis` | `checklist`, `local` or `agent` diagnosis |
| POST | `/v1/investigations` | Create saved state without executing it |
| GET | `/v1/investigations/{id}` | Inspect saved progress |
| POST | `/v1/investigations/{id}/resume` | Continue within saved budgets |
| POST | `/v1/repairs` | Plan one fixed repair |
| GET | `/v1/repairs/{id}` | Inspect its exact plan and state |
| POST | `/v1/repairs/{id}/approve` | Acknowledge exact plan/input hashes |
| POST | `/v1/repairs/{id}/execute` | Execute an unused approval |

Send `Authorization: Bearer TOKEN` using the appropriate local capability.
Mutations use `Content-Type: application/json`. Read the hashes returned by a
repair plan before sending the separate approval request. Repair execution
accepts an empty object and cannot carry an implicit approval or action override.
Logs/validation evidence may use one bounded `limit` query; other query fields
are refused. The authenticated `/openapi.json` describes the closed JSON shapes.
Interactive documentation is disabled; it loads no remote documentation assets.

## Request and failure limits

Mutations require JSON objects of at most 4 KiB with closed fields and a
2-second body-read deadline. Buffered responses are capped at 512 KiB. Duplicate keys,
nonfinite numbers, unexpected fields, oversized bodies and unsupported content
types are refused. Error responses contain a short code without echoing inputs,
local paths or credentials. Evidence reads use fixed allowlisted tools and bounds.
Missing model configuration is an explicit failure; there is no cloud fallback.

Core operation limits still apply. A client disconnect or HTTP timeout does not
prove that the underlying operation was cancelled or rolled back. Inspect the
saved investigation/repair and its target UUID before deciding what to do next.
An uncertain pending intent refuses automatic replay. API access does not bypass
exact-plan approval, stale-input checks or independent repair verification.

## Verification scope

The live HTTP audit uses synthetic data and a server owned by its driver.
Approval requests in that audit are **scripted tests**, not human approvals.
Separate controlled local HTTP model fixtures test transport and saved progress;
they perform no actual model inference and establish no diagnosis accuracy.
The audit compares original/unrelated files and actual derived database rows,
then stops its own server and verifies that the port is closed.

Run the optional API checks and a fresh owned-server audit with:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python \
  -W error::ResourceWarning -m unittest discover -s tests -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python scripts/smoke_api.py \
  --output reports/local/api-smoke.json
```

The audit refuses an existing report path so earlier outcomes are retained.
The pinned test transport currently emits a Starlette deprecation warning about
`httpx`; it does not affect the API's actual HTTP server audit.

## Observed on 1 November 2026

The [saved HTTP audit](../reports/api-smoke.json) ran once against frozen source:
**51 audited requests received 51 responses**, plus one separate startup health
probe. All 16 request-boundary checks passed, including role separation,
Host/origin/forwarded-header checks, duplicate authorization and JSON fields,
oversized input, closed fields and explicit unconfigured-model failures.

All **3/3 derived repairs** matched literal expected warehouse, quarantine and
run metadata through fresh SQLite reads. Their original/unrelated files were
preserved. Two simultaneous execution requests returned 200 and 503, producing
one target. A separate fixture deliberately changed source whitespace after
approval; execution returned 409 without changing its saved record or outputs.

There were zero case/driver errors and **zero actual model requests**. Approval
records use `trusted_api`, meaning a local API caller; every approval in this
audit was scripted and establishes no human review. Separate software tests used
three simulated generation responses to check local model transport and saved
resume without replay. Those responses were not actual LLM inference.

The owned foreground server stopped without forced termination. Its port then
refused connections, its unchanged private session file was removed, and all 18
recorded implementation/dependency hashes matched. The public report contains
no local absolute paths or bearer credentials. This demonstrates local HTTP
integration, not diagnosis accuracy, production security or every crash outcome.

## Browser workspace (2 November)

The same foreground server now serves the small [local UI](UI.md) at `/`.
Only six literal static GET/HEAD paths are public. They contain no session data;
API reads and operations still require their existing roles. Encoded aliases,
unknown files, session-file URLs and caller-supplied paths do not gain access.
The security boundary applies to static requests too, with a closed content
security policy, no framing, no cache, no cookies and no remote assets.
