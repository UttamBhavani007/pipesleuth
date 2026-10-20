# PipeSleuth cost policy

## Requirement

The project must use **no paid services at any stage**. It runs locally with free software and an appropriately licensed local model. Do not add paid APIs, credits, trials requiring payment, hosted databases, cloud hosting, or a cloud fallback.

## Allowed implementation

- Python and SQLite for the pipeline, incident sandboxes, and state.
- Free local libraries and a loopback Ollama runtime when Stage 7 is reached.
- FastAPI and a locally served UI in their planned stages.
- Local tests, logs, result files, and screenshots.
- A GitHub Free public repository is preferred for the portfolio, subject to actual user authorization and current applicable terms. This document does not mean a repository has been created or published.

Do not enable GitHub Actions or another hosted CI service until its current terms and the intended configuration have been checked and verified to fit this policy. Local checks are sufficient for the staged build. Docker packaging is optional and requires a free, appropriately licensed local configuration and an actual validation run before claiming support.

## Forbidden additions

- OpenAI or other remote model APIs, even as an optional automatic fallback.
- Collection or use of API keys, billing credentials, payment cards, or paid trial accounts.
- Paid vector databases, hosted databases, remote execution, deployment, analytics, or telemetry services.
- Remote UI assets or integrations that silently contact a third party at runtime.
- Automatic purchases, upgrades, paid model downloads, or cloud inference when local hardware is insufficient.

If a local dependency or model is unavailable, stop that stage, record the limitation, and use an acceptable smaller local model only after verifying its license and capabilities. Do not silently switch providers or claim an unrun smoke test passed.

## Network and configuration boundaries

Model calls must target an explicitly validated loopback endpoint. When introduced, the adapter must disable inherited proxies and redirects and reject nonlocal addresses. Disable cloud features, cloud authentication, fallback paths, and telemetry in the selected runtime where applicable. Test these boundaries before treating the adapter as ready.

Downloading free dependencies or model weights is distinct from executing inference through a remote service. Check source, license, download size, and local disk/memory suitability; do not introduce a billable endpoint. Runtime investigation and evaluation should operate without external network services.

## What “zero service fees” means

Expected paid external service fees are **€0**. This is not a claim that computation has no resource cost: the user's computer, electricity, storage, internet access, and download time still exist. Record runtime and model size when available; do not invent electricity or hardware prices.

Codex usage is governed by the user's existing account and limits. This project does not purchase a Codex plan, redeem credits, or guarantee that account use is free. A local automation also depends on the desktop app and host being available; its configuration must be verified separately before claiming it is active.

Any proposed change that cannot meet this policy remains unimplemented until the user changes the requirement. Do not reinterpret a free tier or a temporary credit as permission to use a paid service.
