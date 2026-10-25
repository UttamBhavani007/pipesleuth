# Troubleshooting checklist

The deterministic baseline diagnoses supported pipeline faults from the same
read-only tools planned for the local model and investigator. It uses explicit
rules, without a model, raw database access, fixture labels, or repair authority.

## Usage

```sh
PYTHONPATH=src python3 -m pipesleuth diagnose RUN_ID
```

Use `--data-dir PATH` to select a trusted local run root. The result is compact
JSON with findings, evidence references, uncertainty, the observed evidence
bundle, tool-call count, and measured elapsed time.

| Status | Meaning |
|---|---|
| `diagnosed` | Complete, consistent evidence supports a known fault |
| `no_incident` | A successful run has consistent counts and no unresolved fault in the supported checks |
| `unknown` | Evidence is missing, malformed, truncated, unsupported, or inconsistent |

Exit codes are 0 for `diagnosed` or `no_incident`, 1 for `unknown`, and 2 for
invalid command arguments. A diagnosis never executes or approves a repair.

```python
from pipesleuth.checklist import ChecklistBaseline
from pipesleuth.evidence import EvidenceGateway

result = ChecklistBaseline(EvidenceGateway("var/sandboxes")).diagnose(run_id)
```

## Evidence and rules

The baseline calls `read_run`, `read_logs`, `inspect_schema`, and
`inspect_validation` once each, in that order. Log and validation limits are
20. These are the complete bounded tool responses, not evaluator answers.

It checks requested run/tool/arguments, content hashes, identifiers, types,
truncation, availability, lifecycle events, schema, and cross-response counts
before making a supported conclusion. A valid content hash binds returned
content; it does not authenticate its truth.

- Schema failures require matching observed column differences and failure
  evidence. The checklist does not guess a column mapping or execute a rename.
- Duplicate-key and conflicting-retry failures require matching failure facts.
- Invalid-row and integer-overflow failures use observed validation facts.
  Completed quarantine runs group retained rejection reasons by code.
- Identical retries skipped under the explicit policy are a handled control:
  `no_incident`, with a note, rather than a request for another repair.
- Failed runs with unsupported storage/source-change faults, unfinished runs,
  absent legacy evidence, or incomplete samples return `unknown`.

All evidence text remains untrusted data. Findings use static explanations and
validated facts, with references to the actual returned evidence identifiers.
Instructions embedded in a record cannot change the checklist or add an action.

## Limits

The four calls use the existing [evidence limits](EVIDENCE_TOOLS.md). Default
per-tool time budgets total up to eight seconds of cooperative reads; they are
not an operating-system timeout. The compact result is capped at 80 KiB.
Truncation is treated conservatively, so large otherwise valid runs may return
`unknown` rather than a partial confident diagnosis.

Each tool reads a separate SQLite snapshot. The checklist rejects observable
disagreements but does not make the four calls an atomic snapshot. It does not
verify every accepted row or prove that an externally modified database is
correct. Exact replay verification remains a separate pipeline operation;
approved repair verification belongs to a later stage.

## Development probe

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 scripts/checklist_development.py \
  --output reports/checklist-development.json
```

The probe uses ten AI-authored synthetic development cases: healthy and handled
retry controls; schema, duplicate, and invalid-row incidents; and unsupported,
missing, truncated, and inconsistent evidence. It records expected and observed
behavior, actual timing, source hashes, and whether run files stayed unchanged.
Case expectations stay in the evaluator; only neutral run IDs reach the baseline.

The [25 October results](../reports/checklist-development.json) record 10/10
expected behaviors: four diagnoses, two clean or handled controls, and four
abstentions. The 40 tool calls raised no exceptions, and all run-file snapshots
and independent database counts stayed unchanged. These cases
were used during implementation and are not a frozen holdout, human review,
AI accuracy measurement, production evaluation, or recovery result. The full
three-approach comparison remains [planned](EVALUATION.md).
