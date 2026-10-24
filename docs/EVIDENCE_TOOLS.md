# Evidence tools

The evidence gateway lets a caller inspect a pipeline run without changing it.
It provides the shared evidence boundary for the planned checklist, local model
baseline, and investigator. It does not diagnose incidents or execute repairs.

## Usage

```sh
PYTHONPATH=src python3 -m pipesleuth evidence tools
PYTHONPATH=src python3 -m pipesleuth evidence read_run RUN_ID
PYTHONPATH=src python3 -m pipesleuth evidence read_logs RUN_ID --limit 10
PYTHONPATH=src python3 -m pipesleuth evidence inspect_schema RUN_ID
PYTHONPATH=src python3 -m pipesleuth evidence inspect_validation RUN_ID --limit 10
```

Replace `RUN_ID` with the neutral identifier returned by `pipeline run`.
Use `--data-dir PATH` on an evidence read to select the trusted local run root.
The programmatic tool arguments never accept paths.

| Tool | Evidence | Arguments |
|---|---|---|
| `read_run` | Run state, policies, and observed counts | `run_id` |
| `read_logs` | Observed pipeline events and selected diagnostic fields | `run_id`, optional `limit` |
| `inspect_schema` | Source columns and warehouse schema against the order contract | `run_id` |
| `inspect_validation` | Accepted/quarantined counts, retained rejection evidence, and available failure diagnostics | `run_id`, optional `limit` |

```python
from pipesleuth.evidence import EvidenceGateway

gateway = EvidenceGateway("var/sandboxes")
evidence = gateway.call("read_logs", {"run_id": run_id, "limit": 5})
```

## Responses and limits

Each response includes its run, tool, validated arguments, observation time,
evidence identifier, content SHA-256, data, and an explicit truncation flag.
The content fingerprint binds the returned bounded evidence and arguments;
observation time is separate. It is not proof that the database is correct.

- `run_id` must be exactly 32 lowercase hexadecimal characters.
- Log and validation reads default to 10 records and accept integer limits
  from 1 to 20. Other tools reject `limit`.
- Responses are capped at 16,384 UTF-8 bytes of compact JSON by default.
  Long text and collections may be shortened and marked as truncated.
- A caller may lower the response cap to at least 1,024 bytes. Evidence that
  cannot fit safely is rejected instead of returned beyond the cap.
- The default elapsed-time budget is two seconds, configurable up to five.
  SQLite uses a short lock wait and an interrupting progress handler. Deadline
  checks also surround reads and rendering. These are cooperative bounds,
  not an operating-system process timeout.
- Source inspection reads at most 2 MiB and verifies the stored source hash.
  Corrupt or changed source is rejected; logs and run status remain separately
  readable to investigate a failed load.

## Safety and limitations

Tool names and arguments are allowlisted. There is no arbitrary SQL, shell,
filesystem read, network access, or mutation operation. Fixed SQLite queries
use read-only connections and a read transaction. Existing path and symlink
guards still apply. These are safeguards for a trusted local workspace, not
an operating-system security sandbox.

The gateway supports the rollback-journal mode created by this pipeline. It
rejects WAL-mode databases and existing journal sidecars before opening SQLite,
because a SQLite read can otherwise create WAL shared-memory files. Journal mode
must stay stable while evidence is read; changing it concurrently is unsupported.

Only selected runtime fields are exposed. The gateway does not import fixture
generators, read evaluator tables, or expose local paths. Missing historical
tables are reported as unavailable, without inventing events or rejections.
Validation samples expose canonical order fields only; extra fields are omitted
and flagged. The original records remain in quarantine. Free exception messages
are omitted from log evidence because they can contain local paths.

All source text, log messages, and retained records are **untrusted data**.
Instructions embedded in them cannot add a tool, approve a repair, or execute
an action. Future model callers must preserve this distinction.

The tools inspect a snapshot; they do not independently verify a successful
repair or prevent another process changing data afterward. Repairs and their
approval/fingerprint checks belong to a later stage.
