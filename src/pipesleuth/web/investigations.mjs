/* Local diagnosis and checkpoint views. API data is rendered as text only. */

const METHODS = new Map([
  ["deterministic_checklist", "Deterministic checklist"],
  ["single_call_local_model", "Single-call local model · experimental"],
  ["bounded_local_agent", "Bounded local tool agent · experimental"],
]);
const STATUSES = new Map([
  ["diagnosed", "Incident reported"],
  ["no_incident", "No incident reported"],
  ["unknown", "Unknown — no reliable diagnosis"],
]);
const UUID = /^[0-9a-f]{32}$/;
const SHA = /^ev_[0-9a-f]{32}$/;

function fail(message) { throw new Error(message); }
function object(value) { return value !== null && typeof value === "object" && !Array.isArray(value); }
function count(value) { return Number.isSafeInteger(value) && value >= 0; }

export function investigationId(value) {
  if (typeof value !== "string" || !UUID.test(value)) fail("Enter a 32-character lowercase investigation ID.");
  return value;
}

export function resumeOperations(value) {
  if (!/^(?:[1-9]|1[0-6])$/.test(String(value))) fail("Choose between 1 and 16 operations.");
  return Number(value);
}

/* Validate the fields that change the meaning or enabled actions of a view. */
export function diagnosisView(result) {
  if (!object(result) || !METHODS.has(result.method) || !STATUSES.has(result.status)
      || !UUID.test(result.run_id ?? "") || !Array.isArray(result.findings)
      || result.findings.length > 4 || !Array.isArray(result.limitations)
      || !result.limitations.every(item => typeof item === "string")
      || !Array.isArray(result.evidence) || result.evidence.length > 4
      || !count(result.tool_calls) || (result.model_calls !== undefined && !count(result.model_calls))
      || typeof result.elapsed_ms !== "number" || !Number.isFinite(result.elapsed_ms) || result.elapsed_ms < 0
      || (result.findings.length > 0) !== (result.status === "diagnosed")) fail("The diagnosis response could not be displayed safely.");
  const references = new Set(result.evidence.map(item => item?.evidence_id));
  if (result.evidence.some(item => !object(item) || !SHA.test(item.evidence_id ?? "")
      || item.run_id !== result.run_id || typeof item.truncated !== "boolean")
      || result.findings.some(item => !object(item) || typeof item.code !== "string"
        || typeof item.summary !== "string" || !Array.isArray(item.evidence_refs)
        || !item.evidence_refs.length || item.evidence_refs.some(ref => !references.has(ref)))) {
    fail("The diagnosis response could not be displayed safely.");
  }
  return {
    method: METHODS.get(result.method), status: STATUSES.get(result.status),
    isUnknown: result.status === "unknown", experimental: result.method !== "deterministic_checklist",
    toolCalls: result.tool_calls, modelCalls: result.model_calls ?? 0,
    elapsedMs: result.elapsed_ms, findings: result.findings,
    limitations: result.limitations, evidence: result.evidence,
    trace: Array.isArray(result.trace) ? result.trace : [],
    semanticSupportVerified: false,
  };
}

export function savedView(record) {
  if (!object(record) || !UUID.test(record.investigation_id ?? "")
      || !Number.isSafeInteger(record.revision) || record.revision < 1
      || !object(record.state) || typeof record.state.done !== "boolean"
      || !(record.pending === null || object(record.pending))) fail("The saved response could not be displayed safely.");
  return {
    id: record.investigation_id, revision: record.revision, done: record.state.done,
    pending: record.pending !== null, canResume: record.pending === null && !record.state.done,
    result: diagnosisView(record.state.result), next: record.state.next ?? null,
  };
}

function node(tag, text, attrs = {}) {
  const element = document.createElement(tag);
  if (text !== undefined && text !== null) element.textContent = String(text);
  for (const [name, value] of Object.entries(attrs)) element.setAttribute(name, String(value));
  return element;
}
function labeled(label, control) {
  const wrap = node("div", null, {class: "field"});
  wrap.append(node("label", label, {for: control.id}), control);
  return wrap;
}
function button(text, id) { return node("button", text, {type: "button", id}); }
function details(title, value) {
  const wrap = node("details");
  wrap.append(node("summary", title), node("pre", JSON.stringify(value, null, 2)));
  return wrap;
}

function renderDiagnosis(container, result) {
  const view = diagnosisView(result);
  container.replaceChildren();
  container.append(node("h4", view.status), node("p", view.method, {class: "muted"}));
  container.append(node("p", `Source run: ${result.run_id}`, {class: "mono"}));
  if (view.isUnknown) container.append(node("p", "The method did not establish a diagnosis. Review the limitations and evidence before deciding what to do.", {class: "notice"}));
  else if (view.experimental) container.append(node("p", "Model output is experimental. Valid references do not establish that the finding is correct.", {class: "notice"}));
  else container.append(node("p", "This is a fixed-rule checklist result. It is not an AI assessment or a guarantee that all incidents were found.", {class: "muted"}));
  container.append(node("p", `Read tools: ${view.toolCalls} · Model requests: ${view.modelCalls} · Elapsed: ${view.elapsedMs.toFixed(1)} ms`));
  if (view.findings.length) {
    const list = node("ul");
    for (const finding of view.findings) {
      const item = node("li");
      item.append(node("strong", finding.code), node("p", finding.summary), node("p", `Evidence: ${finding.evidence_refs.join(", ")}`, {class: "mono"}));
      list.append(item);
    }
    container.append(list);
  }
  if (view.limitations.length) {
    const list = node("ul");
    for (const limitation of view.limitations) list.append(node("li", limitation));
    container.append(node("h5", "Limitations"), list);
  }
  const evidence = node("div", null, {class: "stack"});
  evidence.append(node("h5", "Evidence used by this method"));
  if (!view.evidence.length) evidence.append(node("p", "No evidence has been read yet.", {class: "muted"}));
  for (const item of view.evidence) {
    evidence.append(details(`${item.tool} · ${item.evidence_id}${item.truncated ? " · TRUNCATED" : ""}`, item));
  }
  container.append(evidence);
  if (view.trace.length) container.append(details("Operation trace (observed calls and failures)", view.trace));
}

function errorMessage(error, saved = false) {
  if (error?.uncertain) return saved
    ? "Request outcome uncertain. Inspect the saved investigation before another action. If no ID was returned, inspect the local checkpoint store with the CLI before starting again. No retry was sent."
    : "Request outcome uncertain. The server may still be working; no retry was sent. This result is not recorded as a completed diagnosis.";
  const messages = {
    model_unconfigured: "No pinned local model is configured. The checklist and saved-record reads remain available.",
    busy: "Another operation is active. Wait, then inspect saved state before deciding whether to continue.",
    operation_rejected: "The operation was refused. Inspect saved state; its input or configuration may have changed.",
    read_unavailable: "This saved investigation is unavailable. Check its ID and the server's state directory.",
    unauthorized: "The session is no longer authorized. Reconnect using the current local session file.",
    forbidden: "This session does not have the required operation capability.",
  };
  return messages[error?.code] ?? "The request could not be completed. Check the connection and inspect current state before trying another action.";
}

export function mountInvestigations({root, client, getRunId, notify}) {
  let epoch = 0, diagnosisSerial = 0, savedSerial = 0;
  let connected = false, modelConfigured = false, busy = false, record = null;
  let uncertain = false;
  root.classList.add("stack");
  root.append(node("h2", "Diagnose & investigate"), node("p", "Compare a deterministic checklist with experimental local-model methods. Earlier local-model smoke runs failed to produce accepted diagnoses; diagnosis accuracy has not been established.", {class: "muted"}));
  const method = node("select", null, {id: "diag-method"});
  method.append(node("option", "Checklist · deterministic", {value: "checklist"}),
    node("option", "Single-call model · experimental", {value: "local"}),
    node("option", "Tool agent · experimental", {value: "agent"}));
  const diagnose = button("Run diagnosis", "diag-run");
  const diagnosticControls = node("div", null, {class: "button-row"});
  diagnosticControls.append(labeled("Diagnosis method", method), diagnose);
  const modelNote = node("p", "Connect a local session to use this view.", {class: "muted", id: "diag-model-note"});
  const diagnosisOutput = node("div", null, {class: "result", id: "diag-result", "aria-live": "polite"});
  root.append(diagnosticControls, modelNote, diagnosisOutput);

  const savedSection = node("section", null, {class: "stack", "aria-labelledby": "investigation-title"});
  savedSection.append(node("h3", "Saved investigation", {id: "investigation-title"}), node("p", "Start a checkpoint, then resume a chosen number of read/model operations. Starting only saves the cursor; it does not contact the model. Pending outcomes cannot be replayed.", {class: "muted"}));
  const start = button("Start saved investigation", "investigation-start");
  const id = node("input", null, {id: "investigation-id", type: "text", maxlength: "32", pattern: "[0-9a-f]{32}", autocomplete: "off", spellcheck: "false", placeholder: "32-character saved ID"});
  const inspect = button("Inspect saved investigation", "investigation-inspect");
  const operations = node("input", null, {id: "investigation-operations", type: "number", min: "1", max: "16", step: "1", value: "1"});
  const resume = button("Resume saved investigation", "investigation-resume");
  const idControls = node("div", null, {class: "button-row"});
  idControls.append(labeled("Investigation ID", id), inspect);
  const resumeControls = node("div", null, {class: "button-row"});
  resumeControls.append(labeled("Maximum operations this time", operations), resume);
  const savedOutput = node("div", null, {id: "investigation-result", class: "result", "aria-live": "polite"});
  savedSection.append(start, idControls, resumeControls, savedOutput);
  root.append(savedSection);

  function update() {
    const blocked = !connected || busy;
    diagnose.disabled = blocked || (method.value !== "checklist" && !modelConfigured);
    method.disabled = blocked;
    for (const option of method.options) if (option.value !== "checklist") option.disabled = !modelConfigured;
    start.disabled = blocked || !modelConfigured || uncertain;
    inspect.disabled = blocked;
    id.disabled = blocked;
    operations.disabled = blocked;
    resume.disabled = blocked || !modelConfigured || uncertain || !record || !savedView(record).canResume || id.value !== record.investigation_id;
  }
  function showSaved(value) {
    const view = savedView(value);
    record = value;
    id.value = view.id;
    savedOutput.replaceChildren(node("h4", view.pending ? "Pending — outcome uncertain" : view.done ? "Investigation finished" : "Checkpoint saved"));
    savedOutput.append(node("p", `Investigation ${view.id} · revision ${view.revision}`, {class: "mono"}));
    if (view.pending) savedOutput.append(node("p", "An admitted operation has no completed checkpoint. Counts are confirmed lower bounds. Resume is disabled; do not replay this operation.", {class: "notice"}));
    else if (!view.done) savedOutput.append(node("p", "This is an unfinished investigation, not a final diagnosis. Continue explicitly when ready.", {class: "notice"}));
    const result = node("div");
    renderDiagnosis(result, value.state.result);
    savedOutput.append(result);
    if (view.next) savedOutput.append(details("Next saved operation", view.next));
    savedOutput.append(details("Saved record (including input binding)", value));
    update();
  }
  async function run(action, area, kind) {
    if (!connected || busy) return;
    const currentEpoch = epoch;
    const serial = kind === "diagnosis" ? ++diagnosisSerial : ++savedSerial;
    busy = true; update();
    area.replaceChildren(node("p", "Request in progress. Do not send it again.", {class: "muted"}));
    try {
      const result = await action();
      if (currentEpoch !== epoch || serial !== (kind === "diagnosis" ? diagnosisSerial : savedSerial)) return;
      if (kind === "diagnosis") renderDiagnosis(area, result);
      else { uncertain = false; showSaved(result); }
      notify(kind === "diagnosis" ? "Diagnosis response received. Review its limitations." : "Saved investigation state loaded.");
    } catch (error) {
      if (currentEpoch !== epoch) return;
      if (kind !== "diagnosis" && error?.uncertain) uncertain = true;
      const message = errorMessage(error, kind !== "diagnosis");
      area.replaceChildren(node("p", message, {class: "notice"}));
      notify(message, "error");
    } finally {
      if (currentEpoch === epoch) { busy = false; update(); }
    }
  }
  method.addEventListener("change", update);
  id.addEventListener("input", () => {
    if (record && id.value !== record.investigation_id) {
      record = null;
      savedOutput.replaceChildren(node("p", "Inspect this ID before resuming.", {class: "muted"}));
    }
    update();
  });
  diagnose.addEventListener("click", () => {
    if (diagnose.disabled) return;
    let runId;
    try { runId = getRunId(); } catch { notify("Load a valid run before requesting a diagnosis.", "error"); return; }
    const selectedMethod = method.value;
    run(() => client.request(`/v1/runs/${runId}/diagnosis`, {method: "POST", role: "operation", body: {method: selectedMethod}}), diagnosisOutput, "diagnosis");
  });
  start.addEventListener("click", () => {
    if (start.disabled) return;
    let runId;
    try { runId = getRunId(); } catch { notify("Load a valid run before starting an investigation.", "error"); return; }
    run(() => client.request("/v1/investigations", {method: "POST", role: "operation", body: {run_id: runId}}), savedOutput, "start");
  });
  inspect.addEventListener("click", () => {
    if (inspect.disabled) return;
    let selectedId;
    try { selectedId = investigationId(id.value); } catch (error) { notify(error.message, "error"); return; }
    run(() => client.request(`/v1/investigations/${selectedId}`, {role: "read"}), savedOutput, "inspect");
  });
  resume.addEventListener("click", () => {
    if (resume.disabled) return;
    let selectedId, maxOperations;
    try { selectedId = investigationId(id.value); maxOperations = resumeOperations(operations.value); }
    catch (error) { notify(error.message, "error"); return; }
    run(() => client.request(`/v1/investigations/${selectedId}/resume`, {method: "POST", role: "operation", body: {max_operations: maxOperations}}), savedOutput, "resume");
  });
  function reset() {
    epoch += 1; diagnosisSerial += 1; savedSerial += 1;
    connected = false; modelConfigured = false; busy = false; record = null; uncertain = false;
    method.value = "checklist"; id.value = ""; operations.value = "1";
    diagnosisOutput.replaceChildren(); savedOutput.replaceChildren();
    modelNote.textContent = "Connect a local session to use this view.";
    update();
  }
  function setCapabilities(capabilities) {
    connected = true; modelConfigured = capabilities?.model_configured === true;
    if (!modelConfigured) method.value = "checklist";
    modelNote.textContent = modelConfigured
      ? "A local model digest is pinned. Model responses remain experimental; semantic support is not verified."
      : "Local model unavailable: no digest is pinned. Use the checklist or inspect an existing saved investigation.";
    update();
  }
  reset();
  return {reset, setCapabilities};
}
