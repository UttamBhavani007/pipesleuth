import test from "node:test";
import assert from "node:assert/strict";
import {diagnosisView, savedView, investigationId, resumeOperations, mountInvestigations} from "../src/pipesleuth/web/investigations.mjs";

const run = "a".repeat(32);
const saved = "b".repeat(32);
const evidenceId = `ev_${"c".repeat(32)}`;
function result(overrides = {}) {
  return {method: "bounded_local_agent", run_id: run, status: "unknown", findings: [],
    limitations: ["invalid_decision"], evidence: [], tool_calls: 2, model_calls: 1,
    elapsed_ms: 40.5, semantic_support_verified: false, trace: [], ...overrides};
}
function record(overrides = {}) {
  return {investigation_id: saved, revision: 3, state: {done: false, result: result(), next: {action: "tool", tool: "read_logs"}}, pending: null, ...overrides};
}

test("unknown model results preserve the failure and never become successful diagnoses", () => {
  const view = diagnosisView(result());
  assert.equal(view.isUnknown, true);
  assert.equal(view.experimental, true);
  assert.match(view.status, /Unknown/);
  assert.deepEqual(view.limitations, ["invalid_decision"]);
  assert.equal(view.semanticSupportVerified, false);
  assert.equal(view.modelCalls, 1);
});

test("checklist remains deterministic and absent model counts show zero", () => {
  const view = diagnosisView(result({method: "deterministic_checklist", model_calls: undefined, status: "no_incident", limitations: []}));
  assert.equal(view.experimental, false);
  assert.equal(view.modelCalls, 0);
  assert.equal(view.status, "No incident reported");
});

test("inconsistent status/cardinality is refused instead of displaying success", () => {
  assert.throws(() => diagnosisView(result({status: "diagnosed"})), /safely/);
  assert.throws(() => diagnosisView(result({findings: [{summary: "fault"}]})), /safely/);
});

test("evidence references are tied to this run before findings are shown", () => {
  const finding = {code: "invalid_row", summary: "<img src=x onerror=alert(1)>", evidence_refs: [evidenceId]};
  const evidence = [{evidence_id: evidenceId, run_id: run, truncated: false, tool: "inspect_validation"}];
  const view = diagnosisView(result({status: "diagnosed", findings: [finding], evidence}));
  assert.equal(view.findings[0].summary, finding.summary);
  assert.throws(() => diagnosisView(result({status: "diagnosed", findings: [finding], evidence: [{...evidence[0], run_id: saved}]})), /safely/);
  assert.throws(() => diagnosisView(result({status: "diagnosed", findings: [finding], evidence: []})), /safely/);
});

test("nonfinite timings and negative or coerced call counts are refused", () => {
  for (const overrides of [{elapsed_ms: Infinity}, {elapsed_ms: -1}, {tool_calls: "2"}, {model_calls: -1}]) {
    assert.throws(() => diagnosisView(result(overrides)), /safely/);
  }
});

test("pending saved operation blocks replay even when its cursor is unfinished", () => {
  const view = savedView(record({pending: {operation: "model", token: "d".repeat(32)}}));
  assert.equal(view.pending, true);
  assert.equal(view.canResume, false);
  assert.equal(view.result.modelCalls, 1);
});

test("terminal unknown stays terminal and cannot be resumed", () => {
  const view = savedView(record({state: {done: true, result: result(), next: null}}));
  assert.equal(view.done, true);
  assert.equal(view.canResume, false);
  assert.equal(view.result.isUnknown, true);
});

test("only nonpending unfinished checkpoints are resumable", () => {
  const view = savedView(record());
  assert.equal(view.canResume, true);
  assert.equal(view.revision, 3);
  assert.deepEqual(view.next, {action: "tool", tool: "read_logs"});
});

test("malformed saved state is refused rather than enabling resume", () => {
  for (const overrides of [{pending: false}, {revision: 0}, {state: {done: "false", result: result()}}, {investigation_id: "../../file"}]) {
    assert.throws(() => savedView(record(overrides)), /safely/);
  }
});

test("investigation IDs are literal and operation counts are closed integers", () => {
  assert.equal(investigationId(saved), saved);
  for (const value of ["../" + saved, saved.toUpperCase(), ` ${saved}`, `${saved}/resume`]) assert.throws(() => investigationId(value));
  assert.equal(resumeOperations("1"), 1);
  assert.equal(resumeOperations("16"), 16);
  for (const value of ["0", "17", "1.0", "01", "1e1", "", " 2", true]) assert.throws(() => resumeOperations(value));
});

// A deliberately small DOM seam for controller behavior, not a browser/layout audit.
class Element {
  constructor(tag) {
    this.tag = tag; this.children = []; this.listeners = new Map(); this.disabled = false;
    this.attributes = {}; this.ownText = ""; this.ownValue = undefined;
    this.classList = {add() {}};
  }
  setAttribute(key, value) { this.attributes[key] = value; if (key === "value") this.value = value; }
  get id() { return this.attributes.id ?? ""; }
  get options() { return this.children.filter(item => item.tag === "option"); }
  get value() { return this.ownValue ?? (this.tag === "select" ? this.options[0]?.value ?? "" : ""); }
  set value(value) { this.ownValue = value; }
  set textContent(value) { this.ownText = String(value); this.children = []; }
  get textContent() { return this.ownText + this.children.map(item => item.textContent).join(""); }
  append(...items) { this.children.push(...items); }
  replaceChildren(...items) { this.ownText = ""; this.children = items; }
  addEventListener(type, listener) { this.listeners.set(type, listener); }
  dispatch(type) { if (!(type === "click" && this.disabled)) this.listeners.get(type)?.({target: this}); }
  find(id) { return this.id === id ? this : this.children.map(item => item.find(id)).find(Boolean); }
}
const flush = () => new Promise(resolve => setTimeout(resolve, 0));
function controller(responder) {
  globalThis.document = {createElement: tag => new Element(tag)};
  const root = new Element("section"), calls = [], notices = [];
  const client = {request: async (path, options) => { calls.push({path, options}); return responder(path, options); }};
  const mounted = mountInvestigations({root, client, getRunId: () => run, notify: (...args) => notices.push(args)});
  return {root, calls, notices, ...mounted, get: id => root.find(id)};
}

test("no model pin leaves checklist usable and saved inspection separate from resume", () => {
  const ui = controller(() => assert.fail("unexpected request"));
  ui.setCapabilities({model_configured: false});
  assert.equal(ui.get("diag-run").disabled, false);
  assert.equal(ui.get("investigation-inspect").disabled, false);
  assert.equal(ui.get("investigation-start").disabled, true);
  assert.equal(ui.get("investigation-resume").disabled, true);
  assert.equal(ui.get("diag-method").options[1].disabled, true);
});

test("reset discards a late diagnosis response without repainting the new session", async () => {
  let complete;
  const ui = controller(() => new Promise(resolve => { complete = resolve; }));
  ui.setCapabilities({model_configured: true});
  ui.get("diag-run").dispatch("click");
  assert.equal(ui.calls.length, 1);
  ui.reset();
  ui.setCapabilities({model_configured: false});
  complete(result());
  await flush();
  assert.equal(ui.get("diag-result").textContent, "");
  assert.equal(ui.notices.length, 0);
});

test("pending record is shown as uncertain and cannot issue a resume request", async () => {
  const ui = controller(() => record({pending: {operation: "generate"}}));
  ui.setCapabilities({model_configured: true});
  ui.get("investigation-id").value = saved;
  ui.get("investigation-inspect").dispatch("click");
  await flush();
  assert.match(ui.get("investigation-result").textContent, /Counts are confirmed lower bounds/);
  assert.equal(ui.get("investigation-resume").disabled, true);
  ui.get("investigation-resume").dispatch("click");
  assert.equal(ui.calls.length, 1);
  assert.equal(ui.calls[0].options.role, "read");
});

test("resume sends exactly the chosen bounded operation count and becomes terminal", async () => {
  const ui = controller(path => path.endsWith("/resume")
    ? record({revision: 5, state: {done: true, result: result(), next: null}}) : record());
  ui.setCapabilities({model_configured: true});
  ui.get("investigation-id").value = saved;
  ui.get("investigation-inspect").dispatch("click");
  await flush();
  ui.get("investigation-operations").value = "2";
  ui.get("investigation-resume").dispatch("click");
  await flush();
  assert.deepEqual(ui.calls[1], {path: `/v1/investigations/${saved}/resume`, options: {method: "POST", role: "operation", body: {max_operations: 2}}});
  assert.match(ui.get("investigation-result").textContent, /Investigation finished/);
  assert.match(ui.get("investigation-result").textContent, /Unknown/);
  assert.equal(ui.get("investigation-resume").disabled, true);
});

test("uncertain mutation is not retried and blocks another start until inspection", async () => {
  const ui = controller(() => { const error = new Error("do not echo this"); error.uncertain = true; throw error; });
  ui.setCapabilities({model_configured: true});
  ui.get("investigation-start").dispatch("click");
  await flush();
  assert.equal(ui.calls.length, 1);
  assert.equal(ui.get("investigation-start").disabled, true);
  assert.equal(ui.get("investigation-inspect").disabled, false);
  assert.match(ui.get("investigation-result").textContent, /No retry was sent/);
  assert.doesNotMatch(ui.root.textContent, /do not echo this/);
});
