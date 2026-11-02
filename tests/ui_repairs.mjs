import assert from 'node:assert/strict';
import {test} from 'node:test';
import {approvalBody, repairView, receiptSummary, mountRepairs} from '../src/pipesleuth/web/repairs.mjs';

function planned() {
  return {investigation_id: 'a'.repeat(32), revision: 1, pending: null,
    state: {status: 'planned', plan_sha256: 'b'.repeat(64), input_fingerprint_sha256: 'c'.repeat(64),
      approval: null, execution: null,
      plan: {version: 1, action: 'rename_amount_column', source_run_id: 'd'.repeat(32), target_run_id: 'e'.repeat(32),
        input: {source_sha256: 'f'.repeat(64), database_sha256: '0'.repeat(64)},
        effects: {rows_loaded: 2, rows_quarantined: 0, duplicates_skipped: 0},
        warnings: ['Same-unit interpretation requires review.']}}};
}
function approved() {
  const record = planned(); record.state.status = 'approved';
  record.state.approval = {plan_sha256: record.state.plan_sha256,
    input_fingerprint_sha256: record.state.input_fingerprint_sha256,
    origin: 'trusted_api', approved_at: '2026-11-02T20:00:00Z'};
  return record;
}

test('planned record enables only exact-plan approval and sends precisely two hashes', () => {
  const record = planned();
  assert.equal(repairView(record).canApprove, true);
  assert.equal(repairView(record).canExecute, false);
  assert.deepEqual(approvalBody(record), {plan_sha256: 'b'.repeat(64), input_fingerprint_sha256: 'c'.repeat(64)});
});
test('approval permits separate execution, never second approval', () => {
  const record = approved();
  assert.equal(repairView(record).canApprove, false);
  assert.equal(repairView(record).canExecute, true);
  assert.throws(() => approvalBody(record), /repair_not_approvable/);
});
test('any unresolved pending operation blocks both actions', () => {
  for (const record of [planned(), approved()]) {
    record.pending = {operation: 'execute_repair', token: 'f'.repeat(32)};
    const view = repairView(record);
    assert.equal(view.pending, true); assert.equal(view.canApprove, false); assert.equal(view.canExecute, false);
  }
});
test('malformed identity, fingerprints, actions, counters or approval never enable mutations', () => {
  const mutations = [r => r.investigation_id = '../secret', r => r.revision = true,
    r => r.state.plan_sha256 = 'B'.repeat(64), r => r.state.plan.action = 'run_sql',
    r => r.state.plan.target_run_id = r.state.plan.source_run_id, r => r.state.plan.input.database_sha256 = '',
    r => r.state.plan.effects.rows_loaded = -1, r => r.state.plan.effects.rows_loaded = true,
    r => r.state.plan.warnings = [null], r => r.pending = false,
    r => r.state.approval = {plan_sha256: 'b'.repeat(64)}];
  for (const mutation of mutations) {
    const record = planned(); mutation(record);
    assert.deepEqual(repairView(record), {valid: false, canApprove: false, canExecute: false});
  }
  const record = approved(); record.state.approval.input_fingerprint_sha256 = '1'.repeat(64);
  assert.equal(repairView(record).valid, false);
});
test('terminal failed and succeeded records remain inspectable without replay controls', () => {
  for (const status of ['succeeded', 'failed']) {
    const record = approved(); record.state.status = status; record.state.execution = {status: 'failed'};
    const view = repairView(record);
    assert.equal(view.valid, true); assert.equal(view.canExecute, false); assert.equal(view.canApprove, false);
  }
});
test('receipt summary distinguishes exact verification, committed failure, rollback and uncertainty', () => {
  const verified = {status: 'verified', committed: true, rolled_back: false, outcome_uncertain: false,
    post_input_verified: true, verification: {verified: true}};
  assert.match(receiptSummary(verified), /independently verified/);
  assert.match(receiptSummary({...verified, post_input_verified: false}), /full verification did not succeed/);
  assert.match(receiptSummary({...verified, verification: {verified: false}}), /full verification did not succeed/);
  assert.match(receiptSummary({committed: false, rolled_back: true}), /rolled back/);
  assert.match(receiptSummary({...verified, committed: null}), /uncertain/);
  assert.match(receiptSummary({...verified, outcome_uncertain: true}), /uncertain/);
  assert.match(receiptSummary(null), /No execution receipt/);
});

// Controller seam: real layout and browser fetch are checked separately.
class Element {
  constructor(tag) { this.tag = tag; this.children = []; this.listeners = new Map(); this.disabled = false;
    this.id = ''; this.ownText = ''; this.checked = false; this.ownValue = undefined; }
  setAttribute() {}
  get options() { return this.children.filter(child => child.tag === 'option'); }
  get value() { return this.ownValue ?? (this.tag === 'select' ? this.options[0]?.value ?? '' : ''); }
  set value(value) { this.ownValue = value; }
  set textContent(value) { this.ownText = String(value); this.children = []; }
  get textContent() { return this.ownText + this.children.map(child => child.textContent).join(''); }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.ownText = ''; this.children = children; }
  addEventListener(type, listener) { this.listeners.set(type, listener); }
  dispatch(type) { if (!this.disabled) this.listeners.get(type)?.({target: this, preventDefault() {}}); }
  find(id) { return this.id === id ? this : this.children.map(child => child.find(id)).find(Boolean); }
}
const flush = () => new Promise(resolve => setTimeout(resolve, 0));
function controller(responder) {
  globalThis.document = {createElement: tag => new Element(tag)};
  const root = new Element('section'), calls = [], notices = [];
  const client = {request: async (path, options) => { calls.push({path, options}); return responder(path, options); }};
  const mounted = mountRepairs({root, client, getRunId: () => 'd'.repeat(32), notify: (...args) => notices.push(args)});
  return {root, calls, notices, ...mounted, get: id => root.find(id),
    connect() { mounted.setCapabilities({repair_actions: ['rename_amount_column', 'skip_identical_duplicates', 'quarantine_invalid_rows']}); }};
}
async function inspect(ui) {
  ui.get('repair-id').value = 'a'.repeat(32);
  ui.get('repair-inspect-form').dispatch('submit'); await flush();
}

test('editing a saved ID clears the displayed plan and acknowledgement before any further action', async () => {
  const ui = controller(() => planned()); ui.connect(); await inspect(ui);
  ui.get('repair-reviewed').checked = true; ui.get('repair-reviewed').dispatch('change');
  assert.equal(ui.get('repair-approve').disabled, false);
  ui.get('repair-id').value = 'f'.repeat(32); ui.get('repair-id').dispatch('input');
  assert.equal(ui.get('repair-reviewed').checked, false);
  assert.equal(ui.get('repair-approve').disabled, true);
  assert.equal(ui.get('repair-execute').disabled, true);
  assert.equal(ui.get('repair-output').textContent, '');
  ui.get('repair-approve').dispatch('click'); assert.equal(ui.calls.length, 1);
});
test('reset disables repair controls and ignores a late response from the old session', async () => {
  let complete;
  const ui = controller(() => new Promise(resolve => { complete = resolve; }));
  assert.equal(ui.get('repair-id').disabled, true);
  ui.connect(); ui.get('repair-id').value = 'a'.repeat(32);
  ui.get('repair-inspect-form').dispatch('submit');
  ui.reset(); complete(planned()); await flush();
  assert.equal(ui.get('repair-id').disabled, true);
  assert.equal(ui.get('repair-action').disabled, true);
  assert.equal(ui.get('repair-output').textContent, '');
  assert.equal(ui.calls.length, 1); assert.equal(ui.notices.length, 0);
});
test('approval and execution require separate clicks with exact roles, body and acknowledgement', async () => {
  const ui = controller(path => path.endsWith('/approve') ? approved() : planned());
  ui.connect(); await inspect(ui);
  assert.equal(ui.get('repair-approve').disabled, true);
  ui.get('repair-reviewed').checked = true; ui.get('repair-reviewed').dispatch('change');
  ui.get('repair-approve').dispatch('click'); await flush();
  assert.deepEqual(ui.calls[1], {path: '/v1/repairs/' + 'a'.repeat(32) + '/approve',
    options: {method: 'POST', role: 'approval', body: {plan_sha256: 'b'.repeat(64), input_fingerprint_sha256: 'c'.repeat(64)}}});
  assert.equal(ui.get('repair-reviewed').checked, false);
  assert.equal(ui.get('repair-execute').disabled, false);
  assert.equal(ui.calls.length, 2);
  ui.get('repair-execute').dispatch('click'); await flush();
  assert.deepEqual(ui.calls[2], {path: '/v1/repairs/' + 'a'.repeat(32) + '/execute',
    options: {method: 'POST', role: 'operation', body: {}}});
});
test('failed approval is never retried and actions remain blocked until explicit saved inspection', async () => {
  const ui = controller(path => {
    if (path.endsWith('/approve')) { const error = new Error('secret must not be displayed'); error.code = 'no_approval_authority'; throw error; }
    return planned();
  }); ui.connect(); await inspect(ui);
  ui.get('repair-reviewed').checked = true; ui.get('repair-reviewed').dispatch('change');
  ui.get('repair-approve').dispatch('click'); await flush();
  assert.equal(ui.calls.length, 2); assert.equal(ui.get('repair-approve').disabled, true);
  assert.equal(ui.get('repair-reviewed').checked, false);
  assert.match(ui.get('repair-status').textContent, /No automatic retry/);
  assert.doesNotMatch(ui.root.textContent, /secret must not be displayed/);
  await inspect(ui); assert.equal(ui.get('repair-reviewed').disabled, false);
});
