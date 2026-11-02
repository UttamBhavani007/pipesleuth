/* Explicit operator workflow. This module never receives session credentials. */
const UUID = /^[0-9a-f]{32}$/;
const SHA = /^[0-9a-f]{64}$/;
const ACTIONS = Object.freeze({
  rename_amount_column: 'Rename gross_total_minor to amount_minor',
  skip_identical_duplicates: 'Skip identical duplicate rows',
  quarantine_invalid_rows: 'Quarantine invalid rows',
});
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const fingerprint = value => typeof value === 'string' && SHA.test(value);
const identifier = value => typeof value === 'string' && UUID.test(value);

/** A presentation guard, not an alternative to the server's approval checks. */
export function repairView(record) {
  const invalid = {valid: false, canApprove: false, canExecute: false};
  if (!object(record) || !identifier(record.investigation_id)
      || !Number.isSafeInteger(record.revision) || record.revision < 1
      || !(record.pending === null || object(record.pending)) || !object(record.state)) return invalid;
  const state = record.state;
  const plan = state.plan;
  if (!['planned', 'approved', 'succeeded', 'failed'].includes(state.status)
      || !fingerprint(state.plan_sha256) || !fingerprint(state.input_fingerprint_sha256)
      || !object(plan) || plan.version !== 1 || !Object.hasOwn(ACTIONS, plan.action)
      || !identifier(plan.source_run_id) || !identifier(plan.target_run_id)
      || plan.source_run_id === plan.target_run_id
      || !object(plan.input) || !fingerprint(plan.input.source_sha256) || !fingerprint(plan.input.database_sha256)
      || !object(plan.effects) || !['rows_loaded', 'rows_quarantined', 'duplicates_skipped'].every(
        key => Number.isSafeInteger(plan.effects[key]) && plan.effects[key] >= 0 && plan.effects[key] <= 10000)
      || !Array.isArray(plan.warnings) || plan.warnings.length > 4
      || !plan.warnings.every(warning => typeof warning === 'string' && warning.length <= 1024)) return invalid;
  if (state.status === 'planned') {
    if (state.approval !== null || state.execution !== null) return invalid;
  } else {
    if (!object(state.approval) || state.approval.plan_sha256 !== state.plan_sha256
        || state.approval.input_fingerprint_sha256 !== state.input_fingerprint_sha256
        || !['trusted_api', 'trusted_cli', 'scripted_test'].includes(state.approval.origin)
        || typeof state.approval.approved_at !== 'string') return invalid;
    if (state.status === 'approved' ? state.execution !== null : !object(state.execution)) return invalid;
  }
  const pending = record.pending !== null;
  return {valid: true, id: record.investigation_id, revision: record.revision,
    status: state.status, pending, plan, planSha: state.plan_sha256,
    inputSha: state.input_fingerprint_sha256, execution: state.execution,
    canApprove: !pending && state.status === 'planned',
    canExecute: !pending && state.status === 'approved'};
}

export function approvalBody(record) {
  const view = repairView(record);
  if (!view.canApprove) throw new Error('repair_not_approvable');
  return {plan_sha256: view.planSha, input_fingerprint_sha256: view.inputSha};
}

export function receiptSummary(receipt) {
  if (!object(receipt)) return 'No execution receipt.';
  if (receipt.outcome_uncertain === true || receipt.committed === null)
    return 'Execution outcome is uncertain. Do not repeat the action.';
  if (receipt.status === 'verified' && receipt.committed === true && receipt.rolled_back === false
      && receipt.post_input_verified === true && object(receipt.verification)
      && receipt.verification.verified === true)
    return 'Derived database committed and independently verified; original input rechecked.';
  if (receipt.committed === true)
    return 'A derived database was committed, but the full verification did not succeed. Inspect the receipt; do not repeat.';
  if (receipt.rolled_back === true)
    return 'Execution failed and the transaction was rolled back. A new empty target directory may remain.';
  return 'Execution did not report a verified commit. Inspect the saved receipt before acting.';
}

function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
}
function field(label, control) {
  const wrapper = element('label', undefined, 'field');
  wrapper.append(element('span', label), control);
  return wrapper;
}
function button(label, type = 'button') {
  const node = element('button', label);
  node.type = type;
  return node;
}
function row(list, name, value) {
  list.append(element('dt', name), element('dd', String(value)));
}
function safeCode(error) {
  const value = error?.code || error?.message;
  return typeof value === 'string' && /^[a-z_]{1,64}$/.test(value) ? value : 'request_failed';
}

export function mountRepairs({root, client, getRunId, notify}) {
  let generation = 0;
  let busy = false;
  let connected = false;
  let record = null;
  let needsInspect = false;
  let availableActions = new Set(Object.keys(ACTIONS));
  root.replaceChildren();
  root.append(element('h2', 'Fixed repairs'), element('p',
    'Choose a supported action, review its exact plan, then approve and execute separately. Repairs create a new derived run.'),
    element('p', 'Approval records a caller acknowledgement; it does not establish human review. The model cannot approve repairs.', 'muted'));
  const planForm = element('form'); planForm.id = 'repair-plan-form';
  const action = element('select');
  action.id = 'repair-action';
  action.name = 'repair-action';
  for (const [value, label] of Object.entries(ACTIONS)) {
    const option = element('option', label); option.value = value; action.append(option);
  }
  const planButton = button('Create repair plan', 'submit');
  planForm.append(field('Repair action', action), planButton);
  const inspectForm = element('form'); inspectForm.id = 'repair-inspect-form';
  const repairId = element('input');
  repairId.id = 'repair-id'; repairId.name = 'repair-id'; repairId.type = 'text';
  repairId.autocomplete = 'off'; repairId.spellcheck = false; repairId.maxLength = 32;
  repairId.pattern = '[0-9a-f]{32}'; repairId.required = true;
  repairId.placeholder = '32-character repair ID';
  const inspectButton = button('Inspect saved repair', 'submit');
  inspectForm.append(field('Saved repair ID', repairId), inspectButton);
  const message = element('p', 'No repair selected.', 'status');
  message.id = 'repair-status'; message.setAttribute('role', 'status');
  const output = element('div'); output.id = 'repair-output';
  const acknowledgement = element('input');
  acknowledgement.type = 'checkbox'; acknowledgement.id = 'repair-reviewed';
  const acknowledgeLabel = element('label', undefined, 'check-field');
  acknowledgeLabel.append(acknowledgement, element('span',
    'I reviewed this displayed plan, warnings, new target and both fingerprints.'));
  const approveButton = button('Approve this exact plan'); approveButton.id = 'repair-approve';
  const executeButton = button('Execute approved repair'); executeButton.id = 'repair-execute';
  const controls = element('div', undefined, 'actions'); controls.append(approveButton, executeButton);
  root.append(planForm, inspectForm, message, output, acknowledgeLabel,
    element('p', 'Approval requires the separate approval capability loaded in this browser session.', 'muted'), controls);

  function updateControls() {
    const view = repairView(record);
    const blocked = !connected || busy;
    action.disabled = blocked;
    planButton.disabled = blocked || !availableActions.has(action.value);
    repairId.disabled = blocked; inspectButton.disabled = blocked;
    acknowledgement.disabled = blocked || needsInspect || !view.canApprove;
    approveButton.disabled = blocked || needsInspect || !view.canApprove || !acknowledgement.checked;
    executeButton.disabled = blocked || needsInspect || !view.canExecute;
  }
  function paint(value) {
    record = value;
    needsInspect = false;
    acknowledgement.checked = false;
    output.replaceChildren();
    const view = repairView(record);
    if (!view.valid) {
      message.textContent = 'Unsupported repair record. Approval and execution are disabled.';
      updateControls();
      return;
    }
    repairId.value = view.id;
    message.textContent = view.pending
      ? 'Pending operation: its outcome is unresolved. Approval and execution are blocked; do not repeat it.'
      : `Repair ${view.status} · revision ${view.revision}`;
    const heading = element('h3', 'Plan to review');
    const list = element('dl', undefined, 'facts');
    row(list, 'Repair ID', view.id);
    row(list, 'Action', ACTIONS[view.plan.action]);
    row(list, 'Source run', view.plan.source_run_id);
    row(list, 'New derived run', view.plan.target_run_id);
    row(list, 'Rows loaded', view.plan.effects.rows_loaded);
    row(list, 'Rows quarantined', view.plan.effects.rows_quarantined);
    row(list, 'Identical duplicates skipped', view.plan.effects.duplicates_skipped);
    row(list, 'Plan SHA-256', view.planSha);
    row(list, 'Input fingerprint SHA-256', view.inputSha);
    output.append(heading, list);
    const warnings = element('div', undefined, 'notice');
    warnings.append(element('h4', 'Warnings'));
    if (view.plan.warnings.length) {
      const items = element('ul');
      for (const warning of view.plan.warnings) items.append(element('li', warning));
      warnings.append(items);
    } else warnings.append(element('p', 'No additional warnings in this fixed plan. Preconditions still apply.'));
    output.append(warnings);
    if (view.execution !== null) output.append(element('p', receiptSummary(view.execution), 'notice'));
    const details = element('details');
    details.append(element('summary', 'Inspect complete saved record'), element('pre', JSON.stringify(record, null, 2)));
    output.append(details);
    updateControls();
  }
  async function request(operation, mutating = false) {
    if (!connected || busy) return;
    const epoch = generation;
    busy = true; updateControls(); message.textContent = 'Waiting for the local API…';
    try {
      const value = await operation();
      if (epoch !== generation) return;
      paint(value);
      notify('Repair record updated.');
    } catch (error) {
      if (epoch !== generation) return;
      if (mutating) { needsInspect = true; acknowledgement.checked = false; }
      const caution = mutating
        ? ' No automatic retry. Inspect the saved repair before acting again; a lost response does not prove rollback.'
        : ' Check the ID and connection, then inspect again when ready.';
      message.textContent = `Repair request failed (${safeCode(error)}).${caution}`;
      notify(message.textContent, 'error');
    } finally {
      if (epoch === generation) { busy = false; updateControls(); }
    }
  }
  planForm.addEventListener('submit', event => {
    event.preventDefault();
    request(() => {
      const runId = getRunId();
      if (!identifier(runId) || !availableActions.has(action.value)) throw new Error('invalid_repair_request');
      return client.request('/v1/repairs', {method: 'POST', role: 'operation', body: {run_id: runId, action: action.value}});
    }, true);
  });
  inspectForm.addEventListener('submit', event => {
    event.preventDefault();
    request(() => {
      const id = repairId.value.trim();
      if (!identifier(id)) throw new Error('invalid_repair_id');
      return client.request(`/v1/repairs/${id}`);
    });
  });
  repairId.addEventListener('input', () => {
    if (busy) return;
    if (record && repairId.value.trim() !== record.investigation_id) {
      record = null; needsInspect = false; acknowledgement.checked = false;
      output.replaceChildren();
      message.textContent = 'Inspect this saved repair before approving or executing it.';
    }
    updateControls();
  });
  action.addEventListener('change', updateControls);
  acknowledgement.addEventListener('change', updateControls);
  approveButton.addEventListener('click', () => {
    if (approveButton.disabled || !acknowledgement.checked) return;
    const current = repairView(record);
    if (!current.canApprove || needsInspect) return;
    const body = approvalBody(record);
    request(() => client.request(`/v1/repairs/${current.id}/approve`,
      {method: 'POST', role: 'approval', body}), true);
  });
  executeButton.addEventListener('click', () => {
    if (executeButton.disabled) return;
    const current = repairView(record);
    if (!current.canExecute || needsInspect) return;
    request(() => client.request(`/v1/repairs/${current.id}/execute`,
      {method: 'POST', role: 'operation', body: {}}), true);
  });
  updateControls();
  return {
    reset() {
      generation += 1; busy = false; connected = false; record = null; needsInspect = false;
      repairId.value = ''; acknowledgement.checked = false; output.replaceChildren();
      message.textContent = 'No repair selected.'; updateControls();
    },
    setCapabilities(capabilities) {
      connected = true;
      availableActions = new Set(Array.isArray(capabilities?.repair_actions)
        ? capabilities.repair_actions.filter(value => Object.hasOwn(ACTIONS, value)) : []);
      for (const option of action.options) option.disabled = !availableActions.has(option.value);
      updateControls();
    },
  };
}
