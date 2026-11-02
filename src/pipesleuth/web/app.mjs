import {createClient, runId, ApiError} from './core.mjs';
import {mountInvestigations} from './investigations.mjs';
import {mountRepairs} from './repairs.mjs';
const $ = id => document.getElementById(id);
const client = createClient({origin: location.origin, onMutationChange: busy => {
  $('workspace').disabled = busy || !client.connected;
  $('session-file').disabled = busy;
  $('disconnect').disabled = busy || !client.connected;
}});
let currentRun = null, generation = 0, evidenceRequest = 0, capabilities = null;
function notify(message, kind = 'info') { $('notice').textContent = message; $('notice').dataset.kind = kind; }
function text(tag, value, className) { const n = document.createElement(tag); n.textContent = String(value); if (className) n.className = className; return n; }
function jsonDetails(title, value) { const d = document.createElement('details'); d.append(text('summary', title), text('pre', JSON.stringify(value, null, 2))); return d; }
const modules = [mountInvestigations({root: $('investigations'), client, getRunId, notify}), mountRepairs({root: $('repairs'), client, getRunId, notify})];
function getRunId() { if (!currentRun) throw new ApiError('invalid_request'); return currentRun; }
function clearRun() {
  generation++; evidenceRequest++; currentRun = null;
  $('run-summary').replaceChildren(text('p', 'Your run summary will appear here.', 'empty'));
  $('evidence-content').replaceChildren(text('p', 'Open a run to inspect its events, schema and validation.', 'empty'));
  for (const module of modules) { module.reset(); if (capabilities) module.setCapabilities(capabilities); }
}
function disconnect() {
  client.disconnect(); capabilities = null; clearRun(); $('workspace').disabled = true;
  $('disconnect').disabled = true; $('approval-file').disabled = true;
  $('connection-status').textContent = 'Disconnected · no access loaded';
  $('approval-status').textContent = 'Approval access locked';
}
async function readFile(input) {
  const file = input.files?.[0];
  input.value = '';
  if (!file || file.size > 4096) throw new ApiError('invalid_session');
  return file.text();
}
$('session-file').addEventListener('change', async event => {
  disconnect(); const ticket = generation;
  try {
    const raw = await readFile(event.target); if (ticket !== generation) return;
    client.connect(raw);
    const caps = await client.request('/v1/capabilities'); if (ticket !== generation) return;
    capabilities = caps;
    $('workspace').disabled = false; $('disconnect').disabled = false; $('approval-file').disabled = false;
    $('connection-status').textContent = 'Connected · read and operation access loaded';
    for (const module of modules) module.setCapabilities(caps);
    notify('Connected locally. Open a run to inspect its recorded evidence.');
  } catch (error) { if (ticket !== generation) return; disconnect(); notify(error.message, 'error'); }
});
$('approval-file').addEventListener('change', async event => {
  const ticket = generation;
  try { const raw = await readFile(event.target); if (ticket !== generation) return; client.unlockApproval(raw); $('approval-status').textContent = 'Approval access loaded for this session'; notify('Approval access loaded. Review and acknowledge the exact plan before approving.'); }
  catch (error) { if (ticket === generation) notify(error.message, 'error'); }
});
$('disconnect').addEventListener('click', () => { disconnect(); $('run-id').value = ''; notify('Disconnected. Access and displayed records were cleared from this tab.'); });
$('run-id').addEventListener('input', clearRun);
function renderRun(value) {
  const grid = document.createElement('div'); grid.className = 'stats';
  for (const [label, data] of [['Status', value.status], ['Rows loaded', value.rows_loaded], ['Quarantined', value.rows_quarantined ?? 0], ['Duplicates skipped', value.duplicates_skipped ?? 0]]) {
    const n = document.createElement('div'); n.className = 'stat' + (label === 'Status' && value.status === 'failed' ? ' error' : ''); n.append(text('span', label), text('strong', data)); grid.append(n);
  }
  $('run-summary').replaceChildren(grid);
  if (value.error) $('run-summary').append(text('p', value.error, 'notice'));
  $('run-summary').append(jsonDetails('Inspect run summary', value));
}
function renderEvidence(value) {
  const target = $('evidence-content');
  target.replaceChildren(text('p', `${value.tool} · ${value.observed_at} · ${value.evidence_id}`, 'evidence-meta'));
  if (value.truncated) target.append(text('p', 'This evidence is truncated. It is not a complete record.', 'notice'));
  const events = value.data?.events;
  if (value.tool === 'read_logs' && Array.isArray(events)) {
    const list = document.createElement('ol'); list.className = 'timeline';
    for (const event of events) { const item = document.createElement('li'); item.append(text('time', event.created_at ?? event.occurred_at ?? event.timestamp ?? ''), text('strong', event.event ?? event.event_type ?? event.code ?? 'Recorded event')); item.append(text('p', event.message ?? event.details?.message ?? JSON.stringify(event.details ?? event.data ?? event))); list.append(item); }
    if (!events.length) target.append(text('p', 'No events were returned in this bounded read.', 'muted'));
    target.append(list);
  } else target.append(text('pre', JSON.stringify(value.data, null, 2)));
  target.append(jsonDetails('Inspect evidence and content fingerprint', value));
}
async function showEvidence(tool, ticket = generation) {
  const order = ++evidenceRequest;
  const id = getRunId(); const value = await client.request(`/v1/runs/${id}/evidence/${tool}`);
  if (ticket !== generation || id !== currentRun || order !== evidenceRequest) return;
  renderEvidence(value);
}
$('run-form').addEventListener('submit', async event => {
  event.preventDefault(); clearRun(); const ticket = generation; $('open-run').disabled = true;
  try {
    const id = runId($('run-id').value.trim()); const value = await client.request(`/v1/runs/${id}`);
    if (ticket !== generation) return;
    currentRun = id; renderRun(value); notify('Run opened. The event timeline below is a bounded read of recorded facts.');
    await showEvidence('read_logs', ticket);
  } catch (error) { if (ticket === generation) notify(error.message, 'error'); }
  finally { $('open-run').disabled = false; }
});
for (const button of document.querySelectorAll('[data-tool]')) button.addEventListener('click', async () => {
  const ticket = generation, order = ++evidenceRequest; button.disabled = true;
  try { const id = getRunId(); const value = await client.request(`/v1/runs/${id}/evidence/${button.dataset.tool}`); if (ticket === generation && order === evidenceRequest) renderEvidence(value); }
  catch (error) { if (ticket === generation) notify(error.message, 'error'); }
  finally { button.disabled = false; }
});
window.addEventListener('pagehide', disconnect);
disconnect();
