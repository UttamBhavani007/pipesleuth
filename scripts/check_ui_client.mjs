/* Actual loopback HTTP probe through the unmodified browser-client module.
 * This does not drive a browser or measure repaint. Only the healthy checklist
 * uses POST; it reads evidence and does not modify pipeline/checkpoint state.
 */
import {createHash} from 'node:crypto';
import {readFile, readdir, writeFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {dirname, join, relative} from 'node:path';
import {createClient} from '../src/pipesleuth/web/core.mjs';
import {diagnosisView} from '../src/pipesleuth/web/investigations.mjs';
import {repairView} from '../src/pipesleuth/web/repairs.mjs';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const workspace = join(root, 'reports/local/ui-smoke');
const output = join(root, 'reports/local/ui-client-probe.json');
const sources = ['src/pipesleuth/web/core.mjs', 'src/pipesleuth/web/investigations.mjs',
  'src/pipesleuth/web/repairs.mjs', 'src/pipesleuth/api.py', 'src/pipesleuth/api_security.py',
  'src/pipesleuth/api_contracts.py', 'src/pipesleuth/checklist.py', 'src/pipesleuth/evidence.py',
  'scripts/check_ui_client.mjs'];
const digest = value => createHash('sha256').update(value).digest('hex');
async function hashes(paths) { return Object.fromEntries(await Promise.all(paths.map(async path => [path, digest(await readFile(join(root, path)))]))); }
async function snapshot(directory) {
  const files = {};
  async function visit(current) {
    for (const entry of await readdir(current, {withFileTypes: true})) {
      const path = join(current, entry.name);
      if (entry.isSymbolicLink()) throw new Error('unsafe_workspace');
      if (entry.isDirectory()) await visit(path);
      else if (entry.isFile()) files[relative(directory, path)] = digest(await readFile(path));
    }
  }
  await visit(directory);
  return JSON.stringify(Object.entries(files).sort(([a], [b]) => a.localeCompare(b)));
}
function safeCode(error) { return typeof error?.code === 'string' && /^[a-z_]{1,64}$/.test(error.code) ? error.code : 'probe_failed'; }

async function main() {
  if (process.argv.length !== 4 || process.argv[2] !== '--repair-id' || !/^[0-9a-f]{32}$/.test(process.argv[3])) throw new Error('invalid_arguments');
  const repairId = process.argv[3];
  const before = await snapshot(workspace), bound = await hashes(sources);
  const sessionText = await readFile(join(workspace, 'private-session.json'), 'utf8');
  const session = JSON.parse(sessionText);
  const manifest = JSON.parse(await readFile(join(workspace, 'manifest.json'), 'utf8'));
  const incident = manifest.cases.find(item => item.case_id === 'case-01');
  const healthy = manifest.cases.find(item => item.case_id === 'case-04');
  if (!incident || !healthy || !/^[0-9a-f]{32}$/.test(incident.run_id) || !/^[0-9a-f]{32}$/.test(healthy.run_id)
      || healthy.observed_pipeline_status !== 'succeeded') throw new Error('invalid_manifest');
  const mutationStates = [];
  const client = createClient({origin: session.base_url, timeoutMs: 10000, onMutationChange: busy => mutationStates.push(busy)});
  client.connect(sessionText);
  const report = {generated_at: new Date().toISOString(),
    scope: 'Exact JavaScript client against the actual local API. This is not a browser rendering, human review or model-quality check.',
    runtime: {node_version: process.versions.node, platform: process.platform, architecture: process.arch},
    source_sha256: bound, responses: [], actual_model_generation_requests: 0,
    inference_scope: 'Only evidence reads and deterministic healthy checklist; no local-model or agent request.',
    browser_driven: false};
  async function check(name, method, path, options, verify) {
    const started = performance.now();
    const entry = {name, method, elapsed_ms: null, response_json_bytes: null, passed: false, error_code: null};
    try {
      const value = await client.request(path, options);
      entry.response_json_bytes = Buffer.byteLength(JSON.stringify(value));
      entry.checks = verify(value);
      entry.passed = Object.values(entry.checks).every(value => value === true);
    } catch (error) { entry.error_code = safeCode(error); }
    finally { entry.elapsed_ms = Math.round((performance.now() - started) * 1000) / 1000; report.responses.push(entry); }
  }
  try {
    await check('capabilities', 'GET', '/v1/capabilities', {}, value => ({fixed_methods: JSON.stringify(value.diagnosis_methods) === JSON.stringify(['checklist', 'local', 'agent']), known_repair_actions: Array.isArray(value.repair_actions) && value.repair_actions.length === 3}));
    await check('schema_evidence', 'GET', `/v1/runs/${incident.run_id}/evidence/inspect_schema`, {}, value => ({same_run: value.run_id === incident.run_id, expected_tool: value.tool === 'inspect_schema', content_present: value.data !== null && typeof value.data === 'object'}));
    await check('log_evidence', 'GET', `/v1/runs/${incident.run_id}/evidence/read_logs`, {}, value => ({same_run: value.run_id === incident.run_id, expected_tool: value.tool === 'read_logs', events_present: Array.isArray(value.data?.events) && value.data.events.length > 0}));
    await check('completed_saved_repair', 'GET', `/v1/repairs/${repairId}`, {}, value => {
      const view = repairView(value);
      return {same_record: value.investigation_id === repairId, presentation_valid: view.valid,
        succeeded: value.state.status === 'succeeded', no_replay: view.canExecute === false && view.canApprove === false,
        verified_receipt: view.execution?.verification?.verified === true && view.execution?.post_input_verified === true};
    });
    await check('healthy_checklist', 'POST', `/v1/runs/${healthy.run_id}/diagnosis`, {method: 'POST', role: 'operation', body: {method: 'checklist'}}, value => {
      const view = diagnosisView(value);
      return {same_run: value.run_id === healthy.run_id, method_is_checklist: value.method === 'deterministic_checklist',
        no_incident: value.status === 'no_incident' && value.findings.length === 0,
        no_generation: view.modelCalls === 0, not_experimental_model: view.experimental === false};
    });
  } finally { client.disconnect(); }
  report.workspace_files_unchanged = before === await snapshot(workspace);
  report.bound_sources_unchanged = JSON.stringify(bound) === JSON.stringify(await hashes(sources));
  report.mutation_callback_balanced = mutationStates.filter(Boolean).length === 1 && mutationStates.at(-1) === false;
  report.summary = {responses: report.responses.length, responses_passed: report.responses.filter(item => item.passed).length,
    actual_model_generation_requests: 0, browser_rendering_measured: false, workspace_files_unchanged: report.workspace_files_unchanged};
  report.passed = report.responses.length === 5 && report.responses.every(item => item.passed)
    && report.workspace_files_unchanged && report.bound_sources_unchanged && report.mutation_callback_balanced;
  const content = JSON.stringify(report, null, 2) + '\n';
  if (['/Users/', '/private/', '/tmp/', 'Bearer ', session.read_token, session.operation_token, session.approval_token].some(value => content.includes(value))) throw new Error('private_report_content');
  await writeFile(output, content, {flag: 'wx', mode: 0o600});
  console.log(JSON.stringify({...report.summary, passed: report.passed}));
  process.exitCode = report.passed ? 0 : 1;
}
main().catch(error => { console.error(JSON.stringify({error_code: safeCode(error)})); process.exitCode = 1; });
