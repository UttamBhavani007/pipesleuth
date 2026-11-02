import assert from 'node:assert/strict';
import test from 'node:test';
import {ApiError, createClient, parseSession, runId} from '../src/pipesleuth/web/core.mjs';

const origin = 'http://127.0.0.1:8765';
const id = 'a'.repeat(32);
const session = {base_url: origin, read_token: 'r'.repeat(40), operation_token: 'o'.repeat(40), approval_token: 'a'.repeat(40)};
const encoded = value => JSON.stringify(value ?? session);
const json = (value = {ok: true}, status = 200) => new Response(JSON.stringify(value), {status, headers: {'Content-Type': 'application/json; charset=utf-8'}});
function clientWith(fetchImpl, options = {}) {
  const client = createClient({origin, fetchImpl, ...options});
  client.connect(encoded());
  return client;
}
async function rejectsCode(operation, code, uncertain = false) {
  await assert.rejects(operation, error => error instanceof ApiError && error.code === code && error.uncertain === uncertain);
}
function deferred() { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return {promise, resolve, reject}; }

test('session requires a closed object, matching literal loopback origin and distinct bounded tokens', () => {
  assert.deepEqual(parseSession(encoded(), origin), session);
  const invalid = [null, [], {}, {...session, extra: 'ignored'}, {...session, read_token: 'short'},
    {...session, read_token: session.operation_token}, {...session, approval_token: 'x'.repeat(257)},
    {...session, read_token: 'r'.repeat(39) + '\n'}, {...session, base_url: origin + '/'},
    {...session, base_url: 'http://localhost:8765'}, {...session, base_url: 'https://127.0.0.1:8765'}];
  for (const value of invalid) assert.throws(() => parseSession(JSON.stringify(value), origin), {code: 'invalid_session'});
  for (const value of ['', 'not json', ' '.repeat(4097), 42]) assert.throws(() => parseSession(value, origin), {code: 'invalid_session'});
  for (const value of ['http://127.0.0.1:80', 'http://127.0.0.1:65536', 'http://127.0.0.1:08765', 'http://localhost:8765', 'http://example.org:8765', 'http://127.0.0.1:8765/']) {
    assert.throws(() => parseSession(encoded({...session, base_url: value}), value), {code: 'invalid_session'});
  }
});

test('connection imports no approval authority and role routing cannot substitute capabilities', async () => {
  const calls = [];
  const client = clientWith(async (url, request) => { calls.push({url, request}); return json(); });
  assert.equal(client.connected, true); assert.equal(client.approvalUnlocked, false);
  await client.request('/v1/capabilities');
  await client.request(`/v1/runs/${id}/diagnosis`, {method: 'POST', role: 'operation', body: {method: 'checklist'}});
  await rejectsCode(client.request(`/v1/repairs/${id}/approve`, {method: 'POST', role: 'approval', body: {}}), 'no_approval_authority');
  await rejectsCode(client.request(`/v1/repairs/${id}/approve`, {method: 'POST', role: 'operation', body: {}}), 'invalid_request');
  await rejectsCode(client.request(`/v1/repairs/${id}/execute`, {method: 'POST', role: 'approval', body: {}}), 'invalid_request');
  assert.equal(calls.length, 2);
  assert.equal(calls[0].request.headers.Authorization, `Bearer ${session.read_token}`);
  assert.equal(calls[1].request.headers.Authorization, `Bearer ${session.operation_token}`);
  assert.ok(calls.every(call => !call.url.includes('token')));
});

test('approval requires explicit matching session import and reconnect or disconnect relocks it', async () => {
  const calls = [];
  const client = clientWith(async (url, request) => { calls.push(request); return json(); });
  assert.throws(() => client.unlockApproval(encoded({...session, operation_token: 'z'.repeat(40)})), {code: 'invalid_session'});
  assert.equal(client.approvalUnlocked, false);
  client.unlockApproval(encoded());
  assert.equal(client.approvalUnlocked, true);
  await client.request(`/v1/repairs/${id}/approve`, {method: 'POST', role: 'approval', body: {plan_sha256: 'f'.repeat(64), input_fingerprint_sha256: 'b'.repeat(64)}});
  assert.equal(calls[0].headers.Authorization, `Bearer ${session.approval_token}`);
  client.connect(encoded()); assert.equal(client.approvalUnlocked, false);
  client.unlockApproval(encoded()); client.disconnect();
  assert.equal(client.connected, false); assert.equal(client.approvalUnlocked, false);
  assert.throws(() => client.unlockApproval(encoded()), {code: 'invalid_session'});
  await rejectsCode(client.request('/v1/capabilities'), 'no_session');
});

test('closed route allowlist rejects foreign URLs, arbitrary paths, mutations by GET and query injection before fetch', async () => {
  let calls = 0;
  const client = clientWith(async () => { calls++; return json(); });
  const paths = ['https://example.org/v1/capabilities', '//example.org/v1/capabilities', '/v1/capabilities?token=secret',
    `/v1/runs/${id}/evidence/read_logs?limit=21`, `/v1/runs/${id}/evidence/read_logs?limit=1&limit=2`,
    `/v1/runs/${id}/evidence/inspect_schema?sql=select`, `/v1/runs/${id}/evidence/read_logs?limit=%31`,
    `/v1/repairs/${id}/execute`, '/.runtime/api-session.json', '/v1/runs/../../secret', '/v1/runs/' + id.toUpperCase()];
  for (const path of paths) await rejectsCode(client.request(path), 'invalid_request');
  for (const method of ['PUT', 'DELETE', 'PATCH', 'HEAD']) await rejectsCode(client.request('/v1/capabilities', {method}), 'invalid_request');
  assert.equal(calls, 0);
});

test('every request uses a same-origin private transport with redirects, cookies, cache and referrer disabled', async () => {
  const calls = [];
  const client = clientWith(async (url, request) => { calls.push({url, request}); return json(); });
  await client.request(`/v1/runs/${id}/evidence/read_logs?limit=20`);
  await client.request('/v1/investigations', {method: 'POST', role: 'operation', body: {run_id: id}});
  for (const {url, request} of calls) {
    assert.equal(new URL(url).origin, origin);
    assert.equal(request.mode, 'same-origin'); assert.equal(request.credentials, 'omit');
    assert.equal(request.redirect, 'error'); assert.equal(request.cache, 'no-store');
    assert.equal(request.referrerPolicy, 'no-referrer'); assert.ok(request.signal instanceof AbortSignal);
    assert.ok(Object.keys(request.headers).every(key => ['Authorization', 'Content-Type'].includes(key)));
  }
  assert.equal(calls[0].request.body, undefined);
  assert.deepEqual(JSON.parse(calls[1].request.body), {run_id: id});
  assert.equal(calls[1].request.headers['Content-Type'], 'application/json');
});

test('mutations require an object within the 4 KiB UTF-8 request budget before admission', async () => {
  let calls = 0;
  const client = clientWith(async () => { calls++; return json(); });
  for (const body of [null, [], 'value', true, {text: 'x'.repeat(4096)}, {text: 'ü'.repeat(2048)}]) {
    await rejectsCode(client.request('/v1/repairs', {method: 'POST', role: 'operation', body}), 'invalid_request');
  }
  assert.equal(calls, 0);
  await client.request('/v1/repairs', {method: 'POST', role: 'operation', body: {}});
  assert.equal(calls, 1);
});

test('response parsing rejects HTML, invalid UTF-8, malformed JSON and non-object results', async () => {
  const responses = [new Response('<h1>secret</h1>', {headers: {'Content-Type': 'text/html'}}),
    new Response(new Uint8Array([0xff]), {headers: {'Content-Type': 'application/json'}}),
    new Response('{} {}', {headers: {'Content-Type': 'application/json'}}), json([]), json(null), json('literal')];
  const client = clientWith(async () => responses.shift());
  for (let i = 0; i < 6; i++) await rejectsCode(client.request('/v1/capabilities'), 'invalid_response');
});

test('response body is capped while streaming and cancels oversized input', async () => {
  let canceled = false, chunks = 0;
  const stream = new ReadableStream({pull(controller) { chunks++; controller.enqueue(new Uint8Array(262145)); }, cancel() { canceled = true; }});
  const client = clientWith(async () => new Response(stream, {headers: {'Content-Type': 'application/json'}}));
  await rejectsCode(client.request('/v1/capabilities'), 'invalid_response');
  assert.equal(canceled, true); assert.ok(chunks <= 3);
});

test('network failures distinguish uncertain submitted operations from failed reads without retry', async () => {
  let calls = 0;
  const client = clientWith(async () => { calls++; throw new TypeError('provider detail or local path'); });
  await rejectsCode(client.request('/v1/capabilities'), 'connection_lost');
  await rejectsCode(client.request('/v1/repairs', {method: 'POST', role: 'operation', body: {}}), 'connection_lost', true);
  assert.equal(calls, 2);
});

test('HTTP errors expose bounded safe codes and ambiguous server failures remain uncertain', async () => {
  let response = json({error: {code: 'model_unconfigured', detail: 'do not echo'}}, 409);
  const client = clientWith(async () => response);
  await assert.rejects(client.request(`/v1/runs/${id}/diagnosis`, {method: 'POST', role: 'operation', body: {method: 'local'}}), error => {
    assert.equal(error.code, 'model_unconfigured'); assert.equal(error.uncertain, false);
    assert.ok(!error.message.includes('do not echo')); return true;
  });
  response = json({error: {code: 'busy'}}, 503);
  await rejectsCode(client.request('/v1/repairs', {method: 'POST', role: 'operation', body: {}}), 'busy', true);
  response = json({error: {code: '../../secret or token'}}, 500);
  await rejectsCode(client.request('/v1/repairs', {method: 'POST', role: 'operation', body: {}}), 'invalid_response', true);
});

test('a competing mutation fails immediately while bounded reads remain usable', async () => {
  const waiting = deferred(); let posts = 0;
  const client = clientWith(async (_url, request) => { if (request.method === 'POST') { posts++; return posts === 1 ? waiting.promise : json(); } return json(); });
  const first = client.request('/v1/repairs', {method: 'POST', role: 'operation', body: {}});
  await rejectsCode(client.request('/v1/investigations', {method: 'POST', role: 'operation', body: {}}), 'client_busy');
  assert.deepEqual(await client.request('/v1/capabilities'), {ok: true});
  assert.equal(posts, 1); waiting.resolve(json()); await first;
  await client.request('/v1/repairs', {method: 'POST', role: 'operation', body: {}});
  assert.equal(posts, 2);
});

test('disconnect aborts active transport and rejects a late response even when a fixture ignores the abort', async () => {
  const waiting = deferred(); let signal;
  const client = clientWith(async (_url, request) => { signal = request.signal; return waiting.promise; });
  const pending = client.request('/v1/capabilities');
  client.disconnect(); assert.equal(signal.aborted, true);
  waiting.resolve(json({sensitive: 'late stale value'}));
  await rejectsCode(pending, 'session_changed');
  assert.equal(client.connected, false);
});

test('reconnect rejects old mutation completion without unlocking the new mutation guard', async () => {
  const old = deferred(), current = deferred(); let calls = 0;
  const client = clientWith(async () => (++calls === 1 ? old.promise : current.promise));
  const first = client.request('/v1/repairs', {method: 'POST', role: 'operation', body: {}});
  client.connect(encoded());
  const second = client.request('/v1/repairs', {method: 'POST', role: 'operation', body: {}});
  old.resolve(json()); await rejectsCode(first, 'session_changed', true);
  await rejectsCode(client.request('/v1/repairs', {method: 'POST', role: 'operation', body: {}}), 'client_busy');
  current.resolve(json()); await second;
});

test('disconnect during response streaming rejects old data after the read has completed', async () => {
  const waiting = deferred(); let entered;
  const reading = new Promise(resolve => entered = resolve);
  const client = clientWith(async () => ({ok: true, headers: new Headers({'Content-Type': 'application/json'}), body: {getReader: () => ({
    async read() { entered(); await waiting.promise; return {done: true}; }, releaseLock() {},
  })}}));
  const pending = client.request('/v1/capabilities'); await reading;
  client.disconnect(); waiting.resolve();
  await rejectsCode(pending, 'session_changed');
});

test('request deadline aborts the transport and preserves uncertain mutation outcome', async () => {
  let calls = 0;
  const client = clientWith(async (_url, request) => { calls++; return new Promise((_resolve, reject) => request.signal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), {once: true})); }, {timeoutMs: 10});
  await rejectsCode(client.request('/v1/repairs', {method: 'POST', role: 'operation', body: {}}), 'connection_lost', true);
  assert.equal(calls, 1);
});

test('literal run identifier rejects paths, uppercase, prefixes and wrong length', () => {
  assert.equal(runId(id), id);
  for (const value of [id.toUpperCase(), '../' + id, id + '/resume', id.slice(1), ' ' + id, null, {}]) assert.throws(() => runId(value), {code: 'invalid_request'});
});
