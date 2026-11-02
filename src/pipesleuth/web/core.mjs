/* Local capabilities stay in this module's closure, never in browser storage. */
export const UUID = /^[0-9a-f]{32}$/;
const TOKEN = /^[!-~]{32,256}$/;
const ID = '[0-9a-f]{32}';
const READ = new RegExp(`^/v1/(capabilities|runs/${ID}|runs/${ID}/evidence/(read_run|read_logs|inspect_schema|inspect_validation)(\\?limit=([1-9]|1[0-9]|20))?|investigations/${ID}|repairs/${ID})$`);
const OPERATE = new RegExp(`^/v1/(runs/${ID}/diagnosis|investigations|investigations/${ID}/resume|repairs|repairs/${ID}/execute)$`);
const APPROVE = new RegExp(`^/v1/repairs/${ID}/approve$`);
const MESSAGES = {
  unauthorized: 'Access was rejected. Reconnect using the current local session file.',
  forbidden: 'This action needs a different access role.',
  busy: 'Another operation is running. Inspect its saved state before trying again.',
  model_unconfigured: 'The server has no pinned local model. The checklist and evidence remain available.',
  operation_rejected: 'The operation was refused. Inspect the saved record and current input before continuing.',
  read_unavailable: 'This record is unavailable in the server’s configured directories.',
  no_session: 'Choose the private session file to connect.',
  no_approval_authority: 'Load approval access separately before approving this plan.',
  invalid_session: 'Choose a valid session file from this running local server.',
  invalid_request: 'Check the supplied values.',
  invalid_response: 'The server returned an unreadable response.',
  connection_lost: 'The request did not finish. A submitted operation may still be running; inspect its saved record. Do not retry automatically.',
  session_changed: 'The connection changed. Inspect saved state before continuing.',
  client_busy: 'Wait for the current operation to finish.',
};
export class ApiError extends Error {
  constructor(code, uncertain = false) { super(MESSAGES[code] || 'The local request failed. Inspect saved state before continuing.'); this.code = code; this.uncertain = uncertain; }
}
export function parseSession(text, origin) {
  try {
    if (typeof text !== 'string' || new TextEncoder().encode(text).length > 4096) throw 0;
    const value = JSON.parse(text);
    if (!value || Array.isArray(value) || Object.keys(value).sort().join(',') !== 'approval_token,base_url,operation_token,read_token') throw 0;
    if (!/^http:\/\/127\.0\.0\.1:([1-9][0-9]{3,4})$/.test(origin) || value.base_url !== origin) throw 0;
    const port = Number(new URL(origin).port);
    if (port < 1024 || port > 65535) throw 0;
    const tokens = [value.read_token, value.operation_token, value.approval_token];
    if (tokens.some(t => typeof t !== 'string' || !TOKEN.test(t)) || new Set(tokens).size !== 3) throw 0;
    return value;
  } catch { throw new ApiError('invalid_session'); }
}
export function createClient({origin, fetchImpl = globalThis.fetch, timeoutMs = 150000, onMutationChange = () => {}}) {
  let session = null, approval = null, epoch = 0, mutating = false;
  const active = new Set();
  function disconnect() { epoch++; session = null; approval = null; mutating = false; for (const c of active) c.abort(); active.clear(); onMutationChange(false); }
  function connect(text) {
    const parsed = parseSession(text, origin);
    disconnect();
    session = {read: parsed.read_token, operation: parsed.operation_token};
  }
  function unlockApproval(text) {
    const parsed = parseSession(text, origin);
    if (!session || parsed.read_token !== session.read || parsed.operation_token !== session.operation) throw new ApiError('invalid_session');
    approval = parsed.approval_token;
  }
  async function request(path, {method = 'GET', body, role = 'read'} = {}) {
    const post = method === 'POST';
    if (!(method === 'GET' && role === 'read' && READ.test(path)) &&
        !(post && role === 'operation' && OPERATE.test(path)) &&
        !(post && role === 'approval' && APPROVE.test(path))) throw new ApiError('invalid_request');
    if (!session) throw new ApiError('no_session');
    const token = role === 'approval' ? approval : session[role];
    if (!token) throw new ApiError('no_approval_authority');
    if (post && mutating) throw new ApiError('client_busy');
    const payload = post ? JSON.stringify(body ?? {}) : undefined;
    if (post && (typeof body !== 'object' || body === null || Array.isArray(body) || new TextEncoder().encode(payload).length > 4096)) throw new ApiError('invalid_request');
    if (post) { mutating = true; onMutationChange(true); }
    const generation = epoch, controller = new AbortController(); active.add(controller);
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetchImpl(origin + path, {method, headers: {Authorization: `Bearer ${token}`, ...(post ? {'Content-Type': 'application/json'} : {})}, body: payload,
        cache: 'no-store', credentials: 'omit', redirect: 'error', referrerPolicy: 'no-referrer', mode: 'same-origin', signal: controller.signal});
      if (epoch !== generation) throw new ApiError('session_changed', post);
      if (!response.headers.get('content-type')?.toLowerCase().startsWith('application/json')) throw new ApiError('invalid_response', post);
      const reader = response.body?.getReader();
      if (!reader) throw new ApiError('invalid_response', post);
      const chunks = []; let size = 0;
      try {
        while (true) { const {done, value} = await reader.read(); if (done) break; size += value.length; if (size > 524288) { await reader.cancel(); throw new ApiError('invalid_response', post); } chunks.push(value); }
      } finally { reader.releaseLock(); }
      if (epoch !== generation) throw new ApiError('session_changed', post);
      const bytes = new Uint8Array(size); let offset = 0;
      for (const c of chunks) { bytes.set(c, offset); offset += c.length; }
      let value;
      try { value = JSON.parse(new TextDecoder('utf-8', {fatal: true}).decode(bytes)); } catch { throw new ApiError('invalid_response', post); }
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new ApiError('invalid_response', post);
      if (!response.ok) { const code = value.error?.code; throw new ApiError(typeof code === 'string' && /^[a-z][a-z0-9_]{0,63}$/.test(code) ? code : 'invalid_response', post && response.status >= 500); }
      return value;
    } catch (error) {
      if (error instanceof ApiError) throw error;
      throw new ApiError(epoch !== generation ? 'session_changed' : 'connection_lost', post);
    } finally { clearTimeout(timer); active.delete(controller); if (post && epoch === generation) { mutating = false; onMutationChange(false); } }
  }
  return Object.freeze({connect, disconnect, unlockApproval, request, get connected() { return session !== null; }, get approvalUnlocked() { return approval !== null; }});
}
export function runId(value) { if (!UUID.test(value)) throw new ApiError('invalid_request'); return value; }
