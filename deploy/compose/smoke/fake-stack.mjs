// In-process fakes of the compose stack for smoke.test.mjs. TEST_ONLY / LOCAL_TEST_ONLY /
// NOT_QUALIFIED: no signature, no authority, no persistence. Every response shape mirrors a Java
// source (named next to it) so the smoke client is exercised against the real wire contract:
//   identity  POST /api/v1/auth/login           signaling-identity IdentityController.login
//   control   POST /v1/signaling/bootstrap       signaling-control-api BootstrapController
//   gateways  wss /ws, Origin allow-list, subprotocol webrtc-signaling.v1   GatewayServer
//   frames    AUTH_OK (AuthHandler), ACK_COMMITTED / COMMAND_RESULT / ERROR (NativeGatewayCommands),
//             control events (GatewayDeliveryStream), relay frames (GatewayRelayStream),
//             close codes (GatewayRejection)
//   admin     POST /v1/admin/revocations, mTLS   signaling-local-platform AdminHandler
import { randomUUID } from 'node:crypto';
import { createServer as createHttp } from 'node:http';
import { createServer as createHttps } from 'node:https';
import { WebSocketServer } from 'ws';

const SUBPROTOCOL = 'webrtc-signaling.v1';
const RELAY = new Set(['OFFER', 'ANSWER', 'ICE_CANDIDATES', 'END_OF_CANDIDATES']);
const b64 = (value) => Buffer.from(JSON.stringify(value)).toString('base64url');
const listen = (server) => new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve(server.address().port)));
const body = (req) => new Promise((resolve) => {
  const chunks = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
});
const send = (res, status, value) => {
  const text = value === undefined ? '' : JSON.stringify(value);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text) });
  res.end(text);
};
const claims = (token) => JSON.parse(Buffer.from(String(token).split('.')[1] ?? '', 'base64url').toString('utf8') || '{}');

/**
 * Starts identity, control, two gateways and the admin listener.
 * @param pki   paths and PEM text from the test's TEST_ONLY openssl PKI
 * @param users {username: {password, userId}}
 * @param faults sameCell | authReject | noAck | nonMonotonic | noTerminatedEvent | revokeClose [code, reason]
 */
export async function startFakeStack({ pki, users, origin, faults = {} }) {
  const issued = [];
  const clientFrames = [];
  const revocations = [];
  const sessions = new Map(); // userId -> session
  const calls = new Map();
  const servers = [];

  // --- identity (HTTP) -------------------------------------------------------------
  const identity = createHttp(async (req, res) => {
    if (req.method !== 'POST' || req.url !== '/api/v1/auth/login') return send(res, 404);
    let request;
    try { request = JSON.parse(await body(req)); } catch { return send(res, 400, { error: 'invalid_request' }); }
    const account = users[request.username];
    if (!account || account.password !== request.password) return send(res, 401, { error: 'invalid_credentials' });
    const now = Math.floor(Date.now() / 1000);
    const token = `${b64({ alg: 'RS256', kid: 'TEST_ONLY', typ: 'JWT' })}.${b64({
      iss: 'http://identity.signaling.local:8081', aud: 'webrtc-signaling', userId: account.userId,
      jti: randomUUID(), iat: now, exp: now + 900 })}.TEST_ONLY_UNSIGNED`;
    issued.push(token);
    send(res, 200, { access_token: token, token_type: 'Bearer', expires_in: 900 });
  });
  servers.push(identity);
  const identityPort = await listen(identity);

  // --- gateways (WSS) ----------------------------------------------------------------
  const gatewayPorts = {};
  for (const cell of ['c001', 'c002']) {
    const server = createHttps({ cert: pki.text[`${cell}.crt`], key: pki.text[`${cell}.key`] });
    const wss = new WebSocketServer({ noServer: true, handleProtocols: (set) => (set.has(SUBPROTOCOL) ? SUBPROTOCOL : false) });
    server.on('upgrade', (req, socket, head) => {
      const protocols = String(req.headers['sec-websocket-protocol'] ?? '').split(',').map((s) => s.trim());
      if (req.url !== '/ws' || req.headers.origin !== origin || !protocols.includes(SUBPROTOCOL)) {
        socket.end('HTTP/1.1 403 Forbidden\r\ncontent-length: 0\r\n\r\n');
        return;
      }
      wss.handleUpgrade(req, socket, head, (ws) => gateway(cell, ws));
    });
    servers.push(server);
    gatewayPorts[cell] = await listen(server);
  }

  function frameFor(session, type, requestId, extra = {}) {
    return { v: 1, type, requestId, sessionIncarnation: session.incarnation, connectionGeneration: '1',
      serverTime: new Date().toISOString(), ...extra };
  }
  function event(session, type, call, version, payload = {}) {
    session.ws.send(JSON.stringify({ v: 1, type, eventId: randomUUID(), callId: call.callId, callVersion: String(version),
      sessionIncarnation: session.incarnation, connectionGeneration: '1',
      payload: { type, callId: call.callId, callVersion: String(version), ...payload } }));
  }
  const ack = (session, f, call, version, code, state) => frameFor(session, 'ACK_COMMITTED', f.requestId, {
    ackCommitted: true, callVersion: String(version), callId: call.callId,
    result: { status: 'FINAL', code, state, eventIds: [] } });
  const pending = (session, f, call) => frameFor(session, 'COMMAND_RESULT', f.requestId, {
    ackCommitted: false, callVersion: '0', callId: call.callId,
    result: { status: 'PENDING', code: 'WORKFLOW_PENDING', state: 'PENDING', eventIds: [] } });
  const later = (fn) => setTimeout(fn, 40);

  function command(session, f) {
    const reply = (value) => session.ws.send(JSON.stringify(value));
    if (f.type === 'EVENT_RECEIVED')
      return reply(frameFor(session, 'EVENT_RECEIPT', f.requestId, { ackCommitted: false }));
    if (f.type === 'INVITE') {
      let call = [...calls.values()].find((c) => c.inviteRequest === f.requestId);
      if (!call) {
        call = { callId: `${session.cell}.e1.${randomUUID()}`, inviteRequest: f.requestId, caller: session,
          callee: sessions.get(f.payload.targetUserId), version: 1, invite: null, accept: null };
        calls.set(call.callId, call);
        later(() => { // cross-cell RING step: invite result final, RINGING to caller and offered callee
          call.version = 2; call.invite = 2;
          event(call.callee, 'RINGING', call, 2, { state: 'RINGING' });
          event(call.caller, 'RINGING', call, 2, { state: 'RINGING' });
        });
      }
      return reply(call.invite && !faults.noAck ? ack(session, f, call, call.invite, 'RINGING', 'RINGING') : pending(session, f, call));
    }
    const call = calls.get(f.callId);
    if (!call) return reply(frameFor(session, 'ERROR', f.requestId, { ackCommitted: false, error: { code: 'UNAUTHORIZED' } }));
    const peer = session === call.caller ? call.callee : call.caller;
    switch (f.type) {
      case 'ACCEPT':
        if (call.accept === null) {
          call.accept = 0;
          later(() => {
            call.version = 3; call.accept = 3;
            for (const s of [call.caller, call.callee]) event(s, 'ACCEPTED_PENDING_ACTIVATION', call, 3, { state: 'ACCEPTED' });
            later(() => {
              call.version = 5;
              for (const s of [call.caller, call.callee]) event(s, 'CALL_READY', call, 5, { state: 'CONNECTING' });
            });
          });
        }
        return reply(call.accept
          ? ack(session, f, call, faults.nonMonotonic ? 1 : call.accept, 'ACCEPTED_PENDING_ACTIVATION', 'ACCEPTED')
          : pending(session, f, call));
      case 'NEGOTIATE_REQUEST':
        call.version = 6;
        reply(ack(session, f, call, 6, 'NEGOTIATION_GRANTED', 'CONNECTING'));
        for (const s of [session, peer]) event(s, 'NEGOTIATION_GRANTED', call, 6,
          { negotiationId: '1', iceGeneration: '1', offerer: session.userId, negotiationUntil: new Date().toISOString() });
        return undefined;
      case 'HANGUP':
        call.version = 7;
        reply(ack(session, f, call, 7, 'HANGUP', 'TERMINAL'));
        if (!faults.noTerminatedEvent) event(call.caller, 'CALL_TERMINATED', call, 7, { reason: 'HANGUP' });
        return undefined;
      default:
        if (!RELAY.has(f.type))
          return reply(frameFor(session, 'ERROR', f.requestId, { ackCommitted: false, error: { code: 'INVALID_MESSAGE' } }));
        reply(frameFor(session, 'COMMAND_RESULT', f.requestId, { ackCommitted: false, callId: call.callId,
          callVersion: String(call.version), negotiationId: f.negotiationId, iceGeneration: f.iceGeneration,
          result: { status: 'VOLATILE', code: 'WRITE_COMPLETED', frameType: f.type } }));
        peer.ws.send(JSON.stringify({ v: 1, type: f.type, requestId: f.requestId, callId: call.callId,
          callVersion: String(call.version), negotiationId: f.negotiationId, iceGeneration: f.iceGeneration,
          sessionIncarnation: peer.incarnation, connectionGeneration: '1',
          sender: { userId: session.userId, key: { issuer: 'http://identity.signaling.local:8081', jti: session.jti },
            incarnation: { value: session.incarnation }, connectionGeneration: 1, connectionId: randomUUID() },
          payload: f.payload }));
        return undefined;
    }
  }

  function gateway(cell, ws) {
    let session = null;
    ws.on('message', (data) => {
      let f;
      try { f = JSON.parse(String(data)); } catch { ws.close(1002, 'PROTOCOL_REJECTED'); return; }
      clientFrames.push(f);
      if (f.type === 'AUTH') {
        if (faults.authReject) { ws.close(1008, 'AUTHORIZATION_REJECTED'); return; }
        const { userId, jti } = claims(f.payload?.token);
        session = { cell, ws, userId, jti, incarnation: randomUUID() };
        sessions.set(userId, session);
        ws.send(JSON.stringify({ v: 1, type: 'AUTH_OK', sessionIncarnation: session.incarnation, connectionGeneration: '1' }));
        return;
      }
      if (!session) { ws.close(1008, 'AUTH_REQUIRED'); return; }
      command(session, f);
    });
  }

  // --- control (HTTPS) ----------------------------------------------------------------
  const control = createHttps({ cert: pki.text['bootstrap.crt'], key: pki.text['bootstrap.key'] }, async (req, res) => {
    await body(req);
    if (req.method !== 'POST' || req.url !== '/v1/signaling/bootstrap') return send(res, 404);
    const auth = req.headers.authorization ?? '';
    if (!auth.startsWith('Bearer ') || !issued.includes(auth.slice(7))) return send(res, 401);
    const { userId } = claims(auth.slice(7));
    const cell = faults.sameCell || userId === users.admin.userId ? 'c001' : 'c002';
    send(res, 200, { wssUrl: `wss://${cell}.signaling.local:${gatewayPorts[cell]}/ws`, protocol: SUBPROTOCOL,
      directoryEpoch: '1', retryAfterMillis: 0 });
  });
  servers.push(control);
  const controlPort = await listen(control);

  // --- local-platform admin listener (HTTPS, client certificate required) --------------
  const admin = createHttps({ cert: pki.text['platform.crt'], key: pki.text['platform.key'], ca: pki.text['admin-ca.crt'],
    requestCert: true, rejectUnauthorized: true }, async (req, res) => {
    const text = await body(req);
    if (req.url !== '/v1/admin/revocations') return send(res, 404);
    if (req.method !== 'POST') return send(res, 405);
    if (!/^application\/json(\s*;\s*charset=utf-8)?$/i.test(req.headers['content-type'] ?? '')) return send(res, 415);
    let revoke;
    try { revoke = JSON.parse(text); } catch { return send(res, 400); }
    if (typeof revoke.userId !== 'string' || Object.keys(revoke).some((k) => !['userId', 'jti', 'epoch'].includes(k))) return send(res, 400);
    revocations.push(revoke);
    const [code, reason] = faults.revokeClose ?? [1008, 'AUTH_REVOKED'];
    for (const s of sessions.values())
      if (s.userId === revoke.userId && (revoke.jti === undefined || revoke.jti === s.jti))
        setTimeout(() => s.ws.close(code, reason), 50);
    send(res, 201, { offset: revocations.length - 1, epoch: 0 });
  });
  servers.push(admin);
  const adminPort = await listen(admin);

  return {
    identityUrl: `http://127.0.0.1:${identityPort}`,
    bootstrapUrl: `https://bootstrap.signaling.local:${controlPort}`,
    adminUrl: `https://127.0.0.1:${adminPort}/v1/admin/revocations`,
    issued, clientFrames, revocations,
    async close() {
      for (const s of sessions.values()) s.ws.terminate();
      await Promise.all(servers.map((s) => new Promise((resolve) => { s.closeAllConnections?.(); s.close(() => resolve()); })));
    },
  };
}
