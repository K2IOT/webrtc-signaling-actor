// Bounded HTTP(S) requests and one authenticated WSS peer. LOCAL_TEST_ONLY / NOT_QUALIFIED.
// Every network operation has a deadline. Received frames are classified like the browser client
// (native-client.mjs receive): control events carry eventId (acknowledged with EVENT_RECEIVED, as
// loadgen ScenarioRunner.ackEvent does), relay frames carry a sender, replies echo our requestId.
import { randomUUID } from 'node:crypto';
import { lookup as dnsLookup } from 'node:dns';
import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { isIP } from 'node:net';
import WebSocket from 'ws';
import { authFrame, encode, eventReceivedFrame, SUBPROTOCOL } from './payloads.mjs';

const MAX_BODY = 65536;
const RELAY = new Set(['OFFER', 'ANSWER', 'ICE_CANDIDATES', 'END_OF_CANDIDATES']);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** A failed assertion; its message is the one-line reason (never a token or secret). */
export class SmokeFailure extends Error {}
/** A bounded wait that ran out; request() turns it into a null reply. */
class WaitTimeout extends SmokeFailure {}

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** dns.lookup with SMOKE_RESOLVE names pinned; other names use the system resolver (/etc/hosts). */
export function makeLookup(pinned) {
  return (hostname, options, callback) => {
    const cb = typeof options === 'function' ? options : callback;
    const opts = typeof options === 'function' ? {} : options ?? {};
    const ip = pinned.get(String(hostname).toLowerCase());
    if (!ip) return dnsLookup(hostname, opts, cb);
    const family = isIP(ip);
    return opts.all ? cb(null, [{ address: ip, family }]) : cb(null, ip, family);
  };
}

/** One request with a hard deadline; the body is capped and parsed as JSON when possible. */
export function httpJson(url, { method = 'POST', headers = {}, body, tls = {}, lookup, timeoutMs }) {
  const target = new URL(url);
  const request = target.protocol === 'https:' ? httpsRequest : httpRequest;
  const payload = body === undefined ? '' : JSON.stringify(body);
  const allHeaders = { ...headers, 'content-length': Buffer.byteLength(payload) };
  if (body !== undefined) allHeaders['content-type'] = 'application/json';
  return new Promise((resolve, reject) => {
    const req = request(target, { method, headers: allHeaders, agent: false, lookup, timeout: timeoutMs, ...tls }, (res) => {
      const chunks = [];
      let size = 0;
      res.on('data', (chunk) => {
        size += chunk.length;
        if (size > MAX_BODY) req.destroy(new Error(`response body above ${MAX_BODY} bytes`));
        else chunks.push(chunk);
      });
      res.on('end', () => {
        clearTimeout(deadline);
        const text = Buffer.concat(chunks).toString('utf8');
        let json = null;
        try { json = text ? JSON.parse(text) : null; } catch { json = null; }
        resolve({ status: res.statusCode, json });
      });
      res.on('error', reject);
    });
    const deadline = setTimeout(() => req.destroy(new Error(`no response within ${timeoutMs} ms`)), timeoutMs);
    req.on('timeout', () => req.destroy(new Error(`socket idle for ${timeoutMs} ms`)));
    req.on('error', (error) => { clearTimeout(deadline); reject(error); });
    req.end(payload);
  });
}

/** One WSS connection to a gateway, authenticated with AUTH -> AUTH_OK. */
export class Peer {
  static open({ url, ca, origin, lookup, timeoutMs, name }) {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url, SUBPROTOCOL, {
        ca, origin, lookup, handshakeTimeout: timeoutMs, perMessageDeflate: false, maxPayload: 98304,
      });
      const fail = (reason) => { ws.terminate(); reject(new SmokeFailure(`${name}: ${reason}`)); };
      ws.once('unexpected-response', (_req, res) => fail(`WSS upgrade refused: HTTP ${res.statusCode}`));
      ws.once('error', (error) => fail(`WSS connect failed: ${[error.code, error.message].filter(Boolean).join(' ')}`));
      ws.once('open', () => {
        ws.removeAllListeners('error');
        if (ws.protocol !== SUBPROTOCOL) return fail(`server selected subprotocol "${ws.protocol}"`);
        return resolve(new Peer(ws, name));
      });
    });
  }

  constructor(ws, name) {
    this.ws = ws;
    this.name = name;
    this.route = null;
    this.events = [];
    this.relays = [];
    this.replies = new Map();
    this.waiters = new Set();
    this.closed = null;
    this.violation = null;
    ws.on('message', (data, binary) => this.receive(data, binary));
    ws.on('error', () => {}); // The close event that follows carries the outcome.
    ws.on('close', (code, reason) => {
      this.closed = { code, reason: String(reason) };
      for (const waiter of this.waiters) waiter.check();
    });
  }

  receive(data, binary) {
    let frame;
    try {
      if (binary) throw new Error('binary');
      frame = JSON.parse(String(data));
      if (!frame || frame.v !== 1 || typeof frame.type !== 'string') throw new Error('envelope');
    } catch {
      this.violation = 'received a frame that is not a v:1 JSON text frame';
    }
    if (frame && typeof frame.eventId === 'string') {
      this.events.push(frame);
      if (typeof frame.callId === 'string') this.ackEvent(frame);
    } else if (frame && RELAY.has(frame.type) && frame.sender) {
      this.relays.push(frame);
    } else if (frame && frame.type === 'OUTCOME_UNKNOWN' && frame.requestId === undefined) {
      // AuthHandler.respond deadline reply: uncorrelated, so every in-flight request is unknown.
      for (const [id, entry] of this.replies) if (!entry.frame) this.replies.set(id, { frame });
    } else if (frame && this.replies.has(frame.requestId)) {
      this.replies.set(frame.requestId, { frame });
    } else if (frame && frame.type === 'AUTH_OK') {
      this.authOk = frame;
    }
    for (const waiter of this.waiters) waiter.check();
  }

  ackEvent(event) {
    try {
      this.send(eventReceivedFrame({ requestId: randomUUID(), callId: event.callId, eventId: event.eventId }));
    } catch {
      this.violation = 'received a control event with a malformed eventId or callId';
    }
  }

  send(frame) {
    if (this.ws.readyState !== WebSocket.OPEN) throw new SmokeFailure(`${this.name}: socket not open${this.closeText()}`);
    this.ws.send(encode(frame));
  }

  closeText() {
    return this.closed ? ` (closed ${this.closed.code} ${this.closed.reason})` : '';
  }

  /** Resolves with the first value `probe` returns that is not undefined; bounded by timeoutMs. */
  wait(probe, timeoutMs, label, { allowClosed = false } = {}) {
    return new Promise((resolve, reject) => {
      const waiter = {
        check: () => {
          if (this.violation) return finish(reject, new SmokeFailure(`${this.name}: ${this.violation}`));
          const value = probe();
          if (value !== undefined) return finish(resolve, value);
          if (this.closed && !allowClosed)
            return finish(reject, new SmokeFailure(`${this.name}: socket closed while waiting for ${label}${this.closeText()}`));
          return undefined;
        },
      };
      const timer = setTimeout(() => finish(reject, new WaitTimeout(`${this.name}: no ${label} within ${timeoutMs} ms`)), timeoutMs);
      const finish = (settle, value) => { clearTimeout(timer); this.waiters.delete(waiter); settle(value); };
      this.waiters.add(waiter);
      waiter.check();
    });
  }

  async authenticate(token, timeoutMs) {
    this.send(authFrame(token));
    const ok = await this.wait(() => this.authOk, timeoutMs, 'AUTH_OK');
    if (!UUID.test(String(ok.sessionIncarnation)) || !/^[1-9][0-9]{0,18}$/.test(String(ok.connectionGeneration)))
      throw new SmokeFailure(`${this.name}: AUTH_OK without a sessionIncarnation UUID and a decimal connectionGeneration`);
    this.route = { sessionIncarnation: ok.sessionIncarnation, connectionGeneration: ok.connectionGeneration };
  }

  /** Sends `frame` and resolves with its reply, or null after replyMs. */
  async request(frame, replyMs) {
    this.replies.set(frame.requestId, {});
    this.send(frame);
    try {
      return await this.wait(() => this.replies.get(frame.requestId)?.frame, replyMs, `${frame.type} reply`);
    } catch (error) {
      if (error instanceof WaitTimeout) return null;
      throw error;
    } finally {
      this.replies.delete(frame.requestId);
    }
  }

  waitEvent(type, callId, timeoutMs) {
    return this.wait(() => this.events.find((e) => e.type === type && e.callId === callId), timeoutMs, `${type} event`);
  }

  waitRelay(type, callId, requestId, timeoutMs) {
    return this.wait(() => this.relays.find((r) => r.type === type && r.callId === callId && r.requestId === requestId),
      timeoutMs, `${type} relay`);
  }

  waitClose(timeoutMs) {
    return this.wait(() => this.closed ?? undefined, timeoutMs, 'socket close', { allowClosed: true });
  }

  /** Clean close, then a forced one if the peer does not answer within a second. */
  async shutdown() {
    if (this.ws.readyState === WebSocket.CLOSED) return;
    if (this.ws.readyState === WebSocket.OPEN) this.ws.close(1000, 'SMOKE_DONE');
    try { await this.waitClose(1000); } catch { /* forced below */ }
    this.ws.terminate();
  }
}
