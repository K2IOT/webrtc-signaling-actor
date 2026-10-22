#!/usr/bin/env node
// Cross-cell smoke test of the two-cell compose environment (spec 2026-10-21 section 7).
// LOCAL_TEST_ONLY / NOT_QUALIFIED: a pass shows one protocol path works on this machine, nothing
// about capacity, availability or production readiness.
//
//   1 login     POST {IDENTITY_URL}/api/v1/auth/login for the caller (admin) and callee (mod)
//   2 bootstrap POST {BOOTSTRAP_URL}/v1/signaling/bootstrap for each; the two land in different cells
//   3 wss-auth  WSS to each returned wssUrl (Origin, subprotocol webrtc-signaling.v1), AUTH -> AUTH_OK
//   4 call      cross-cell INVITE/ACCEPT/NEGOTIATE/OFFER/ANSWER/ICE_CANDIDATES/HANGUP (call-flow.mjs)
//   5 revoke    POST {ADMIN_URL} over mutual TLS -> the callee socket closes 1008 AUTH_REVOKED
//
// TLS: CA_FILE (the workload CA) verifies the control, gateway and local-platform admin server
// certificates. ADMIN_CERT_FILE/ADMIN_KEY_FILE are the client identity the admin listener checks
// against its own trust (CA-admin); ADMIN_CA_FILE, when given, is only used to check locally that
// the client certificate was issued by CA-admin. It never verifies a server.
// Revoke scope: REVOKE_SCOPE=token (default) revokes only the callee's current jti, so the smoke
// can run again. REVOKE_SCOPE=user sends {"userId"} with the default epoch 0, which revokes every
// token of that user whose securityEpoch is <= 0 - all identity tokens, as they carry none - so it
// PERMANENTLY locks the callee (`mod`) out of that source database until `make reset`. A warning
// line is printed first when it is used.
// Output: one `STEP n ok <name>` line per step, then SMOKE_PASSED; or `SMOKE_FAILED step n: <reason>`
// and exit 1. Tokens, passwords and key material are never printed.
import { X509Certificate } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { crossCellCall } from './call-flow.mjs';
import { resolveConfig } from './config.mjs';
import { SUBPROTOCOL } from './payloads.mjs';
import { httpJson, makeLookup, Peer, SmokeFailure } from './transport.mjs';

const NOTE = 'LOCAL_TEST_ONLY / NOT_QUALIFIED: protocol path only; no capacity, availability or production claim';
const JWT = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/;
const progress = { step: 0 };
const netError = (error) => [error.code, error.message].filter(Boolean).join(' ');

function readFile(path, variable) {
  if (!path) throw new SmokeFailure(`${variable} is not set`);
  try {
    return readFileSync(path);
  } catch (error) {
    throw new SmokeFailure(`${variable} is not readable (${error.code ?? 'error'})`);
  }
}

function claims(token, label) {
  try {
    const value = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString('utf8'));
    if (typeof value.userId !== 'string' || !value.userId || typeof value.jti !== 'string' || !value.jti) throw new Error();
    return value;
  } catch {
    throw new SmokeFailure(`login ${label}: the token has no userId and jti claims`);
  }
}

async function login(ctx, account) {
  const url = new URL('/api/v1/auth/login', ctx.config.identityUrl);
  let res;
  try {
    res = await httpJson(url, { body: { username: account.username, password: account.password },
      lookup: ctx.lookup, timeoutMs: ctx.config.httpTimeoutMs });
  } catch (error) {
    throw new SmokeFailure(`login ${account.username}: ${netError(error)}`);
  }
  if (res.status !== 200) throw new SmokeFailure(`login ${account.username}: HTTP ${res.status}`);
  const token = res.json?.access_token;
  if (typeof token !== 'string' || !JWT.test(token) || res.json.token_type !== 'Bearer')
    throw new SmokeFailure(`login ${account.username}: response is not {access_token, token_type: Bearer}`);
  ctx.secrets.push(token);
  const { userId, jti } = claims(token, account.username);
  if (account.expectedUserId && userId !== account.expectedUserId)
    throw new SmokeFailure(`login ${account.username}: userId claim differs from topology.env`);
  return { username: account.username, token, userId, jti };
}

async function bootstrap(ctx, user) {
  const ca = readFile(ctx.config.caFile, 'CA_FILE');
  let res;
  try {
    res = await httpJson(new URL('/v1/signaling/bootstrap', ctx.config.bootstrapUrl), {
      headers: { authorization: `Bearer ${user.token}` }, tls: { ca }, lookup: ctx.lookup, timeoutMs: ctx.config.httpTimeoutMs });
  } catch (error) {
    throw new SmokeFailure(`bootstrap ${user.username}: ${netError(error)}`);
  }
  if (res.status !== 200) throw new SmokeFailure(`bootstrap ${user.username}: HTTP ${res.status}`);
  const { wssUrl, protocol, directoryEpoch } = res.json ?? {};
  let url;
  try { url = new URL(wssUrl); } catch { throw new SmokeFailure(`bootstrap ${user.username}: wssUrl is not a URL`); }
  if (url.protocol !== 'wss:' || url.pathname !== '/ws') throw new SmokeFailure(`bootstrap ${user.username}: wssUrl is not wss://<host>/ws`);
  if (protocol !== SUBPROTOCOL) throw new SmokeFailure(`bootstrap ${user.username}: protocol is not ${SUBPROTOCOL}`);
  if (typeof directoryEpoch !== 'string' || !/^[1-9][0-9]{0,18}$/.test(directoryEpoch))
    throw new SmokeFailure(`bootstrap ${user.username}: directoryEpoch is not a positive decimal string`);
  return { ...user, wssUrl: url.href, endpoint: url.host, cell: url.hostname.split('.')[0] };
}

async function connect(ctx, user, role) {
  const peer = await Peer.open({ url: user.wssUrl, ca: readFile(ctx.config.caFile, 'CA_FILE'), origin: ctx.config.origin,
    lookup: ctx.lookup, timeoutMs: ctx.config.httpTimeoutMs, name: role });
  ctx.peers.push(peer);
  await peer.authenticate(user.token, ctx.config.httpTimeoutMs);
  return peer;
}

function adminTls(config) {
  const cert = readFile(config.adminCertFile, 'ADMIN_CERT_FILE');
  const key = readFile(config.adminKeyFile, 'ADMIN_KEY_FILE');
  if (config.adminCaFile) {
    let issued = false;
    try {
      const leaf = new X509Certificate(cert);
      const ca = new X509Certificate(readFile(config.adminCaFile, 'ADMIN_CA_FILE'));
      issued = leaf.checkIssued(ca) && leaf.verify(ca.publicKey);
    } catch (error) {
      if (error instanceof SmokeFailure) throw error;
    }
    if (!issued) throw new SmokeFailure('ADMIN_CERT_FILE was not issued by ADMIN_CA_FILE');
  }
  return { cert, key, ca: readFile(config.caFile, 'CA_FILE') };
}

async function revoke(ctx) {
  const { callee } = ctx.users;
  const tls = adminTls(ctx.config);
  const body = ctx.config.revokeScope === 'token' ? { userId: callee.userId, jti: callee.jti } : { userId: callee.userId };
  let res;
  try {
    res = await httpJson(ctx.config.adminUrl, { body, tls, lookup: ctx.lookup, timeoutMs: ctx.config.httpTimeoutMs });
  } catch (error) {
    throw new SmokeFailure(`admin revoke: ${netError(error)}`);
  }
  if (res.status !== 201 || !Number.isSafeInteger(res.json?.offset) || !Number.isSafeInteger(res.json?.epoch))
    throw new SmokeFailure(`admin revoke: expected 201 {offset, epoch}, got HTTP ${res.status}`);
  const closed = await ctx.peers[1].waitClose(ctx.config.timeoutMs);
  if (closed.code !== 1008 || closed.reason !== 'AUTH_REVOKED')
    throw new SmokeFailure(`callee socket closed ${closed.code} ${closed.reason || '(no reason)'}, expected 1008 AUTH_REVOKED`);
  if (ctx.peers[0].closed) throw new SmokeFailure(`caller socket closed too (${ctx.peers[0].closed.code} ${ctx.peers[0].closed.reason})`);
  return `${ctx.config.revokeScope}-scoped, callee closed 1008 AUTH_REVOKED`;
}

const STEPS = [
  [1, 'login', async (ctx) => {
    ctx.users = { caller: await login(ctx, ctx.config.caller), callee: await login(ctx, ctx.config.callee) };
    return `${ctx.users.caller.username} ${ctx.users.callee.username}`;
  }],
  [2, 'bootstrap', async (ctx) => {
    const caller = await bootstrap(ctx, ctx.users.caller);
    const callee = await bootstrap(ctx, ctx.users.callee);
    if (caller.endpoint === callee.endpoint)
      throw new SmokeFailure(`both users were sent to the same cell (${caller.endpoint})`);
    ctx.users = { caller, callee };
    return `${caller.username}=${caller.cell} ${callee.username}=${callee.cell}`;
  }],
  [3, 'wss-auth', async (ctx) => {
    await connect(ctx, ctx.users.caller, 'caller');
    await connect(ctx, ctx.users.callee, 'callee');
    return `${SUBPROTOCOL} AUTH_OK x2`;
  }],
  [4, 'cross-cell-call', (ctx) => crossCellCall({ caller: ctx.peers[0], callee: ctx.peers[1],
    calleeUserId: ctx.users.callee.userId, callerCell: ctx.users.caller.cell, timeoutMs: ctx.config.timeoutMs })],
  [5, 'revoke', revoke],
];

/** Replaces any secret this run handled; a last guard on every printed line. */
function redact(text, secrets) {
  let out = String(text).replace(/\s+/g, ' ').trim();
  for (const secret of secrets) if (secret && secret.length >= 4) out = out.split(secret).join('[REDACTED]');
  return out || 'unknown failure';
}

export async function main(env = process.env, print = (line) => process.stdout.write(`${line}\n`)) {
  const ctx = { peers: [], secrets: [], users: null, config: null, lookup: undefined };
  try {
    ctx.config = resolveConfig(env);
    ctx.secrets.push(ctx.config.caller.password, ctx.config.callee.password);
    ctx.lookup = ctx.config.resolve.size ? makeLookup(ctx.config.resolve) : undefined;
    if (ctx.config.revokeScope === 'user')
      print(`WARNING REVOKE_SCOPE=user: step 5 permanently revokes every token of ${ctx.config.callee.username} in this source database; later runs fail until the stack is reset`);
    for (const [n, name, run] of STEPS) {
      progress.step = n;
      const detail = await run(ctx);
      print(redact(`STEP ${n} ok ${name}${detail ? ` (${detail})` : ''}`, ctx.secrets));
    }
    print(`SMOKE_PASSED ${NOTE}`);
    return 0;
  } catch (error) {
    const reason = error instanceof SmokeFailure ? error.message : `${error?.name ?? 'Error'}: ${error?.message ?? error}`;
    print(`SMOKE_FAILED step ${progress.step}: ${redact(reason, ctx.secrets)}`);
    return 1;
  } finally {
    await Promise.allSettled(ctx.peers.map((peer) => peer.shutdown()));
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  let total = 300000;
  try { total = resolveConfig(process.env).totalTimeoutMs; } catch { /* main reports the config error */ }
  const guard = setTimeout(() => {
    process.stdout.write(`SMOKE_FAILED step ${progress.step}: overall deadline of ${total} ms exceeded\n`);
    process.exit(1);
  }, total);
  guard.unref();
  const code = await main();
  process.exit(code);
}
