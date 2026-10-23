#!/usr/bin/env node
// Peer-to-peer check of the two-cell compose environment. LOCAL_TEST_ONLY / NOT_QUALIFIED.
// Two real RTCPeerConnections (the native @roamhq/wrtc endpoint) negotiate through the stack's
// signaling: login, bootstrap, WSS auth, cross-cell INVITE/ACCEPT/NEGOTIATE_REQUEST, then a real
// OFFER and ANSWER and trickled ICE_CANDIDATES relayed by the gateways. The media path is a data
// channel on this host: the caller sends a message, the callee echoes it back, then HANGUP.
// The module is found through WRTC_MODULE or the integration tests' node_modules (npm ci there).
// Output: STEP lines, then P2P_PASSED, or `P2P_FAILED <reason>` and exit 1. No token is printed.
import { randomUUID } from 'node:crypto';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { commit, relay } from './call-flow.mjs';
import { resolveConfig } from './config.mjs';
import { acceptFrame, hangupFrame, iceFrame, inviteFrame, negotiateFrame, offerFrame, answerFrame } from './payloads.mjs';
import { bootstrap, connect, login } from './smoke.mjs';
import { makeLookup, SmokeFailure, sleep } from './transport.mjs';

const WRTC_DEFAULT = fileURLToPath(new URL('../../../signaling-integration-tests/src/test/webrtc/node_modules/@roamhq/wrtc', import.meta.url));
const INVITE_ATTEMPTS = Number(process.env.P2P_INVITE_ATTEMPTS || 4);
// A call that timed out keeps both users reserved until its lease lapses (USER_BUSY meanwhile).
const REDIAL_PAUSE_MS = Number(process.env.P2P_REDIAL_PAUSE_MS || 15000);
const GATHER_MS = 10000;
const ECHO_MS = 15000;

function loadWrtc() {
  const path = process.env.WRTC_MODULE || WRTC_DEFAULT;
  try {
    return createRequire(import.meta.url)(path);
  } catch (error) {
    throw new SmokeFailure(`@roamhq/wrtc is not loadable from ${path} (${error.code ?? error.message}); run npm ci in signaling-integration-tests/src/test/webrtc`);
  }
}

const within = (promise, ms, label) => Promise.race([
  promise,
  new Promise((_, reject) => setTimeout(() => reject(new SmokeFailure(`${label} did not happen within ${ms} ms`)), ms).unref()),
]);

function gathered(pc) {
  return new Promise((resolve) => {
    if (pc.iceGatheringState === 'complete') return resolve();
    pc.addEventListener('icegatheringstatechange', () => pc.iceGatheringState === 'complete' && resolve());
    return undefined;
  });
}

async function leg(from, to, frame, label, timeoutMs) {
  await relay(from, frame, label, timeoutMs);
  return to.waitRelay(frame.type, frame.callId, frame.requestId, timeoutMs);
}

async function invite(caller, calleeUserId, callerCell, timeoutMs, print) {
  let last;
  for (let attempt = 1; attempt <= INVITE_ATTEMPTS; attempt++) {
    try {
      const reply = await commit(caller, inviteFrame({ requestId: randomUUID(), targetUserId: calleeUserId }), 'INVITE', timeoutMs);
      if (reply.result?.code !== 'RINGING' || !reply.callId?.startsWith(`${callerCell}.e`))
        throw new SmokeFailure(`INVITE: expected RINGING routed to ${callerCell}, got ${reply.result?.code}`);
      print(`INFO invite attempts=${attempt}`);
      return reply.callId;
    } catch (error) {
      last = error;
      print(`INFO invite attempt ${attempt} failed: ${error.message}`);
      if (attempt < INVITE_ATTEMPTS) await sleep(REDIAL_PAUSE_MS);
    }
  }
  throw last;
}

export async function main(env = process.env, print = (line) => process.stdout.write(`${line}\n`)) {
  const peers = [];
  const connections = [];
  try {
    const { RTCPeerConnection } = loadWrtc();
    const config = resolveConfig(env);
    const ctx = { config, peers, secrets: [], lookup: config.resolve.size ? makeLookup(config.resolve) : undefined };
    const users = { caller: await login(ctx, config.caller), callee: await login(ctx, config.callee) };
    users.caller = await bootstrap(ctx, users.caller);
    users.callee = await bootstrap(ctx, users.callee);
    if (users.caller.endpoint === users.callee.endpoint) throw new SmokeFailure('both users were sent to the same cell');
    print(`STEP 1 ok login+bootstrap (${users.caller.cell} -> ${users.callee.cell})`);
    const caller = await connect(ctx, users.caller, 'caller');
    const callee = await connect(ctx, users.callee, 'callee');
    print('STEP 2 ok wss-auth');

    const t = config.timeoutMs;
    const callId = await invite(caller, users.callee.userId, users.caller.cell, t, print);
    await callee.waitEvent('RINGING', callId, t);
    await commit(callee, acceptFrame({ requestId: randomUUID(), callId }), 'ACCEPT', t);
    await caller.waitEvent('CALL_READY', callId, t);
    await callee.waitEvent('CALL_READY', callId, t);
    const grant = await commit(caller, negotiateFrame({ requestId: randomUUID(), callId, iceRestart: false }), 'NEGOTIATE_REQUEST', t);
    const event = await caller.waitEvent('NEGOTIATION_GRANTED', callId, t);
    const round = { negotiationId: event.payload?.negotiationId, iceGeneration: event.payload?.iceGeneration };
    await callee.waitEvent('NEGOTIATION_GRANTED', callId, t);
    print(`STEP 3 ok call-setup (negotiation ${grant.result?.code})`);

    const scope = () => ({ requestId: randomUUID(), callId, ...round });
    const pc1 = new RTCPeerConnection({ iceServers: [] });
    const pc2 = new RTCPeerConnection({ iceServers: [] });
    connections.push(pc1, pc2);
    const found = { caller: [], callee: [] };
    pc1.onicecandidate = (e) => e.candidate && found.caller.push(e.candidate.toJSON());
    pc2.onicecandidate = (e) => e.candidate && found.callee.push(e.candidate.toJSON());
    const channel = pc1.createDataChannel('echo');
    const opened = new Promise((resolve) => { channel.onopen = resolve; });
    const echoed = new Promise((resolve) => { channel.onmessage = (m) => resolve(String(m.data)); });
    pc2.ondatachannel = (e) => { e.channel.onmessage = (m) => e.channel.send(`echo:${m.data}`); };

    const offer = await pc1.createOffer();
    await pc1.setLocalDescription(offer);
    const gotOffer = await leg(caller, callee, offerFrame({ ...scope(), sdp: offer.sdp }), 'OFFER', t);
    await pc2.setRemoteDescription({ type: 'offer', sdp: gotOffer.payload.sdp });
    const answer = await pc2.createAnswer();
    await pc2.setLocalDescription(answer);
    const gotAnswer = await leg(callee, caller, answerFrame({ ...scope(), sdp: answer.sdp }), 'ANSWER', t);
    await pc1.setRemoteDescription({ type: 'answer', sdp: gotAnswer.payload.sdp });
    print('STEP 4 ok offer-answer (real SDP relayed through both gateways)');

    await within(Promise.all([gathered(pc1), gathered(pc2)]), GATHER_MS, 'ICE gathering');
    for (const [from, to, pc, list, label] of [[caller, callee, pc2, found.caller, 'caller'], [callee, caller, pc1, found.callee, 'callee']]) {
      if (!list.length) throw new SmokeFailure(`${label} gathered no ICE candidate`);
      const frame = iceFrame({ ...scope(), startSequence: '1', candidates: list.slice(0, 20) });
      const delivered = await leg(from, to, frame, `ICE_CANDIDATES ${label}`, t);
      for (const candidate of delivered.payload.candidates) await pc.addIceCandidate(candidate);
    }
    print(`STEP 5 ok ice (${found.caller.length} + ${found.callee.length} candidates relayed)`);

    await within(opened, ECHO_MS, 'data channel open');
    channel.send('ping');
    const reply = await within(echoed, ECHO_MS, 'data channel echo');
    if (reply !== 'echo:ping') throw new SmokeFailure(`data channel echo mismatch (${reply})`);
    print(`STEP 6 ok p2p-data-channel (ice ${pc1.iceConnectionState}/${pc2.iceConnectionState}, echo received)`);

    await commit(callee, hangupFrame({ requestId: randomUUID(), callId }), 'HANGUP', t);
    await caller.waitEvent('CALL_TERMINATED', callId, t);
    print('STEP 7 ok hangup');
    print('P2P_PASSED LOCAL_TEST_ONLY / NOT_QUALIFIED: one peer-to-peer path on this host; no capacity or production claim');
    return 0;
  } catch (error) {
    print(`P2P_FAILED ${error instanceof SmokeFailure ? error.message : `${error?.name}: ${error?.message}`}`);
    return 1;
  } finally {
    for (const pc of connections) try { pc.close(); } catch { /* best effort */ }
    await Promise.allSettled(peers.map((p) => p.shutdown()));
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const guard = setTimeout(() => { process.stdout.write('P2P_FAILED overall deadline exceeded\n'); process.exit(1); }, 300000);
  guard.unref();
  main().then((code) => { clearTimeout(guard); process.exit(code); });
}
