// Step 4: one cross-cell call, caller in one cell, callee in the other. LOCAL_TEST_ONLY /
// NOT_QUALIFIED. The sequence is the one loadgen ScenarioRunner drives and the storage code commits:
//   caller INVITE            -> ACK_COMMITTED RINGING (NativeSetupCommandExecutor runs the RESERVE/RING
//                               saga inline within the 2 s budget, CallCommandService.create)
//   callee RINGING event     -> ACCEPT -> ACK_COMMITTED ACCEPTED_PENDING_ACTIVATION (accept and activate run
//                               inline too; the stored ACCEPT result is CallWorkflowService's ACCEPT step)
//   caller CALL_READY event  -> NEGOTIATE_REQUEST -> ACK_COMMITTED NEGOTIATION_GRANTED, the event
//                               on both sides carries negotiationId/iceGeneration (CallCommandService.negotiate)
//   caller OFFER, callee ANSWER, caller ICE_CANDIDATES -> COMMAND_RESULT VOLATILE WRITE_COMPLETED
//                               and the relay frame at the other side (NativeGatewayCommands, GatewayRelayStream)
//   callee HANGUP            -> ACK_COMMITTED, CALL_TERMINATED event at the caller (CallCommandService.terminal
//                               addresses the caller only, so the callee hangs up to observe the other side)
// A command answering COMMAND_RESULT PENDING (WORKFLOW_PENDING) or a transient ERROR is re-sent
// identically (same requestId, same intent) for a bounded time. For INVITE this cannot rescue a
// slow first attempt: the gateway mints a fresh callId per INVITE (NativeGatewayCommands), and the
// stored result of the first attempt names the old one (CallCommandService.authorizedRead ->
// AuthorizationRejected -> OUTCOME_UNKNOWN). A first INVITE slower than 2 s therefore fails here
// with the last code; the retry is harmless, it is not a recovery.
// Relay payloads are compared key-order independently: the server relays ProtocolValidator's
// canonical (recursively key-sorted) payload, not the sender's text.
import { randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import {
  acceptFrame, answerFrame, hangupFrame, iceFrame, inviteFrame, negotiateFrame, offerFrame,
  TEST_ONLY_ANSWER_SDP, TEST_ONLY_CANDIDATE, TEST_ONLY_OFFER_SDP,
} from './payloads.mjs';
import { SmokeFailure, sleep } from './transport.mjs';

const TRANSIENT = new Set(['OVERLOADED', 'UNAVAILABLE', 'OUTCOME_UNKNOWN']);
const RETRY_PAUSE_MS = 500;
const REPLY_MS = 5000;
const DECIMAL = /^(0|[1-9][0-9]{0,18})$/;
const word = (value) => (typeof value === 'string' && /^[A-Z0-9_]{1,64}$/.test(value) ? value : '?');

/** Type and codes only; never payload text. */
function describe(reply) {
  if (!reply) return 'no reply';
  const parts = [word(reply.type)];
  if (reply.error) parts.push(word(reply.error.code));
  if (reply.result) parts.push(`${word(reply.result.status)}/${word(reply.result.code)}`);
  return parts.join(' ');
}

function version(frame, label) {
  if (typeof frame.callVersion !== 'string' || !DECIMAL.test(frame.callVersion))
    throw new SmokeFailure(`${label}: callVersion is not a decimal string`);
  return BigInt(frame.callVersion);
}

function sameRoute(peer, reply, label) {
  if (reply.sessionIncarnation !== undefined && reply.sessionIncarnation !== peer.route.sessionIncarnation)
    throw new SmokeFailure(`${label}: reply bound to another session incarnation`);
}

/** Re-sends the identical frame until ACK_COMMITTED FINAL; PENDING and transient errors retry. */
export async function commit(peer, frame, label, deadlineMs) {
  const end = Date.now() + deadlineMs;
  let last = 'no reply';
  while (Date.now() < end) {
    const reply = await peer.request(frame, Math.max(1, Math.min(REPLY_MS, end - Date.now())));
    last = describe(reply);
    if (reply?.type === 'ACK_COMMITTED' && reply.ackCommitted === true && reply.result?.status === 'FINAL') {
      sameRoute(peer, reply, label);
      return reply;
    }
    const pending = reply?.type === 'COMMAND_RESULT' && reply.ackCommitted === false && reply.result?.status === 'PENDING';
    const transient = !reply || reply.type === 'OUTCOME_UNKNOWN' || (reply.type === 'ERROR' && TRANSIENT.has(reply.error?.code));
    if (!pending && !transient) throw new SmokeFailure(`${label}: expected ACK_COMMITTED, got ${last}`);
    await sleep(Math.min(RETRY_PAUSE_MS, Math.max(0, end - Date.now())));
  }
  throw new SmokeFailure(`${label}: no ACK_COMMITTED within ${deadlineMs} ms (last: ${last})`);
}

/** A relay write: COMMAND_RESULT, not committed, status VOLATILE, code WRITE_COMPLETED. */
export async function relay(peer, frame, label, deadlineMs) {
  const end = Date.now() + deadlineMs;
  let last = 'no reply';
  while (Date.now() < end) {
    const reply = await peer.request(frame, Math.max(1, Math.min(REPLY_MS, end - Date.now())));
    last = describe(reply);
    if (reply?.type === 'COMMAND_RESULT' && reply.ackCommitted === false && reply.result?.status === 'VOLATILE'
        && reply.result?.code === 'WRITE_COMPLETED') {
      sameRoute(peer, reply, label);
      if (reply.negotiationId !== frame.negotiationId || reply.iceGeneration !== frame.iceGeneration)
        throw new SmokeFailure(`${label}: write receipt for another negotiation round`);
      return reply;
    }
    const transient = !reply || reply.type === 'OUTCOME_UNKNOWN' || (reply.type === 'ERROR' && TRANSIENT.has(reply.error?.code));
    if (!transient) throw new SmokeFailure(`${label}: expected COMMAND_RESULT VOLATILE/WRITE_COMPLETED, got ${last}`);
    await sleep(Math.min(RETRY_PAUSE_MS, Math.max(0, end - Date.now())));
  }
  throw new SmokeFailure(`${label}: no relay write receipt within ${deadlineMs} ms (last: ${last})`);
}

function expectCode(reply, label, code, state) {
  if (reply.result.code !== code || (state && reply.result.state !== state))
    throw new SmokeFailure(`${label}: expected ${code}${state ? `/${state}` : ''}, got ${word(reply.result.code)}/${word(reply.result.state)}`);
}

/** Committed callVersions of one call must strictly increase, in command order. */
class Versions {
  constructor() { this.last = null; this.trail = []; }
  commit(label, frame) {
    const value = version(frame, label);
    if (this.last !== null && value <= this.last)
      throw new SmokeFailure(`${label}: callVersion ${value} is not above the previous committed ${this.last} (not monotonic)`);
    this.last = value;
    this.trail.push(`${label}=${value}`);
    return value;
  }
}

function expectEventVersion(event, label, expected) {
  const value = version(event, label);
  if (value !== expected) throw new SmokeFailure(`${label}: event callVersion ${value}, committed ${expected}`);
}

async function relayLeg(from, to, frame, label, timeoutMs, committedFloor) {
  const receipt = await relay(from, frame, label, timeoutMs);
  if (version(receipt, label) < committedFloor) throw new SmokeFailure(`${label}: relay callVersion below the granted round`);
  const delivered = await to.waitRelay(frame.type, frame.callId, frame.requestId, timeoutMs);
  if (delivered.negotiationId !== frame.negotiationId || delivered.iceGeneration !== frame.iceGeneration)
    throw new SmokeFailure(`${label}: the other side received the relay for another negotiation round`);
  // Key order is not compared (canonical relay); values, nesting and array order are.
  if (!isDeepStrictEqual(delivered.payload, frame.payload))
    throw new SmokeFailure(`${label}: the other side received a different ${frame.type} payload`);
  if (delivered.sessionIncarnation !== undefined && delivered.sessionIncarnation !== to.route.sessionIncarnation)
    throw new SmokeFailure(`${label}: relay addressed to another session incarnation`);
}

/** Runs the call; returns a printable summary (cells and versions only). */
export async function crossCellCall({ caller, callee, calleeUserId, callerCell, timeoutMs }) {
  const versions = new Versions();
  const invite = await commit(caller, inviteFrame({ requestId: randomUUID(), targetUserId: calleeUserId }), 'INVITE', timeoutMs);
  expectCode(invite, 'INVITE', 'RINGING', 'RINGING');
  const callId = invite.callId;
  if (typeof callId !== 'string' || !callId.startsWith(`${callerCell}.e`))
    throw new SmokeFailure(`INVITE: callId is not routed to the caller's cell ${callerCell}`);
  const ringing = versions.commit('INVITE', invite);
  expectEventVersion(await callee.waitEvent('RINGING', callId, timeoutMs), 'RINGING at the callee', ringing);

  const accept = await commit(callee, acceptFrame({ requestId: randomUUID(), callId }), 'ACCEPT', timeoutMs);
  expectCode(accept, 'ACCEPT', 'ACCEPTED_PENDING_ACTIVATION');
  const accepted = versions.commit('ACCEPT', accept);
  for (const [peer, side] of [[caller, 'caller'], [callee, 'callee']]) {
    const ready = await peer.waitEvent('CALL_READY', callId, timeoutMs);
    if (version(ready, `CALL_READY at the ${side}`) <= accepted)
      throw new SmokeFailure(`CALL_READY at the ${side}: callVersion not above the ACCEPT commit`);
  }

  const negotiate = await commit(caller, negotiateFrame({ requestId: randomUUID(), callId, iceRestart: false }), 'NEGOTIATE_REQUEST', timeoutMs);
  expectCode(negotiate, 'NEGOTIATE_REQUEST', 'NEGOTIATION_GRANTED');
  const granted = versions.commit('NEGOTIATE_REQUEST', negotiate);
  let round = null;
  for (const [peer, side] of [[caller, 'caller'], [callee, 'callee']]) {
    const event = await peer.waitEvent('NEGOTIATION_GRANTED', callId, timeoutMs);
    expectEventVersion(event, `NEGOTIATION_GRANTED at the ${side}`, granted);
    const ids = { negotiationId: event.payload?.negotiationId, iceGeneration: event.payload?.iceGeneration };
    if (round && (round.negotiationId !== ids.negotiationId || round.iceGeneration !== ids.iceGeneration))
      throw new SmokeFailure('NEGOTIATION_GRANTED: the two sides were granted different rounds');
    round = ids;
  }
  const scope = (requestId) => ({ requestId, callId, ...round });
  try {
    offerFrame({ ...scope(randomUUID()), sdp: TEST_ONLY_OFFER_SDP });
  } catch {
    throw new SmokeFailure('NEGOTIATION_GRANTED: negotiationId/iceGeneration are not decimal string counters');
  }
  await relayLeg(caller, callee, offerFrame({ ...scope(randomUUID()), sdp: TEST_ONLY_OFFER_SDP }), 'OFFER', timeoutMs, granted);
  await relayLeg(callee, caller, answerFrame({ ...scope(randomUUID()), sdp: TEST_ONLY_ANSWER_SDP }), 'ANSWER', timeoutMs, granted);
  await relayLeg(caller, callee, iceFrame({ ...scope(randomUUID()), startSequence: '1', candidates: [TEST_ONLY_CANDIDATE] }),
    'ICE_CANDIDATES', timeoutMs, granted);

  const hangup = await commit(callee, hangupFrame({ requestId: randomUUID(), callId }), 'HANGUP', timeoutMs);
  expectCode(hangup, 'HANGUP', 'HANGUP', 'TERMINAL');
  const terminal = versions.commit('HANGUP', hangup);
  expectEventVersion(await caller.waitEvent('CALL_TERMINATED', callId, timeoutMs), 'CALL_TERMINATED at the caller', terminal);
  return `call ${callerCell}.e* versions ${versions.trail.join(' ')}`;
}
