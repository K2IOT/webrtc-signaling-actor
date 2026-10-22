// Pure builders of the public WSS v1 frames the smoke test sends. LOCAL_TEST_ONLY / NOT_QUALIFIED.
// Shapes follow signaling-protocol ProtocolValidator (field allow-lists, decimal-string counters,
// UUID requestIds, routed callIds) and frame exactly like clients/native-browser/native-client.mjs
// (key order v, type, requestId, callId, negotiationId, iceGeneration, payload). Pinned by
// payloads.test.mjs and, through fixtures/frames.json, by the Java ComposeSmokeFramesTest.
// `node payloads.mjs --emit-fixtures` rewrites fixtures/frames.json.
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

export const SUBPROTOCOL = 'webrtc-signaling.v1';
const MAX_COUNTER = 9223372036854775807n;
const MAX_FRAME_BYTES = 81920;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const CALL_ID = /^[a-z][a-z0-9-]{0,23}\.e[1-9][0-9]*\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const COUNTER = /^[1-9][0-9]{0,18}$/;
const bytes = (text) => Buffer.byteLength(text, 'utf8');

/** TEST_ONLY SDP bodies on documentation addresses (RFC 5737); the gateway relays them opaquely. */
export const TEST_ONLY_OFFER_SDP = 'v=0\r\no=- 1 1 IN IP4 192.0.2.10\r\ns=LOCAL_TEST_ONLY smoke offer\r\nt=0 0\r\n'
  + 'm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 192.0.2.10\r\na=mid:0\r\n';
export const TEST_ONLY_ANSWER_SDP = 'v=0\r\no=- 2 1 IN IP4 192.0.2.20\r\ns=LOCAL_TEST_ONLY smoke answer\r\nt=0 0\r\n'
  + 'm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 192.0.2.20\r\na=mid:0\r\n';
export const TEST_ONLY_CANDIDATE = Object.freeze({
  candidate: 'candidate:1 1 udp 2122260223 192.0.2.10 54400 typ host', sdpMid: '0', sdpMLineIndex: 0,
});

function fail(name, rule) {
  throw new TypeError(`${name} ${rule}`);
}

/** ProtocolValidator.string: textual, not blank, bounded UTF-8, no lone surrogate. */
function text(value, name, maxBytes) {
  if (typeof value !== 'string' || !value.trim() || bytes(value) > maxBytes || !value.isWellFormed())
    fail(name, `must be a non-blank well-formed string of at most ${maxBytes} bytes`);
  return value;
}

/** Identity.UserId: additionally no surrounding whitespace (never trimmed). */
function userId(value, name) {
  text(value, name, 256);
  if (value !== value.trim() || value !== value.normalize('NFC')) fail(name, 'must be NFC with no surrounding whitespace');
  return value;
}

/** ProtocolValidator.counter: a positive decimal string that fits a signed 64-bit long. */
export function counter(value, name) {
  if (typeof value !== 'string' || !COUNTER.test(value) || BigInt(value) > MAX_COUNTER)
    fail(name, 'must be a positive decimal string counter');
  return value;
}

function uuid(value, name) {
  if (typeof value !== 'string' || !UUID.test(value)) fail(name, 'must be a canonical lower-case UUID');
  return value;
}

function callId(value) {
  if (typeof value !== 'string' || bytes(value) > 96 || !CALL_ID.test(value)) fail('callId', 'must be <cell>.e<epoch>.<uuid>');
  return value;
}

function candidate(value) {
  const allowed = ['candidate', 'sdpMid', 'sdpMLineIndex', 'usernameFragment'];
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some((k) => !allowed.includes(k)))
    fail('candidate', 'must be an object with only candidate, sdpMid, sdpMLineIndex, usernameFragment');
  const result = { candidate: text(value.candidate, 'candidate.candidate', 2048) };
  if (value.sdpMid == null && value.sdpMLineIndex == null) fail('candidate', 'needs sdpMid or sdpMLineIndex');
  if (value.sdpMid != null) result.sdpMid = text(value.sdpMid, 'candidate.sdpMid', 256);
  if (value.sdpMLineIndex != null) {
    if (!Number.isInteger(value.sdpMLineIndex) || value.sdpMLineIndex < 0 || value.sdpMLineIndex > 65535)
      fail('candidate.sdpMLineIndex', 'must be an integer in 0..65535');
    result.sdpMLineIndex = value.sdpMLineIndex;
  }
  if (value.usernameFragment !== undefined) result.usernameFragment = text(value.usernameFragment, 'candidate.usernameFragment', 256);
  return result;
}

/** AUTH carries no requestId, callId or counters (ProtocolValidator AUTH branch). */
export function authFrame(jwt) {
  return { v: 1, type: 'AUTH', payload: { token: text(jwt, 'token', 8192) } };
}

/** INVITE is scoped to the caller's home cell: no callId or counters. */
export function inviteFrame({ requestId, targetUserId }) {
  return { v: 1, type: 'INVITE', requestId: uuid(requestId, 'requestId'), payload: { targetUserId: userId(targetUserId, 'targetUserId') } };
}

function callCommand(type, { requestId, callId: call }, payload = {}) {
  return { v: 1, type, requestId: uuid(requestId, 'requestId'), callId: callId(call), payload };
}

export const acceptFrame = (scope) => callCommand('ACCEPT', scope);
export const hangupFrame = (scope) => callCommand('HANGUP', scope);

export function negotiateFrame({ iceRestart = false, ...scope }) {
  if (typeof iceRestart !== 'boolean') fail('iceRestart', 'must be a boolean');
  return callCommand('NEGOTIATE_REQUEST', scope, { iceRestart });
}

export function eventReceivedFrame({ eventId, ...scope }) {
  return callCommand('EVENT_RECEIVED', scope, { eventId: uuid(eventId, 'eventId') });
}

function relay(type, { requestId, callId: call, negotiationId, iceGeneration }, payload) {
  return {
    v: 1, type, requestId: uuid(requestId, 'requestId'), callId: callId(call),
    negotiationId: counter(negotiationId, 'negotiationId'), iceGeneration: counter(iceGeneration, 'iceGeneration'), payload,
  };
}

export const offerFrame = ({ sdp, ...scope }) => relay('OFFER', scope, { sdp: text(sdp, 'sdp', 65536) });
export const answerFrame = ({ sdp, ...scope }) => relay('ANSWER', scope, { sdp: text(sdp, 'sdp', 65536) });

export function iceFrame({ startSequence, candidates, ...scope }) {
  if (!Array.isArray(candidates) || candidates.length < 1 || candidates.length > 20) fail('candidates', 'must hold 1..20 entries');
  const start = counter(startSequence, 'startSequence');
  if (BigInt(start) > MAX_COUNTER - BigInt(candidates.length)) fail('startSequence', 'leaves no room for the batch');
  const payload = { startSequence: start, candidates: candidates.map(candidate) };
  if (bytes(JSON.stringify(payload)) > 8192) fail('candidates', 'exceed the 8192-byte ICE batch');
  return relay('ICE_CANDIDATES', scope, payload);
}

/** The wire text; bounded like the gateway's public frame limit. */
export function encode(frame) {
  const encoded = JSON.stringify(frame);
  if (bytes(encoded) > MAX_FRAME_BYTES) fail('frame', `exceeds ${MAX_FRAME_BYTES} bytes`);
  return encoded;
}

/** Fixed TEST_ONLY inputs for fixtures/frames.json, read by ComposeSmokeFramesTest. */
export function buildFixtures() {
  const id = (n) => `00000000-0000-4000-8000-${n.toString(16).padStart(12, '0')}`;
  const call = `c001.e1.${id(0xff)}`;
  const scope = (n) => ({ requestId: id(n), callId: call, negotiationId: '1', iceGeneration: '1' });
  const frames = [
    ['auth', authFrame('TEST_ONLY_NOT_A_JWT')],
    ['invite', inviteFrame({ requestId: id(1), targetUserId: 'TEST_ONLY_callee' })],
    ['accept', acceptFrame({ requestId: id(2), callId: call })],
    ['negotiate', negotiateFrame({ requestId: id(3), callId: call, iceRestart: false })],
    ['offer', offerFrame({ ...scope(4), sdp: TEST_ONLY_OFFER_SDP })],
    ['answer', answerFrame({ ...scope(5), sdp: TEST_ONLY_ANSWER_SDP })],
    ['ice', iceFrame({ ...scope(6), startSequence: '1', candidates: [TEST_ONLY_CANDIDATE] })],
    ['hangup', hangupFrame({ requestId: id(7), callId: call })],
    ['event-received', eventReceivedFrame({ requestId: id(8), callId: call, eventId: id(9) })],
  ];
  return {
    status: 'TEST_ONLY / LOCAL_TEST_ONLY / NOT_QUALIFIED. Generated by `node deploy/compose/smoke/payloads.mjs --emit-fixtures`; do not edit.',
    frames: frames.map(([name, frame]) => ({ name, frame })),
  };
}

if (process.argv[1] === fileURLToPath(import.meta.url) && process.argv.includes('--emit-fixtures')) {
  const target = fileURLToPath(new URL('./fixtures/frames.json', import.meta.url));
  writeFileSync(target, `${JSON.stringify(buildFixtures(), null, 2)}\n`);
  console.log(`wrote ${target}`);
}
