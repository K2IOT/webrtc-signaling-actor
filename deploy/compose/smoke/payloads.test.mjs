// Pins the smoke wire builders. LOCAL_TEST_ONLY / NOT_QUALIFIED. Run: node --test payloads.test.mjs
// Three references: fixtures/java-frames.json (frames copied or derived from the Java validator
// tests, source cited per entry), the real browser client clients/native-browser/native-client.mjs,
// and fixtures/frames.json, which ComposeSmokeFramesTest feeds through the real ProtocolValidator.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import * as payloads from './payloads.mjs';
import { NativeBrowserClient } from '../../../clients/native-browser/native-client.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const javaFrames = JSON.parse(readFileSync(join(here, 'fixtures/java-frames.json'), 'utf8'));
const REQUEST = '00000000-0000-4000-8000-0000000000a1';
const CALL = 'c001.e1.00000000-0000-4000-8000-0000000000b2';
const EVENT = '00000000-0000-4000-8000-0000000000c3';
const INCARNATION = '00000000-0000-4000-8000-0000000000d4';

test('auth_frame_shape', () => {
  const frame = payloads.authFrame('TEST_ONLY_NOT_A_JWT');
  assert.deepStrictEqual(frame, { v: 1, type: 'AUTH', payload: { token: 'TEST_ONLY_NOT_A_JWT' } });
  assert.equal(Object.hasOwn(frame, 'requestId'), false, 'AUTH carries no requestId');
  assert.throws(() => payloads.authFrame(''), TypeError);
  assert.throws(() => payloads.authFrame('x'.repeat(8193)), TypeError);
});

test('invite_frame_shape_and_decimal_string_counters', () => {
  const invite = payloads.inviteFrame({ requestId: REQUEST, targetUserId: '6acdfda663f4055d3f6672d7' });
  assert.deepStrictEqual(invite, {
    v: 1, type: 'INVITE', requestId: REQUEST, payload: { targetUserId: '6acdfda663f4055d3f6672d7' },
  });
  for (const forbidden of ['callId', 'negotiationId', 'iceGeneration'])
    assert.equal(Object.hasOwn(invite, forbidden), false, `INVITE has no ${forbidden}`);
  assert.throws(() => payloads.inviteFrame({ requestId: REQUEST, targetUserId: ' bob' }), TypeError, 'no trimming');
  assert.throws(() => payloads.inviteFrame({ requestId: 'not-a-uuid', targetUserId: 'bob' }), TypeError);
  assert.throws(() => payloads.inviteFrame({ requestId: REQUEST.toUpperCase(), targetUserId: 'bob' }), TypeError);

  const scope = { requestId: REQUEST, callId: CALL, negotiationId: '9007199254740993', iceGeneration: '1' };
  const offer = payloads.offerFrame({ ...scope, sdp: 'v=0' });
  assert.equal(offer.negotiationId, '9007199254740993', 'full 64-bit precision as a decimal string');
  assert.equal(typeof offer.iceGeneration, 'string');
  const ice = payloads.iceFrame({ ...scope, startSequence: '1', candidates: [payloads.TEST_ONLY_CANDIDATE] });
  assert.equal(ice.payload.startSequence, '1');
  for (const bad of [1, '0', '01', '-1', '9223372036854775808', 1n, '']) {
    assert.throws(() => payloads.offerFrame({ ...scope, negotiationId: bad, sdp: 'v=0' }), TypeError, `negotiationId ${bad}`);
    assert.throws(() => payloads.iceFrame({ ...scope, startSequence: bad, candidates: [payloads.TEST_ONLY_CANDIDATE] }), TypeError);
  }
  assert.throws(() => payloads.offerFrame({ ...scope, callId: 'c001.e0.' + REQUEST, sdp: 'v=0' }), TypeError);
  assert.throws(() => payloads.iceFrame({ ...scope, startSequence: '1', candidates: [] }), TypeError);
  assert.throws(() => payloads.iceFrame({ ...scope, startSequence: '1',
    candidates: Array(21).fill(payloads.TEST_ONLY_CANDIDATE) }), TypeError);
  assert.throws(() => payloads.iceFrame({ ...scope, startSequence: '1',
    candidates: [{ candidate: 'candidate' }] }), TypeError, 'sdpMid or sdpMLineIndex required');
});

test('frames_validate_against_java_fixtures', () => {
  assert.ok(javaFrames.frames.length >= 9);
  const builders = new Set();
  for (const entry of javaFrames.frames) {
    assert.match(entry.source, /\S/, `${entry.name} cites its Java source`);
    const build = payloads[entry.builder];
    assert.equal(typeof build, 'function', entry.builder);
    assert.deepStrictEqual(build(...entry.args), entry.frame, entry.name);
    builders.add(entry.builder);
  }
  for (const name of ['authFrame', 'inviteFrame', 'acceptFrame', 'offerFrame', 'answerFrame', 'iceFrame', 'hangupFrame'])
    assert.ok(builders.has(name), `java fixture for ${name}`);
});

/** A real NativeBrowserClient on a capturing fake socket; the builders must frame identically. */
function nativeClient(requestId) {
  const sent = [];
  const socket = {
    readyState: 1, bufferedAmount: 0, send: (text) => sent.push(text),
    addEventListener() {}, removeEventListener() {},
  };
  const client = new NativeBrowserClient({
    peerConnection: { addIceCandidate: async () => {} }, authorityDomain: 'TEST_ONLY',
    handleControlEvent: () => {}, onResyncRequired: () => {}, requestId: () => requestId,
  });
  client.attachWebSocket(socket);
  return { client, sent };
}

test('frames_match_the_native_browser_client', () => {
  const { client, sent } = nativeClient(REQUEST);
  try {
    client.authenticate('TEST_ONLY_NOT_A_JWT');
    client.command('INVITE', null, { targetUserId: 'bob' }, REQUEST);
    client.releaseCommand(REQUEST);
    client.pending.clear();
    client.command('ACCEPT', CALL, {}, REQUEST); client.pending.clear();
    client.command('NEGOTIATE_REQUEST', CALL, { iceRestart: false }, REQUEST); client.pending.clear();
    client.command('HANGUP', CALL, {}, REQUEST); client.pending.clear();
    client.command('EVENT_RECEIVED', CALL, { eventId: EVENT }, REQUEST); client.pending.clear();
    client.sender = Object.freeze({ callId: CALL, negotiationId: '1', iceGeneration: '1', senderSessionIncarnation: INCARNATION });
    client.sendDescription('OFFER', 'v=0', REQUEST);
    client.sendDescription('ANSWER', 'v=0', REQUEST);
    client.sendIceCandidates([payloads.TEST_ONLY_CANDIDATE]);
  } finally {
    client.dispose();
  }
  const scope = { requestId: REQUEST, callId: CALL, negotiationId: '1', iceGeneration: '1' };
  const expected = [
    payloads.authFrame('TEST_ONLY_NOT_A_JWT'),
    payloads.inviteFrame({ requestId: REQUEST, targetUserId: 'bob' }),
    payloads.acceptFrame({ requestId: REQUEST, callId: CALL }),
    payloads.negotiateFrame({ requestId: REQUEST, callId: CALL, iceRestart: false }),
    payloads.hangupFrame({ requestId: REQUEST, callId: CALL }),
    payloads.eventReceivedFrame({ requestId: REQUEST, callId: CALL, eventId: EVENT }),
    payloads.offerFrame({ ...scope, sdp: 'v=0' }),
    payloads.answerFrame({ ...scope, sdp: 'v=0' }),
    payloads.iceFrame({ ...scope, startSequence: '1', candidates: [payloads.TEST_ONLY_CANDIDATE] }),
  ].map(payloads.encode);
  // Byte-for-byte, so key order and counter encoding match the browser client too.
  assert.deepStrictEqual(sent, expected);
});

test('emitted_fixtures_are_current_for_the_java_validator_test', () => {
  const committed = JSON.parse(readFileSync(join(here, 'fixtures/frames.json'), 'utf8'));
  assert.deepStrictEqual(committed, payloads.buildFixtures(),
    'fixtures/frames.json is stale: run `node payloads.mjs --emit-fixtures`');
  const types = committed.frames.map((f) => f.frame.type);
  for (const type of ['AUTH', 'INVITE', 'ACCEPT', 'NEGOTIATE_REQUEST', 'OFFER', 'ANSWER', 'ICE_CANDIDATES', 'HANGUP', 'EVENT_RECEIVED'])
    assert.ok(types.includes(type), `frames.json carries ${type}`);
  assert.match(committed.status, /TEST_ONLY/);
});

test('encode_bounds_the_frame_to_the_public_limit', () => {
  const big = payloads.offerFrame({ requestId: REQUEST, callId: CALL, negotiationId: '1', iceGeneration: '1',
    sdp: 'a'.repeat(65536) });
  assert.ok(payloads.encode(big).length <= 81920);
  assert.throws(() => payloads.offerFrame({ requestId: REQUEST, callId: CALL, negotiationId: '1', iceGeneration: '1',
    sdp: 'a'.repeat(65537) }), TypeError);
});
