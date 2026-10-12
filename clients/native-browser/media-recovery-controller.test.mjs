import test from 'node:test';
import assert from 'node:assert/strict';
import { MediaRecoveryController } from './media-recovery-controller.mjs';

function fixture() {
  let now = 0;
  const sent = [], offers = [], states = [];
  const client = {
    pc: new EventTarget(), pcBlocked: false,
    sender: { callId: 'c001.e1.test', negotiationId: '1', iceGeneration: '1', senderSessionIncarnation: 'sender' },
    route: { sessionIncarnation: 'sender', connectionGeneration: '1' }, socket: { readyState: 1 },
    now: () => now, requestId: () => 'original-restart',
    command: (...args) => sent.push(args), observeMedia: (...args) => sent.push(args),
    sendDescription: (...args) => offers.push(args),
  };
  let resolve;
  const physical = new Promise(done => { resolve = done; });
  const controller = new MediaRecoveryController(client, { createRestartOffer: () => physical, onRecoveryState: state => states.push(state.status) });
  controller.observe('failed'); controller.tick();
  controller.reply({ type: 'ACK_COMMITTED', requestId: 'original-restart', callVersion: '2',
    result: { status: 'FINAL', code: 'NEGOTIATION_GRANTED' } });
  client.sender = { ...client.sender, negotiationId: '2', iceGeneration: '2' };
  now = 29000;
  controller.snapshot({ callId: client.sender.callId, callVersion: '2', result: { negotiation: {
    state: 'OFFER_GRANTED', offerer: client.route } } });
  return { client, controller, offers, sent, states, resolve, setNow: value => { now = value; } };
}

const settle = () => new Promise(resolve => setImmediate(resolve));

test('restart SDP physically completed after original 30s deadline cannot send an offer', async () => {
  const f = fixture();
  try {
    await settle();
    f.setNow(30000); f.controller.tick();
    f.resolve({ sdp: 'late' }); await settle();
    assert.deepEqual(f.offers, []);
    assert.equal(f.client.pcBlocked, true);
    assert.equal(f.sent.filter(args => args[0] === 'NEGOTIATE_REQUEST').length, 1);
  } finally { f.controller.dispose(); }
});

test('completion from retired PC cannot send or block its replacement', async () => {
  const f = fixture();
  try {
    await settle();
    f.client.pc = new EventTarget();
    f.resolve({ sdp: 'old-PC' }); await settle();
    assert.deepEqual(f.offers, []);
    assert.equal(f.client.pcBlocked, false);
    assert.equal(f.states.includes('RESTART_FAILED'), false);
  } finally { f.controller.dispose(); }
});
