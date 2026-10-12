/** TEST ONLY: actual Chromium IndexedDB/RTCPeerConnection; no production authority fixture. */
const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs/promises');
const path = require('node:path');
const { chromium } = require('../../signaling-integration-tests/src/test/webrtc/node_modules/playwright');

test('durable journal privacy/reload/concurrent tabs/disposal and actual pre-DTLS ICE gap/retry/end ingestion', { timeout: 60000 }, async () => {
  const allowed = new Set(['durable-command-journal.mjs', 'native-client.mjs', 'media-recovery-controller.mjs']);
  const server = http.createServer(async (req, res) => {
    const name = (req.url ?? '').slice(1);
    if (name === '') { res.end('<!doctype html><title>TEST ONLY</title>'); return; }
    if (!allowed.has(name)) { res.writeHead(404); res.end(); return; }
    res.setHeader('Content-Type', 'text/javascript');
    try { res.end(await fs.readFile(path.join(__dirname, name))); }
    catch { res.writeHead(500); res.end(); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  let browser;
  try {
    browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || '/usr/bin/google-chrome', headless: true, args: ['--no-sandbox'] });
    const context = await browser.newContext();
    const url = `http://127.0.0.1:${server.address().port}/`;
    const pages = [await context.newPage(), await context.newPage()];
    for (const page of pages) await page.goto(url);
    const setup = async page => page.evaluate(async () => {
      const { DurableCommandJournal, principalBinding } = await import('/durable-command-journal.mjs');
      window.journal = new DurableCommandJournal();
      window.owner = principalBinding({ issuer: 'TEST_ONLY', jti: 'A', userId: 'a' }, 'c001.e1');
      window.encode = (id, payload = { targetUserId: 'b' }) => JSON.stringify({ v: 1, type: 'INVITE', requestId: id, payload });
    });
    await setup(pages[0]);
    const id = await pages[0].evaluate(async () => {
      const id = crypto.randomUUID(); await journal.persist(owner, encode(id)); return id;
    });
    await pages[0].reload(); await setup(pages[0]);
    const result = await pages[0].evaluate(async id => {
      const { principalBinding } = await import('/durable-command-journal.mjs');
      const rows = await journal.unresolved(owner);
      let privateError, duplicateError;
      try { await journal.persist(owner, encode(crypto.randomUUID(), { targetUserId: 'b', nested: { jwt: 'TEST_ONLY_SECRET' } })); } catch (e) { privateError = e.message; }
      try { await journal.persist(owner, encode(id, { targetUserId: 'changed' })); } catch (e) { duplicateError = e.message; }
      const other = await journal.unresolved(principalBinding({ issuer: 'TEST_ONLY', jti: 'B', userId: 'a' }, 'c001.e1'));
      const domain = await journal.unresolved(principalBinding({ issuer: 'TEST_ONLY', jti: 'A', userId: 'a' }, 'c002.e1'));
      return { ids: rows.map(r => r.requestId), privateError, duplicateError, other: other.length, domain: domain.length, leaked: JSON.stringify(rows).includes('TEST_ONLY_SECRET') };
    }, id);
    assert.deepEqual(result, { ids: [id], privateError: 'INVALID_DURABLE_USER', duplicateError: 'REQUEST_ID_REUSED', other: 0, domain: 0, leaked: false });
    await pages[0].evaluate(async id => journal.settle(owner, id), id);
    const disposal = await pages[0].evaluate(async () => {
      const { NativeBrowserClient } = await import('/native-client.mjs');
      const pc = new RTCPeerConnection();
      const client = new NativeBrowserClient({ peerConnection: pc, authorityDomain: 'c001.e1', journal, principal: { issuer: 'TEST_ONLY', jti: 'A', userId: 'a' }, handleControlEvent: async () => {}, onResyncRequired: () => {} });
      let sends = 0; client.sendEncoded = () => { sends++; };
      const id = crypto.randomUUID(), pending = client.persistCommand('INVITE', null, { targetUserId: 'b' }, id);
      client.dispose();
      let error; try { await pending; } catch (e) { error = e.message; }
      const rows = await journal.unresolved(owner); pc.close(); await journal.settle(owner, id);
      return { error, sends, persisted: rows.some(r => r.requestId === id) };
    });
    assert.deepEqual(disposal, { error: 'CLIENT_CLOSED', sends: 0, persisted: true });
    // TEST ONLY transport boundary: hold the answer so no DTLS/data channel exists.
    const ice = await pages[0].evaluate(async () => {
      const { NativeBrowserClient } = await import('/native-client.mjs');
      const pcs = [new RTCPeerConnection(), new RTCPeerConnection()];
      const peers = [crypto.randomUUID(), crypto.randomUUID()];
      const callId = `c001.e1.${crypto.randomUUID()}`;
      const errors = [], receipts = [], original = new Map(), sends = new Map();
      let dropped, tail = Promise.resolve();
      const clients = pcs.map(pc => new NativeBrowserClient({ peerConnection: pc, authorityDomain: 'c001.e1', initialIceReceipts: true,
        handleControlEvent: async () => {}, onResyncRequired: value => errors.push(value) }));
      try {
        for (let n = 0; n < 2; n++) {
          const route = { sessionIncarnation: peers[n], connectionGeneration: '1' };
          await clients[n].receive({ v: 1, type: 'AUTH_OK', ...route, initialIceReceiptsV1: true });
          await clients[n].receive({ v: 1, type: 'CALL_SNAPSHOT', ...route, callId, callVersion: '4', result: {
            state: 'CONNECTING', resetPeerConnection: false,
            caller: { sessionIncarnation: peers[0], connectionGeneration: '1' }, winner: { sessionIncarnation: peers[1], connectionGeneration: '1' },
            negotiation: { negotiationId: '1', iceGeneration: '1' } } });
          clients[n].sendEncoded = encoded => {
            const frame = JSON.parse(encoded);
            const old = original.get(frame.requestId);
            if (old && old !== encoded) errors.push('IDENTITY_CHANGED');
            original.set(frame.requestId, encoded);
            sends.set(frame.requestId, (sends.get(frame.requestId) ?? 0) + 1);
            if (n === 0 && frame.type === 'ICE_CANDIDATES' && !dropped) { dropped = frame.requestId; return; }
            if (frame.type === 'INITIAL_ICE_RECEIPT') receipts.push({ payload: frame.payload, state: pcs[n].connectionState, channel: clients[n].receiptChannel });
            tail = tail.then(() => clients[1 - n].receive({ ...frame, sessionIncarnation: peers[1 - n], connectionGeneration: '1', sender: { incarnation: { value: peers[n] } } })).catch(e => errors.push(e.message));
          };
        }
        pcs[0].createDataChannel('TEST_ONLY');
        const candidates = [];
        const gathered = new Promise((resolve, reject) => {
          const timer = setTimeout(() => reject(Error('ICE_GATHER_TIMEOUT')), 5000);
          pcs[0].onicecandidate = event => {
            if (event.candidate) candidates.push(event.candidate.toJSON());
            else { clearTimeout(timer); resolve(); }
          };
        });
        await pcs[0].setLocalDescription(await pcs[0].createOffer()); await gathered;
        if (!candidates.length) throw Error('NO_ACTUAL_ICE');
        const sdp = pcs[0].localDescription.sdp.split('\r\n').filter(line => !line.startsWith('a=candidate:') && line !== 'a=end-of-candidates').join('\r\n');
        clients[0].sendDescription('OFFER', sdp); await tail;
        clients[0].sendIceCandidates([candidates[0]]);
        clients[0].sendIceCandidates([candidates[0]]); await tail;
        clients[0].sendEndOfCandidates();
        const deadline = performance.now() + 5000;
        while (!receipts.some(r => r.payload.endReceived) && performance.now() < deadline) {
          await new Promise(resolve => setTimeout(resolve, 25)); await tail;
        }
        return { errors, retransmittedOriginal: sends.get(dropped) > 1,
          gap: receipts.some(r => r.payload.highestContiguous === '0' && r.payload.missingRanges.some(p => p[0] === '1')),
          ingested: receipts.some(r => r.payload.highestContiguous === '2' && r.payload.endReceived),
          beforeDtls: receipts.length > 0 && receipts.every(r => r.state !== 'connected' && r.channel === null) };
      } finally { clients.forEach(c => c.dispose()); pcs.forEach(pc => pc.close()); }
    });
    assert.deepEqual(ice, { errors: [], retransmittedOriginal: true, gap: true, ingested: true, beforeDtls: true });
    await setup(pages[1]);
    const outcomes = await Promise.all(pages.map(page => page.evaluate(async () => {
      const results = await Promise.allSettled(Array.from({ length: 140 }, () => journal.persist(owner, encode(crypto.randomUUID()))));
      return { ok: results.filter(r => r.status === 'fulfilled').length, errors: results.filter(r => r.status === 'rejected').map(r => r.reason.message) };
    })));
    assert.equal(outcomes.reduce((n, r) => n + r.ok, 0), 256);
    assert.equal(outcomes.flatMap(r => r.errors).length, 24);
    assert.ok(outcomes.flatMap(r => r.errors).every(e => e === 'DURABLE_COMMAND_WINDOW_FULL'));
  } finally {
    if (browser) await browser.close();
    await new Promise(resolve => server.close(resolve));
  }
});
