'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const {chromium} = require('playwright');
const wrtc = require('@roamhq/wrtc');
const base = process.argv[2];
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
async function post(path, body) {
  const response = await fetch(base + path, {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify(body), signal: AbortSignal.timeout(2000)});
  assert.equal(response.status, 200, `boundary rejected ${path}`);
  return response.json();
}
async function until(predicate, milliseconds = 10000) {
  const deadline = performance.now() + milliseconds;
  while (!await predicate()) { assert.ok(performance.now() < deadline, 'WebRTC bounded wait expired'); await pause(10); }
}
(async () => {
  const browser = await chromium.launch({executablePath: process.env.CHROMIUM_PATH || '/usr/bin/chromium', headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage']});
  const peer = new wrtc.RTCPeerConnection({iceServers: []});
  try {
    const page = await browser.newPage();
    await page.goto(base + '/client');
    await page.evaluate(() => {
      window.pc = new RTCPeerConnection({iceServers: []}); window.candidates = {}; window.round = 0; window.calls = 0; window.echoes = [];
      pc.onicecandidate = event => candidates[round].push(event.candidate ? event.candidate.toJSON() : null);
      window.channel = pc.createDataChannel('interop'); channel.onmessage = event => echoes.push(event.data);
    });
    let nativeChannel, round = 0, nativeCandidates = {}, nativeCalls = 0;
    peer.ondatachannel = event => { nativeChannel = event.channel; nativeChannel.onmessage = message => nativeChannel.send(message.data); };
    peer.onicecandidate = event => nativeCandidates[round].push(event.candidate ? {candidate: event.candidate.candidate, sdpMid: event.candidate.sdpMid, sdpMLineIndex: event.candidate.sdpMLineIndex, usernameFragment: event.candidate.usernameFragment || null} : null);
    let stats;
    for (round = 1; round <= 2; round++) {
      nativeCandidates[round] = [];
      await post('/init', {round});
      if (round === 2) {
        const stale = await post('/candidate', {round: 1, side: 'browser', items: [{sequence: 1, candidate: 'old-generation', sdpMid: '0', sdpMLineIndex: 0, end: false}]});
        assert.equal(stale.code, 'STALE_GENERATION');
      }
      const offer = await page.evaluate(async r => {
        window.round = r; candidates[r] = []; if (r > 1) pc.restartIce();
        const offer = await pc.createOffer({iceRestart: r > 1}); await pc.setLocalDescription(offer); return {type: offer.type, sdp: offer.sdp};
      }, round);
      const forwardedOffer = await post('/description', {side: 'browser', sdp: offer.sdp, requestId: crypto.randomUUID()});
      await peer.setRemoteDescription({type: 'offer', sdp: forwardedOffer.sdp});
      const answer = await peer.createAnswer(); await peer.setLocalDescription(answer);
      const forwardedAnswer = await post('/description', {side: 'native', sdp: answer.sdp, requestId: crypto.randomUUID()});
      await page.evaluate(sdp => pc.setRemoteDescription({type: 'answer', sdp}), forwardedAnswer.sdp);
      const browserUfrag = offer.sdp.match(/^a=ice-ufrag:(.+)$/m)[1].trim();
      const nativeUfrag = answer.sdp.match(/^a=ice-ufrag:(.+)$/m)[1].trim();
      await post('/ready', {side: 'browser', ufrag: browserUfrag}); await post('/ready', {side: 'native', ufrag: nativeUfrag});
      await until(async () => nativeCandidates[round].includes(null) && await page.evaluate(r => candidates[r].includes(null), round));
      const bySide = {browser: await page.evaluate(r => candidates[r], round), native: nativeCandidates[round]};
      for (const side of ['browser', 'native']) {
        const raw = bySide[side].filter(Boolean); if (side === 'native') assert.ok(raw.length > 0, `real native host candidates required: round=${round}`);
        const items = raw.map((candidate, index) => ({...candidate, usernameFragment: candidate.usernameFragment || (side === 'browser' ? browserUfrag : nativeUfrag), sequence: index + 1, end: false}));
        items.push({sequence: items.length + 1, candidate: null, usernameFragment: side === 'browser' ? browserUfrag : nativeUfrag, end: true});
        // Early end, reorder, repeated batch and retry during an uncompleted ICE-agent call.
        await post('/candidate', {round, side, items: [items.at(-1)]});
        if (items.length > 2) await post('/candidate', {round, side, items: [items[1]]});
        await post('/candidate', {round, side, items: [items[0]]});
        await post('/candidate', {round, side, items: [items[0]]});
        for (const item of items.slice(1)) await post('/candidate', {round, side, items: [item]});
        for (const expected of items) {
          const item = await post('/next', {side}); assert.equal(item.sequence, expected.sequence);
          if (side === 'browser') {
            await peer.addIceCandidate(item.end ? {candidate: '', sdpMid: '0', sdpMLineIndex: 0} : new wrtc.RTCIceCandidate(item)); nativeCalls++;
          } else {
            await page.evaluate(async candidate => { await pc.addIceCandidate(candidate.end ? null : candidate); window.calls++; }, item);
          }
          await post('/applied', {side, sequence: item.sequence});
        }
        assert.equal((await post('/candidate', {round, side, items: [items[0]]})).code, 'ENDED');
        stats = await post('/stats', {}); assert.equal(stats[side + round], items.length);
      }
      await until(async () => nativeChannel && nativeChannel.readyState === 'open' && await page.evaluate(() => channel.readyState === 'open' && pc.iceConnectionState !== 'failed'));
      const message = `round-${round}`; await page.evaluate(text => channel.send(text), message);
      await until(() => page.evaluate(text => echoes.includes(text), message));
    }
    assert.equal(nativeCalls, stats.browser1 + stats.browser2);
    assert.equal(await page.evaluate(() => calls), stats.native1 + stats.native2);
    assert.ok(stats.browserEnded && stats.nativeEnded);
    console.log(JSON.stringify({browser: browser.version(), native: require('@roamhq/wrtc/package.json').version, rounds: 2, dataChannelMessages: 2, nativeIceCalls: nativeCalls, browserIceCalls: await page.evaluate(() => calls), browserEnded: stats.browserEnded, nativeEnded: stats.nativeEnded}));
  } finally { peer.close(); await browser.close(); }
})().then(() => process.exit(0), failure => { console.error(failure.name + ': ' + failure.message); process.exit(1); });
