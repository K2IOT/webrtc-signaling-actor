const scopeKey = value => value && JSON.stringify([value.callId, value.negotiationId, value.iceGeneration, value.senderSessionIncarnation]);
/** Real PC observations; authoritative command grant and snapshot gate local offer creation. */
export class MediaRecoveryController {
  constructor(client, { createRestartOffer, onRecoveryState = () => {} } = {}) {
    if (typeof createRestartOffer !== 'function' || typeof onRecoveryState !== 'function') throw Error('MEDIA_APPLICATION_CALLBACK_REQUIRED');
    Object.assign(this, { client, createRestartOffer, onRecoveryState });
    this.sequence = 0n; this.episode = null; this.operation = null; this.closed = false;
    this.bind(); this.timer = setInterval(() => this.tick(), 100);
  }
  bind() {
    const pc = this.client.pc; this.boundPC = pc;
    this.listener = () => {
      if (this.closed || pc !== this.client.pc || !this.client.sender || this.client.pcBlocked) return;
      try { this.observe(pc.connectionState === 'failed' || pc.iceConnectionState === 'failed' ? 'failed'
        : pc.connectionState === 'connected' || ['connected', 'completed'].includes(pc.iceConnectionState) ? 'connected'
        : pc.connectionState === 'disconnected' || pc.iceConnectionState === 'disconnected' ? 'disconnected' : 'other'); }
      catch (error) { this.publish({ status: 'OBSERVATION_FAILED', code: error.message }); }
    };
    pc.addEventListener('connectionstatechange', this.listener);
    pc.addEventListener('iceconnectionstatechange', this.listener);
  }
  unbind() {
    this.boundPC?.removeEventListener('connectionstatechange', this.listener);
    this.boundPC?.removeEventListener('iceconnectionstatechange', this.listener);
    this.episode = null;
  }
  publish(value) {
    if (this.uiPhysical) return;
    const physical = Promise.resolve().then(() => this.onRecoveryState(Object.freeze(value)));
    this.uiPhysical = physical;
    let timer;
    Promise.race([physical, new Promise((_, reject) => { timer = setTimeout(() => reject(Error('MEDIA_UI_TIMEOUT')), 10000); })])
      .catch(() => {}).finally(() => clearTimeout(timer));
    physical.then(() => { this.uiPhysical = null; }, () => { this.uiPhysical = null; });
  }
  send(type, extra = {}) {
    if (this.sequence >= 9223372036854775807n) throw Error('MEDIA_SEQUENCE_EXHAUSTED');
    return this.client.observeMedia(type, { senderSequence: (++this.sequence).toString(), ...extra });
  }
  currentEpisode() {
    const episode = this.episode;
    if (episode && (episode.pc !== this.client.pc || episode.scope !== scopeKey(this.client.sender))) {
      this.episode = null;
      return null;
    }
    return episode;
  }
  observe(state) {
    const current = scopeKey(this.client.sender);
    this.currentEpisode();
    if (!current || this.closed || this.client.pcBlocked) return;
    if (state === 'connected') {
      if (this.lastState === 'connected' && this.lastScope === current) return;
      this.send(this.episode ? 'MEDIA_RECOVERED' : 'MEDIA_CONNECTED');
      this.episode = null;
    } else if (['disconnected', 'failed'].includes(state)) {
      if (!this.episode) {
        const now = this.client.now();
        this.episode = { pc: this.client.pc, scope: current, originalScope: Object.freeze({ ...this.client.sender }), callId: this.client.sender.callId,
          started: now, restartAt: now + (state === 'disconnected' ? 5000 : 0), request: null, grant: null, exhausted: false };
        this.send(state === 'failed' ? 'MEDIA_FAILED' : 'MEDIA_DISCONNECTED', state === 'failed' ? { reason: 'ICE_FAILED' } : {});
        this.publish({ status: 'RECOVERING' });
      }
    }
    this.lastState = state; this.lastScope = current;
  }
  tick() {
    const episode = this.currentEpisode(), client = this.client;
    if (!episode || this.closed || episode.pc !== client.pc || client.pcBlocked) return;
    const elapsed = client.now() - episode.started;
    if (elapsed >= 30000 && !episode.exhausted) {
      episode.exhausted = true;
      try { this.send('MEDIA_FAILED', { reason: 'RECOVERY_EXHAUSTED' }); } catch (_) { /* no replacement intent */ }
      this.publish({ status: 'RECOVERY_EXHAUSTED' }); return;
    }
    if (!episode.exhausted && !episode.request && client.now() >= episode.restartAt
        && episode.scope === scopeKey(client.sender) && client.route && client.socket?.readyState === 1) {
      episode.request = client.requestId(); // Consume exactly once, including an ambiguous send.
      try { client.command('NEGOTIATE_REQUEST', episode.callId, { iceRestart: true }, episode.request); }
      catch (error) { this.publish({ status: 'RESTART_OUTCOME_UNKNOWN', requestId: episode.request, code: error.message }); }
    }
  }
  reply(frame) {
    const episode = this.currentEpisode();
    if (!episode || frame.requestId !== episode.request || frame.result?.status !== 'FINAL'
        || !['ACK_COMMITTED', 'COMMAND_RESULT'].includes(frame.type)) return;
    if (frame.result.code === 'NEGOTIATION_GRANTED') {
      episode.grant = frame.callVersion;
      try { this.client.command('SYNC_CALL', episode.callId); }
      catch (error) { this.publish({ status: 'SNAPSHOT_REQUIRED', code: error.message }); }
    } else {
      // MEDIA_DEBOUNCE is a definitive consumed UUID; never schedule another request.
      episode.exhausted = true; this.publish({ status: 'RESTART_DENIED', code: frame.result.code });
    }
  }
  snapshot(frame) {
    const episode = this.episode, client = this.client, round = frame.result?.negotiation;
    if (!episode) return;
    const current = scopeKey(client.sender);
    if (current !== episode.scope || episode.pc !== client.pc) {
      // Only the exact version/next round produced by this original grant may extend its lifetime.
      const ownTransition = episode.pc === client.pc && episode.grant && frame.callId === episode.callId
        && frame.callVersion === episode.grant && client.sender
        && client.sender.senderSessionIncarnation === episode.originalScope.senderSessionIncarnation
        && BigInt(client.sender.negotiationId) === BigInt(episode.originalScope.negotiationId) + 1n
        && BigInt(client.sender.iceGeneration) === BigInt(episode.originalScope.iceGeneration) + 1n
        && round?.state === 'OFFER_GRANTED'
        && round.offerer?.sessionIncarnation === client.route?.sessionIncarnation
        && round.offerer?.connectionGeneration === client.route?.connectionGeneration;
      if (!ownTransition) { this.episode = null; return; }
      episode.scope = current;
    }
    if (!episode.grant || this.operation || episode.offerStarted || episode.exhausted
        || this.closed || client.pcBlocked || !client.sender
        || frame.callId !== episode.callId || frame.callVersion !== episode.grant
        || current === scopeKey(episode.originalScope)
        || round?.state !== 'OFFER_GRANTED' || round.offerer?.sessionIncarnation !== client.route?.sessionIncarnation
        || round.offerer?.connectionGeneration !== client.route?.connectionGeneration) return;
    episode.offerStarted = true;
    const sender = scopeKey(client.sender), pc = client.pc;
    let timer;
    const physical = Promise.resolve().then(async () => {
      if (episode !== this.episode || sender !== scopeKey(client.sender) || this.closed) throw Error('STALE_MEDIA_RESTART');
      this.send('ICE_RESTARTING');
      // The application callback performs actual createOffer({iceRestart:true})/setLocalDescription.
      const result = await this.createRestartOffer({ peerConnection: pc, scope: Object.freeze({ ...client.sender }), iceRestart: true });
      if (episode !== this.episode || pc !== client.pc || sender !== scopeKey(client.sender) || this.closed || client.pcBlocked) throw Error('STALE_MEDIA_RESTART');
      client.sendDescription('OFFER', result?.sdp);
    });
    this.operation = physical;
    Promise.race([physical, new Promise((_, reject) => { timer = setTimeout(() => reject(Error('MEDIA_RESTART_TIMEOUT')), 10000); })])
      .then(() => this.publish({ status: 'OFFER_SENT' }), error => {
        if (pc === client.pc) client.pcBlocked = true;
        this.publish({ status: 'RESTART_FAILED', code: error.message });
      }).finally(() => clearTimeout(timer));
    physical.then(() => { this.operation = null; }, () => { this.operation = null; });
  }
  dispose() { this.closed = true; clearInterval(this.timer); this.unbind(); }
}
