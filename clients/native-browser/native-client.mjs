import { principalBinding } from "./durable-command-journal.mjs";
import { MediaRecoveryController } from "./media-recovery-controller.mjs";
// Native public WSS v1. No HTTP fixture or invented ICE_ACK server operation.
export const VERSION = "1.0.0";
export const RECEIPT_PROTOCOL = "webrtc-signaling-ice-receipts.v1";
export const INITIAL_RECEIPT_PROTOCOL = "native-initial-ice-receipt-v1";
const MAX = 9223372036854775807n;
const bytes = value => new TextEncoder().encode(typeof value === "string" ? value : JSON.stringify(value)).length;
const uuid = value => {
  if (typeof value !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) throw Error("INVALID_UUID");
  return value;
};
export function decimal64(value, zero = false) {
  if (typeof value !== "string" || !/^(0|[1-9][0-9]{0,18})$/.test(value)) throw Error("INVALID_COUNTER");
  const result = BigInt(value);
  if (result > MAX || (!zero && result === 0n)) throw Error("INVALID_COUNTER");
  return result;
}
function scope(callId, negotiationId, iceGeneration, senderSessionIncarnation) {
  if (typeof callId !== "string" || bytes(callId) > 96 || !/^[a-z][a-z0-9-]{0,23}\.e[1-9][0-9]*\.[0-9a-f-]{36}$/.test(callId)) throw Error("INVALID_CALL");
  decimal64(negotiationId); decimal64(iceGeneration); uuid(senderSessionIncarnation);
  return Object.freeze({ callId, negotiationId, iceGeneration, senderSessionIncarnation });
}
const key = value => JSON.stringify([value.callId, value.negotiationId, value.iceGeneration, value.senderSessionIncarnation]);
function freezeJson(value) {
  if (value && typeof value === "object") {
    Object.values(value).forEach(freezeJson);
    Object.freeze(value);
  }
  return value;
}
function candidate(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)
      || Object.keys(value).some(k => !["candidate", "sdpMid", "sdpMLineIndex", "usernameFragment"].includes(k))
      || typeof value.candidate !== "string" || !value.candidate || bytes(value.candidate) > 2048
      || (value.sdpMid == null && value.sdpMLineIndex == null)
      || (value.sdpMid != null && (typeof value.sdpMid !== "string" || !value.sdpMid || bytes(value.sdpMid) > 256))
      || (value.sdpMLineIndex != null && (!Number.isInteger(value.sdpMLineIndex) || value.sdpMLineIndex < 0 || value.sdpMLineIndex > 65535))
      || (value.usernameFragment !== undefined && (typeof value.usernameFragment !== "string" || !value.usernameFragment || bytes(value.usernameFragment) > 256))) throw Error("INVALID_CANDIDATE");
  return structuredClone(value);
}

/** One active call, one ordered ICE agent. All application callbacks must settle within 10s. */
export class NativeBrowserClient {
  constructor({ peerConnection, authorityDomain, handleControlEvent, onMessage, onResyncRequired,
    onResetPeerConnection, journal, principal, mediaRecovery, initialIceReceipts = false,
    requestId = () => crypto.randomUUID(), now = () => performance.now() }) {
    if (!peerConnection || typeof peerConnection.addIceCandidate !== "function"
        || typeof handleControlEvent !== "function" || typeof onResyncRequired !== "function"
        || typeof authorityDomain !== "string" || !authorityDomain.trim() || bytes(authorityDomain) > 256) throw Error("CLIENT_CONTRACT_REQUIRED");
    // Supplied by trusted enrollment; never inferred from a relay, snapshot or socket reconnect.
    Object.defineProperty(this, "authorityDomain", { value: authorityDomain, enumerable: true });
    Object.assign(this, { pc: peerConnection, handleControlEvent, onMessage: onMessage ?? (() => {}),
      onResyncRequired, onResetPeerConnection: onResetPeerConnection ?? (() => {}), requestId, now });
    this.journal = journal ?? null;
    this.principal = principal ? principalBinding(principal, authorityDomain) : null;
    this.durableRequests = new Set();
    if (typeof initialIceReceipts !== "boolean") throw Error("INVALID_RECEIPT_CAPABILITY");
    this.initialIceRequested = initialIceReceipts; this.initialIceEnabled = false;
    this.initialReceipts = new Map(); this.lastNativeReceipt = null; this.lastIceResend = -Infinity;
    this.nativePeerSupported = false;
    this.media = mediaRecovery ? new MediaRecoveryController(this, mediaRecovery) : null;
    this.socket = null; this.route = null; this.round = null; this.receiver = null; this.sender = null;
    this.outbound = new Map(); this.outboundBytes = 0; this.outboundCount = 0;
    this.handledEvents = new Map(); this.pending = new Map(); this.pendingBytes = 0;
    this.tail = Promise.resolve(); this.inboundCount = 0; this.inboundBytes = 0; this.epoch = 0;
    this.receiptChannel = null; this.receiptBinding = null; this.closed = false; this.transportEpoch = 0;
    this.pcBlocked = false;
    this.abandonedScopes = new Set();
    this.snapshotVersions = new Map();
    this.recoveryStates = new Map(); this.recoveryOperations = new Map();
    this.recoveryCompletion = Promise.resolve();
    this.timer = setInterval(() => this.tick(), 100);
  }

  /** Attach an actual browser WebSocket connected to the native WSS gateway. */
  attachWebSocket(socket) {
    if (this.closed) throw Error("CLIENT_CLOSED");
    this.detachWebSocket();
    this.socket = socket; this.route = null; this.transportEpoch++; this.clearVolatile();
    this.messageListener = event => {
      if (this.socket !== socket) return;
      this.enqueue(event.data).catch(error => this.fail(error.message));
    };
    this.closeListener = () => {
      if (this.socket === socket) { this.route = null; this.transportEpoch++; this.clearVolatile(); }
    };
    socket.addEventListener("message", this.messageListener);
    socket.addEventListener("close", this.closeListener);
  }
  detachWebSocket() {
    if (!this.socket) return;
    this.socket.removeEventListener("message", this.messageListener);
    this.socket.removeEventListener("close", this.closeListener);
    this.socket = null;
  }
  sendFrame(frame) {
    const encoded = JSON.stringify(frame);
    return this.sendEncoded(encoded, frame.requestId);
  }
  sendEncoded(encoded, request) {
    if (!this.socket || this.socket.readyState !== 1 || bytes(encoded) > 81920
        || this.socket.bufferedAmount + bytes(encoded) > 262144) throw Error("TRANSPORT_UNAVAILABLE");
    this.socket.send(encoded); // Queued socket bytes are neither committed nor application received.
    return request;
  }
  authenticate(token, refresh = false) {
    if (typeof token !== "string" || bytes(token) > 8192) throw Error("INVALID_TOKEN");
    this.sendFrame({ v: 1, type: refresh ? "AUTH_REFRESH" : "AUTH", payload: { token,
      ...(this.initialIceRequested ? { initialIceReceiptsV1: true } : {}) } });
  }
  command(type, callId, payload = {}, request = this.requestId()) {
    if (!["INVITE", "ACCEPT", "CANCEL", "HANGUP", "REJECT", "DECLINE_ALL", "NEGOTIATE_REQUEST", "RESUME", "SYNC_CALL", "EVENT_RECEIVED"].includes(type)) throw Error("UNSUPPORTED_COMMAND");
    uuid(request);
    const frame = { v: 1, type, requestId: request, ...(callId ? { callId } : {}), payload };
    const existing = this.pending.get(request), encoded = JSON.stringify(frame);
    if (existing && existing.encoded !== encoded) throw Error("REQUEST_ID_REUSED");
    if (!existing) {
      if (this.pending.size >= 256 || bytes(encoded) + this.pendingBytes > 262144) throw Error("COMMAND_WINDOW_FULL");
      const captured = { status: "UNKNOWN" };
      Object.defineProperties(captured, {
        frame: { value: freezeJson(JSON.parse(encoded)), enumerable: true },
        encoded: { value: encoded, enumerable: true }
      });
      this.pending.set(request, captured); this.pendingBytes += bytes(encoded);
    }
    this.sendEncoded(encoded, request);
    return request;
  }
  /** Explicit durable admission; transaction completion precedes socket send. */
  async persistCommand(type, callId, payload = {}, request = this.requestId()) {
    if (!this.journal || !this.principal || this.closed) throw Error("DURABLE_ENROLLMENT_REQUIRED");
    uuid(request);
    const encoded = JSON.stringify({ v: 1, type, requestId: request, ...(callId ? { callId } : {}), payload });
    const existing = this.pending.get(request);
    if (existing && existing.encoded !== encoded) throw Error("REQUEST_ID_REUSED");
    if (!existing && (this.pending.size >= 256 || this.pendingBytes + bytes(encoded) > 262144)) throw Error("COMMAND_WINDOW_FULL");
    await this.journal.persist(this.principal, encoded);
    // The admitted intent remains durable even if disposal wins the transaction race.
    if (this.closed) { const error = Error("CLIENT_CLOSED"); error.requestId = request; throw error; }
    this.durableRequests.add(request);
    // Recheck synchronous map admission after concurrent transaction completion.
    try { return this.command(type, callId, JSON.parse(encoded).payload, request); }
    catch (error) { error.requestId = request; throw error; }
  }
  async restoreCommands() {
    if (!this.journal || !this.principal || this.closed) throw Error("DURABLE_ENROLLMENT_REQUIRED");
    const records = await this.journal.unresolved(this.principal);
    for (const record of records) {
      const frame = freezeJson(JSON.parse(record.encoded)); uuid(frame.requestId);
      const existing = this.pending.get(frame.requestId);
      if (existing && existing.encoded !== record.encoded) throw Error("REQUEST_ID_REUSED");
      if (!existing) {
        if (this.pending.size >= 256 || this.pendingBytes + bytes(record.encoded) > 262144) throw Error("COMMAND_WINDOW_FULL");
        this.pending.set(frame.requestId, { frame, encoded: record.encoded, status: "UNKNOWN" });
        this.pendingBytes += bytes(record.encoded);
      }
      this.durableRequests.add(frame.requestId);
    }
    return records.map(record => record.requestId); // No implicit send, new UUID or new principal.
  }
  observeMedia(type, payload = {}, request = this.requestId()) {
    if (!this.sender || !this.route || this.pcBlocked || !["MEDIA_CONNECTED", "MEDIA_DISCONNECTED", "ICE_RESTARTING", "MEDIA_FAILED", "MEDIA_RECOVERED"].includes(type)) throw Error("AUTHORITATIVE_MEDIA_SCOPE_REQUIRED");
    const allowed = type === "MEDIA_FAILED" ? ["senderSequence", "quality", "reason"] : ["senderSequence", "quality"];
    if (Object.keys(payload).some(k => !allowed.includes(k))) throw Error("INVALID_MEDIA_PAYLOAD");
    if (payload.senderSequence !== undefined) decimal64(payload.senderSequence);
    if (type === "MEDIA_FAILED" && !["ICE_FAILED", "TURN_UNAVAILABLE", "STUN_UNAVAILABLE", "RECOVERY_EXHAUSTED"].includes(payload.reason)) throw Error("INVALID_MEDIA_REASON");
    if (payload.quality !== undefined) {
      const quality = payload.quality;
      if (!quality || Object.keys(quality).some(k => !["rttMillis", "jitterMicros", "packetsLost", "framesDropped"].includes(k))
          || !["rttMillis", "jitterMicros", "packetsLost", "framesDropped"].every(k => Number.isSafeInteger(quality[k]) && quality[k] >= 0)) throw Error("INVALID_MEDIA_QUALITY");
    }
    const { callId, negotiationId, iceGeneration } = this.sender;
    return this.sendFrame({ v: 1, type, requestId: uuid(request), callId, negotiationId, iceGeneration, payload });
  }
  retryCommand(request) {
    const pending = this.pending.get(uuid(request));
    if (!pending || !["UNKNOWN", "PENDING"].includes(pending.status)) throw Error("NO_UNKNOWN_COMMAND");
    return this.sendEncoded(pending.encoded, request);
  }
  lookupCommandResult(request) {
    const pending = this.pending.get(uuid(request));
    if (!pending || ["EVENT_RECEIVED", "SYNC_CALL", "RESUME"].includes(pending.frame.type)) throw Error("NO_COMMAND_RESULT_SCOPE");
    // Native result lookup addresses the original UUID in its original INVITE/CALL scope.
    // This read never replaces the captured command intent used by retryCommand.
    const frame = { v: 1, type: "GET_COMMAND_RESULT", requestId: request,
      ...(pending.frame.callId ? { callId: pending.frame.callId } : {}), payload: {} };
    return this.sendFrame(frame);
  }
  releaseCommand(request) {
    const pending = this.pending.get(request);
    if (pending && !this.durableRequests.has(request) && !["UNKNOWN", "PENDING"].includes(pending.status)) {
      this.pending.delete(request); this.pendingBytes -= bytes(pending.encoded);
    }
  }

  enqueue(data) {
    if (this.closed || typeof data !== "string" || bytes(data) > 81920) return Promise.reject(Error("INVALID_FRAME"));
    const size = bytes(data), epoch = this.transportEpoch;
    this.inboundCount++; this.inboundBytes += size;
    if (this.inboundCount > 256 || this.inboundBytes > 262144) {
      this.inboundCount--; this.inboundBytes -= size;
      return Promise.reject(Error("INBOUND_WINDOW_FULL"));
    }
    const next = this.tail.then(() => epoch === this.transportEpoch ? this.receive(JSON.parse(data)) : undefined);
    this.tail = next.catch(() => {});
    return next.finally(() => { this.inboundCount--; this.inboundBytes -= size; });
  }
  currentRoute(frame) {
    return this.route && frame.sessionIncarnation === this.route.sessionIncarnation
      && frame.connectionGeneration === this.route.connectionGeneration;
  }
  async receive(frame) {
    if (!frame || frame.v !== 1 || typeof frame.type !== "string") throw Error("INVALID_FRAME");
    if (frame.type === "AUTH_OK") {
      uuid(frame.sessionIncarnation); decimal64(frame.connectionGeneration);
      this.route = { sessionIncarnation: frame.sessionIncarnation, connectionGeneration: frame.connectionGeneration };
      this.initialIceEnabled = this.initialIceRequested && frame.initialIceReceiptsV1 === true;
      await this.bounded(() => this.onMessage(frame)); return;
    }
    if (frame.type === "AUTH_EXPIRING" || frame.type === "OVERLOADED") {
      await this.bounded(() => this.onMessage(frame)); return;
    }
    if (!this.currentRoute(frame)) return; // Old socket/session frames cannot mutate the new route.
    if (frame.type === "INITIAL_ICE_RECEIPT") {
      this.acceptInitialIceReceipt(frame); return;
    }
    if (frame.type === "CALL_SNAPSHOT" && !this.applySnapshot(frame)) {
      const pending = this.pending.get(frame.requestId);
      if (pending?.frame.type === "SYNC_CALL") pending.status = "READ";
      return;
    }
    if (["OFFER", "ANSWER", "ICE_CANDIDATES", "END_OF_CANDIDATES"].includes(frame.type)) {
      if (!this.acceptRelayScope(frame)) return;
      if (["OFFER", "ANSWER"].includes(frame.type)) {
        await this.installRemoteDescription(frame);
      } else this.bufferIce(frame);
      await this.drainIce(); return;
    }
    if (frame.eventId !== undefined) {
      uuid(frame.eventId); decimal64(frame.callVersion);
      const receiptKey = JSON.stringify([frame.eventId, frame.callId, frame.callVersion]);
      if (!this.handledEvents.has(receiptKey)) {
        await this.bounded(() => this.handleControlEvent(frame));
        this.handledEvents.set(receiptKey, true);
        if (this.handledEvents.size > 256) this.handledEvents.delete(this.handledEvents.keys().next().value);
      }
      // Exact public payload; gateway binds the active route and issued event version itself.
      const handled = this.handledEvents.get(receiptKey);
      const receiptRequest = typeof handled === "string" ? handled : uuid(this.requestId());
      this.handledEvents.set(receiptKey, receiptRequest);
      this.command("EVENT_RECEIVED", frame.callId, { eventId: frame.eventId }, receiptRequest);
      return;
    }
    const pending = this.pending.get(frame.requestId);
    if (pending) {
      if (frame.type === "ACK_COMMITTED" && frame.ackCommitted === true) pending.status = "COMMITTED";
      else if (frame.type === "COMMAND_RESULT" && frame.ackCommitted === false)
        pending.status = frame.result?.status === "FINAL" ? "FINAL" : frame.result?.status === "PENDING" ? "PENDING" : "UNKNOWN";
      else if (frame.type === "EVENT_RECEIPT" || frame.type === "CALL_SNAPSHOT") pending.status = "READ";
      // ERROR/OUTCOME_UNKNOWN retains the original identity for retry/reconciliation.
      if (pending.frame.type === "EVENT_RECEIVED" && pending.status === "READ") this.releaseCommand(frame.requestId);
    }
    if (pending && this.durableRequests.has(frame.requestId)
        && ((frame.type === "ACK_COMMITTED" && frame.ackCommitted === true && frame.result?.status === "FINAL")
          || (frame.type === "COMMAND_RESULT" && frame.ackCommitted === false && frame.result?.status === "FINAL"))) {
      await this.journal.settle(this.principal, frame.requestId);
      this.durableRequests.delete(frame.requestId);
    }
    this.media?.reply(frame);
    await this.bounded(() => this.onMessage(frame));
  }
  applySnapshot(frame) {
    const value = frame.result;
    if (!value || typeof value.resetPeerConnection !== "boolean") throw Error("INVALID_SNAPSHOT");
    const version = decimal64(frame.callVersion);
    if (typeof frame.callId !== "string" || bytes(frame.callId) > 96
        || !/^[a-z][a-z0-9-]{0,23}\.e[1-9][0-9]*\.[0-9a-f-]{36}$/.test(frame.callId)) throw Error("INVALID_CALL");
    const versionKey = JSON.stringify([this.authorityDomain, frame.callId]);
    const highest = this.snapshotVersions.get(versionKey);
    if (highest !== undefined && version < highest) return false;
    if (highest === undefined && this.snapshotVersions.size >= 256) throw Error("SNAPSHOT_VERSION_WINDOW_FULL");
    const round = value.negotiation;
    let remoteScope = null, senderScope = null;
    if (round && decimal64(round.negotiationId, true) > 0n) {
      decimal64(round.iceGeneration);
      const participants = [value.caller, value.winner].filter(Boolean);
      if (!participants.some(p => p.sessionIncarnation === this.route.sessionIncarnation
          && p.connectionGeneration === this.route.connectionGeneration)) throw Error("UNAUTHORIZED_SNAPSHOT");
      const remote = participants.find(p => p.sessionIncarnation !== this.route.sessionIncarnation);
      if (remote) {
        remoteScope = scope(frame.callId, round.negotiationId, round.iceGeneration, remote.sessionIncarnation);
        senderScope = scope(frame.callId, round.negotiationId, round.iceGeneration, this.route.sessionIncarnation);
      }
    }
    const terminal = ["TERMINAL", "FAILED"].includes(value.state);
    this.snapshotVersions.set(versionKey, version);
    const unchanged = remoteScope && this.round && key(remoteScope) === key(this.round);
    if (!unchanged || terminal || value.code === "RESYNC_REQUIRED" || value.resetPeerConnection) {
      this.clearVolatile();
      if (remoteScope && !terminal && value.code !== "RESYNC_REQUIRED" && !value.resetPeerConnection) {
        if (this.abandonedScopes.has(key(remoteScope))) {
          this.superviseRecovery("resync", { code: "RESYNC_REQUIRED", reason: "PREVIOUS_VOLATILE_SCOPE_LOST" });
        } else { this.round = remoteScope; this.sender = senderScope; }
      }
    }
    if (value.resetPeerConnection) this.superviseRecovery("reset", value);
    // false preserves pc and its healthy media; a WSS reconnect never calls close/restartIce.
    if (value.code === "RESYNC_REQUIRED") this.fail("RESYNC_REQUIRED");
    this.media?.snapshot(frame);
    return true;
  }
  acceptRelayScope(frame) {
    // Native sender is Jackson's AuthenticatedSession; incarnation is a UUID wrapper.
    const remote = scope(frame.callId, frame.negotiationId, frame.iceGeneration, frame.sender?.incarnation?.value);
    if (this.abandonedScopes.has(key(remote))) return false;
    if (!this.round || key(remote) !== key(this.round)) throw Error("AUTHORITATIVE_ROUND_REQUIRED");
    if (!this.receiver) this.receiver = { scope: remote, cursor: 0n, high: 0n, buffered: new Map(), bytes: 0,
      terminal: null, ended: false, waitingSince: null, active: true, description: null, descriptionReady: false };
    return true;
  }
  async installRemoteDescription(frame) {
    const receiver = this.receiver, request = uuid(frame.requestId), sdp = frame.payload?.sdp;
    if (typeof sdp !== "string" || !sdp || bytes(sdp) > 65536) throw Error("INVALID_SDP");
    const installed = receiver.description;
    if (installed) {
      if (installed.request === request && installed.type === frame.type && installed.sdp === sdp) return;
      throw Error("DESCRIPTION_ROUND_CONFLICT");
    }
    await this.iceOperation(pc => pc.setRemoteDescription({ type: frame.type.toLowerCase(), sdp }));
    if (!receiver.active || receiver !== this.receiver) throw Error("STALE_DESCRIPTION_SCOPE");
    // One bounded SDP identity/content record per exact authoritative receiver scope.
    receiver.description = Object.freeze({ request, type: frame.type, sdp });
    receiver.descriptionReady = true;
  }
  bufferIce(frame) {
    const receiver = this.receiver, payload = frame.payload;
    if (!payload || bytes(payload) > 8192) throw Error("ICE_BATCH_BOUND");
    if (payload.receiptProtocol !== undefined && payload.receiptProtocol !== INITIAL_RECEIPT_PROTOCOL) throw Error("UNSUPPORTED_RECEIPT_PROTOCOL");
    if (payload.receiptProtocol === INITIAL_RECEIPT_PROTOCOL && this.initialIceEnabled) {
      receiver.nativeReceipts = true; this.nativePeerSupported = true;
    }
    if (frame.type === "END_OF_CANDIDATES") {
      const terminal = decimal64(payload.terminalSequence);
      if ((receiver.terminal !== null && receiver.terminal !== terminal) || terminal < receiver.cursor
          || terminal < receiver.high) throw Error("ICE_TERMINAL_CONFLICT");
      receiver.terminal = terminal; receiver.high = terminal;
    } else {
      const start = decimal64(payload.startSequence), values = payload.candidates;
      if (!Array.isArray(values) || values.length < 1 || values.length > 20 || start > MAX - BigInt(values.length)) throw Error("ICE_BATCH_BOUND");
      for (let index = 0; index < values.length; index++) {
        const sequence = start + BigInt(index), value = candidate(values[index]);
        if (receiver.terminal !== null && sequence > receiver.terminal) throw Error("ICE_AFTER_END");
        if (sequence <= receiver.cursor) continue;
        if (receiver.ended) throw Error("ICE_AFTER_END");
        const existing = receiver.buffered.get(sequence);
        if (existing) {
          if (JSON.stringify(existing.value) !== JSON.stringify(value)) throw Error("ICE_SEQUENCE_CONFLICT");
          continue;
        }
        const size = bytes(value);
        if (receiver.buffered.size >= 256 || receiver.bytes + size > 262144) throw Error("ICE_RECEIVE_WINDOW_FULL");
        receiver.buffered.set(sequence, { value, size }); receiver.bytes += size;
        if (sequence > receiver.high) receiver.high = sequence;
      }
    }
    if (receiver.waitingSince === null && (receiver.buffered.size || receiver.terminal !== null)) receiver.waitingSince = this.now();
  }
  async drainIce() {
    const receiver = this.receiver;
    if (!receiver || !receiver.active || !receiver.descriptionReady) return;
    while (receiver.active && receiver.buffered.has(receiver.cursor + 1n)) {
      const sequence = receiver.cursor + 1n, item = receiver.buffered.get(sequence);
      await this.iceOperation(pc => pc.addIceCandidate(item.value));
      if (!receiver.active) return;
      receiver.buffered.delete(sequence); receiver.bytes -= item.size; receiver.cursor = sequence;
    }
    if (receiver.active && !receiver.ended && receiver.terminal !== null && receiver.cursor === receiver.terminal) {
      await this.iceOperation(pc => pc.addIceCandidate(null));
      if (!receiver.active) return;
      receiver.ended = true;
    }
    if (receiver.buffered.size === 0 && (receiver.terminal === null || receiver.ended)) receiver.waitingSince = null;
    this.sendIceReceipt();
  }
  receipt() {
    const receiver = this.receiver;
    if (!receiver) return null;
    const missingRanges = []; let start = receiver.cursor + 1n;
    for (const sequence of [...receiver.buffered.keys()].sort((a, b) => a < b ? -1 : a > b ? 1 : 0)) {
      if (sequence > start) missingRanges.push([start.toString(), (sequence - 1n).toString()]);
      start = sequence + 1n;
    }
    if (start <= receiver.high) missingRanges.push([start.toString(), receiver.high.toString()]);
    return { v: 1, scope: receiver.scope, highestContiguous: receiver.cursor.toString(), missingRanges,
      endReceived: receiver.ended };
  }
  /** Only an already negotiated, authenticated peer RTCDataChannel supports receipt traffic. */
  bindIceReceiptChannel(channel, remoteScope) {
    const binding = scope(remoteScope.callId, remoteScope.negotiationId, remoteScope.iceGeneration, remoteScope.senderSessionIncarnation);
    if (!this.round || key(binding) !== key(this.round) || channel.protocol !== RECEIPT_PROTOCOL
        || channel.readyState !== "open" || !channel.ordered) throw Error("NEGOTIATED_RECEIPT_CHANNEL_REQUIRED");
    this.unbindReceiptChannel(); this.receiptChannel = channel; this.receiptBinding = key(binding);
    this.receiptListener = event => {
      try { this.acceptIceReceipt(event.data); } catch (error) { this.fail(error.message); }
    };
    channel.addEventListener("message", this.receiptListener);
    this.sendIceReceipt();
  }
  unbindReceiptChannel() {
    this.receiptChannel?.removeEventListener("message", this.receiptListener);
    this.receiptChannel = null; this.receiptBinding = null;
  }
  sendIceReceipt() {
    const receipt = this.receipt();
    if (receipt && this.initialIceEnabled && this.receiver?.nativeReceipts) {
      const payload = { receiptVersion: 1, senderSessionIncarnation: receipt.scope.senderSessionIncarnation,
        highestContiguous: receipt.highestContiguous, missingRanges: receipt.missingRanges.slice(0, 32), endReceived: receipt.endReceived };
      const identity = JSON.stringify(payload);
      let item = this.lastNativeReceipt;
      if (!item || item.identity !== identity) {
        const { callId, negotiationId, iceGeneration } = receipt.scope;
        const requestId = uuid(this.requestId());
        item = { identity, encoded: JSON.stringify({ v: 1, type: "INITIAL_ICE_RECEIPT", requestId,
          callId, negotiationId, iceGeneration, payload }), requestId, created: this.now(), lastSent: -Infinity };
        this.lastNativeReceipt = item; this.initialReceipts.set(requestId, item);
        if (this.initialReceipts.size > 32) this.initialReceipts.delete(this.initialReceipts.keys().next().value);
      }
      // Receipt metadata is volatile. Unavailable WSS retains the same UUID for bounded retry.
      if (this.now() - item.lastSent >= 100 && this.now() - item.created < 10000) {
        try { this.sendEncoded(item.encoded, item.requestId); item.lastSent = this.now(); }
        catch (_) { /* tick retries within the original finite receipt window */ }
      }
      return;
    }
    if (!receipt || !this.receiptChannel || this.receiptBinding !== key(this.round)) return;
    if (this.receiptChannel.readyState !== "open" || this.receiptChannel.bufferedAmount > 8192) throw Error("RECEIPT_TRANSPORT_UNAVAILABLE");
    this.receiptChannel.send(JSON.stringify(receipt));
  }
  acceptInitialIceReceipt(frame) {
    const payload = frame.payload;
    if (!this.initialIceEnabled || !this.sender || !this.round || frame.callId !== this.sender.callId
        || frame.negotiationId !== this.sender.negotiationId || frame.iceGeneration !== this.sender.iceGeneration
        || frame.sender?.incarnation?.value !== this.round.senderSessionIncarnation
        || payload?.receiptVersion !== 1 || payload.senderSessionIncarnation !== this.sender.senderSessionIncarnation
        || !Array.isArray(payload.missingRanges) || payload.missingRanges.length > 32) throw Error("STALE_ICE_RECEIPT");
    this.acceptIceReceipt(JSON.stringify({ v: 1, scope: this.sender,
      highestContiguous: payload.highestContiguous, missingRanges: payload.missingRanges, endReceived: payload.endReceived }), true);
    this.nativePeerSupported = true;
  }
  acceptIceReceipt(encoded, native = false) {
    if (typeof encoded !== "string" || bytes(encoded) > 8192 || !this.sender) throw Error("INVALID_ICE_RECEIPT");
    const receipt = JSON.parse(encoded);
    if (receipt.v !== 1 || key(receipt.scope) !== key(this.sender)
        || (!native && this.receiptBinding !== key(this.round))) throw Error("STALE_ICE_RECEIPT");
    const cursor = decimal64(receipt.highestContiguous, true);
    if (cursor > this.sentSequence) throw Error("INVALID_ICE_RECEIPT");
    if (cursor < this.ackedSequence) return; // Reordered volatile receipts cannot regress progress.
    if (!Array.isArray(receipt.missingRanges) || receipt.missingRanges.length > 257 || typeof receipt.endReceived !== "boolean") throw Error("INVALID_ICE_RECEIPT");
    let previous = cursor;
    const ranges = receipt.missingRanges.map(pair => {
      if (!Array.isArray(pair) || pair.length !== 2) throw Error("INVALID_ICE_RECEIPT");
      const start = decimal64(pair[0]), end = decimal64(pair[1]);
      if (start <= previous || end < start || end > this.sentSequence) throw Error("INVALID_ICE_RECEIPT");
      previous = end;
      return [start, end];
    });
    if (receipt.endReceived && (!this.sentEnd || cursor !== this.sentSequence || ranges.length)) throw Error("INVALID_ICE_END_RECEIPT");
    this.ackedSequence = cursor;
    const resend = this.now() - this.lastIceResend >= 100;
    let retransmitted = false;
    for (const [request, item] of this.outbound) {
      if (item.end <= cursor && (!item.terminal || receipt.endReceived)) this.retireOutbound(request, item);
      else if (resend && !retransmitted && (ranges.some(([start, end]) => item.start <= end && item.end >= start) || (item.terminal && !receipt.endReceived))) {
        if (this.now() - item.created >= 10000) throw Error("RESEND_WINDOW_EXPIRED");
        this.sendFrame(item.frame); item.lastSent = this.now(); retransmitted = true; // Original native identity/sequence.
      }
    }
    if (retransmitted) this.lastIceResend = this.now();
  }
  sendIceCandidates(values) {
    if (!this.sender || !Array.isArray(values) || values.length < 1 || values.length > 20 || this.sentEnd) throw Error("INVALID_ICE_SEND");
    const candidates = values.map(candidate), start = this.sentSequence + 1n;
    if (start > MAX - BigInt(values.length)) throw Error("ICE_SEQUENCE_EXHAUSTED");
    const payload = { startSequence: start.toString(), candidates,
      ...(this.initialIceEnabled ? { receiptProtocol: INITIAL_RECEIPT_PROTOCOL } : {}) };
    if (bytes(payload) > 8192) throw Error("ICE_BATCH_BOUND");
    const end = start + BigInt(values.length - 1);
    const request = this.cacheAndSendIce("ICE_CANDIDATES", payload, start, end, values.length);
    this.sentSequence = end; return request;
  }
  sendDescription(type, sdp, request = this.requestId()) {
    if (!this.sender || !["OFFER", "ANSWER"].includes(type) || typeof sdp !== "string"
        || !sdp || bytes(sdp) > 65536) throw Error("INVALID_DESCRIPTION_SEND");
    const { callId, negotiationId, iceGeneration } = this.sender;
    return this.sendFrame({ v: 1, type, requestId: uuid(request), callId, negotiationId, iceGeneration, payload: { sdp } });
  }
  sendEndOfCandidates() {
    if (!this.sender || this.sentEnd || this.sentSequence < 1n) throw Error("INVALID_ICE_END");
    const request = this.cacheAndSendIce("END_OF_CANDIDATES", { terminalSequence: this.sentSequence.toString(),
      ...(this.initialIceEnabled ? { receiptProtocol: INITIAL_RECEIPT_PROTOCOL } : {}) }, this.sentSequence, this.sentSequence, 0);
    this.sentEnd = true; return request;
  }
  cacheAndSendIce(type, payload, start, end, count) {
    const { callId, negotiationId, iceGeneration } = this.sender;
    const frame = { v: 1, type, requestId: uuid(this.requestId()), callId, negotiationId, iceGeneration, payload };
    const size = bytes(frame);
    if (this.outbound.size >= 256 || this.outboundCount + count > 256 || this.outboundBytes + size > 262144) throw Error("ICE_RESEND_WINDOW_FULL");
    this.outbound.set(frame.requestId, { frame, start, end, count, size, created: this.now(), terminal: type === "END_OF_CANDIDATES" });
    this.outboundBytes += size; this.outboundCount += count;
    // Reserve identity before any ambiguous socket failure so it can never be reused.
    this.sentSequence = end; if (type === "END_OF_CANDIDATES") this.sentEnd = true;
    try { this.sendFrame(frame); } catch (error) { error.requestId = frame.requestId; throw error; }
    return frame.requestId;
  }
  retryIce(request) {
    const item = this.outbound.get(uuid(request));
    if (!item || this.now() - item.created >= 10000) throw Error("RESEND_WINDOW_EXPIRED");
    return this.sendFrame(item.frame);
  }
  retireOutbound(request, item) {
    this.outbound.delete(request); this.outboundBytes -= item.size; this.outboundCount -= item.count;
  }
  async iceOperation(operation) {
    if (this.pcBlocked) throw Error("ICE_AGENT_RESYNC_REQUIRED");
    const pc = this.pc;
    try { return await this.bounded(() => operation(pc)); }
    catch (error) {
      // Browser promises cannot be cancelled. Prevent a later scope from using an ambiguous agent.
      if (this.pc === pc) this.pcBlocked = true;
      throw error;
    }
  }
  replacePeerConnection(peerConnection) {
    if (!peerConnection || peerConnection === this.pc || typeof peerConnection.addIceCandidate !== "function") throw Error("NEW_ICE_AGENT_REQUIRED");
    this.media?.unbind(); this.clearVolatile(); this.pc = peerConnection; this.pcBlocked = false; this.media?.bind();
  }
  async bounded(operation) {
    const epoch = this.epoch;
    let timer;
    try {
      const result = await Promise.race([Promise.resolve().then(operation), new Promise((_, reject) => {
        timer = setTimeout(() => reject(Error("CLIENT_OPERATION_TIMEOUT")), 10000);
      })]);
      if (epoch !== this.epoch) throw Error("STALE_CLIENT_OPERATION");
      return result;
    } finally { clearTimeout(timer); }
  }
  tick() {
    if (this.closed) return;
    if (this.receiver?.waitingSince !== null && this.receiver?.waitingSince !== undefined
        && this.now() - this.receiver.waitingSince >= 10000) this.fail("ICE_GAP_OR_DESCRIPTION_EXPIRED");
    for (const [request, item] of this.outbound) if (this.now() - item.created >= 10000) {
      if (this.initialIceEnabled && this.nativePeerSupported) { this.fail("INITIAL_ICE_RECEIPT_EXPIRED"); break; }
      this.retireOutbound(request, item);
    }
    if (this.initialIceEnabled && this.nativePeerSupported && this.now() - this.lastIceResend >= 100) {
      for (const item of this.outbound.values()) if (this.now() - (item.lastSent ?? item.created) >= 1000) {
        try { this.sendFrame(item.frame); item.lastSent = this.now(); this.lastIceResend = this.now(); }
        catch (_) { /* Original cache and deadline retain ambiguous send ownership. */ }
        break; // At most one resend per tick; no burst over the gateway input rate budget.
      }
    }
    for (const [request, item] of this.initialReceipts) if (this.now() - item.created >= 10000) this.initialReceipts.delete(request);
    if (this.receiver?.nativeReceipts) this.sendIceReceipt();
  }
  clearVolatile() {
    this.epoch++;
    if (this.round) {
      this.abandonedScopes.add(key(this.round));
      if (this.abandonedScopes.size > 64) this.abandonedScopes.delete(this.abandonedScopes.values().next().value);
    }
    if (this.receiver) this.receiver.active = false;
    this.receiver = null; this.round = null; this.sender = null;
    this.initialReceipts.clear(); this.lastNativeReceipt = null; this.lastIceResend = -Infinity;
    this.nativePeerSupported = false;
    this.sentSequence = 0n; this.ackedSequence = 0n; this.sentEnd = false;
    this.outbound.clear(); this.outboundBytes = 0; this.outboundCount = 0;
    for (const [request, item] of this.pending) if (item.frame.type === "EVENT_RECEIVED") {
      this.pending.delete(request); this.pendingBytes -= bytes(item.encoded);
    }
    // A new route needs a newly issued application receipt even for an already handled event.
    for (const event of this.handledEvents.keys()) this.handledEvents.set(event, true);
    this.unbindReceiptChannel();
  }
  fail(reason) {
    this.transportEpoch++;
    this.clearVolatile();
    this.superviseRecovery("resync", { code: "RESYNC_REQUIRED", reason });
  }
  superviseRecovery(hook, value) {
    const existing = this.recoveryOperations.get(hook);
    if (existing) return existing; // At most one in-flight operation for each of the two hooks.
    const callback = hook === "reset" ? this.onResetPeerConnection : this.onResyncRequired;
    const state = Object.freeze({ hook, status: "PENDING" });
    this.recoveryStates.set(hook, state);
    let timer;
    const physical = Promise.resolve().then(() => callback(value));
    const bounded = Promise.race([physical, new Promise((_, reject) => {
      timer = setTimeout(() => reject(Error("RECOVERY_CALLBACK_TIMEOUT")), 10000);
    })]);
    const operation = bounded.then(
      () => this.recoveryStates.set(hook, Object.freeze({ hook, status: "COMPLETED" })),
      () => this.recoveryStates.set(hook, Object.freeze({ hook, status: "FAILED", code: "RECOVERY_CALLBACK_FAILED_OR_TIMEOUT" }))
    ).finally(() => clearTimeout(timer));
    // Timeout never means physical cancellation. Retain admission until the original settles.
    this.recoveryOperations.set(hook, operation);
    physical.then(
      () => this.recoveryOperations.delete(hook),
      () => this.recoveryOperations.delete(hook)
    );
    this.recoveryCompletion = operation;
    return operation; // Always supervised; hook failure cannot recursively invoke fail().
  }
  dispose() {
    this.media?.dispose(); this.closed = true; clearInterval(this.timer); this.detachWebSocket(); this.clearVolatile();
  }
}
