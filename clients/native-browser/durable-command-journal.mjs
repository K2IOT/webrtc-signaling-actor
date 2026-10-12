const size = text => new TextEncoder().encode(text).length;
const TYPES = new Set(['INVITE', 'ACCEPT', 'CANCEL', 'HANGUP', 'REJECT', 'DECLINE_ALL', 'NEGOTIATE_REQUEST']);
export function principalBinding(value, authorityDomain) {
  if (!value || Object.keys(value).some(k => !['issuer', 'jti', 'userId'].includes(k))) throw Error('TRUSTED_PRINCIPAL_REQUIRED');
  for (const field of ['issuer', 'jti', 'userId']) if (typeof value[field] !== 'string' || !value[field] || size(value[field]) > 512) throw Error('TRUSTED_PRINCIPAL_REQUIRED');
  return Object.freeze({ issuer: value.issuer, jti: value.jti, userId: value.userId, authorityDomain });
}
const bindingKey = value => JSON.stringify([value.authorityDomain, value.issuer, value.jti, value.userId]);
const plain = value => value !== null && typeof value === 'object' && !Array.isArray(value)
  && Object.getPrototypeOf(value) === Object.prototype;
const identifier = (value, bound) => typeof value === 'string' && value.length > 0 && value.trim() === value
  && size(value) <= bound && !/[\u0000-\u001f\u007f-\u009f]/u.test(value)
  && !Array.from(value).some(c => { const n = c.codePointAt(0); return n >= 0xd800 && n <= 0xdfff; });
function validateDurableEnvelope(encoded) {
  if (typeof encoded !== 'string' || size(encoded) > 81920) throw Error('INVALID_DURABLE_ENVELOPE');
  const frame = JSON.parse(encoded);
  // Reject duplicate-key/noncanonical encodings that could conceal discarded private values.
  if (JSON.stringify(frame) !== encoded) throw Error('INVALID_DURABLE_ENCODING');
  if (!plain(frame) || frame.v !== 1 || !TYPES.has(frame.type)
      || Object.keys(frame).some(k => !['v', 'type', 'requestId', 'callId', 'payload'].includes(k))
      || typeof frame.requestId !== 'string'
      || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(frame.requestId)) throw Error('INVALID_DURABLE_ENVELOPE');
  if (frame.type === 'INVITE') {
    if (Object.hasOwn(frame, 'callId')) throw Error('INVALID_DURABLE_CALL');
  } else {
    const match = typeof frame.callId === 'string' && frame.callId.match(/^[a-z][a-z0-9-]{0,23}\.e([1-9][0-9]*)\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    if (!match || size(frame.callId) > 96 || BigInt(match[1]) > 9223372036854775807n) throw Error('INVALID_DURABLE_CALL');
  }
  const payload = frame.payload;
  if (!plain(payload)) throw Error('INVALID_DURABLE_PAYLOAD');
  if (frame.type === 'INVITE') {
    if (Object.keys(payload).length !== 1 || !identifier(payload.targetUserId, 256)
        || size(payload.targetUserId.normalize('NFC')) > 256) throw Error('INVALID_DURABLE_USER');
  } else if (frame.type === 'NEGOTIATE_REQUEST') {
    if (Object.keys(payload).length !== 1 || typeof payload.iceRestart !== 'boolean') throw Error('INVALID_DURABLE_RESTART');
  } else if (Object.keys(payload).length !== 0) throw Error('PRIVATE_OR_UNSUPPORTED_DURABLE_PAYLOAD');
  return frame;
}
/** A single store transaction serializes aggregate admission across tabs and principals. */
export class DurableCommandJournal {
  constructor({ indexedDB = globalThis.indexedDB } = {}) {
    if (!indexedDB) throw Error('INDEXEDDB_REQUIRED');
    this.ready = new Promise((resolve, reject) => {
      const request = indexedDB.open('webrtc-native-unresolved-v1', 1);
      request.onupgradeneeded = () => request.result.createObjectStore('commands', { keyPath: 'id' });
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
      request.onblocked = () => reject(Error('JOURNAL_UPGRADE_BLOCKED'));
    });
  }
  async transaction(operation) {
    const db = await this.ready;
    return new Promise((resolve, reject) => {
      const tx = db.transaction('commands', 'readwrite'), store = tx.objectStore('commands');
      let result, failure;
      tx.oncomplete = () => resolve(result);
      tx.onabort = tx.onerror = () => reject(failure ?? tx.error ?? Error('JOURNAL_TRANSACTION_FAILED'));
      const read = store.getAll();
      read.onsuccess = () => {
        try { result = operation(store, read.result); }
        catch (error) { failure = error; tx.abort(); }
      };
    });
  }
  persist(binding, encoded) {
    const frame = validateDurableEnvelope(encoded);
    const owner = bindingKey(binding), id = JSON.stringify([owner, frame.requestId]);
    const record = { id, owner, requestId: frame.requestId, encoded };
    return this.transaction((store, records) => {
      const existing = records.find(r => r.id === id);
      if (existing && existing.encoded !== encoded) throw Error('REQUEST_ID_REUSED');
      if (existing) return existing;
      const total = records.reduce((n, r) => n + size(JSON.stringify(r)), 0);
      if (records.length >= 256 || total + size(JSON.stringify(record)) > 262144) throw Error('DURABLE_COMMAND_WINDOW_FULL');
      store.add(record); return record;
    });
  }
  unresolved(binding) {
    const owner = bindingKey(binding);
    return this.transaction((_, records) => records.filter(r => r.owner === owner));
  }
  settle(binding, requestId) {
    const id = JSON.stringify([bindingKey(binding), requestId]);
    return this.transaction(store => store.delete(id));
  }
}
