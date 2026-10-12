# Task3 independent browser review

Verdict: **BLOCK**. Two HIGH findings prevent Task3 acceptance.

Scope: direct inspection of the untracked native browser modules and README, Task3 plan/report, and the actual ProtocolValidator, CallCommandService, NativeGatewayCommands and NativeCallSnapshot contracts. No source edits, tests, builds, syntax checks, browser execution or CI verification were performed. This is a local specification/code review; PR merge readiness was not available in the assigned scope.

## HIGH: Durable admission allows private nested payloads to reach IndexedDB

Location: `clients/native-browser/durable-command-journal.mjs:38-41` (called by `native-client.mjs:127-131`).

The journal checks only payload property names. `{iceRestart: {token: jwt, sdp, candidates}}` passes the NEGOTIATE_REQUEST allowlist and is committed to IndexedDB before send. The same issue exists for an object-valued targetUserId on INVITE. ProtocolValidator requires iceRestart to be a boolean and targetUserId to be a validated string, but that rejection happens after private material has already been durably stored. Payload arrays and absent required values also pass some journal checks.

Fix: validate the complete durable envelope before the transaction: require a plain object payload, validate targetUserId with the native string/UserId bounds, require a boolean iceRestart, reject objects/arrays in scalar fields, and validate the envelope's version, UUID and callId shape. Keep command-specific empty payload requirements. Never commit unsupported nested material and rely on server validation to remove it afterward.

## HIGH: Expired recovery episode emits failure against an unrelated current scope

Location: `clients/native-browser/media-recovery-controller.mjs:40-55,57-64`; relevant scope replacement at `native-client.mjs:285-297,497-504`.

An episode captures its original PC and sender scope. A new authoritative snapshot on the same PC changes sender/round without clearing the episode. `tick()` verifies PC identity but does not verify the original scope before its 30-second failure branch; `send()` then resolves the current client.sender. Thus a disconnect in call A/round N followed by a scope switch to call B or a replacement unrelated round can emit MEDIA_FAILED/RECOVERY_EXHAUSTED against B/current round. An ensuing disconnected event cannot establish a new episode because the previous episode still exists. CallCommandService validates the emitted current scope, so this is a valid observation attributed to the wrong episode; its mediaObserved handler may begin recovery for that call or terminate it if a restart has already been attempted.

Fix: explicitly bind each episode to its authoritative call/round/PC lifetime and cancel or retire it when that scope is replaced, preserving only the specific new round authorized by its own restart result. Guard expiry and connected/recovered observations with that binding before using current sender. A later independent call/round must admit its own episode and deadline.

## Reviewed properties

- IndexedDB uses one readwrite transaction over the global commands store, reads aggregate records, and admits at most 256 records/262144 serialized bytes without unresolved eviction. This is a sound source-level design for same-origin tab serialization; runtime/crash durability remains unverified.
- Enrollment key includes exact authorityDomain/issuer/jti/userId; restore filters that key and does not automatically send or mint replacement UUIDs. Application-authenticated identity enrollment remains external.
- Durable settlement requires current route correlation and FINAL on native ACK_COMMITTED with ackCommitted=true or COMMAND_RESULT with ackCommitted=false. Unknown, pending, receipt and snapshot results do not settle journal entries.
- Recovery is wired to real PC connection/ICE events and requests one original restart UUID per retained episode. Original FINAL NEGOTIATION_GRANTED triggers a snapshot read, and offer creation checks OFFER_GRANTED, route offerer, changed sender scope, grant version and PC identity. Offer callback timeout retains physical ownership and fences the PC.
- Healthy PC is not closed or restarted merely by attaching/replacing WSS. Browser IndexedDB, media, authenticated principal and actual application offer/answer/UI integration acceptance remain unrun.

The Task3 report's claim that tokens/SDP/ICE are excluded by the payload allowlist is stronger than the implementation; update it after admission validation is fixed. Its scoped-recovery claim also requires the episode lifetime fix above.
