# Native clock source contract

`NativeClockSource` is an adapter for an enrolled clock monitor. Its successful tests
use TEST_ONLY PKI and signing keys; they do not establish infrastructure clock quality.
The process launcher must supply the approved canonical HTTPS endpoint, trusted TLS
context with its client identity, enrolled Ed25519 source keys, cell/storage identity,
actual pod UID and actual process boot UUID. There are no implicit production inputs.

The adapter sends a read-only GET with `X-Signaling-Pod-Uid` and
`X-Signaling-Process-Boot`. The source must authenticate this workload and return
`application/json` with one bounded Content-Length, at most 32768 bytes, and close the
connection. Redirects, chunked/compressed bodies, duplicate headers, extra body bytes,
invalid UTF-8, duplicate JSON keys and unknown DTO properties are rejected.

The JSON object has exactly `report` and `signature`. `report` follows the existing
`ClockSafetyMonitor.Report` record; timestamps are ISO instants. `signature` is the
86-character unpadded base64url Ed25519 signature of
`ClockSafetyMonitor.signingBytes(report)`. Signing the report authenticates the
monitor; the enrolled monitor still has to measure the claimed pair uncertainty,
relative clock rate and continuity. A signature alone is not clock-quality evidence.

The adapter uses one owned TLS1.3 socket per poll, HTTPS hostname validation, no
connection pool and no additional executor. Its fixed SAFETY scheduler job runs at
100ms–1s intervals, with at most one poll in flight and a source I/O budget capped at
one second inside the original job budget. Delayed reports do not acquire a fresh
five-second window. An unavailable or invalid source invalidates cached trust.
Drain stops new polls, invalidates trust immediately and waits for the admitted poll
and its owned socket to finish. An error while closing the TLS socket retains the
physical receipt and admitted slot as UNKNOWN; a failed close never permits a replacement
poll or reports successful drain. Register this owner with the native runtime drain
hooks when installing the concrete process launcher.

Directory, peer/compatibility sources and complete native launcher binding
remain separate required integrations. This adapter does not stand in for those
sources or for production qualification.

The native actor launcher also needs `NativeCellHealthSource`, backed by
`PrimaryCellFacts` on its actual RECOVERY safety pool. Its read-only primary/cell query
checks `pg_is_in_recovery`, the enrolled cell/storage epoch, GROUPED schema and cell
status. A fact is usable for less than one second from the original request; the fixed
SAFETY job polls at100–500ms. Private probes read that cached fact and expire it without
SQL. Drain invalidates cached health, stops new polls and retains admitted native work
until its independent physical receipt and cache processing finish. A throwing factory
without a physical receipt remains UNKNOWN. These health facts grant no mutation or
relay authority and do not replace clock, revocation or compatibility admission.

`NativeRevocationSource` reads the actual durable cursor through the RECOVERY safety
pool, requests one enrolled source page with `X-Signaling-Source-Offset`, verifies
its Ed25519 proof, and commits it through `RevocationReconciler` on the native
MAINTENANCE pool. These three steps share the original poll budget, at most two
seconds. The canonical HTTPS endpoint, workload TLS identity, source verifier and
native reconciler must be supplied explicitly. Clock and revocation adapters share
the bounded TLS/framing transport; neither follows redirects or infers trust.

The response is exactly the existing seven-field `RevocationReconciler.Batch` JSON
record: `fromOffset`, `highWater`, `events`, `checkedAt`, `sourceProof`, `retiredKeys`
and `currentSourceHighWater`. All fields must be present, no unknown/duplicate keys
are accepted, and the original complete page contains at most16 combined events
and key retirements. `sourceProof` uses the enrolled key identifier and unpadded
base64url signature of `RevocationSourceVerifier.signingBytes(cell,issuer,batch)`.
The source must attest complete cell-filtered coverage; deployments with multiple
issuers need an approved complete-source enrollment and cursor scheme. This adapter
does not infer that one issuer page covers another issuer.

Its SAFETY job polls at100ms–1s, with one physically owned invocation. A partial
page can commit revocations while `usable()` remains false. Only a caught-up native
commit can populate the bounded cached health fact; it expires within the configured native reconciliation freshness bound (at most
five seconds) of both the original monotonic request and signed source check. SQL authorization
still independently checks durable progress, security epochs and retired keys.
The cached fact does not authorize sessions. Drain invalidates it immediately and
waits for all original SQL receipts and socket cleanup. A throwing socket factory
or unsuccessful close supplies no physical receipt and keeps admission UNKNOWN.
The actual mTLS/PostgreSQL tests use explicit TEST_ONLY enrollment and PKI. Installing
this source in the complete process launcher and gateway security feed remains open.

`NativeActorSafetySources` binds the three concrete source owners and genuine Pekko
membership to an actor process. The launcher must provide its approved cluster
fingerprint and enrolled AZ-role set. A fingerprint mismatch rejects construction;
no source, topology or production identity is inferred. Its four fixed SAFETY jobs
run every100ms: clock, primary, revocation and membership refresh. Atomic safety
updates preserve native region registration and membership facts. The installed
one-time admission gate checks the live cached source expirations on every admission,
so a paused polling job cannot keep a stale successful snapshot ready. Healthy sources
alone do not set local Up, quorum or region registration.

Compose `sources.jobs()` with the remaining bounded native maintenance jobs, and
register `sources::drain` with `NativeActorRuntimeHooks` source owners. Sources must
continue safety work during framework handoff and drain after native root release.
The native fixture obtains healthy admission from four genuine TCP peers, actual
region registration, mTLS signed source reads and PostgreSQL. Its TEST_ONLY loopback
AZ roles do not attest physical AZ placement. Complete process startup, the directory
and peer/security feeds, relay and remaining native worker installation are still open.

`NativeRelayAuthorization` is the traffic-driven relay cache-miss producer. It
reads the guarded committed native round and original INVITE identity, checks the
original S1 proof and selected participant binding, obtains a fresh hosting-actor
grant and reads both ACTIVE homes under one original deadline (at most two seconds).
Large SDP/ICE bodies remain outside proof requests; those carry the original hash
and bounded command metadata. The resulting process-local snapshot starts at the
original monotonic request and subtracts255ms from absolute expiry (maximum enrolled
250ms pair uncertainty plus five milliseconds of drift). Missing current native
local tenure or trustworthy clock bounds denies the read. Every SQL/home operation
retains its independent physical receipt. The PostgreSQL fixture exercises setup,
activation and negotiation before this read, including connection-generation loss.
Its direct ingress and enrollment remain TEST_ONLY. Installing this producer at the
bounded warm gateway proof reuse and volatile transport
are still required; this helper alone does not implement end-to-end relay.

`NativeActorComposition.relayAuthorization(network)` binds the producer to this
composition's real shard grant and live clock/safety gate. A non-hosting process
cannot refresh relay authority. Existing hosted work may settle through the
business drain before membership and safety sources stop. Complete process
startup and gateway proof reuse remain required.

`NativeGatewayCommands.network(client)` exposes the original tracked session RPC
receipt. Install `relayProofCache(cache)` before ingress starts; logical-only
network adapters cannot install a warm cache. `NativeRelaySessionProofCache`
verifies only pinned public Ed25519 trust, keeps the original R1 expiry, and uses
a live local safety callback plus monotonic expiry. Configure at most 4,096
entries and 64 concurrent refreshes; each retained entry is charged 16 KiB.
UNKNOWN refresh cleanup holds its slot and drain receipt. The gateway's existing
ingress/delivery security checks still apply to every bound session. These caps
are starting limits; complete launcher installation and measured capacity remain
required.
