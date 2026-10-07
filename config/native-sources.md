# Mounted runtime configuration

The native Boot environment processor reads `SIGNALING_RUNTIME_CONTRACT` when
explicitly supplied. The path must be absolute and resolve to a regular file;
Kubernetes projected Secret symlinks are supported. The current YAML format is
one document with a single `signaling` root, containing the existing typed
identity, lease, cluster, protocol, transport and queues sections from
`production-defaults.yaml`. Partial sections retain packaged defaults. Deployment
environment and command-line properties retain Boot precedence; the Secret
overrides packaged configuration. Unsafe values and unknown typed properties
reject binding before runtime installation.

The original read is bounded to 64 KiB, strict UTF-8, depth16 and512 nodes.
Duplicate keys, multiple documents, tags, anchors/aliases, dotted keys, nulls,
sequences and other top-level namespaces are rejected. Parser failures expose
only the fixed CONFIGURATION_REJECTED diagnostic. This is an effective
configuration reader, not an enrollment for issuer keys, native source trust,
calling policy or runtime factories. Those integrations remain required; do not
place guessed production sources or permissive policies in the file.

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

Native gateway relay authorization and transmission share the original one-second
relay budget, including a cold home-proof read. A positive `RelayWriteReceipt`
projects `COMMAND_RESULT` with `status:VOLATILE`, `code:WRITE_COMPLETED` and
`ackCommitted:false`; its request, call, frame kind, call version, negotiation and
ICE generation must match the original operation. This is a transport write
result, not peer ICE application or a durable journal entry. The native delivery
producer must supply the original receipt; the complete launcher/delivery binding
remains required.

Gateway volatile delivery has a separate `GatewayRelayIngress` service. A
`GatewayRelayRpcServer` is bound to exactly one enrolled cell, gateway workload
and boot UUID; use `RpcTlsContexts.gatewayServer` with that gateway's own
certificate. The source must present an actor SPIFFE workload ID, and its cell
must match the call coordinator. Gateway processes remain outside the actor
cluster.

Command-scoped ACTIVE v2 signs the current native gateway workload and boot in
addition to the complete session binding. V1 proof purposes preserve their old
claims; relay authorization requires v2 and never infers a destination from
actor location. Rollout must explicitly enroll compatible producers and
consumers before enabling v2 traffic; older strict decoders fail closed. The
native authorizer exposes the original minimum of both ACTIVE expiries and the
committed negotiation roles/ICE/deadline, with no renewed receiver TTL.

`GatewayRelayRpcClient` accepts an immutable, explicitly bounded map of enrolled
`(cell, gatewayId, bootId)` endpoints. Its gateway TLS factory pins the destination
role, cell and workload before payload transmission. Two relay channels are
created lazily per active destination; their sockets are independent. Stale
boots cannot fall back to another process. Control RPCs use their separate
transport. Both RPC sides retain their own relay admission until original
transport/native cleanup completes; logical deadlines do not attest physical
cleanup.

Bind the listener backend to `GatewayRelayStream.send`, supply bounded CPU
execution, the actual connection registry, current gateway boot/security and
trusted clock sources. Delivery rechecks the full recipient binding and original
ACTIVE expiry immediately before Netty write, retaining the original write
receipt through timeout or unknown close. A WRITE_COMPLETED result remains
volatile. The native coordinator producer and cache/retry integration have joined local
end-to-end validation described below. Complete process startup still requires
installation; these components alone are not deployment or qualification evidence.

`NativeActorComposition.relayProducer(network, capacity, memory, gatewayClient)`
now builds the native source-backed round cache and producer. Install it once
with `backend.nativeRelay(producer)` before opening coordinator RPC ingress.
Every native CallActor invokes the installed producer's invalidation callback
before acknowledging committed terminal, resumed or new-negotiation results;
terminal/binding/round hydration and actor Stop also retire volatile state.
The callback does not renew authority or retire unknown original writes.

The joined local test exercises real PostgreSQL, hosting EntityRefs, both signed
ACTIVE homes, TLS1.3 gateway RPC and original Netty writes for all four relay
kinds, including same-SDP receipt reuse and immediate rejection after HANGUP.
Gateway boot/security adapters in that test remain TEST_ONLY. Full process
startup, authenticated production sources and deployment qualification remain
required.

`NativeActorComposition.rpcIngress(...)` installs the native control/read/setup,
critical, relay and session backends before its listener can start. It requires
already registered native regions and explicit environment/TLS/admission/network,
gateway workload enrollment, relay memory and an owned gateway relay client.
`NativeActorRpcIngress.drain()` waits for original cell server work before
retiring volatile producer state and draining the gateway client; register this
owner in the runtime source drain list. The joined test now crosses both actual
mTLS RPC legs before the original Netty write. This factory is a concrete startup
component; Spring Main enrollment and complete process composition remain open.

Both outbound RPC client drains now join original stream settlement, actual
ManagedChannel termination and callback executor termination. One shutdown
waiter uses a shared2s transport/executor budget after stream settlement. An
unproven close fails drain and cannot authorize database closure; an original
stream with unknown cleanup continues to hold the barrier without a new deadline.

`NativeActorSecurityPolicies` binds the native session, route and user-only home
freshness callbacks to one `RevocationReconciler` and live trusted-clock source.
It reads this cell's actual primary/progress in the already-admitted original
transaction. Missing, future, expired or incomplete progress denies permission;
scoped principal/route reads additionally require an unexpired token, known
signing key and durable subject/key checks. Clock trust is checked again after
SQL. No poll, executor or independently renewed timeout is created here.

The user-only home callback establishes source freshness. It cannot identify an
issuer/jti/key and does not grant scoped identity authorization; native session
and route checks remain independent and mandatory. Existing native home, bucket,
reservation, winner and ownership guards retain their authority. Constructor
clock/source enrollment is explicit; constant suppliers in PostgreSQL correctness
tests remain TEST_ONLY. Complete Main composition and real qualification are
still required.

`NativeActorPolicyConfiguration` is registered as Boot auto-configuration, after
user enrollment definitions. Main installs one native policy bean only on the
actor profile when both actual `RevocationReconciler` and `ClockSafetyMonitor`
beans exist. The bean uses that monitor's live `valid` predicate, not a captured
healthy value. Sources declared in a later enrollment configuration are visible;
policy installation does not depend on component-scan order. Gateway/control
profiles receive no actor policy. No missing source/clock is manufactured.

The native source integration test obtains this bean from Main, binds it to all
three `NativeActorComposition.Inputs` policy callbacks and verifies a guarded
primary freshness read after four real Pekko members, registered regions and
signed mTLS clock/revocation polls are live. Input homes, enrollment and denied
calling policy remain explicit TEST_ONLY fixtures. This bean binding does not yet
assemble all plane factories, listeners or runtime Secret enrollment.

## Main native actor factories

`NativeActorSourceEnrollment` supplies the explicit cell/storage epoch, actual pod
UID and process boot UUID, complete `IdentitySecurityContract` and two enrolled
HTTPS/TLS/public-key endpoints. Endpoint trust maps are copied; credentials,
queries, noncanonical paths and cleartext URLs are rejected. Source factories run
only on the actor profile and require admitted `SqlTransactions`. Enrolled
issuer/audience/skew must match effective typed configuration. Reconciliation
freshness is the smaller of the supplied hard safety bound and the existing
five-second maximum. Bounds below200ms reject because fixed100ms source polls
must retain their half-window margin; no identity policy is invented.

Boot processes native source definitions before actor policy definitions.
`NativeActorBusinessEnrollment` supplies the genuine native ActorSystem, cell/
storage/routing/pod identity, proof signer, directory hints, epoch adoption, bounded
token verifier and explicit calling policy. Main constructs native lease/backends
with all three source-backed policy callbacks and live monitor validity. It
rejects any cell/storage-epoch/pod mismatch with a supplied source enrollment
before installing native lease/backends. Independently enrolled legacy source
beans still undergo the existing native source/SQL/clock guards. Main registers
both regions only after actual local membership Up. An enrollment whose
process has not reached Up cannot proceed to business RPC startup.

`NativeActorIngressEnrollment` supplies internal environment/TLS/workload identity,
bounded RPC admission, native network, original relay memory/capacity, enrolled
gateway transport and native control-delivery callback. Main resolves region
registration before binding the actual `NativeActorRpcIngress`; an already
installed composition still passes the factory's independent registration guard.
Native owner drains retire the listener/source transports; inferred Spring
destruction is disabled so it cannot stop safety sources before lease handoff.

`NativeActorSchedulingEnrollment` supplies the same native ActorSystem, approved
fingerprint/AZ roles, at most12 explicit MAINTENANCE jobs, event reporting
and private health address/cached liveness/metrics. Main rejects a different
scheduling ActorSystem before installing business backends. Main constructs
the original source aggregate, its four fixed100ms SAFETY jobs, bounded scheduler
and private health listener. New source objects remain unhealthy until original
polls complete; healthy sources alone never establish native membership/regions.

Once the native composition, server/client, scheduler, DB boundary/pools and
private health owners are supplied, Main installs `NativeActorRuntimeHooks` into
Pekko's public graph before starting workers. `NativeActorSpringLifecycle` begins
that graph when Spring stops; framework handoff/leave precede native root release,
and source/relay transports, physical DB work and pool closure retain their
original cleanup receipts. Spring waits70s for the <=65s native graph within90s
pod grace. Unknown cleanup never reports graceful native closure. All supplied
native owners must disable inferred bean destruction so Spring cannot close pools
or safety transports ahead of native release.

`NativeGatewayBusinessEnrollment` supplies an exact gateway/boot/home-cell identity,
an independent routing epoch, bounded token verification, scoped cached security,
a mandatory cached global security-freshness predicate,
verified directory homes and pinned R1 verification keys/cache bounds. Main builds
the native boot client, original tracked proof cache, command adapter and services;
invalid clock attestation makes cached security FRESHNESS_UNKNOWN.

`NativeGatewayIngressEnrollment` supplies WSS TLS, exact origins/native upgrade
policy, socket/handshake/event-loop bounds, internal gateway mTLS, two explicit
admission owners and cached-safety sweep settings. Main binds both native listeners
against the same connection registry and boot. One process sweep (25–250ms,
candidate100ms) reads cached state independently of heartbeat/client traffic.
Queued rejection rechecks the current binding and native status; healthy sockets
produce no sweep event-loop tasks. This is not an external revocation source and
does not prove the authoritative commit-to-socket SLO under production load.

`NativeGatewayLifecycleEnrollment` supplies private probe inputs. Main stops new
socket admission, sends jittered RECONNECT advice in batches<=128/100ms and joins
original write, CPU, cache, verifier and client transport completion before retiring boot
and private health. The gateway coordinator has a300s budget; Spring waits310s.
An unknown original cleanup cannot report successful native shutdown.

These are explicit factory bindings. The configuration-only mounted YAML reader
does not enroll these objects or create managed ActorSystem/discovery/management,
gateway source ingestion or a mounted control enrollment. Those integrations, partial
actor startup-failure cleanup and approved source/business-policy/maintenance contracts
remain required. TEST_ONLY native mTLS/PostgreSQL/Pekko/WSS fixtures verify factory
paths and ordered Spring stop; no production topology or capacity claim follows.

Native control factories consume `NativeControlBusinessEnrollment`: native
PostgreSQL directory repository, the exact bounded verifier, scoped cached security
(including signing-key status), cached global source freshness, clock attestation
and directory refresh <=30s. No healthy security predicate is supplied by Main.
`NativeControlIngressEnrollment` requires server TLS, resolved HTTPS binding and
private health/cached liveness/metrics inputs. The control profile runs WebFlux on
owned Netty loops (one acceptor/two workers); absent enrollment rejects before a
plaintext listener and preserves native configuration/startup diagnostics.

Bootstrap rechecks the original principal after asynchronous directory resolution.
Expired/revoked/freshness-unknown identity or lost clock/source trust cannot receive
a successful response. `directoryEpoch` is a decimal JSON string, preserving all
positive PostgreSQL bigint versions in browser clients. Native readiness falls
before Boot's graceful HTTPS drain. The exact verifier and private metrics executor
retire original admitted CPU work; unknown cleanup does not prove a successful stop.

Gateway startup failure observes already-created owners before Spring removes
its singleton registry. Physical ingress/cache/verifier/client drains precede boot
retirement, sharing the original300s partial-owner budget. A complete gateway
lifecycle uses its original graph. Actor shutdown now also joins its original
verifier after framework/root and transport/source cleanup, before DB pool closure.
This does not provide managed ActorSystem/discovery/management or partial actor
startup ownership, mounted Secret-to-enrollment producers, control DB/source
process ownership or an approved source/calling-policy/maintenance/drill contract.
