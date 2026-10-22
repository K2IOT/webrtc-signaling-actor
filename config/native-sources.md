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
token verifier. The selected §49.2 business rule permits authenticated users to
call other authenticated users. Main owns its authoritative implementation and
version `open-authenticated-v1` through `NativeCallingPolicyConfiguration`.
With actual `NativeActorSourceEnrollment`, its decision age is at most the
smaller of the enrolled hard safety bound and five seconds, further capped by
the caller token expiry. The nine-argument business enrollment uses that bean;
an explicitly supplied policy callback or policy bean remains authoritative.
Missing identity/source enrollment does not create an open policy. Original JWT
verification, exact current session/boot, scoped durable revocation, source
freshness, trusted clock and callee route checks still precede commitment.
Main constructs native lease/backends
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
does not enroll these objects, gateway source ingestion or a mounted control enrollment.
Managed actor creation is available only with the explicit process enrollment below. Those integrations and approved source/business-policy/maintenance contracts
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
These factories do not provide mounted Secret-to-enrollment producers or an
approved scoped-security/calling-policy/maintenance/drill contract.

`NativeControlDatabaseEnrollment` explicitly supplies separate regional-directory
and local-cell PostgreSQL endpoints/credentials, per-class admission quotas and
fixed safety/control pool maxima for each database. It also supplies the local
cell/storage epoch and a cached regional-writer gate; no writer permission is
inferred from availability. `NativeControlDatabaseResources` is published before
its two SQL aliases and directory repository. Pool leaves are owned by that
resource, with no independent Spring destruction. Schema migration, PostgreSQL
TLS/HA configuration and aggregate fleet connection qualification remain operator
inputs; creating pools does not certify them.

`NativeControlSourceEnrollment` supplies the approved clock HTTPS endpoint,
explicit TLS and Ed25519 trust, cell/storage epoch, pod/process binding and worker
observations. Main creates the exact attestation monitor, source and fixed100ms
SAFETY polling job. An unavailable source keeps readiness false; it supplies no
security permission or global revocation freshness. Database/source authority
must match when both managed enrollments are present.

`NativeControlProcess` requires both directory SQL owners to be enrolled and the
exact business verifier/clock monitor to match their managed owners. It starts
enrolled workers, drops readiness before Boot's HTTPS drain, and retires original
worker/source/crypto/SQL receipts before private probe/metrics retirement and pool
closure. Independent regional/local boundaries and pools are deduplicated by
identity. A drained owner rejects restart. Installed shutdown and startup-failure
observers share the original stage and Spring's existing30s resource phase budget;
unknown physical completion retains pools. Custom externally supplied pool/source
beans must also disable independent destroy methods so these receipts own closure.

Partial startup tracks resources before Spring removes singletons, including
managed DB resources before SQL aliases exist and a verifier supplied inside the
business enrollment before its alias exists. Published native SQL bindings prove
their own boundary/pool associations; unbound pool objects remain open with
unproven cleanup. Native TEST_ONLY tests cover actual Main with unavailable clock
polling, separate authority pools, original post-COMMIT tails through logical
expiry, worker physical tails, pre-lifecycle failure, early DB publication and
rejected verifier/clock/SQL ownership. Approved scoped security feed ingestion,
mounted production enrollment and full deployment qualification remain open.

Cached clock readers never wait for source signature verification. Original signed
report validation publishes immutable atomic state; invalidation generations and
monotonic signed sequences prevent an in-flight stale positive result from reviving
known loss. Expiry, wall steps and conservative original-request margins remain
enforced. This verifies consumer concurrency, not external clock quality.

Actor startup failure retains both already-initialized beans and pre-refresh native
singletons. A published composition uses the original Pekko handoff/leave graph,
then joins native root release, original listener/worker/source/verifier receipts
and DB closure. An already-installed runtime reuses its original hooks. The runner
and failure event share one cleanup receipt and the original65s observation budget;
Spring cannot reorder the early owner observer through bean discovery.

Partial native ingress owns its server receipt directly, even before a server alias
bean exists. Original RPC verification must settle before lease release. A pool with
no published SQL boundary cannot attest cleanup: the failure is marked unproven and
the pool remains retained. Startup requires the native lifecycle and hooks as well
as bound listeners and sources. The actual Main fixtures cover partial refresh,
runner rejection and a complete owned but unhealthy TEST_ONLY source graph; a
successful process launch does not imply business readiness or production
qualification. Mounted trust/provider enrollment and external qualification remain required.

`NativeActorProcessEnrollment` supplies the exact process name, resolved native
Config and explicit server/client management SSLContexts. The actor profile
constructs one `NativeActorProcess` before management binding, retains it in the
early startup observer, and exposes `nativeActorSystem` only after original
management/bootstrap completion and local membership Up. Formation uses the
native Kubernetes API namespace/selector/named-port discovery; no static seed
or direct join path is added to Main. Normal joining never creates an isolated
cluster. The original 30s budget covers binding, bootstrap and membership.

Management engines require TLS1.3 and peer client certificates; outgoing management
probes require HTTPS hostname verification. The pinned Pekko1.1.3 Artery engine
actually reads `pekko.remote.artery.ssl.config-ssl-engine`. That branch now carries
keystore/truststore inputs, TLS1.3, AES GCM TLS1.3 suites and mutual authentication.
Validation rejects a different engine provider, weaker protocol or disabled mTLS.
It also pins the actual Kubernetes discovery implementation/delegation, management
named port, six contact points and enabled join compatibility checks.
Deployment enrollment must provide real cell peer PKI and routable member addresses;
TEST_ONLY node PKI is never a production fallback.

Managed startup failure settles original admitted work before running the original
Pekko graph, even before a business composition is published. A conditional native
root drain retires installed lease dependencies before system termination. Pool
closure retains the original root/process/SQL receipts. ActorSystem bean destruction
is disabled; the managed owner shares original binding, stop and termination receipts.
A drained owner rejects later startup calls. Business enrollment must use the same
managed ActorSystem, and the factory rejects a different process before installing
backends.

The native fixtures use real TLS remoting/management, a separate Main JVM, HTTPS
Kubernetes discovery and six not-ready pod records against an existing TEST_ONLY
cluster. They cover binding rejection, local Up ordering, held root release,
discovery loss and client identity rejection. The fixture's existing peers join
manually only to establish its test cluster; the Main child joins exclusively
through Kubernetes discovery. This verifies the explicit owner factory, not a
mounted production enrollment, full serving topology or capacity/HA qualification.

## Local process profile

LOCAL_TEST_ONLY containers read the mounted native contract (`MountedNativeContract`
schema v1, unchanged) plus a per-container process profile. Neither is a production
enrollment, and none of it qualifies a deployment. The loader, the plane installers and
the Spring registration are `io.webrtc.signaling.app.runtime.local`
(`LocalRuntimeInitializer`, registered in `META-INF/spring.factories`).

### Files and activation

| Variable | Default | File |
|---|---|---|
| `SIGNALING_NATIVE_PROVIDER_CONTRACT` | `/run/signaling/provider.json` | the mounted native contract |
| `SIGNALING_LOCAL_PROCESS_PROFILE` | `/run/signaling/process.json` | the process profile described below |

The gate opens only when all three hold: `SIGNALING_DEPLOYMENT_MODE=local-minikube`,
`SIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY`, and an explicit, non-blank
`SIGNALING_NATIVE_PROVIDER_CONTRACT` or `SIGNALING_LOCAL_PROCESS_PROFILE` (the other
variable then takes its default). Markers alone, as in legacy tests and tooling, register
nothing and startup keeps failing with `NATIVE_RUNTIME_NOT_INSTALLED`. With the gate open
the contract and the profile are always loaded, and a missing, unreadable or invalid file
stops startup with the specific failure below. The one quiet exit is a readable contract
that declares an environment other than `local-test-only`: nothing is registered. Exactly
one of the Spring profiles `gateway`, `actor` or `control` must be active, and the
profile's `plane` must be that plane; a mismatch stops startup with the generic
`STARTUP_FAILED` diagnostic.

Scope in SP1: the adapter is compose-only. Nothing renders `process.json` for it yet; the
compose renderer (SP3) owns producing the profile and the mounted contract. The Helm chart
always sets `SIGNALING_NATIVE_PROVIDER_CONTRACT`, so a Helm `local-minikube` release now
opens the gate and fails closed with `LOCAL_RUNTIME_REJECTED` `profile/MISSING` (before this
adapter it failed with `NATIVE_RUNTIME_NOT_INSTALLED`). It could never start anyway: the
chart's `SIGNALING_POD_UID` comes from the pod's `metadata.uid` and cannot equal the
profile's `podUid`. Helm is not a supported consumer of this adapter; the chart and its
render checks are unchanged.

Other variables the adapter reads (through the Spring `Environment` unless noted):

| Variable | Planes | Rule |
|---|---|---|
| `SIGNALING_POD_UID` | all | lower-case canonical UUID equal to the profile's `podUid`, else `identity/IDENTITY_MISMATCH` |
| `SIGNALING_CELL_ID` | all | equal to the contract's `cell`, else `identity/IDENTITY_MISMATCH` |
| `SIGNALING_IDENTITY_ISSUER`, `SIGNALING_IDENTITY_AUDIENCE` | all | equal to the contract's `identity.issuer` and `identity.audience`, else `identity/IDENTITY_MISMATCH` |
| `SIGNALING_CLUSTER_FINGERPRINT` | all | 64 lower-case hex; absent is `identity/MISSING`, malformed `identity/INVALID` |
| `SIGNALING_FORMATION_PROFILE` | actor | `initial-six` enables `new-cluster-enabled` and `signaling.controlled-initial-formation`; any other value or none joins an existing cluster only |
| `SIGNALING_KUBE_API_HOST`, `SIGNALING_KUBE_API_PORT` | actor | must be real OS process environment variables equal to the profile's `kube.apiHost` and `kube.apiPort`, else `profile/IDENTITY_MISMATCH`; Pekko discovery reads the address from the process environment, so a system property or an application property never satisfies the check |
| `SIGNALING_REMOTING_KEYSTORE_PASSWORD`, `SIGNALING_REMOTING_KEY_PASSWORD`, `SIGNALING_REMOTING_TRUSTSTORE_PASSWORD` | actor | 1 to 512 characters for the contract's PKCS12 remoting stores; absent is `contract/MISSING` |

The adapter layers its values over the packaged `application-actor.conf`, so the older
`SIGNALING_POD_IP`, `SIGNALING_NAMESPACE`, `SIGNALING_ACTOR_SERVICE`,
`SIGNALING_DISCOVERY_SELECTOR`, `SIGNALING_REMOTING_KEYSTORE` and
`SIGNALING_REMOTING_TRUSTSTORE` are not needed: pod address, namespace, selector, service
name and store paths come from the profile and the contract.

### Path confinement

Every path in the contract and in the profile is absolute, at most 512 characters, and its
real location (after following every symbolic link) must lie inside the real directory of
`provider.json`. The profile file itself must live in that same directory, otherwise
startup fails with `profile/PATH_ESCAPE`. A path that does not exist is `MISSING`; one that
leaves the directory is `PATH_ESCAPE`. A later read of a mounted key or store that fails is
`path/MISSING`, an empty one `contract/INVALID`, an oversized one `contract/TOO_LARGE`.

### Failure rendering

Every adapter failure carries a role (`contract`, `profile`, `identity` or `path`) and a
category (`MISSING`, `TOO_LARGE`, `MALFORMED`, `DUPLICATE_KEY`, `INVALID`,
`IDENTITY_MISMATCH`, `PATH_ESCAPE`, `ENVIRONMENT_MISMATCH`, `KEY_MISMATCH`), never a path,
value, key material or cause. Startup renders exactly one fixed line, for example:

```json
{"component":"PLATFORM","level":"ERROR","errorCode":"LOCAL_RUNTIME_REJECTED","role":"contract","category":"MISSING"}
```

This section writes a failure as `role/CATEGORY`. `NATIVE_RUNTIME_NOT_INSTALLED` instead
means the adapter registered nothing (gate not open, or the quiet exit for a contract of another
environment); `STARTUP_FAILED` is the generic diagnostic for a failure that is
not an adapter rejection. `qualification/scenarios/image-contract.sh` proves the rendering
above from the packaged image for a contract path that does not exist in the container.

### Process profile

`process.json` is one UTF-8 JSON object of at most 64 KiB, read as strictly as the contract:
unknown, duplicate and missing keys, trailing content, non-UTF-8 text, scalar coercion and
nesting beyond depth 8 are rejected. Every key below must be present; optional values are
explicit `null`. Durations are ISO-8601 text. A syntactic failure is `profile/MALFORMED` or
`profile/DUPLICATE_KEY`; every validation failure is `profile/INVALID`.

| Field | Rule |
|---|---|
| `schemaVersion` | `1` |
| `plane` | `ACTOR`, `GATEWAY` or `CONTROL` |
| `podUid` | UUID; equals `SIGNALING_POD_UID` |
| `podIp` | literal dotted-quad IPv4; never resolved by name |
| `businessPort` | actor: cell RPC listener; gateway: WSS; control: HTTPS |
| `healthPort` | private probe on `podIp:healthPort`; differs from `businessPort` |
| `relayPort` | gateway relay RPC listener; differs from the other two on the gateway; still a valid port on every plane |
| `azRole` | actor: `az-[a-z0-9][a-z0-9-]{0,62}`; may be `null` elsewhere |
| `cellAzRoles` | see below |
| `gatewayId`, `gatewayBootId` | both or neither; the gateway requires both |
| `wal` | actor only in effect; see below |
| `compatibility` | actor, gateway and control; see below |
| `kube` | actor requires it; see below |
| `transferCells` | `[]` or `null` except on control; see below |
| `privacyPseudonymKey` | actor requires it; see below |
| `allowedOrigins` | at most 32 of `https?://host[:port]`; the gateway requires at least one |

`cellAzRoles`: 2 to 3 distinct names matching `az-[a-z0-9][a-z0-9-]{0,62}`, including this
actor's own `azRole` (one zone can never open the two-zone compatibility window). Cluster
membership counts and classifies peers by this set: a peer in a zone outside it is invalid.
Every actor of a cell carries the same list. Gateway and control profiles must leave it
empty (`[]` or `null`). While an enrolled zone has no Up member, the actor logs a WARN with
the attribute `missingZone` (the zone name only) each time the set changes; such a cell
forms but stays not ready. "Local actor zones complete" (INFO) is logged only once at least
two distinct enrolled zones have Up members.

`wal` (actor): the observed writer facts for `NativeWalEnrollment`, always with
`localSingleWriter=false`. Fields: `systemIdentifier` (1 to 20 digits), `timeline` (at
least 1), `replicas` (at most 16 names), `synchronousReplicas`, `maximumLagBytes` (not
negative), `freshness` and `maximumReplyAge` (positive durations). An empty `replicas` set
or `synchronousReplicas < 1` is refused; there is no standby-less local mode.

`compatibility`: `major` (at least 1), `minor`, `executableId`, `features`, `lowerMinor`,
`upperMinor`. `executableId` is 64 lower-case hex. `features` holds only known feature
names (the `CompatibilityWindow.Feature` values spelled in lower case with hyphens, for
example `session-s1`); an unknown name is refused. `upperMinor` must equal `lowerMinor + 1`
and `minor` must be one of the two. The build identity binds `SIGNALING_CLUSTER_FINGERPRINT`.
On the actor the values become the Pekko `compat-*` roles and
`signaling.compatibility.*`; every plane builds its compatibility observation from them.
The contract's `cells` and the directory snapshot's gateway boots decide whom the plane
observes.

`kube` (actor): `apiHost`, `apiPort`, `caPath`, `tokenPath` (both confined as above) and
`namespace` (a DNS label). Discovery selects pods by
`app=webrtc-signaling,plane=actor,cell=<cell>` in that namespace. The API address must also
be exported as real environment variables (table above).

`transferCells` (control): EXACTLY ONE entry, another cell the contract enrolls, with that
cell's enrolled storage epoch. Zero, several, this cell itself, a cell the contract does not
enroll, a `storageEpoch` that differs from the contract's entry for it or a database URL
shared with this cell or the directory is `profile/INVALID`. Each entry is `cell`, `storageEpoch` and `database` with `jdbcUrl` (exactly
one `sslmode=verify-full`), `username`, `password`, `quotas` (a count per database class),
`safetyMax` and `controlMax`; the file carries counts only, never resource profiles. Control
is the only plane that holds credentials for more than one cell database.

`privacyPseudonymKey` (actor): absolute path of a file holding the raw privacy pseudonym key,
32 to 64 bytes, no encoding. The file fails like every mounted file: absent or unreadable is
`path/MISSING`, outside the mount `path/PATH_ESCAPE`. Every defect of the key itself (field
absent, file empty, shorter than 32 or longer than 64 bytes) is `profile/INVALID`.

`gatewayId` and `gatewayBootId` (gateway): the gateway's own identity and the boot UUID it
runs as. The profile pins the boot because the actors' compatibility enrollment and relay
targets need every gateway boot in advance (see "Directory snapshot"). Consequence: the
boot id is fixed for a provisioned run. Restarting only the gateway container is
unsupported, since a restarted process must not reuse a boot id. Recovery is to
re-provision and restart the cell's containers (compose down, then up).

### Plane behaviour

Actor (`LocalActorInstaller`): opens only `cellDatabase`; it never reads
`directoryDatabase`. Artery (`25520`) and Pekko management (`8558`) bind only `podIp`, never
every interface. The cell RPC listener (`businessPort`), the gateway's WSS and relay
listeners and the control HTTPS listener (`businessPort`) bind every interface, so the container network design must isolate them. The
private health address is `podIp:healthPort`.

Gateway (`LocalGatewayInstaller`): opens no database. It verifies relay proofs with public
keys only and never loads the cell's proof signing key. It admits only the profile's browser
origins; native clients without an `Origin` are refused.

Control (`LocalControlInstaller`): opens the regional directory, its own cell database and
the transfer cell's database. It presents the control identity to the actors (below), is
not bound to a cell for observation, and polls the actors through their `CONTROL` admission
lane. Regional writer admission is a constant `true`: compose runs exactly one control
container, and a real fence waits for the authority work in the Minikube plan (A3/A4).

The mounted leaf (`tls.certificate`) is used both for the plane's public listener and for
its cell RPC identity. Its single URI SAN is `spiffe://signaling/local-test-only/cell/<cell>/actor`,
`.../gateway/<gatewayId>` or `.../control` (an optional `/<id>` suffix is parsed for each
role). The control identity is admitted by actor servers for compatibility observation only
and refused for execute, relay and sync.

### Refuse-all stand-ins

Two collaborators have no production implementation yet, so the actor enrolls stand-ins that
fail closed rather than fake success:

- Privacy detachment refuses every detachment: deletion requests stay pending and are never
  reported as complete.
- Epoch adoption is refused: call participation stays on the enrolled epochs.

Consequently a local container offers no privacy deletion and no epoch adoption, promotion or
migration.

### Directory snapshot

`directorySnapshot` names a UTF-8 JSON file of at most 64 KiB. Its exact bytes must hash
(SHA-256, lower-case hex) to `directorySnapshotSha256`. The parser is as strict as the
contract parser: unknown, duplicate and missing fields, trailing content and fractional
numbers (`"first": 1.9`) are rejected.

```json
{
  "schemaVersion": 1,
  "qualification": "LOCAL_TEST_ONLY",
  "buckets": [
    {"first": 0, "last": 8191, "cell": "c001", "directoryEpoch": 1, "storageEpoch": 1},
    {"first": 8192, "last": 16383, "cell": "c002", "directoryEpoch": 1, "storageEpoch": 1}
  ],
  "gatewayBoots": [
    {"cell": "c001", "gatewayId": "gw-c001-a", "bootId": "<the gateway profile's gatewayBootId>"}
  ]
}
```

- `buckets`: 1 to 512 inclusive ranges over buckets 0..16383 (the bucket of a user is
  `SessionRegistryService.bucket`). Ranges must not overlap. Each cell must be one of the
  contract's `cells`, with that cell's `storageEpoch`. A range maps every bucket in it to the
  trusted home `(cell, directoryEpoch, storageEpoch)`. The split must equal the seeded
  `directory_bucket` rows. No process cross-checks the snapshot against those rows: the control
  plane resolves bootstrap routes from the rows, while actor and gateway use only the snapshot,
  so the renderer is responsible for keeping them equal.
  A bucket that no range covers has no trusted home and every home check for its users fails.
- `gatewayBoots`: at most 128 entries, exactly one for every contract `gateways` entry, and
  no other. It pins the boot id the actor's gateway relay client and compatibility
  observation address for that gateway; it must equal the `gatewayBootId` of that gateway's
  process profile, and the gateway refuses to start with `identity/IDENTITY_MISMATCH` when
  it does not.

Text that is not UTF-8 JSON, or has trailing content, fails startup with `contract/MALFORMED`;
a repeated key with `contract/DUPLICATE_KEY`; a digest mismatch or any other defect with
`contract/INVALID`.

The JWT public keys are parsed and checked (RSA, modulus of at least 2048 bits) before any
bean is registered, so a bad key fails startup with `contract/INVALID`. Worker outcomes other
than completion are logged at WARN with the job name and status only.

## Local platform source (LOCAL_TEST_ONLY)

Module `signaling-local-platform` (image `Dockerfile.local-platform`, executable jar
`signaling-local-platform-0.1.0-SNAPSHOT-exec.jar`, main `LocalPlatformApplication`) serves the
two sources of this contract, the signed clock report and the signed revocation page, for the
local compose environment. It is a plain Java process: it is not part of `signaling-app` and not in
its auto-configuration. Status is LOCAL_TEST_ONLY and NOT_QUALIFIED. Nothing here is evidence of
host clock discipline, high availability, production readiness or capacity.

### Start and configuration

- `SIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY` is required before anything else is read.
  `SIGNALING_IDENTITY_ISSUER` (the issuer carried by signed events; it must satisfy the session key
  rules: non-blank, no edge whitespace, no control characters, at most 512 bytes, otherwise `IDENTITY`) and `SIGNALING_PLATFORM_CONFIG`
  (absolute path of a strict JSON file, at most 64 KiB) follow. There is no default for any of them.
- A refusal prints exactly one line to stderr, `LOCAL_PLATFORM_REJECTED <CATEGORY>`, and exits 1. The
  categories are `ACKNOWLEDGEMENT`, `CONFIG`, `IDENTITY`, `KEY`, `TLS` and `DATABASE`. No path, value,
  message or stack trace is printed. A bind failure is `CONFIG`; an unreadable DB password or a
  failed migration is `DATABASE`. The process prints nothing else on a successful start (the Flyway log, which would carry the JDBC URL, is
  switched off explicitly).
- The configuration has exactly these keys; unknown, duplicate or missing keys, trailing content and
  relative paths are `CONFIG`:
  - `cells`: object of 1 to 8 cell names (`[a-z][a-z0-9-]{0,23}`) to a storage epoch (integer >= 1).
  - `keyId`: key identifier of the proof signature, `[A-Za-z0-9_-]{1,64}` (the signer's pattern, the
    stricter of the two consumer patterns).
  - `signingKey`: absolute path of the Ed25519 private key (PKCS#8). The process only signs; it never
    needs or derives the public key. Provisioning publishes the public half to the consumers.
  - `source`, `admin`: `bind` (a strict dotted-quad IPv4 address, four decimal octets 0-255 without
    leading zeros, or an IPv6 literal; never a name, a short form such as `127.1`, or a wildcard
    address; no name lookup takes place), `port` (1 to 65535),
    `keystore` (PKCS12 with exactly one private-key entry), `keystorePasswordFile`, `clientTrust`
    (PEM bundle of 1 to 16 CA certificates).
  - `health`: `bind`, `port`.
  - `database`: `jdbcUrl` (must start with `jdbc:postgresql://`; its query may only carry `sslmode`,
    `sslrootcert`, `sslcert`, `sslkey`, `connectTimeout`, `socketTimeout` and `ApplicationName`, each at
    most once with a value, so no `password` and no driver parameter that instantiates a class such
    as `socketFactory`), `user`, `passwordFile`. Passwords come from files, never from the
    environment.
- Conventional ports (compose chooses them): source 9443, admin 9444, health 8559.

### Listeners and caller identity

| Listener | Protocol | Trusts | Caller identity |
|---|---|---|---|
| source | TLS 1.3, client certificate required | `source.clientTrust` | `spiffe://signaling/local-test-only/cell/<cell>/(actor\|gateway\|control)[/<instance>]`, `<cell>` one of `cells` |
| admin | TLS 1.3, client certificate required | `admin.clientTrust` (a CA separate from the workload CA) | `spiffe://signaling/local-test-only/admin/<name>` |
| health | plain HTTP on a concrete address | nothing | none |

- The caller identity is the single URI SAN of the verified client certificate. A header never
  supplies identity. A certificate with zero or several URI SANs, another trust domain or path shape,
  or an unconfigured cell is refused with 403. A workload identity is not accepted on the admin
  listener and an admin identity is not accepted on the source listener.
- A pod UID is bound to the SPIFFE identity that first claimed it. A different identity presenting
  the same pod gets 403. A new boot of the same pod and identity replaces the old one; a boot that a
  newer one already replaced gets 403.
- Every response has an exact `Content-Length`, `Connection: close` and `Cache-Control: no-store`,
  is never chunked, compressed or redirected, and a body above 32768 bytes is never sent (503
  with an empty body instead). Error bodies are empty, except for the JDK-generated 400 and 501 under "Known limits".

### Endpoints and statuses

The two source-plane requests are `GET` with no query and no body.

- `GET /v1/clock-bound` (source listener): `X-Signaling-Pod-Uid` and `X-Signaling-Process-Boot`,
  each exactly once and in canonical 8-4-4-4-12 hexadecimal form. 200 returns `{"report","signature"}`
  as described in "Native clock source contract". The offset header is ignored here.
- `GET /v1/revocations` (source listener): the same two headers plus exactly one
  `X-Signaling-Source-Offset`, a decimal `0|[1-9][0-9]{0,18}` that fits a long. 200 returns the
  seven-field page with at most 16 events, `retiredKeys` always empty, and a proof
  `<keyId>.<base64url signature>` that verifies only for the requesting cell and the configured issuer.
  An offset above the high-water mark is 409.
- `POST /v1/admin/revocations` (admin listener): `Content-Type: application/json` (optionally
  `;charset=utf-8`), a body of at most 4096 bytes, strict UTF-8 JSON with the keys `userId`
  (required), `jti` (string; `null` rejected; omitted means the user scope) and `epoch` (integer;
  optional). 201 returns `{"offset":n,"epoch":e}`.
- `GET /ready` (health listener): 200 only while the clock observer reports a continuous
  measurement and the database answers `SELECT 1 FROM source_state`; otherwise 503.

| Status | Meaning |
|---|---|
| 400 | malformed request: header, offset, query, body, JSON or user id; duplicate `Content-Type` |
| 403 | identity refused, pod bound to another identity, replaced boot, pod table full (new pod beyond 4096), unconfigured cell |
| 404 / 405 | unknown path / wrong method (405 carries `Allow`) |
| 409 | revocation offset above the high-water mark; admin epoch not above the latest of the subject |
| 413 / 415 | admin body above 4096 bytes / wrong admin `Content-Type` |
| 503 | database unavailable, oversized page, any unexpected failure; the body is empty |

### Clock report semantics

- Figures are this process's own measurement of its wall clock against its monotonic clock, sampled
  every 100 ms over the last 5 seconds, plus a fixed consumer observation allowance.
  `pairUncertaintyMicros` is the largest bracketed read uncertainty in the window plus
  `CONSUMER_OBSERVATION_ALLOWANCE_MICROS` (100000 microseconds, `ClockReportService`). The
  allowance is a stated constant for the consumer's own wall and monotonic reads (separate reads
  across JVM, GC and scheduler jitter and an HTTP hop), which the source cannot measure. The
  consumer's `ClockSafetyMonitor.valid()` bounds that jitter with the reported figures; without the
  allowance the genuine figures (tens of microseconds) made the planes flip to unsafe between polls.
  `relativeRateErrorPpm` is reported as measured. Validity is 2 seconds from the observation time.
- `continuous` is false until the window holds at least 2 seconds of clean samples, for 5 seconds
  after a step or stall (an offset jump above 50 ms between consecutive samples stays in the window
  that long), and whenever the reported uncertainty exceeds 250000 microseconds or the rate error
  exceeds 1000 ppm. A measured uncertainty above 150000 microseconds therefore reports unsafe. A
  snapshot older than 500 ms, or a failed clock read, is reported unsafe with figures of at least
  1000000. An unsafe figure is never rewritten into a safe-looking one.
- The sequence of each pod is `max(last + 1, epoch microseconds)`: strictly increasing within a
  process, and above earlier sequences after a restart if the wall clock has advanced.
- This measures only this container or process. A drift that moves the wall and monotonic clocks
  together, or an error against any external reference, is invisible to it. It is not a claim about
  host clock discipline.

### Revocation epoch semantics

- The token verifier (`Rs256TokenVerifier`) reads a missing `securityEpoch` claim as 0, and
  `RevocationState` revokes a token when the stored epoch of its user or session subject is
  `>=` the token's epoch; a subject with no stored row counts as -1. So the first admin revoke of a
  user (epoch 0) revokes an epoch-0 token, and a user with no events is not affected. This is pinned
  by `RevocationPageServiceTest` and `LocalPlatformCompositionIT` (an admin revoke returns epoch 0,
  the user is denied on the running planes, and a bystander is not). `RevocationState` is unchanged.
- An event with epoch `e` revokes every token of the subject with a security epoch `<= e`. A revoke
  needs `epoch >= 0`. When the admin request omits `epoch`, the platform uses the latest epoch of
  the subject plus one, or 0 when there is none. An explicit epoch not above the latest of the subject
  is 409. A user-scope event (no `jti`) and a session-scope event are separate subjects.
- Revoking a user is permanent in this environment: there is no unrevoke, and the log is append-only.

### Durable log

- The `source` database is migrated by the platform itself at start, before any listener opens, with
  Flyway location `classpath:db/source/migration` and its own history table
  `source_schema_history`. The cell schema's migrations and history are not touched.
- `revocation_log` and `source_state` are append-only: row triggers reject update and delete, statement
  triggers reject `TRUNCATE`, and the high-water mark only moves forward by one per append. Offsets are
  gapless and committed in offset order. Limit: this is enforced by triggers, not by privileges. The
  platform connects as the owner of the tables, and an owner can `DROP TRIGGER`. A database
  owner is trusted in this environment.

### Known limits

- JDK `HttpsServer` pre-authentication worker pinning. The source and admin listeners are the JDK
  `HttpsServer`, which performs the TLS handshake and reads the request on its worker threads. A
  single byte sent to the source port, without any certificate, pins one of its 4 workers
  indefinitely; 4 such connections stall the source. The admin listener has 2 workers. This is an
  availability limit only: the actors fail closed when the source stops answering. No scenario may
  assume the source survives a hostile peer.
- No per-request deadline and unbounded executor queues. A slow client ties up a worker. The JDK
  server also drains a declared but unread request body before closing.
- Malformed request lines and unsupported transfer encodings are answered by the JDK itself with a
  400 or 501 that carries a fixed `text/html` body (the request is not echoed). This is the one case
  where an error response is not empty.
- The boot replay window is bounded: only the last 8 replaced boots per pod are remembered, so an
  older boot is forgotten and can be replayed by the holder of the same identity. The pod table
  holds at most 4096 pods and is never reclaimed; a new pod beyond it gets 403.
- All six actors of a cell may present one SPIFFE id (`.../cell/<cell>/actor`, no instance segment).
  The boot registry keys on (identity, pod UID), so any actor of the cell can register a new boot for
  a sibling's pod UID, after which the sibling's own boot is refused (403): an availability effect
  only, since reports and pages stay signed per caller. For the same reason any enrolled peer can
  fill the 4096-pod table with made-up pod UIDs and starve later pods. SP3 should issue per-pod
  instance ids (`.../actor/<name>`) so that the instance segment separates the pods.
- Recreating the `source` database while the cell databases keep their revocation cursors makes
  every revocation page a 409 (the cell's cursor is above the new, empty log's high-water mark).
  That is the intended fail-closed outcome; the cells must be reprovisioned (cursors reset) before
  the sources are usable again.
- One new database connection (with TLS and SCRAM when the JDBC URL and database require them) is
  opened per revocation page, per admin append and per readiness probe. There is no pool, and host
  load can exceed a consumer's connection deadline.
- `retiredKeys` is always empty: no key retirement is modelled.
