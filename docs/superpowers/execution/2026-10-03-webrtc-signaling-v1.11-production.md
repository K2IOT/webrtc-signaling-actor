# Native inline execution status

Plan: `docs/superpowers/plans/2026-10-03-webrtc-signaling-v1.11-production-implementation.md`.
Spec: full 1,762-line v1.11 document, synchronized from origin at `a3301a8` and read in full. The 665-line plan was read in full. Implementation runs inline on `work` in the dedicated managed cloud checkout.

| Task | Status | Evidence |
| --- | --- | --- |
| 1 | Complete | `98d1bb3`; config tests RED→GREEN; startup rejection/defaults/fingerprint; reactor package succeeds |
| 2 | Complete | `5bf8797`; protocol, config-adapter and internal-envelope RED→GREEN; protobuf/gRPC generation; N/N−1 schema tests |
| 3 | Implemented contract | `44872bf`; strict RS256, bounded verification, policy and revocation interfaces; production identity settings required and unconfigured |
| 4 | Implemented contract | `a3a8990`; bootstrap, bucket hashing, durable-directory interfaces and freeze/install/publication protocol; JDBC adapter follows storage |
| 5 | Complete | `884e19c`; real PostgreSQL 17.6 migrations and safety constraints; two fixed pools, admitted VTs, native authority barriers and JPA-bound transaction tests |
| 6 | Complete | `62df892`; generation/incarnation fences, five-session admission, refresh, old-close protection, irreversible gateway boots and bounded pulse batches |
| 7 | Complete | `5fd5d4f`; reciprocal reservations, Release-before-Reserve tombstone, immutable winner and 100 ACCEPT race |
| 8 | Implemented transaction layer | `c4b474b`; scoped result/call/outbox COMMIT, primary replay and fenced outbox completion; workflow transitions follow actors/RPC |
| 9 | Implemented lease adapter | `5fa4179`; public LeaseProvider, physical cleanup/reconciliation, sequence replay and stale callback gates |
| 10 | Complete local runtime tests | PostgreSQL acquire-before-child, warm/cold routes, exact placement release, coordinator restart, shard relocation, full actor restart, incompatible fingerprint rejection |
| 11 | Complete | Seven actor tests plus real-primary session/reservation/winner hydration; physical cleanup gates; 60s passivation; restored-storage boot fence regression |
| 12–16 | Complete local verification | Native actors, proof/RPC/WSS, physical backpressure and ordered WebRTC relay; details below |
| 17 | Complete local verification | Native reconnect/media deadlines, current-session proofs and terminal reads; 242 reactor tests pass |
| 18 | Complete local verification | Native bounded workers, authenticated catch-up and opaque privacy deletion; storage reactor passes |
| 19 | Complete local verification | Privacy-safe logs/traces, enum metrics, four recovery SLIs, validated dashboards / Prometheus rules |
| 20 | Complete deployment contracts | Rendered Helm object semantics / unsafe overrides / lint; no deployed HA qualification |
| 21 | Complete local lifecycle/restore verification | Ordered physical drain, actual Pekko shutdown graph, N/N-1 feature gates and native recovery epoch repair |
| 22–24 | In progress / NOT_QUALIFIED | Native gateway factories/safety/shutdown added; complete launcher/source contracts, security drivers and measured qualification remain open |

`./mvnw verify` at Task 2 completion: 43 tests. After the review fix: **51 tests, zero failures/errors/skips**. Empty-module JAR warnings are expected scaffold warnings; startup-failure warnings belong to negative config tests. Tests exercise configuration/protocol contracts, not any unimplemented runtime or production scale.

## Required identity input

Before production deployment and qualification, supply:

- Concrete trusted issuer and signaling audience.
- Maximum accepted JWT lifetime and enforceable issuance-time policy.
- Preserved-jti refresh behavior (the spec baseline) or an approved replacement transition contract.
- `revocationPropagationSLO` and `hardSafetyBound`.
- Durable ordered revocation event source and authoritative high-water/cursor interface.
- Explicit calling policy and its authoritative source: open authenticated users or tenant/contact/block rules.

The full spec still says the concrete revocation bound is obtained from the identity platform. Its restoration did not supply that external contract. Private signing keys are not required by signaling.

## Decisions recorded during execution

1. Retain the native dedicated cloud checkout on `work` instead of creating a nested worktree, matching the user's native execution request. Cost if wrong: weaker git-worktree isolation.
2. Cloud skill packages did not expose their referenced task-start/test-guidance/reviewer-template assets. Native brief extraction, test logs and ledger updates preserve task text and test/commit gates. Cost if wrong: helper bookkeeping differs.
3. Protocol exposes a narrow `ProtocolLimits` contract mapped by app config. Direct protocol→app consumption would create a cycle once app launches protocol consumers. Cost if wrong: one adapter/interface adjustment.
4. Pin a candidate BOM and exact image versions without qualification claims. Public Lease integration, dependency security, image digests and infrastructure still require their planned gates. Cost if wrong: replace dependencies and rerun affected gates.

Native ledger, exact task briefs and test logs are preserved in the ignored `.superpowers/sdd/2026-10-03-webrtc-signaling-v1.11-production-implementation/` directory while execution continues. Resume from the ledger; do not redo completed tasks.

6. Continue implementation using required unconfigured identity settings and explicit test-only fixtures, as authorized by the user; never fabricate production policy. Cost if wrong: deployment remains fail-closed until the real settings are supplied.
7. Isolate directory migrations in `db/directory/migration` to avoid duplicate V001 versions on a shared launcher classpath. Cost if wrong: adjust the configured Flyway location.
8. Select named integration tests through Failsafe `-Dit.test`, because the plan’s `-Dtest` would select Surefire rather than execute the named IT. Cost if wrong: runner invocation differs.

Current verified reactor result after Task 6: **87 tests, zero failures/errors/skips**. PostgreSQL tests use real single-node containers and do not certify cross-AZ WAL durability.

## Fresh-context review and fix

One fresh-context whole-implemented-branch review inspected `a3301a8..5bf8797` using Superpowers requesting-code-review. No Critical findings. One Important finding: the Jackson byte decoder admitted UTF-16 and invalid UTF-8 inside strings. Eight regression cases failed first; strict UTF-8 decoding with REPORT and Unicode scalar validation then made them pass. Final reactor verification passes 51 tests.

The previously deferred Minor finding concerned lone UTF-16 surrogates in issuer/audience, whose UTF-8 replacement could collide with `?` in the fingerprint. Task22 now reproduces and rejects these invalid operator inputs with six RED→GREEN cases while preserving valid supplementary Unicode. Production compatibility and security gates remain blocked; this is not a production-ready release.

5. Later cumulative ICE/order, current authorization/ownership, unknown-COMMIT reconciliation, stale-actor fencing, AZ capacity and revocation freshness were not counted as defects or passed gates in Tasks 1–2. They remain Tasks 3–24. Cost if wrong: overstating foundation readiness; no runtime or production claim is permitted.

## Qualification

Release status: **NOT_QUALIFIED**. There is no deployed candidate image, complete native launcher/relay path, PostgreSQL HA, production client-matrix interop, distributed load, AZ-loss, security/DR drill or 24-hour evidence. Tasks 23–24 additionally need the specified infrastructure and measured staged execution; architecture or unit tests cannot substitute for those gates.

Task 10 verification: `./mvnw -B -pl signaling-actors -am clean verify` passes **90 tests** in the selected reactor, including nine PostgreSQL/public Lease tests and two multi-node scenarios. Tests use loopback TCP with explicit test-only seed discovery; production requires cell-scoped Kubernetes API discovery and supplied mTLS contexts. The test-only full reformation has no automatic production reformation counterpart. Expected fault-injection and shutdown warnings are retained; these tests do not certify Kubernetes, PKI, shutdown deadlines, PostgreSQL HA or 10M throughput.

9. Avoid Pekko 1.1.3 Java `stateStoreModeDdata()`, which returns persistence; read the mode from config and assert its runtime name plus coordinator conversion. Cost if wrong: rerun compatibility gates after any BOM update.
10. Match the public `Address.hostPort()` owner format and observe the real shard parent via public Adapter/DeathWatch to gate and release exact placement tenures; the pinned Shard lifecycle does not itself call release on termination. This shares the existing controller and introduces no competing ownership loop. Cost if wrong: rollout remains blocked by lease lifecycle qualification.

Task 11 full-reactor verification: `./mvnw -B verify` passes **126 tests, zero failures/errors/skips**. UserActor keeps bounded queues, a fresh incarnation and one physical mutation lifecycle, and refuses uncertain replacement work. Snapshot hydration uses native primary projections; mutation APIs preserve physical completion and remaining deadline. A PostgreSQL restore-epoch regression failed first, then passed after gateway/session presence and registration were fenced against current cell authority.

Task 12 selected-reactor verification: `./mvnw -pl signaling-actors -am verify` passes **111 tests**, zero failures/errors/skips. CallActor serializes physical DB lifecycles, checks the full local tenure, rebuilds committed deadlines and gates unknown outcomes. Native workflow CAS/replay/outbox, CANCEL/ACCEPT, terminal winner preservation, lost wake repair, actor exception and actor-host recreation are exercised against PostgreSQL. SQL rejects mutations with less than 5s root slack. The 12-call healthy indexed reconciliation measurement is <=5s; it does not qualify OS/pod kill, distributed load or active-call preservation.

Tasks 13–14: native shared-call grants, signed current home proof reads, two-home workflow composition, mTLS destination/workload authorization, isolated RPC lanes, original deadlines/retries and typed actor facade are implemented. Gateway WSS, actual RS256 workers, exact Origin/no URL credentials/no compression, fragment bounds, local heartbeat wheel, per-IP/global TLS admission, independently verified native session RPC and conservative boot lifecycle pass full selected reactors. Task 14 full verification: 169 tests, zero failures/errors/skips. Task 13 final full verification: 154 tests, zero failures/errors/skips. Boot/session admission is home-cell affined; bootstrap must route users to their cell, and cross-home native boot distribution is not invented. Task 15 extends physical completion across EntityRefs and outbound delivery; external qualification remains NOT_QUALIFIED.

Task 15 full selected reactor: 181 tests, zero failures/errors/skips. Native EntityRef requests preserve independent physical receipts through logical UNKNOWN and entity death; RPC credits follow cleanup. Admission reserves safety capacity and divides entity credits across at most seven actor ingress processes, including rollout surge. Gateway decodes later control frames independently of earlier business futures, charges physical outbound writes to per-channel/aggregate bounds, and closes sustained pressure with RESYNC_REQUIRED. Full-route delivery returns WRITE_COMPLETED with ackCommitted=false. Application receipt/outbox workers remain Task 18; staged throughput and topology remain unqualified.

Task 16 full clean selected reactor: **212 tests, zero failures/errors/skips**. Ordered generation-scoped ICE cursors, hash-safe retries, bounded bidirectional 5s authorization cache, native metadata-only negotiation CAS/replay, and volatile SDP retry retention pass. Active payloads have process byte/ticket limits and idle expiry; physical cleanup holds credits through UNKNOWN. Chromium 151.0.7922.173 and native WebRTC 0.10.0 complete initial/restart rounds with two real DataChannel echoes, 18 browser ICE-agent calls and two native end-marker calls. This browser advertises no host candidates; native host candidates establish the actual connection. Loopback fixture authority does not qualify production routing/TURN/client matrix. Native two-home new-round proof composition and complete production producer wiring remain dependencies of Tasks 17/20.

Task 17 full clean selected reactor: **242 tests, zero failures/errors/skips**. Native RESUME preserves session incarnation and committed winner history while replacing the route generation. Auth-only S1 proofs keep terminal SYNC/result reads available after participation/group release. Two-home signed ACTIVE evidence gates negotiation and matching media observations; durable recovery/grace metadata survives cold hydration. Route-loss native reads and production runtime/compatibility wiring remain later-task integrations. No external qualification claim.

Task 18 full clean storage reactor: **114 tests, zero failures/errors/skips**, including ten MaintenanceWorkersIT cases. Lost hints and expired claims recover from the native outbox; verified application receipt is distinct from WRITE_COMPLETED. Worker chains wait for real cleanup before replacing SQL work. Bounded index pagination, live-origin / 24h retention, authenticated security high-water, existing-session key retirement, native route-loss reads and HMAC privacy detachment pass. Production schedulers, authenticated source adapters and private negative-proof consumers remain explicit Task 20 composition.

Task 19 clean app reactor: **63 tests, zero failures/errors/skips**. Seven telemetry contracts test redaction, production Logback formatting, enum dimensions, distinct recovery eligibility and real Prometheus control/relay histograms. Quantiles use aggregable histogram_quantile queries (Micrometer omits client-side summaries when histograms are enabled). Actual promtool3.6.0 validates the burn-rate/recovery/revocation rules. Runtime producer adapters and measured qualification remain Tasks20–24.

Task 20 deployment contract passes with pinned Helm3.19 and actual resolved amd64 CNPG1.27/PG17.6 digests. Required cross-AZ synchronous durability, private discovery/management, quorum PDB, least-privilege RBAC and bounded JVM/native resources are rendered and validated. **Native producer/runtime composition remains open** and is carried into Task22 native end-to-end fault tests, which consume all modules; no application or HA release qualification is implied by chart validation.

Task 21 full `./mvnw clean verify`: **293 tests, zero failures/errors/skips**. Actual Pekko application-phase shutdown retains DB until native release/final physical settlement, within a 63s graph; standalone gateway drain batches128/100ms within5min. Three PostgreSQL RecoveryEpochIT cases enforce external epoch high-water, bounded restore abortion, original recovery replay and native security/privacy reactivation checks. Six reviewed runbooks and executable local dry-run scripts exist. Genuine Kubernetes rolling/HA/PITR/privacy replay remains NOT_QUALIFIED; enrolled verifier/hooks and runtime startup/worker wiring stay open Task22 dependencies.

Task 22–24 continuation: full clean verification at `fb84640` passed **405 tests, zero failures/errors/skips**. The subsequent full clean suite at `53383c1` passed **407 tests, zero failures/errors/skips**, including the actual clock-source adapter and physical-close fault. The evidence verifier passed 28 Python tests. WSS now negotiates the required subprotocol. Native snapshots reject other callee jti values after winner selection. The enrolled clock adapter uses actual mTLS/Ed25519, strict bounded input and physically retained source drain; it does not measure or invent production clock quality. Worker evidence includes live gauges and monotonic counters, checked against original summaries and interval coverage.

Task 22 still requires concrete process startup, other authenticated source adapters, native relay/delivery and recovery scheduling. Task 23 still requires actual hot/security execution and approved source 10k/100k smoke runs. Task 24 still requires a pinned deployed candidate, the measured staged workload and same-candidate HA/AZ-loss/security/DR/deployment/24-hour evidence. No task completion or production qualification is inferred from local fixtures.

Latest continuation baseline at `4852362`: full clean verification passed **435 tests, zero failures/errors/skips**. The enrolled revocation adapter uses actual mTLS/Ed25519 and PostgreSQL, retains physical ownership on uncertain socket cleanup, rejects malformed/replayed source pages and keeps partial catch-up unhealthy. Its freshness matches native SQL policy. Python verifier **43 tests** now binds original worker timing and checks shared P2 source observation windows/rates. Complete launcher/security feed, native relay and actual production qualification remain open; no production evidence is inferred from these TEST_ONLY fixtures.

Latest continuation baseline at `f6f9a7a`: full clean verification passed **450 tests across 108 reports, zero failures/errors/skips** (2026-10-05T08:45:59Z). Genuine hosting compositions now expose native relay authorization. Known warm hydration overload retries after original cleanup within its original budget; volatile gateway replies reject durable ACK claims; native clock/revocation HTTP parsers reject trailing JSON. Python verifier remains **43 tests passing**. Reusable relay session proof ingress, volatile delivery and complete startup remain open; these local checks do not qualify production.

Reusable native relay proof baseline at `10722ac`: full clean verification **454 tests / 109 reports, zero failures/errors/skips** (2026-10-05T09:01:45Z); Python verifier **43 tests passing**. A separate R1 auth-only namespace preserves original native expiry and exact sender/call/round/ICE/home binding. Metadata-only READ_RELAY_PROOF uses independent home token/security/current-route validation; ordinary READ_PROOF still rejects relay frames. Warm proof cache and volatile delivery remain open.

Warm relay proof and safety receipt baseline at `514a1bf`: full clean verification **463 tests / 111 reports, zero failures/errors/skips** (2026-10-05T09:24:07Z); Python verifier **43 tests passing**. Gateway cache retains original native proof expiry, bounded single-flight credit and independent RPC cleanup; actual mTLS/native-home reads coalesce across two frames. Clock jobs retain invocation-local receipts; clock/revocation source drains recheck reentrant work. Volatile delivery, complete process startup and external qualification remain open.

Original cleanup, relay wire and workload-stop baseline at `250a7ea`: full clean verification **487 tests / 115 reports, zero failures/errors/skips** (2026-10-05T10:23:01Z); Python verifier **46 tests passing**. Setup reconciliation retries known native read contention after original cleanup within its original budget. SDP retries retain authorization/write receipts; relay shares one second end to end and exposes scoped volatile write results. Real local WSS/RS256 tests preserve native OFFER/ANSWER ICE IDs. Worker stage timing excludes cleanup and the verifier binds original UTC/monotonic stop. Complete startup/native volatile delivery and external qualification remain open; release status stays **NOT_QUALIFIED**.

Gateway route/TLS/stream baseline at `9808dc0`: full clean verification **501 tests / 120 reports, zero failures/errors/skips** (2026-10-05T14:25:09Z). Python verifier **46 tests passing**; loadgen contract passes. ACTIVE v2 seals native recipient gateway/boot; dedicated gateway relay ingress checks exact target process and original deadline. Real local TLS proves exact gateway role/cell/workload, two independent TCP relay connections and original stream cleanup. Gateway Netty writes retain original expiry/binding/physical receipts. Native coordinator cache/producer, complete startup and production qualification remain open; TEST_ONLY fixtures do not supply release evidence.

Native round cache / SDP baseline at `abb2b54`: full clean verification **510 tests / 121 reports, zero failures/errors/skips** (2026-10-05T14:58:59Z). Native cached rounds preserve source gateway/boot/expiry, committed SDP roles and original negotiation deadline; native PostgreSQL confirms repeated frames reuse the original two home reads. SDP retains the exact immutable command under physical byte credits and propagates remaining budget/original descriptor through transport/authorization adapters. Native coordinator producer, complete startup and external qualification remain open.

## Native producer and committed terminal checkpoint

At `7224113`, fresh `clean verify` passed **514 tests across 122 reports**,
zero failures, errors or skips, in 4:37, finished 2026-10-05T15:35:56Z.
NativeRelayProducer now retains original SDP receipts/physical cleanup and
forwards ICE payloads through the boot-enrolled gateway client. A deterministic
concurrency regression exposed and removed a producer/SDP callback lock cycle.
The joined PostgreSQL/EntityRef/TLS/Netty test passed all four relay kinds and
SDP retry. A real committed HANGUP regression first returned a stale successful
receipt; native actor invalidation now rejects it before acknowledging terminal
state, preserving original write cleanup.

The Python verifier passed 47 unit tests, including an ephemeral complete
12-gate parser fixture. An original TEST_ONLY drill receipt initially survived a
signed manifest; it now blocks release, as do receipt candidate binding mismatches.
The loadgen contract check also passed. These fixtures provide local verification;
complete native Main installation, loadgen skew/abuse behavior and real
staged production/HA/DR evidence remain open. Tasks 22–24 remain in progress and
release remains **NOT_QUALIFIED**.

Fresh full verification at `8c228e2` passed **528 tests in 124 fresh reports**,
zero failures, errors or skips (2026-10-05T16:13:52Z, 4m42s). The Python
verifier passed 47 tests and the static loadgen contract passed. Native cache
callbacks now finish outside cache monitors; original pending authorizations
cannot be reused after their original expiry. Actual WSS tests cover
INVALIDATED/RESYNC recovery, committed new rounds, source-seeded reconnect
jitter and original HTTP Retry-After. At that checkpoint, requested skew/security profiles failed
preflight until their drivers existed. Tasks 22–24 remain in progress; these
local checks do not qualify production release.

Fresh full verification after native ingress/transport drain passed **538 tests
in 126 fresh reports**, zero failures, errors or skips (2026-10-05T16:46:17Z,
4m46s). Python verification subsequently passed52 tests; the static loadgen
contract also passed. The joined native relay test crosses actual gateway→actor
and actor→gateway mTLS transports; original stream, channel and executor cleanup
now gate client drain. Loadgen rejects stale round/ICE frames and binds recovery
SYNC to the original committed restart version. DR verification rejects malformed
fault timelines, unfenced promotion, mixed writer/cell/drill receipts, reused
recovery epochs and foreign/pre-ACK WAL lineage. PITR loss remains explicit and
requires restored-call invalidation. These local checks leave complete Main
source enrollment, aggregate skew measurement, security drivers and genuine staged qualification open;
Tasks22–24 remain in progress and release remains **NOT_QUALIFIED**.

Fresh full verification at `b877ff7` passed **545 tests in 128 fresh reports**,
zero failures, errors or skips (2026-10-05T22:17:00Z, 4m40s). Python verification
passed 55 tests and the static loadgen contract passed. Native five-times
destination/bucket selection now drives actual WSS INVITEs while preserving
original global arrivals and request identities. Original summary/time-series
skew scope, population and counters must agree; unsupported security profiles
cannot claim ordinary traffic as an abuse run. Aggregate achieved skew, complete
Main/source enrollment, security drivers and genuine staged same-candidate
qualification remain open. Tasks 22–24 remain in progress; **NOT_QUALIFIED**.

Fresh full verification at `fffdb85` passed **551 tests in 129 fresh reports**,
zero failures, errors or skips (2026-10-05T22:38:20Z, 4m47s). Python verification
passed 60 tests and the static loadgen contract passed. Aggregate burst/skew now
requires original bound source stages and simultaneous native counter windows;
a checklist alone fails. Worker drain proves actual task/timer termination, and
staged source populations use exact integer arithmetic. Native Main, security
drivers and genuine staged qualification remain open. Required external inputs
and the remaining implementation are listed in `runbooks/qualification-inputs.md`.
Tasks 22–24 remain in progress; release remains **NOT_QUALIFIED**.

Fresh full verification at `e3630d9` passed **557 tests in 131 fresh reports**,
zero failures, errors or skips (2026-10-05T22:59:49Z, 5m02s). All Python tests
passed **63**; the native image smoke passed for all three profiles. The launcher
now refuses a configuration-only context even with valid issuer/audience and
emits bounded diagnostic codes without exception/private values. This is a
startup guard, not complete native Main assembly.

The original local build was exported to a verified OCI layout, preserving actual
Docker config/layer bytes and checking the original native config identity and
all blob hashes/sizes. Native OCI manifest digest is
`sha256:3d330291c09faa4a5a0946dcaf2bab1cc0c4d60d7900af0d93e4cb9172b93b64`;
it is distinct from the Docker image/config ID. Local candidate
`20261005-2302-e3630d9-3d330291c09f` contains only TEST_ONLY build/startup records
and an explicit **NOT_QUALIFIED** decision. Its incomplete manifest fails the
qualification verifier; no trusted signature, enrollment fingerprint, native
production gate, source inventory or measured capacity is invented. No image
was pushed or deployment performed. Complete Main, six security drivers and
genuine staged/drill qualification remain open; Tasks 22–24 remain in progress.

## Native security closure checkpoint (2026-10-06)

Source `879c5e1` passed a fresh `clean verify`: **601 tests / 136 reports**,
zero failures/errors/skips, 4m58s, finished 2026-10-06T00:38:01Z. The static
loadgen contract also passed. The six passive WSS observation tests use native
gateway handlers and explicit TEST_ONLY registration/security sources. They
verify fixed close reasons, original socket ownership and physical retirement;
they do not attest actual revocation drills or wire the six scenario drivers.
Tasks 22–24 remain open, including complete Main composition, and the release
remains **NOT_QUALIFIED**.

## Native security wire checkpoint (2026-10-05)

Source `c4958b7` passed a fresh `clean verify`: **580 tests / 134 reports**,
zero failures/errors/skips, 5m06s, finished2026-10-05T23:53:40Z. The Python
verifier/exporter suite passed **63 tests** in26.108s and the static harness
contract passed. Gateway malformed JSON emits bounded1002; native oversized
rejection retains its original1009 write instead of prematurely closing TLS.
Unknown rejection writes have bounded socket cleanup without forged write success.

Actual WSS tests cover matching original close codes, unrelated/absent closes,
original deadlines, pre-dispatch closure, local write failure followed by the
original server response, held original write completion and exhausted credits.
Security probe receipts do not enter ordinary latency/error counters. TEST_ONLY
authority remains explicit. Six scenario drivers and production source observations
are still required; **Tasks22–24 remain IN PROGRESS / NOT_QUALIFIED**.

The setup microburst keeps call and setup-frame arrivals coupled through §35
`C×S`, including their transient population. No steady-concurrency target is
doubled and no authorization/fencing/TTL rule was relaxed.

## Runtime Secret and native security scheduler checkpoint (2026-10-06)

Source `9f62896` passed fresh `clean verify`: **637 tests / 141 reports**,
zero failures/errors/skips, 5m51s, finished 2026-10-06T10:00:25Z. The static
loadgen contract passed. Mounted runtime Secrets use the existing typed
configuration namespace and reject duplicate, ambiguous and oversized input.
Actual WSS workers schedule malformed/oversized probes and retain separate
original logical/physical source counters; these probes do not enter ordinary
availability or latency denominators. Original scenario duplicate keys reject
before source initialization. Native registration, issuer and candidate fixtures
remain explicitly TEST_ONLY.

Four security scenarios remain unimplemented in the scheduler: slowConsumer,
staleGeneration, revokedJti and retiredSigningKey. Complete native Main/source/
policy composition and genuine same-candidate staged qualification remain open.
Tasks 22–24 remain **IN PROGRESS / NOT_QUALIFIED**.

## Native stale-generation scheduler checkpoint (2026-10-06)

Source `83ff76a` passed fresh `clean verify`: **642 tests / 141 reports**,
zero failures/errors/skips, 5m47s, finished 2026-10-06T10:29:34Z. The full Python
verifier/exporter passed **68 tests** and the static harness contract passed.
Three scheduler modes are implemented: malformed, oversized and staleGeneration.
Stale replacement uses the approved same-session inventory and native binding,
explicit spare local socket budget capped at16 owners, original ACK/close timing
inside5s and both original physical receipts. A late native ACK cannot defeat an
overdue timer. Ordinary replacement AUTH accounting and retained source slot
metadata are independently checked. Native fixture authority is TEST_ONLY.

slowConsumer, revokedJti and retiredSigningKey remain unsupported. Complete Main/
source/policy installation and genuine same-candidate staged/drill qualification
remain open. Tasks22–24 remain **IN PROGRESS / NOT_QUALIFIED**.

### Native slow-consumer scheduling and original observation cleanup

Four security scheduler modes are now implemented: malformed, oversized,
staleGeneration and slowConsumer. Slow scheduling owns the actual read pause,
resumes at original intended+10s and requires native1013/RESYNC_REQUIRED inside
original12s. Mode counters include verified actual pause/resume observations.
The original offered setup arrival scope and ordinary availability/HDR denominator
are unchanged. The verifier requires those original counters in every source
snapshot, with strict finite outcomes, monotonic integers and no pending final
logical/physical receipts.

Worker stop freezes new business followups and admissions. It awaits original
client and replacement logical observations before separate physical drain;
original timers are never renewed and cleanup cannot extend workload duration.
An actual two-second WSS worker retains its slow observation through original
read resume. Pressure fixtures use TEST_ONLY native writability fault injection
and the production bounded queue, not manufactured close frames or measured
production pressure/isolation.

Upstream module verification passed: loadgen128, gateway50unit/16IT, zero
failures/errors/skips (2026-10-06T11:33:30Z). Python verifier/exporter69 passed;
static harness contract passed. Whole-source verification follows this checkpoint.
revokedJti/retiredSigningKey, full Main/source/policy installation and genuine
same-candidate staged qualification remain open. Tasks22–24 are IN PROGRESS and
release remains NOT_QUALIFIED.

Fresh whole-source gate at3fcbf00: `./mvnw clean verify` passed651tests in141fresh
XML reports, zero failures/errors/skips; BUILD SUCCESS6m35, finished
2026-10-06T11:40:48Z. Python69 and static contract PASS are recorded for the same
unchanged implementation. Main/source/policy bindings, revokedJti/retiredSigningKey
and genuine staged qualification remain open. This source has no new qualified
candidate image; the historical e3630d9 TEST_ONLY OCI bundle retains its own source
and NOT_QUALIFIED decision. Tasks22–24 are not marked complete.

### Main native actor policy binding

Native session, route and home-freshness callbacks now have one durable-source
adapter, including primary/cell/storage identity, original revocation catch-up
freshness, token expiry, signing-key and scoped subject checks, with live clock
trust checked around SQL. Eight new real PostgreSQL cases cover denial and positive
receipts. Home freshness remains separate from scoped identity authorization.

Main installs the policy through actor-only Boot auto-configuration from explicit
reconciler and monitor beans. Actual Main contexts verify actor installation and
absence on gateway/control; the native four-member/mTLS/PostgreSQL fixture binds
that same bean to all actor callbacks. A demonstrated component-scan ordering bug
is fixed: later source enrollment definitions are processed before auto-configuration.
Selected21 cases passed (native policy8, native claims2, startup3, revocation7 and
joined source1). Full source verification follows. Complete plane factories,
runtime Secret enrollment/source/policy contracts, revoke/key scheduling and real
qualification remain open; Tasks22–24 IN PROGRESS/NOT_QUALIFIED.

Fresh whole source59288bf: `./mvnw clean verify` passed659tests in142fresh XML
reports, zero failures/errors/skips; BUILD SUCCESS6m30, finished
2026-10-07T03:58:44Z. The same verified executable was built into local image
`signaling-candidate:59288bf`. Actual pinned JRE/nonroot/read-only/network-none
checks and all six negative startup cases passed; no enrolled/native plane was
invented to make startup succeed.

New TEST_ONLY record `20261007-1059-59288bf-be7f7ebe585b` uses Bangkok time in its
candidate ID and retains original UTC build timestamps. Native OCI manifest is
`sha256:be7f7ebe585b3a36720c9f5b4a50728d809395b6a6fc77071a44a48dead65afb`;
Docker/config identity is separately
`sha256:bd0117e89aebf93e16b3d868f61b0a45ca69796d88eecfc5d47db52f001925ca`.
Seven original layers total404870656bytes; every descriptor hash/size and the
original executable bytes matched the fresh verified build. The verifier returns
NOT_QUALIFIED with37 blockers; all12 trusted production gates remain absent.
The historical e3630d9 source/image/decision is unchanged. Main complete factories,
source/policy enrollment, two security modes and genuine staged qualification
remain open. Tasks22–24 remain IN PROGRESS.

### Main factory continuation

Actor-only Boot factories now construct explicit native source owners, assemble
native actor backends with all three durable policies, register both regions after
actual local membership Up, and bind the internal RPC listener. The joined native
PostgreSQL/Pekko/mTLS tests obtain these components through Main; relay tests cross
both actual RPC legs before the original Netty delivery. No default issuer, trust
key, directory policy, calling permission or healthy clock is introduced.

Source enrollment copies public-key trust and validates canonical HTTPS/process
identity and the fixed100ms poll margin. Its issuer/audience/skew must match typed
configuration; freshness never exceeds the enrolled identity hard bound or5s.
The Unicode fingerprint defect now rejects lone surrogates; valid Unicode and
existing valid fingerprints retain their behavior. Selected37 configuration cases
passed after six actual regression failures. Full newer-source verification follows.

Source/actor cell, storage epoch and pod UID must agree before backend/lease
installation. A native Main regression showed mismatched source declarations
reached region startup; the new tuple guard rejects them earlier. Selected40
configuration/native cases passed, including three mismatched tuples, genuine
source polling and both mTLS relay legs. A fresh final whole-source gate follows.

One selected native relay run returned typed OUTCOME_UNKNOWN on the original1s
operation; unchanged reproduction passed. Its cause is not established and no
deadline or safety condition was weakened to conceal it. It remains an unqualified
availability observation pending original timing/qualification evidence.

Complete process formation/discovery/management, mounted source/business
enrollment, gateway/control factories and two approved scoped security drill drivers
remain open. There are no injected SIGNALING variables or supplied production
enrollments in this workspace. Actual staged load, three-AZ HA/DR, N-1 and24h
same-candidate qualification have not been collected. Tasks22–24 stay open;
the existing immutable candidate records still describe their original sources.

Fresh final source9ab4256: `./mvnw -B clean verify` passed671tests in143fresh XML reports, zero failures/errors/skips, finished2026-10-07T04:58:38Z. Both joined Main native tests and the original relay case passed. Fresh Python69 verifier/exporter tests and static loadgen contract also passed. Existing TEST_ONLY candidate verification still returns NOT_QUALIFIED37; no current-source image or production run was created. Remaining implementation and external inputs above keep Tasks22–24 open; the ledger and native originals are retained.

Task22 checkpoint, source d8c31e3 (2026-10-07T07:07:00Z): Main now constructs the
native source aggregate, bounded safety/explicit-maintenance scheduler and private
health, then installs ordered shutdown hooks before starting workers. Spring stop
initiates Pekko's original graph and observes native DB closure. The real blocked
SQL fixture proves lease/pool retention until physical work and framework handoff
settle. A mismatched scheduling ActorSystem rejects before backend/lease install.
Fresh clean verify passed671 tests in143 reports with zero failures/errors/skips;
Python69 and static loadgen contract passed. The original native relay case passed;
its earlier intermittent OUTCOME_UNKNOWN cause remains unestablished.

New immutable local TEST_ONLY candidate
`20261007-1407-d8c31e3-b5575cc39678` contains the actual native image, six negative
startup checks and original OCI/executable integrity record. Its verifier returned
NOT_QUALIFIED37. Earlier candidates were preserved. Managed process formation,
mounted enrollment, gateway/control factories and two scoped security drivers
remain software work; approved identity/calling/source/PKI/drill contracts and
qualification infrastructure are absent. No10k/100k distributed stage, three-AZ
HA/DR, N-1 or24h soak was collected. Tasks22–24 remain IN_PROGRESS/NOT_QUALIFIED.

Native client deadline continuation: four actual mTLS regression cases failed before the fix and passed after it. Original EXECUTE/RELAY/SESSION/DELIVER budgets now include admission and lazy TLS/channel construction; an expired original budget cannot dispatch a fresh stream. Selected client TLS/physical/drain checks passed 20 tests. This establishes a deadline bug, not the cause of the historical intermittent relay OUTCOME_UNKNOWN.

Gateway Main business continuation: native Spring auto-configuration now assembles boot ownership, explicit routing epoch, verified directory homes, bounded tracked relay proof cache and NativeGatewayServices. NativeGatewayCommandIT failed on missing Main services first, then both PostgreSQL/mTLS control paths passed. Invalidating the signed TEST_ONLY clock makes cached security FRESHNESS_UNKNOWN. Gateway listener/lifecycle, mounted enrollment, production security sources and external qualification remain open; Tasks22–24 stay IN_PROGRESS/NOT_QUALIFIED.

Gateway native continuation (runtime source `ec700c1`): Main now creates a lazy cell RPC client from explicit bounded topology/TLS/admission, native gateway boot/command/proof services, actual WSS and gateway relay mTLS listeners, bounded relay CPU/write ownership and private health/Spring lifecycle. Actual WSS AUTH drives the existing PostgreSQL/mTLS committed, pending and replay paths. Native shutdown sends jittered reconnect advice in batches<=128/100ms and retains client/boot/health until original transport and CPU receipts settle.

Four demonstrated runtime defects were corrected by assertion RED→GREEN tests: original RPC deadlines were reset after cold TLS/channel construction; a process-wide client monitor let cold destination/relay construction block unrelated control traffic and drain; gateway closure returned while original CPU work remained active; idle sockets waited for heartbeat/client traffic to observe cached security loss. Native tests preserve deadlines, fencing, authorization and typed close reasons. The single older intermittent relay OUTCOME_UNKNOWN cause remains unestablished; this record does not identify it with a deadline bug.

A typed one-process cached-security sweep (25–250ms, candidate100ms) closes locally revoked, expired or freshness-unknown idle WSS sockets, rechecking the exact binding/status on the event loop. Native source ingestion and the authoritative commit-to-socket production SLO remain unqualified. Managed ActorSystem/discovery/management, mounted runtime-to-enrollment/source ownership, full startup-failure cleanup, native control launcher, approved maintenance/calling-policy contracts and two scoped security drill drivers remain open. No candidate image, release gate or production measurement was fabricated or replaced in this continuation.

Fresh full verification of runtime source `ec700c1f6291017054524c70f3ded331ac836eac`: `./mvnw -B -Dmaven.repo.local=/workspace/.onboarding/m2 clean verify` finished2026-10-07T09:28:56Z in6m58 with BUILD SUCCESS. All147 XML reports were created after this run started; total683 tests, zero failures/errors/skips. The joined four-member native actor/relay test and all new Main gateway/client/safety/lifecycle cases passed. Pinned verifier-venv Python69 tests passed23.141s; loadgen static contract PASS. Fresh verification of the existing immutable `20261007-1407-d8c31e3-b5575cc39678` candidate still fails closed: NOT_QUALIFIED37, including all12 missing trusted gates. That historical image does not cover the newer runtime source; no new image/capacity/release claim is made. Tasks22–24 stay IN_PROGRESS/NOT_QUALIFIED; final completion review/workspace deletion remain pending the complete contract.

Startup/physical cleanup continuation: real SpringApplication tests reproduced accepting WSS after missing lifecycle and after private-health bind failure. Main installs an early native-owner observer and retires ingress/cache/client physical work before boot on startup failure, including owners removed from Spring singleton registry during failed refresh. One original300s budget bounds partial-owner cleanup. Private health now waits for the original metrics executor before reporting stopped; a held actual HTTP scrape failed before the fix. Selected15 unit/native cases passed2026-10-07T09:49:40Z. Gateway startup-failure cleanup is locally covered; partial actor startup, control launcher and mounted production source enrollment remain open. Full new-source verification follows; Tasks22–24 remain IN_PROGRESS/NOT_QUALIFIED.

Native control and verifier continuation: Main's explicit control enrollment now
binds actual HTTPS/WebFlux bootstrap on bounded owned loops, native directory
read/cache and private probes. Scope/global security freshness and trusted clock
are mandatory inputs. Startup without enrollment rejects before plaintext serving
and preserves configuration/startup diagnostics. Native RSA/TLS/PostgreSQL tests
keep directory epoch independent from cell storage epoch.

Demonstrated defects now fixed include delayed directory success after revocation,
expiry or source loss; control readiness staying200 after stop; bounded verifier
closure preceding its original worker; actor/gateway native stop retiring owners
before that verifier; and valid clock incorrectly masking global source staleness.
Bootstrap's64-bit directory epoch is a decimal JSON string, including versions
above2^53. Selected15 final cases passed2026-10-07T10:30:47Z. Full newer-source
verification follows. Partial actor startup, managed/mounted process and source
creation, control DB/source ownership, approved calling/maintenance and two scoped
security drivers remain software/integration gaps requiring explicit contracts.
Staged load, three-AZ HA/DR, N-1 and24h soak remain uncollected. Tasks22–24 remain
IN_PROGRESS/NOT_QUALIFIED; historical candidate/source records are unchanged.
