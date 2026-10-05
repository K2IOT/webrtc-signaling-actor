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
| 22–24 | In progress / pending | Complete native runtime composition and qualification |

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

One Minor finding remains deferred as the inline skill prescribes: config issuer/audience validation accepts lone UTF-16 surrogates, whose UTF-8 replacement can collide with `?` in the fingerprint. Reject these invalid operator inputs before production. The planned production compatibility and security gates remain blocked; this is not a production-ready release.

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
complete native Main installation, loadgen recovery/skew/abuse behavior and real
staged production/HA/DR evidence remain open. Tasks 22–24 remain in progress and
release remains **NOT_QUALIFIED**.
