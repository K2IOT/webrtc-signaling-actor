# WebRTC Signaling v1.11 Production Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement and qualify the v1.11 1:1 WebRTC signaling platform so one pinned candidate can progress from a production-standard specification to a production-qualified implementation, with an evidence path toward the declared 10M-CCU/P2 deployment envelope.

**Architecture:** Build one Maven multi-module Java 21 codebase with separate gateway, actor, and control deployment profiles. Netty owns WSS sockets, Apache Pekko Typed Cluster Sharding owns user/call entity routing and state machines, PostgreSQL is the authoritative control store, JPA/JDBC runs only behind bounded virtual-thread database admission, and direct mTLS gRPC connects cells. Implement correctness and failure semantics before scale work; every scale milestone reuses the same production code paths and effective configuration model.

**Tech Stack:** Java 21+, Spring Boot, Maven Wrapper, Netty, Apache Pekko Typed/Cluster Sharding/Management, Spring Data JPA + Hibernate + pgJDBC + HikariCP, PostgreSQL, gRPC/Protobuf, Micrometer/OpenTelemetry, Flyway, JUnit 5, jqwik, Testcontainers, Toxiproxy, JFR, Kubernetes/Helm; Redis remains optional and non-authoritative.

**Spec:** `docs/superpowers/specs/2026-10-03-webrtc-signaling-10m-design-v1.11-production.md`

## Global Constraints

- Scope is 1:1 WebRTC signaling. Group calls, SFU, recording, billing, chat history, and mobile push are outside v1.11.
- Target is 10M authenticated WSS CCU across independent cells in one three-AZ region; this is a qualification target, not an implementation claim.
- A user participates in at most one establishing/active call and has at most five online sessions.
- Authentication accepts exactly RS256; required claims are `userId`, `jti`, `exp`, `iss`, and `aud`; RSA modulus is at least 2,048 bits; initial JWT clock-skew allowance is 30s.
- Browser WSS authenticates with `AUTH {token}` within 5s; raw JWTs never appear in URLs or normal logs.
- `sessionKey=(issuer,jti)` is permanently bound to `userId`; `connectionGeneration` is monotonic and fences older sockets.
- Home routing uses 16,384 versioned user buckets. UserActor and CallActor each use 1,024 logical shards in separate namespaces; CallActor shard ID equals ownership `groupId`.
- PostgreSQL is authoritative for session registration, bucket authority, reservations, winner claims, call state, idempotency results, outbox, recovery indexes, security epochs, and group ownership.
- Redis is never required for correctness. Kafka/Scylla are outside the hot path and may only receive asynchronous optional exports.
- Netty event loops, Pekko actor/cluster dispatchers, gRPC event loops, and Reactor/WebFlux event loops must never execute JPA/JDBC or wait with `get()`/`join()`.
- Every blocking DB transaction executes as one admitted bounded virtual-thread task. Admission occurs before task creation and before connection-pool acquisition.
- Critical control ACK is emitted only after the authoritative COMMIT. Unknown COMMIT outcome is reconciled with the same operation identity; it is never converted into a new request ID.
- Critical PostgreSQL commits use `synchronous_commit=on` with an eligible cross-AZ synchronous standby; unsafe asynchronous fallback is forbidden.
- Group ownership uses the PostgreSQL-backed public Pekko Lease API. Root TTL is 15s, renewal period 5s, and acquisition is successful only after COMMIT.
- SBR baseline is `stable-after=10s`, `down-removal-margin=10s`, keep-majority for the signaling-actor role, and down-all-when-unstable enabled.
- UserActor idle passivation is 60s. Active CallActors are not idle-passivated; `remember-entities=off`; silent calls recover through durable indexed `WakeCall` reconciliation.
- Public frame limit is 80KiB; JWT 8KiB; SDP 64KiB; ICE candidate 2KiB; ICE batch at most 20 candidates/8KiB; cumulative ICE at most 256KiB/256 candidates per participant/round.
- Application actor envelope is at most 96KiB; application mailbox proposed capacity is 128 with stricter per-entity admission of 64 messages/256KiB.
- Local socket heartbeat is ping every 30s with jitter, pong deadline 10s, edge idle timeout at least 90s. Gateway boot lease renews every 5s with 15s timeout.
- Critical internal RPC deadline is 2s; relay is 1s; at most two retries fit inside the original total operation budget and only one layer owns retries.
- Critical-command latency target is p95 <=150ms and p99 <=500ms; server relay target is p95 <=50ms and p99 <=150ms. Targets are release gates, not assumptions.
- Trickle ICE is generation-scoped and presents accepted candidates/end-of-candidates to the client ICE implementation exactly once and in sender order.
- New-call authorization and immediate security revocation fail closed when required policy/revocation freshness cannot be proven.
- All queues/buffers are bounded by count, bytes, and/or age. Overload shedding protects renewal, termination, committed completions, and recovery before new INVITEs.
- Production status may not be called `production-qualified` until the exact candidate passes the v1.11 §49.9 gates; `10M-qualified` additionally requires the full P0/P2/N-1/soak evidence.

## Review Focus

1. **Unknown database outcome at the authority boundary:** a timeout after COMMIT must never produce a second winner/call/lease. Task 8 and Task 9 add before-COMMIT/after-COMMIT fault tests and result reconciliation.
2. **Paused/stale actor after ownership takeover:** an old actor callback must never mutate current state. Tasks 9, 12, and 22 test epoch/incarnation fencing, lease loss, pause-past-expiry, and late callbacks.
3. **Trickle ICE reordering/duplication across reconnect/cross-cell retry:** the client ICE boundary must observe each accepted item once and in order. Task 16 tests gaps, duplicate batches, old generations, end marker ordering, and unknown delivery outcomes.
4. **AZ loss while P2 work, renewals, cleanup, and reconnect run simultaneously:** safety-critical work must retain capacity while new setup sheds explicitly. Tasks 20, 23, and 24 exercise the complete mixed workload rather than isolated benchmarks.
5. **Revocation stream lag or missed event:** a revoked session must not remain silently authorized beyond the hard freshness bound. Tasks 3, 18, and 22 test durable epochs, high-water reconciliation, stale-cache fail-closed behavior, and socket closure.

---

## Plan Decomposition and Delivery Order

This specification spans several independently testable subsystems. Execute the tasks as eight vertical milestones; do not parallelize tasks that share an authority interface until the producer task is committed and reviewed.

| Milestone | Tasks | Runnable outcome |
|---|---|---|
| M0 Foundation | 1-4 | Buildable release, protocol/auth/bootstrap contracts, no call control yet |
| M1 Durable authority | 5-9 | PostgreSQL safety model, sessions/reservations/calls/outbox/lease adapter |
| M2 Actor control plane | 10-13 | Single-cell and two-cell typed actor control path with durable recovery |
| M3 Transport + WebRTC relay | 14-17 | End-to-end authenticated WSS 1:1 call signaling with reconnect/ICE restart |
| M4 Maintenance + observability | 18-19 | Bounded recovery/retention/privacy workers and complete SLI telemetry |
| M5 Production deployment | 20-21 | Three-AZ manifests, HA, rollout/rollback/DR runbooks and executable drills |
| M6 Correctness/fault qualification | 22 | Model/property/integration/chaos suite with zero invariant violations |
| M7 Capacity certification | 23-24 | Reproducible P0/P2/N-1/24h evidence bundle and release decision |

## Repository and File Map

Use root Java package `io.webrtc.signaling` consistently for this implementation plan.

```text
pom.xml
.mvn/
mvnw
mvnw.cmd
config/
  compatibility-manifest.yaml
  production-defaults.yaml
  schema/signaling-config.schema.json
signaling-protocol/
signaling-auth/
signaling-storage/
signaling-cell-rpc/
signaling-actors/
signaling-gateway/
signaling-control-api/
signaling-observability/
signaling-app/
signaling-integration-tests/
signaling-loadgen/
qualification/
  loadgen/
  scenarios/
  evidence/
deploy/
  helm/signaling/
  postgres/
  directory/
  network-policy/
runbooks/
```

`signaling-app` is the executable Spring Boot launcher. Profiles `gateway`, `actor`, and `control` activate only their plane; production deploys them in separate JVMs/pods from the same release version. Library modules expose narrow interfaces and contain no hidden Spring global state.

---

### Task 1: Bootstrap the Multi-Module Build, Compatibility Manifest, and Typed Configuration

**Files:**
- Create: `pom.xml`, `.mvn/wrapper/maven-wrapper.properties`, `mvnw`, `mvnw.cmd`
- Create: `config/compatibility-manifest.yaml`, `config/production-defaults.yaml`, `config/schema/signaling-config.schema.json`
- Create: `signaling-app/pom.xml`
- Create: `signaling-app/src/main/java/io/webrtc/signaling/app/SignalingApplication.java`
- Create: `signaling-app/src/main/java/io/webrtc/signaling/app/config/SignalingProperties.java`
- Test: `signaling-app/src/test/java/io/webrtc/signaling/app/config/SignalingPropertiesTest.java`

**Interfaces:**
- Consumes: v1.11 global constants and compatibility requirements.
- Produces: `SignalingProperties`, immutable config fingerprint, Maven reactor modules, deployment profiles `gateway|actor|control`.

- [ ] **Step 1: Write the failing configuration tests** asserting startup rejects lease renewal >= lease TTL, negative queue sizes, unsupported protocol versions, missing issuer/audience, and inconsistent SBR/termination settings; assert the default file contains 15s/5s group lease, 10s/10s SBR, 5s AUTH timeout, 80KiB frame limit, and 90s actor termination grace.
- [ ] **Step 2: Run** `./mvnw -pl signaling-app -am test -Dtest=SignalingPropertiesTest` **Expected:** FAIL because the reactor/config types do not exist.
- [ ] **Step 3: Create the Maven reactor and typed configuration**. Pin exact compatible dependency/container/controller versions in `compatibility-manifest.yaml`; no `LATEST`, ranges, or floating image tags. Make config validation fail startup rather than silently accepting unsafe library defaults.
- [ ] **Step 4: Run** `./mvnw -pl signaling-app -am test -Dtest=SignalingPropertiesTest` **Expected:** PASS.
- [ ] **Step 5: Run** `./mvnw -q -DskipTests package` **Expected:** reactor builds every empty/scaffolded module successfully.
- [ ] **Step 6: Commit** `chore: bootstrap signaling production build and config`.

### Task 2: Define Public Protocol, Internal Protobuf, Identity Types, and Compatibility Rules

**Files:**
- Create: `signaling-protocol/src/main/proto/signaling_internal.proto`
- Create: `signaling-protocol/src/main/java/io/webrtc/signaling/protocol/Identity.java`
- Create: `signaling-protocol/src/main/java/io/webrtc/signaling/protocol/CallCommand.java`
- Create: `signaling-protocol/src/main/java/io/webrtc/signaling/protocol/SignalEnvelope.java`
- Create: `signaling-protocol/src/main/java/io/webrtc/signaling/protocol/ErrorCode.java`
- Create: `signaling-protocol/src/main/java/io/webrtc/signaling/protocol/ProtocolValidator.java`
- Test: `signaling-protocol/src/test/java/io/webrtc/signaling/protocol/ProtocolContractTest.java`
- Test: `signaling-protocol/src/test/java/io/webrtc/signaling/protocol/CompatibilityTest.java`

**Interfaces:**
- Consumes: `SignalingProperties` protocol limits.
- Produces: `SessionKey`, `SessionIncarnation`, `CallId`, `CallVersion`, `NegotiationId`, `IceGeneration`, `RequestId`, `CommandScope`, typed commands/events/errors, generated gRPC DTOs.

- [ ] **Step 1: Write failing protocol tests** for bounded identifiers, server-derived command scope, client inability to select coordinator/group epoch, 80KiB frame rejection, 64KiB SDP, 2KiB candidate, 20-candidate/8KiB ICE batch, payload-hash idempotency conflict, and N/N-1 decode compatibility.
- [ ] **Step 2: Run** `./mvnw -pl signaling-protocol test` **Expected:** FAIL on missing protocol types/validators.
- [ ] **Step 3: Implement immutable protocol records and Protobuf schemas**. Keep public `webrtc-signaling.v1` JSON semantics and internal Protobuf separate; prohibit Java native serialization.
- [ ] **Step 4: Run** `./mvnw -pl signaling-protocol test` **Expected:** PASS.
- [ ] **Step 5: Commit** `feat: define signaling protocol and compatibility contracts`.

### Task 3: Implement RS256 Authentication, Calling Authorization, and Durable Revocation Contract

**Files:**
- Create: `signaling-auth/src/main/java/io/webrtc/signaling/auth/TokenVerifier.java`
- Create: `signaling-auth/src/main/java/io/webrtc/signaling/auth/Rs256TokenVerifier.java`
- Create: `signaling-auth/src/main/java/io/webrtc/signaling/auth/CallAuthorizationPolicy.java`
- Create: `signaling-auth/src/main/java/io/webrtc/signaling/auth/RevocationState.java`
- Create: `signaling-auth/src/main/java/io/webrtc/signaling/auth/RevocationConsumer.java`
- Create: `signaling-auth/src/main/resources/identity-security-contract.yaml`
- Test: `signaling-auth/src/test/java/io/webrtc/signaling/auth/Rs256TokenVerifierTest.java`
- Test: `signaling-auth/src/test/java/io/webrtc/signaling/auth/AuthorizationRevocationTest.java`

**Interfaces:**
- Produces: `AuthPrincipal validate(String compactJwt, Instant now)`, `CompletionStage<AuthorizationDecision> authorize(CallAuthorizationRequest request)`, `AuthorizationStatus checkRevocation(AuthPrincipal principal, Instant now)`.
- Consumes later: gateway AUTH, bootstrap API, INVITE authorization, refresh, security reconciliation.

- [ ] **Step 1: Write failing JWT tests** covering valid RS256, wrong key, altered payload/signature, expired/nbf/iss/aud, missing userId/jti, `none`/HS256/PS256/ES256 rejection, unknown-kid single-flight behavior, >=2048-bit RSA requirement, key overlap, and retired-key rejection.
- [ ] **Step 2: Write failing authorization/revocation tests** asserting deny/unknown fail closed, old security epochs cannot resurrect access, duplicate revocation is idempotent, stale freshness rejects AUTH/new critical commands, and a revoked bound session is marked for typed closure. The fixture `identity-security-contract.yaml` must contain the identity platform's concrete `revocationPropagationSLO`, `hardSafetyBound`, maximum JWT lifetime, issuer, audience, jti-refresh behavior, and revocation high-water source; Task 3 is blocked rather than inventing those values if the identity contract cannot supply them.
- [ ] **Step 3: Run** `./mvnw -pl signaling-auth -am test` **Expected:** FAIL.
- [ ] **Step 4: Implement verifier/policy/revocation interfaces** with bounded verification execution and redacted errors. Do not perform per-frame JWT verification or network introspection.
- [ ] **Step 5: Run** `./mvnw -pl signaling-auth -am test` **Expected:** PASS.
- [ ] **Step 6: Commit** `feat: add RS256 authorization and revocation contracts`.

### Task 4: Implement Versioned Directory and Bootstrap Control API

**Files:**
- Create: `signaling-control-api/src/main/java/io/webrtc/signaling/control/DirectoryService.java`
- Create: `signaling-control-api/src/main/java/io/webrtc/signaling/control/DirectoryRepository.java`
- Create: `signaling-control-api/src/main/resources/db/migration/V001__directory_authority.sql`
- Create: `signaling-control-api/src/main/java/io/webrtc/signaling/control/BootstrapController.java`
- Create: `signaling-control-api/src/main/java/io/webrtc/signaling/control/BucketHasher.java`
- Create: `signaling-control-api/src/main/java/io/webrtc/signaling/control/BucketTransferService.java`
- Test: `signaling-control-api/src/test/java/io/webrtc/signaling/control/DirectoryBootstrapTest.java`

**Interfaces:**
- Produces: `HomeRoute resolveHome(UserId userId)`, `BootstrapResponse bootstrap(AuthPrincipal principal)`, durable directory CAS/freeze/publish operations, stable `bucket=stableHash(canonicalUserId)%16384` vectors.
- Consumes later: gateway route validation, cross-cell RPC, migration/drain tooling.

- [ ] **Step 1: Write failing tests** for stable hash vectors, 16,384 buckets, stale cached mapping rejection by local authority, 30s healthy cache refresh ceiling, `WRONG_CELL`, directory outage behavior for existing ACTIVE buckets, and freeze-before-transfer semantics.
- [ ] **Step 2: Run** `./mvnw -pl signaling-control-api -am test -Dtest=DirectoryBootstrapTest` **Expected:** FAIL.
- [ ] **Step 3: Implement the directory abstraction and bootstrap endpoint**; do not use `hash(userId)%liveCellCount` and do not use Redis as authority.
- [ ] **Step 4: Run** the same test command **Expected:** PASS.
- [ ] **Step 5: Commit** `feat: add stable home directory and bootstrap API`.

### Task 5: Create PostgreSQL Schema, Constraints, Native Authority Queries, and DbBoundary

**Files:**
- Create: `signaling-storage/src/main/resources/db/migration/V001__authority_schema.sql`
- Create: `signaling-storage/src/main/resources/db/migration/V002__authority_indexes.sql`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/DbBoundary.java`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/DbAdmission.java`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/AuthoritySql.java`
- Test: `signaling-storage/src/test/java/io/webrtc/signaling/storage/SchemaInvariantIT.java`
- Test: `signaling-storage/src/test/java/io/webrtc/signaling/storage/DbBoundaryTest.java`

**Interfaces:**
- Produces: `<T> CompletionStage<T> submit(DbClass clazz, Duration budget, Callable<T> tx)`, separate `CRITICAL`, `RENEWAL`, `NORMAL`, `MAINTENANCE` admission classes, schema for cell/bucket authority, gateway/session, user guard/reservation/home participation, call, command result, outbox, group owner, active-call index, security epoch.

- [ ] **Step 1: Write failing Testcontainers tests** for unique/foreign/check constraints, monotonic generation/version/epoch guards, command-scope uniqueness, group IDs 0-1023, no resurrection after absorbing release, and schema migration from empty DB.
- [ ] **Step 2: Write failing DbBoundary tests** asserting admission occurs before VT/task creation and connection acquisition; saturation returns typed overload; no JPA entity/proxy escapes the boundary; cancellation/timeout is observable.
- [ ] **Step 3: Run** `./mvnw -pl signaling-storage -am verify` **Expected:** FAIL.
- [ ] **Step 4: Implement Flyway schema and bounded virtual-thread boundary**. Use explicit native SQL/projections for fencing/CAS/high-churn paths; keep transactions short and entirely inside one VT task.
- [ ] **Step 5: Run** `./mvnw -pl signaling-storage -am verify` **Expected:** PASS.
- [ ] **Step 6: Commit** `feat: establish PostgreSQL authority schema and db boundary`.

### Task 6: Implement Gateway Boot Lease and Session Registry

**Files:**
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/SessionRepository.java`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/GatewayLeaseRepository.java`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/SessionRegistryService.java`
- Test: `signaling-storage/src/test/java/io/webrtc/signaling/storage/SessionRegistryIT.java`

**Interfaces:**
- Produces: `registerSession`, `refreshSession`, `closeSessionIfGeneration`, `renewGatewayBoot`, `lookupLiveRoutes` with immutable DTOs.
- Consumes: `DbBoundary`, bucket authority, `SessionKey` and `connectionGeneration`.

- [ ] **Step 1: Write failing race tests** for concurrent reconnect, highest generation wins, old disconnect cannot close new generation, jti/user rebinding rejection, five-session limit, boot expiry irreversibility, paused old gateway late renewal, and batch renewal 5s/15s semantics.
- [ ] **Step 2: Run** `./mvnw -pl signaling-storage -am verify -Dtest=SessionRegistryIT` **Expected:** FAIL.
- [ ] **Step 3: Implement guarded transactions** using fresh READ COMMITTED statements after barriers; never use secondary/replica reads for authority decisions.
- [ ] **Step 4: Run** the same test command **Expected:** PASS.
- [ ] **Step 5: Commit** `feat: add fenced session registry and gateway leases`.

### Task 7: Implement User Reservation, Home Participation History, and Winner Claim

**Files:**
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/UserReservationService.java`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/HomeParticipationService.java`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/AcceptWinnerService.java`
- Test: `signaling-storage/src/test/java/io/webrtc/signaling/storage/UserReservationRaceIT.java`

**Interfaces:**
- Produces: `reserveUser`, `renewReservation`, `releaseIfCallVersion`, `claimAccept`, `queryParticipation`.
- Consumes later: UserActor and cross-cell saga.

- [ ] **Step 1: Write failing tests** for reciprocal A->B/B->A INVITEs, caller-first sorted lock order, 100 simultaneous accepts across five sessions, immutable first committed winner, ACCEPT vs CANCEL/timeout, Release-before-Reserve, delayed old Reserve, stale reservation replacement, and participant TTL expiry.
- [ ] **Step 2: Run** `./mvnw -pl signaling-storage -am verify -Dtest=UserReservationRaceIT` **Expected:** FAIL.
- [ ] **Step 3: Implement atomic home transactions and absorbing history/tombstones**. No check-then-act sequence may cross transaction boundaries.
- [ ] **Step 4: Run** the same test command **Expected:** PASS.
- [ ] **Step 5: Commit** `feat: implement durable reservation and accept arbitration`.

### Task 8: Implement Call Authority, Scoped Idempotency, Outbox, and Unknown-Outcome Recovery

**Files:**
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/CallCommandService.java`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/CommandResultRepository.java`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/OutboxRepository.java`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/CallSnapshotRepository.java`
- Test: `signaling-storage/src/test/java/io/webrtc/signaling/storage/CallCommandIT.java`

**Interfaces:**
- Produces: `executeCallCommand(CallCommand)`, `getCommandResult(CommandScope, RequestId)`, `loadCallSnapshot(CallId)`, `claimOutboxBatch`.
- Guarantees: post-COMMIT result/ACK, monotonic `callVersion`, terminal absorption, payload-hash conflict detection.

- [ ] **Step 1: Write failing tests** for duplicate INVITE, same requestId/different payload conflict, same requestId in different scopes, timeout before COMMIT, timeout after COMMIT, ACK loss, retry returning original result, stale callVersion, and terminal-state non-reversal.
- [ ] **Step 2: Run** `./mvnw -pl signaling-storage -am verify -Dtest=CallCommandIT` **Expected:** FAIL.
- [ ] **Step 3: Implement transaction-scoped barriers/CAS, command result, call mutation, and outbox in one authority transaction**. External RPC is forbidden while transaction locks are held.
- [ ] **Step 4: Run** the same test command **Expected:** PASS.
- [ ] **Step 5: Commit** `feat: add call authority idempotency and transactional outbox`.

### Task 9: Implement `group_owner` and PostgreSQL-Backed Pekko Lease Adapter

**Files:**
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/GroupOwnerRepository.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/lease/PostgresShardLease.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/lease/PostgresShardLeaseProvider.java`
- Test: `signaling-actors/src/test/java/io/webrtc/signaling/actors/lease/PostgresShardLeaseContractIT.java`

**Interfaces:**
- Produces: public Pekko `Lease` implementation for `SignalingCallV1` only; ownership token `(cell/storageEpoch, ownershipHashVersion, groupId, groupEpoch, ownerIncarnation)` and monotonic `leaseSequence`.
- Consumes: `DbBoundary`, `GroupOwnerRepository`, 15s TTL/5s renewal.

- [ ] **Step 1: Write failing lease contract tests** for acquire, duplicate acquire, release, late release, ordered renewal, duplicate/out-of-order pulse, timeout before/after COMMIT, cancellation, pause past expiry, takeover, old callback, shard handoff, process crash, full-cell restart, and PostgreSQL failover.
- [ ] **Step 2: Run** `./mvnw -pl signaling-actors -am verify -Dtest=PostgresShardLeaseContractIT` **Expected:** FAIL.
- [ ] **Step 3: Implement the adapter only through Pekko public Lease APIs**. Acquisition succeeds only after authoritative COMMIT; unknown DB outcome remains unknown until queried; a local callback cannot create ownership.
- [ ] **Step 4: Run** the same test command **Expected:** PASS.
- [ ] **Step 5: Commit** `feat: add fenced PostgreSQL Pekko shard lease`.

### Task 10: Configure Pekko Cluster Bootstrap, Sharding, DData, SBR, and Readiness

**Files:**
- Create: `signaling-actors/src/main/resources/application-actor.conf`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/cluster/ShardingBootstrap.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/cluster/UserShardExtractor.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/cluster/CallShardExtractor.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/cluster/ClusterReadiness.java`
- Test: `signaling-actors/src/test/java/io/webrtc/signaling/actors/cluster/ShardingConfigurationTest.java`
- Test: `signaling-actors/src/test/java/io/webrtc/signaling/actors/cluster/MultiNodeShardingIT.java`

**Interfaces:**
- Produces: entity type keys `SignalingUserV1`, `SignalingCallV1`; 1,024-shard extractors; actor-pod gRPC ingress readiness; config fingerprint.

- [ ] **Step 1: Write failing effective-config tests** asserting `ddata`, `remember-entities=off`, exact type names, role `signaling-actor`, SBR 10s/10s, no UserActor lease, CallActor lease enabled, no Java serialization, bounded remoting/frame settings, and no cross-cell discovery.
- [ ] **Step 2: Write failing multi-node tests** for warm route, cold route, coordinator restart, shard relocation, incompatible fingerprint rejection, and join/readiness separation.
- [ ] **Step 3: Run** `./mvnw -pl signaling-actors -am verify` **Expected:** FAIL.
- [ ] **Step 4: Implement cluster bootstrap/sharding/readiness** with Kubernetes API discovery and PodIP member addresses; business readiness becomes true only after dependencies and entity registration are ready.
- [ ] **Step 5: Run** the same command **Expected:** PASS.
- [ ] **Step 6: Commit** `feat: configure cell-scoped Pekko cluster sharding`.

### Task 11: Implement `UserActor` as the Serialized User-Side State Machine

**Files:**
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/user/UserActor.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/user/UserCommand.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/user/UserState.java`
- Test: `signaling-actors/src/test/java/io/webrtc/signaling/actors/user/UserActorTest.java`

**Interfaces:**
- Consumes: session registry, reservation/winner services, immutable DB completions.
- Produces: typed reserve/accept/release/session-route responses; 60s idle passivation.

- [ ] **Step 1: Write failing actor tests** for up to five routes, stale generation rejection, serialized reservation operations, immutable winner response, completion-after-passivation handling, and 60s idle passivation without persistent socket actors.
- [ ] **Step 2: Run** `./mvnw -pl signaling-actors -am test -Dtest=UserActorTest` **Expected:** FAIL.
- [ ] **Step 3: Implement behavior with asynchronous DbBoundary completions only**; never block the actor dispatcher or return JPA entities.
- [ ] **Step 4: Run** the same test command **Expected:** PASS.
- [ ] **Step 5: Commit** `feat: implement sharded user actor`.

### Task 12: Implement `CallActor`, Durable Timers, Recovery, and Group Fencing

**Files:**
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/call/CallActor.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/call/CallCommandHandler.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/call/CallRecoveryService.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/call/DurableDeadlineScheduler.java`
- Test: `signaling-actors/src/test/java/io/webrtc/signaling/actors/call/CallActorStateMachineTest.java`
- Test: `signaling-actors/src/test/java/io/webrtc/signaling/actors/call/CallRecoveryIT.java`

**Interfaces:**
- Consumes: `executeCallCommand`, call snapshot, group lease token, participant proofs.
- Produces: INVITE/RINGING/ACCEPTED_PENDING_ACTIVATION/ESTABLISHED/terminal transitions, `WakeCall(CallId)`, durable timer reconstruction.

- [ ] **Step 1: Write failing state-machine/property tests** for every legal transition, terminal absorption, stale callVersion, CANCEL vs ACCEPT, timeout, activation, duplicate command, and no actor-local state granting authority without DB proof.
- [ ] **Step 2: Write failing recovery tests** for entity exception, pod kill, lost wake hint, no client traffic, late old-incarnation callback, expired reservation safe terminalization, and complete <=5s healthy active-index reconciliation target as a measurable assertion in the harness.
- [ ] **Step 3: Run** `./mvnw -pl signaling-actors -am verify` **Expected:** FAIL.
- [ ] **Step 4: Implement actor behavior/recovery**. Allocate a fresh actor incarnation after recreation, hydrate committed DTOs, rebuild durable deadlines, and validate full group token before mutations.
- [ ] **Step 5: Run** the same command **Expected:** PASS.
- [ ] **Step 6: Commit** `feat: implement fenced call actor and proactive recovery`.

### Task 13: Implement Authenticated Cross-Cell gRPC, Proofs, Credits, and Saga

**Files:**
- Create: `signaling-cell-rpc/src/main/java/io/webrtc/signaling/rpc/CellRpcServer.java`
- Create: `signaling-cell-rpc/src/main/java/io/webrtc/signaling/rpc/CellRpcClient.java`
- Create: `signaling-cell-rpc/src/main/java/io/webrtc/signaling/rpc/RpcAdmission.java`
- Create: `signaling-cell-rpc/src/main/java/io/webrtc/signaling/rpc/HomeAuthorizationProof.java`
- Create: `signaling-cell-rpc/src/main/java/io/webrtc/signaling/rpc/CrossCellSaga.java`
- Test: `signaling-cell-rpc/src/test/java/io/webrtc/signaling/rpc/CrossCellSagaIT.java`

**Interfaces:**
- Produces: authenticated `ReserveUser`, `ClaimAccept`, `ReleaseIfCallVersion`, `DeliverControlEvent`, `RelayNegotiation`, `SyncCall`; small lazy channel pools per destination cell.
- Consumes: directory routes, actor EntityRefs, original operation deadline/request identity.

- [ ] **Step 1: Write failing tests** for mTLS identity/operation authorization, wrong destination, 2s control/1s relay deadlines, max two retries inside original budget, retry ownership at one layer, Release-before-Reserve, home winner then coordinator timeout, duplicated proof, stale proof, and isolated control/relay connection pools.
- [ ] **Step 2: Run** `./mvnw -pl signaling-cell-rpc -am verify` **Expected:** FAIL.
- [ ] **Step 3: Implement direct destination-cell RPC and saga**; never hold a DB transaction while performing network RPC.
- [ ] **Step 4: Run** the same command **Expected:** PASS.
- [ ] **Step 5: Commit** `feat: add bounded cross-cell signaling saga`.

### Task 14: Implement Netty WSS Gateway, AUTH State, Heartbeat, and Edge Admission

**Files:**
- Create: `signaling-gateway/src/main/java/io/webrtc/signaling/gateway/GatewayServer.java`
- Create: `signaling-gateway/src/main/java/io/webrtc/signaling/gateway/AuthHandler.java`
- Create: `signaling-gateway/src/main/java/io/webrtc/signaling/gateway/ConnectionRegistry.java`
- Create: `signaling-gateway/src/main/java/io/webrtc/signaling/gateway/FrameAdmissionHandler.java`
- Create: `signaling-gateway/src/main/java/io/webrtc/signaling/gateway/HeartbeatHandler.java`
- Test: `signaling-gateway/src/test/java/io/webrtc/signaling/gateway/GatewayProtocolIT.java`

**Interfaces:**
- Produces: local `connectionId -> Channel` registry, authenticated session context, bounded frame admission, gateway boot identity.
- Consumes: TokenVerifier, revocation state, session registry, home route validation, actor ingress.

- [ ] **Step 1: Write failing embedded-channel/integration tests** for WSS-only, exact browser Origin allowlist, native policy, AUTH within 5s, pre-auth AUTH/close-only behavior, URL-token rejection, 80KiB aggregate frame bound, fragmented input bound, permessage-deflate disabled, heartbeat 30s/10s, stale generation closure, and event-loop blocking detector.
- [ ] **Step 2: Run** `./mvnw -pl signaling-gateway -am verify` **Expected:** FAIL.
- [ ] **Step 3: Implement Netty pipeline and local heartbeat**; never send ping/pong to actor/database and never perform JDBC/JPA on the event loop.
- [ ] **Step 4: Run** the same command **Expected:** PASS.
- [ ] **Step 5: Commit** `feat: implement bounded authenticated WSS gateway`.

### Task 15: Implement Outbound Delivery, Slow-Consumer Isolation, and End-to-End Backpressure

**Files:**
- Create: `signaling-gateway/src/main/java/io/webrtc/signaling/gateway/OutboundQueue.java`
- Create: `signaling-gateway/src/main/java/io/webrtc/signaling/gateway/GatewayDeliveryStream.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/admission/EntityAdmission.java`
- Create: `signaling-cell-rpc/src/main/java/io/webrtc/signaling/rpc/DeliveryCreditController.java`
- Test: `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/BackpressureIT.java`

**Interfaces:**
- Produces: bounded count/byte/age credits from client socket through gateway/RPC/entity/DB completion path; typed `OVERLOADED`/`RESYNC_REQUIRED` outcomes.

- [ ] **Step 1: Write failing tests** for one slow client, mailbox saturation, RPC credit exhaustion, delayed DB completion, reserved completion capacity, oversized relay, and overload priority that preserves renewal/termination while shedding new INVITEs.
- [ ] **Step 2: Run** `./mvnw -pl signaling-integration-tests -am verify -Dtest=BackpressureIT` **Expected:** FAIL.
- [ ] **Step 3: Implement bounded queues/credits**. No unbounded stash, executor queue, gRPC pending stream, or retry queue is permitted.
- [ ] **Step 4: Run** the same command **Expected:** PASS with asserted queue maxima.
- [ ] **Step 5: Commit** `feat: enforce end-to-end signaling backpressure`.

### Task 16: Implement Offer/Answer Relay and Ordered Generation-Scoped Trickle ICE

**Files:**
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/relay/NegotiationRelay.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/relay/IceReceiveWindow.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/relay/RelayAuthorizationCache.java`
- Test: `signaling-actors/src/test/java/io/webrtc/signaling/actors/relay/TrickleIceContractTest.java`
- Test: `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/WebRtcInteropIT.java`

**Interfaces:**
- Produces: ordered relay keyed by `(callId, negotiationId, iceGeneration, senderSessionIncarnation)` with monotonic `candidateSequence`, highest-contiguous ACK and bounded missing ranges.

- [ ] **Step 1: Write failing contract tests** for duplicate batches, missing middle sequence, end marker before delayed candidate, no candidate after delivered end marker, reconnect during gap, simultaneous ICE restart, old-generation replay, reordered cross-cell relay, gateway retry after unknown delivery, and exactly-once/in-order calls at the local ICE boundary.
- [ ] **Step 2: Run** `./mvnw -pl signaling-actors -am test -Dtest=TrickleIceContractTest` **Expected:** FAIL.
- [ ] **Step 3: Implement bounded receive/reorder window and relay authorization cache**; relay cache age <=5s and never beyond token/reservation/group authority expiry. SDP/ICE payloads are not durably journaled.
- [ ] **Step 4: Run** the contract test **Expected:** PASS.
- [ ] **Step 5: Run** browser/native interoperability scenario in `WebRtcInteropIT` **Expected:** offer/answer, trickle ICE, restart, and end marker complete without duplicate ICE-agent calls.
- [ ] **Step 6: Commit** `feat: add ordered trickle ICE and negotiation relay`.

### Task 17: Implement RESUME/SYNC_CALL, Signaling Reattachment, and Media/ICE Restart Semantics

**Files:**
- Create: `signaling-gateway/src/main/java/io/webrtc/signaling/gateway/ResumeHandler.java`
- Create: `signaling-actors/src/main/java/io/webrtc/signaling/actors/call/MediaRecoveryPolicy.java`
- Create: `signaling-protocol/src/main/java/io/webrtc/signaling/protocol/MediaTelemetry.java`
- Test: `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/ReconnectMediaRecoveryIT.java`

**Interfaces:**
- Produces: `RESUME`, `SYNC_CALL`, `MEDIA_CONNECTED`, `MEDIA_DISCONNECTED`, `ICE_RESTARTING`, `MEDIA_FAILED`, `MEDIA_RECOVERED`; one active restart round per call.

- [ ] **Step 1: Write failing tests** for old disconnect after reconnect, gateway loss with healthy P2P media, reconnect without PeerConnection reset, volatile SDP loss returning `RESYNC_REQUIRED`, one active restart round, old ICE generation rejection, and bounded terminalization after configured restart attempts/window.
- [ ] **Step 2: Run** `./mvnw -pl signaling-integration-tests -am verify -Dtest=ReconnectMediaRecoveryIT` **Expected:** FAIL.
- [ ] **Step 3: Implement snapshot-based resume and media recovery policy**; media observations never become ownership authority.
- [ ] **Step 4: Run** the same command **Expected:** PASS.
- [ ] **Step 5: Commit** `feat: implement reconnect sync and media recovery semantics`.

### Task 18: Implement Outbox Dispatch, Reconciliation, Retention, Revocation Catch-Up, and Privacy Deletion Workers

**Files:**
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/worker/OutboxDispatcher.java`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/worker/RecoveryScanner.java`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/worker/RetentionWorker.java`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/worker/RevocationReconciler.java`
- Create: `signaling-storage/src/main/java/io/webrtc/signaling/storage/worker/PrivacyDeletionWorker.java`
- Test: `signaling-storage/src/test/java/io/webrtc/signaling/storage/worker/MaintenanceWorkersIT.java`

**Interfaces:**
- Produces: SKIP-LOCKED worker claims where absence is not authority evidence, resumable cursors, bounded batches, post-commit dispatch hint, oldest-unreconciled metrics.

- [ ] **Step 1: Write failing tests** for duplicate outbox dispatch, lost immediate hint, stable pagination under concurrent inserts, 5s healthy active-index sweep target as a measured metric, revocation high-water catch-up, 24h command-result retention, deletion preserving opaque safety tombstones, and maintenance starvation prevention.
- [ ] **Step 2: Run** `./mvnw -pl signaling-storage -am verify -Dtest=MaintenanceWorkersIT` **Expected:** FAIL.
- [ ] **Step 3: Implement bounded workers with separate maintenance admission**. No deletion/cleanup worker may starve renewal/control.
- [ ] **Step 4: Run** the same command **Expected:** PASS.
- [ ] **Step 5: Commit** `feat: add bounded recovery retention and privacy workers`.

### Task 19: Implement Metrics, Tracing, Redaction, SLOs, and Production Dashboards

**Files:**
- Create: `signaling-observability/src/main/java/io/webrtc/signaling/observability/SignalingMetrics.java`
- Create: `signaling-observability/src/main/java/io/webrtc/signaling/observability/TraceContext.java`
- Create: `signaling-observability/src/main/java/io/webrtc/signaling/observability/SensitiveDataRedactor.java`
- Create: `deploy/helm/signaling/dashboards/signaling-overview.json`
- Create: `deploy/helm/signaling/alerts/signaling-rules.yaml`
- Test: `signaling-observability/src/test/java/io/webrtc/signaling/observability/TelemetryContractTest.java`

**Interfaces:**
- Produces: operation latency, relay latency, queue age, DB pool, WAL, lease/revocation/recovery lag, workflow convergence, active-call preservation, signaling reattachment, media continuity, burn-rate, build/config fingerprint metrics.

- [ ] **Step 1: Write failing telemetry tests** asserting no raw JWT/SDP/ICE/TURN credential in logs/traces, bounded metric cardinality, separate business outcome vs service failure counters, p95/p99/p99.9 histogram availability, and four distinct recovery SLIs.
- [ ] **Step 2: Run** `./mvnw -pl signaling-observability -am test` **Expected:** FAIL.
- [ ] **Step 3: Implement instrumentation and dashboards** with fast/slow burn alerts and no userId/callId high-cardinality labels.
- [ ] **Step 4: Run** the same command **Expected:** PASS.
- [ ] **Step 5: Commit** `feat: add signaling production observability`.

### Task 20: Build Kubernetes/Helm Deployment, Network Isolation, PostgreSQL HA, and Cell Topology

**Files:**
- Create: `deploy/helm/signaling/Chart.yaml`
- Create: `deploy/helm/signaling/values-production.yaml`
- Create: `deploy/helm/signaling/templates/gateway-deployment.yaml`
- Create: `deploy/helm/signaling/templates/actor-deployment.yaml`
- Create: `deploy/helm/signaling/templates/control-deployment.yaml`
- Create: `deploy/helm/signaling/templates/services.yaml`
- Create: `deploy/helm/signaling/templates/pdb.yaml`
- Create: `deploy/helm/signaling/templates/networkpolicy.yaml`
- Create: `deploy/postgres/cloudnativepg-cluster.yaml`
- Create: `deploy/directory/cloudnativepg-directory.yaml`
- Test: `qualification/scenarios/deployment-contract.sh`

**Interfaces:**
- Produces: six actor pods/cell across three AZs plus one-surge compatibility, independent gateway/actor scaling, private remoting/management, ready-only business service, cell-scoped discovery, and CloudNativePG-managed PostgreSQL for both per-cell authority and the regional directory authority; exact controller/PostgreSQL versions are pinned by Task 1.

- [ ] **Step 1: Write failing deployment-contract checks** for topology spread/anti-affinity, quorum-aware PDB, actor 90s termination grace, no public remoting/management, discovery seeing not-ready pods while business service selects ready pods, mTLS/network policy, resource limits including direct memory, synchronous standby policy, and no floating images.
- [ ] **Step 2: Run** `bash qualification/scenarios/deployment-contract.sh` **Expected:** FAIL.
- [ ] **Step 3: Implement Helm/HA manifests** using CloudNativePG for the per-cell PostgreSQL authority and a separate three-AZ CloudNativePG PostgreSQL cluster for the regional directory authority. Pin exact controller/PostgreSQL versions in the compatibility manifest, configure synchronous critical durability, and require old-primary fencing before authority traffic resumes.
- [ ] **Step 4: Run** the same script **Expected:** PASS.
- [ ] **Step 5: Commit** `ops: add three-AZ cell and authority deployment`.

### Task 21: Implement Drain, Rolling Upgrade, Schema Compatibility, DR, and Restore Runbooks

**Files:**
- Create: `signaling-app/src/main/java/io/webrtc/signaling/app/ShutdownCoordinator.java`
- Create: `runbooks/gateway-drain.md`
- Create: `runbooks/actor-drain.md`
- Create: `runbooks/postgres-failover.md`
- Create: `runbooks/cell-recovery.md`
- Create: `runbooks/pitr-restore.md`
- Create: `runbooks/privacy-after-restore.md`
- Create: `qualification/scenarios/rolling-upgrade.sh`
- Create: `qualification/scenarios/pitr-restore.sh`

**Interfaces:**
- Produces: gateway <=5m drain, actor ordered <=90s shutdown, N/N-1 wire/schema window, expand-contract migration procedure, new recovery epoch after disaster restore.

- [ ] **Step 1: Write failing lifecycle tests/scripts** for gateway readiness-off/reconnect batches, actor ingress shed -> admitted work settle -> shard handoff/lease release -> cluster leave -> DB close ordering, pool not closed before lease release, N/N-1 protocol, forced rollback, and restore invalidating all recovered nonterminal calls.
- [ ] **Step 2: Run** `bash qualification/scenarios/rolling-upgrade.sh --dry-run && bash qualification/scenarios/pitr-restore.sh --dry-run` **Expected:** FAIL until lifecycle/runbook contracts exist.
- [ ] **Step 3: Implement shutdown coordinator and executable runbooks**; keep renewal/termination resources alive through handoff.
- [ ] **Step 4: Re-run** both scripts **Expected:** PASS in the integration environment.
- [ ] **Step 5: Commit** `ops: add safe drain upgrade rollback and restore procedures`.

### Task 22: Build the Correctness, Property, Race, and Fault-Injection Qualification Suite

**Files:**
- Create: `signaling-integration-tests/src/test/java/io/webrtc/signaling/storage/InvariantPropertyTest.java`
- Create: `signaling-integration-tests/src/test/java/io/webrtc/signaling/storage/CrashScheduleIT.java`
- Create: `signaling-integration-tests/src/test/java/io/webrtc/signaling/storage/PartitionAndPauseIT.java`
- Create: `signaling-integration-tests/src/test/java/io/webrtc/signaling/storage/DatabaseFailoverIT.java`
- Create: `signaling-integration-tests/src/test/java/io/webrtc/signaling/storage/RevocationSecurityIT.java`
- Create: `qualification/scenarios/fault-matrix.yaml`
- Create: `signaling-integration-tests/src/test/java/io/webrtc/signaling/storage/ActorProcessFaultIT.java`
- Create: `signaling-integration-tests/src/test/java/io/webrtc/signaling/storage/DatabaseReplicationIT.java`
- Modify: `signaling-actors/src/test/java/io/webrtc/signaling/actors/cluster/MultiNodeShardingIT.java`
- Modify: `signaling-storage/src/test/java/io/webrtc/signaling/storage/LocalInviteAtomicIT.java`
- Create: `qualification/scenarios/fault_suite.py`, `qualification/scenarios/tests/test_fault_suite.py`, `qualification/scenarios/README.md`

**Execution clarification (2026-10-09):** named native SQL tests declare the storage package and now use its matching source directory. The supplementary tests execute actual local JVM, Artery and synchronous PostgreSQL faults. Method-level local evidence and original fault receipts are separate from external three-AZ qualification; Tasks 23–24 retain production workload and deployed-candidate gates. See [suite contract](../../../qualification/scenarios/README.md).

**Historical checkpoint — local suite (2026-10-09).** Implementation commit `5990a5c`; selected clean reactor 630 tests PASS; full clean reactor 758 tests/166 original XML reports PASS, zero failures/errors/skips; Python regression 78 tests PASS. Fresh final review found no blocking findings. [Original local evidence](../../../qualification/evidence/local-task22-5990a5c/README.md) maps all 24 fault records: 21 local PASS, seven external obligations NOT_RUN. Production remains NOT_QUALIFIED. Immediate concurrent same-address restart/recovery availability is deferred explicitly; the rejoin schedule proves stale-tenure safety after native replacement takeover.

**Complete — concurrent gap closed (2026-10-09):** the user confirmed concurrent same-address restart/recovery belongs to Task 22, superseding the historical deferral above. Verified clean source `92114fe`: full `clean verify` **759 tests/166 reports**, zero failures/errors/skips, six fault receipts; Python 78 PASS. Native six-member partition/rejoin now restarts the same address/fresh UID before takeover, reconciles bounded same-call UNKNOWN reads, fences stale pulse and preserves the exact replacement token. One Important review coverage gap was fixed and verified; no remaining identified findings. [Original current evidence](../../../qualification/evidence/local-task22-92114fe/README.md) maps 25 records: 22 local PASS, seven external NOT_RUN obligations. Local60s acceptance is not production RTO; Tasks 23–24 remain open/NOT_QUALIFIED.

**Interfaces:**
- Consumes: all runtime modules and deployment test environment.
- Produces: machine-readable invariant/fault evidence used by Task 24.

- [x] **Step 1: Write failing model/property tests** covering every state transition plus randomized duplicate/reordered/lost commands, stale generations, stale actors, reciprocal calls, winner races, delayed releases, unknown outcomes, and absorbing terminal states.
- [x] **Step 2: Add failing fault schedules** at before/after DB COMMIT/ACK/outbox, process pause past lease expiry, coordinator loss, shard host loss, old-node rejoin, partial partition, clock uncertainty, revocation lag, and PostgreSQL failover.
- [x] **Step 3: Run** `./mvnw -pl signaling-integration-tests -am verify` **Expected:** failures identify each not-yet-satisfied fault contract before fixes; once runtime tasks are complete, zero invariant violations.
- [x] **Step 4: Fix only implementation defects revealed by the suite, preserving spec safety rules**; never relax fencing, TTL, durability, queue bounds, or authorization to make a test pass.
- [x] **Step 5: Run** `./mvnw verify` **Expected:** all unit/integration/property/fault tests PASS.
- [x] **Step 6: Commit** `test: add signaling safety and fault qualification suite`.

### Task 23: Build Distributed Load Generation and P0/P1/P2/N-1 Scenarios

**Files:**
- Create: `signaling-loadgen/pom.xml`
- Create: `signaling-loadgen/src/main/java/io/webrtc/signaling/loadgen/DistributedLoadGenerator.java`
- Create: `signaling-loadgen/src/main/java/io/webrtc/signaling/loadgen/ScenarioRunner.java`
- Create: `signaling-loadgen/src/main/java/io/webrtc/signaling/loadgen/VirtualClient.java`
- Create: `signaling-loadgen/src/main/java/io/webrtc/signaling/loadgen/EvidenceWriter.java`
- Test: `signaling-loadgen/src/test/java/io/webrtc/signaling/loadgen/LoadGeneratorContractTest.java`
- Create: `qualification/loadgen/README.md`
- Create: `qualification/loadgen/config.schema.json`
- Create: `qualification/scenarios/p0-connections.yaml`
- Create: `qualification/scenarios/p1-baseline.yaml`
- Create: `qualification/scenarios/p2-production.yaml`
- Create: `qualification/scenarios/p2-az-loss.yaml`
- Create: `qualification/scenarios/p2-soak-24h.yaml`
- Create: `qualification/scenarios/p2-skew-burst.yaml`
- Create: `qualification/scenarios/security-abuse.yaml`
- Create: `qualification/evidence/evidence-schema.json`
- Test: `qualification/scenarios/loadgen-contract.sh`

**Interfaces:**
- Produces: distributed WSS generator supporting 10M sockets, multiple jti/session identities, heartbeat, refresh, cross-cell calls, setup frames, reconnect, slow consumer, malformed/security traffic, and reproducible seed/config output.

- [ ] **Step 1: Write failing load-generator contract checks** for distributed source machines/IPs, real TLS/RS256, 8M distinct users for P2, 10M sockets, 10k call attempts/s, 3M established calls at 300s mean duration, 500k inbound setup frames/s, 20k registrations/s, ~98% cross-cell, refresh load, 2x 60s setup burst, 5x hot destination/bucket, and one-AZ reconnect load.
- [ ] **Step 2: Run** `bash qualification/scenarios/loadgen-contract.sh` **Expected:** FAIL.
- [ ] **Step 3: Implement the distributed Java/Netty generator/orchestrator in `signaling-loadgen` and the evidence schema**. A coordinator partitions deterministic user/session/call ranges across worker processes; workers use Netty WSS clients and report generator CPU/NIC/socket/event-loop headroom so the generator cannot silently become the bottleneck.
- [ ] **Step 4: Run contract plus 10k/100k smoke stages** **Expected:** PASS with reproducible scenario/evidence files; do not jump directly to 10M.
- [ ] **Step 5: Commit** `perf: add distributed signaling qualification harness`.

### Task 24: Execute Production Qualification and Publish the Release Evidence Bundle

**Files:**
- Create: `qualification/evidence/<candidate>/manifest.yaml`
- Create: `qualification/evidence/<candidate>/functional.md`
- Create: `qualification/evidence/<candidate>/safety.md`
- Create: `qualification/evidence/<candidate>/capacity.md`
- Create: `qualification/evidence/<candidate>/n-minus-one.md`
- Create: `qualification/evidence/<candidate>/security.md`
- Create: `qualification/evidence/<candidate>/dr-restore.md`
- Create: `qualification/evidence/<candidate>/deployment.md`
- Create: `qualification/evidence/<candidate>/release-decision.md`
- Create: `qualification/scenarios/verify-evidence.sh`

**Interfaces:**
- Candidate ID format: `YYYYMMDD-HHMM-<git7>-<imageDigest12>`; every `<candidate>` path below means exactly that generated immutable ID.
- Consumes: exact candidate image digests, compatibility/config fingerprints, Tasks 1-23 test outputs, raw histograms, fault timelines, topology/hardware metadata.
- Produces: one auditable decision: `NOT_QUALIFIED`, `PRODUCTION_QUALIFIED:<envelope>`, or `10M_QUALIFIED:<exact-envelope>`.

- [ ] **Step 1: Write failing evidence verifier** requiring every §49.9 gate: functional protocol, safety, P0/P2 capacity, burst/skew, >=24h soak, N-1/AZ loss, dependency faults, chaos/timing, security, DR/restore, deployment, and operations. Missing/stale evidence must fail.
- [ ] **Step 2: Run** `bash qualification/scenarios/verify-evidence.sh qualification/evidence/<candidate>` **Expected:** FAIL before qualification runs populate evidence.
- [ ] **Step 3: Execute staged qualification**: 10k -> 100k -> 200k/cell -> multi-cell -> P0 10M -> P2 -> P2+N-1 -> 24h soak. At every stage capture p50/p95/p99/p99.9, CPU, heap/native/RSS, FD, network, mailbox/queue age, DB pool, row/WAL/storage rates, revocation/lease/recovery lag, reconnect and active-call preservation.
- [ ] **Step 4: Execute security/chaos/DR/deployment drills** on the same pinned candidate/effective configuration. Any safety invariant violation blocks release regardless of throughput.
- [ ] **Step 5: Re-run** the evidence verifier **Expected:** PASS only when every applicable gate has current evidence and declared envelope matches measurements.
- [ ] **Step 6: Write `release-decision.md`** using only measured evidence. If a target misses, resize/re-ADR and repeat affected gates; never weaken safety semantics to pass.
- [ ] **Step 7: Commit** `release: record signaling production qualification evidence`.

---

## Spec Coverage Matrix

| Spec area | Primary implementation tasks |
|---|---|
| §1-4 scope, cells, identity, routing | 1, 2, 4, 10, 13 |
| §5 auth lifecycle | 3, 14, 22 |
| §6 sessions/presence | 6, 11, 14 |
| §7 actors/ownership | 9-12 |
| §8-9 call flow/state/invariants | 7, 8, 11-13, 22 |
| §10 wire/delivery | 2, 8, 13-17 |
| §11 recovery/partitions | 9, 10, 12, 18, 21, 22 |
| §12 storage/transactions | 5-9, 18 |
| §13-14 capacity/backpressure | 15, 20, 23, 24 |
| §15 SLO/observability | 19, 24 |
| §16 deployment/operations | 20, 21, 24 |
| §17 module contracts | 1-19 |
| §18 validation | 22-24 |
| §19 implementation sequence | reflected by M0-M7 ordering |
| §20-23 reviews/properties | 5-24 |
| §24 fencing/transactions | 5-9, 12, 22 |
| §25 saga/activation | 7, 8, 13 |
| §26 delivery/recovery/timers | 8, 12, 18 |
| §27 containment/dependencies | 13, 15, 20, 22 |
| §28 capacity/retention/cost | 18, 19, 23, 24 |
| §29 SLI/error budget | 19, 24 |
| §30 HA/DR/recovery epochs | 20-22, 24 |
| §31 security/privacy/WebRTC dependencies | 3, 14, 16-20, 22 |
| §32-33 readiness/decisions | 1, 19-24 |
| §34-39 production review/qualification | 5-24 |
| §40 nonblocking + DB VT | 5, 9-15, 22-24 |
| §41-44 latency/lease/readiness corrections | 8-15, 18, 22-24 |
| §45 grouped ownership | 5, 9, 10, 12, 18, 23 |
| §46 JPA/VT/database contract | 5-9, 18, 22-24 |
| §47 Pekko runtime/ownership | 9-12, 20-22 |
| §48 HA/recovery/routing complexity | 10, 12, 19-24 |
| §49 production closure | 3, 9, 16-24 |
| §49.1 Trickle ICE | 16, 17, 22 |
| §49.2 authorization/revocation | 3, 14, 18, 22 |
| §49.3 recovery SLIs | 19, 22-24 |
| §49.4 media/ICE terminal semantics | 16, 17, 22 |
| §49.5 privacy/deletion | 18-21, 24 |
| §49.6 HA/authority dependencies | 9, 20-24 |
| §49.7 edge security | 3, 14, 15, 20, 22-24 |
| §49.8 compatibility/change safety | 1, 2, 20, 21, 24 |
| §49.9-49.10 qualification/status | 22-24 |

## Milestone Exit Gates

- **M0:** build reproducible; config/BOM pinned; protocol/auth/bootstrap tests green; no production claim.
- **M1:** all PostgreSQL race/unknown-outcome/lease contract tests green against real PostgreSQL; no actor/gateway scale claim.
- **M2:** single-cell and two-cell call-control flows green under process/DB faults; actor dispatchers remain nonblocking.
- **M3:** real browser/native interoperability covers offer/answer, ordered Trickle ICE, reconnect, and ICE restart; slow consumers cannot stall unrelated sessions.
- **M4:** SLI dashboards and maintenance workers demonstrate bounded backlog in soak-scale pretests; sensitive payload redaction tests green.
- **M5:** one-AZ topology, DB failover, rolling upgrade/rollback, PITR restore, and new recovery epoch drills are executable and repeatable.
- **M6:** zero safety-invariant violations across the complete model/race/fault suite; failures in availability are typed/measured rather than converted into unsafe success.
- **M7:** release status is derived exclusively from Task 24 evidence. Architecture alone never upgrades the status.

## Execution Guidance

Prefer **subagent-driven execution** because the plan has 24 reviewable tasks across protocol/security, database concurrency, Pekko runtime, Netty transport, WebRTC semantics, infrastructure, and performance qualification; a mistake in an authority or lease task can invalidate later work. If only one implementation context is available, use native execution but preserve the same task/commit/test gates and require a fresh whole-branch review before qualification.

Do not start Task 23 scale work before Tasks 1-22 are green. Performance optimization discovered during qualification must first identify the measured bottleneck; any change to authority, fencing, TTL, durability, authorization, or failure semantics requires an ADR/spec update and rerun of the affected correctness/fault gates.
