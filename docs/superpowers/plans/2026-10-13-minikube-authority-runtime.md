# Minikube Authority and Native Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cài source/enrollment/native owners thật, giữ mỗi cell và mỗi control scope trong đúng authority boundary.

**Architecture:** Một local-only source module cung cấp measured clock và durable security log qua signed mTLS HTTP. Single-cell transfer stores được điều phối qua authenticated control RPC thay direct two-DB access. Typed enrollment adapter tạo native owners hiện có, không thay actor/SQL/business contracts bằng local implementations.

**Tech Stack:** Java 21, existing Netty/native source framing, Ed25519, PostgreSQL tracked transactions, Spring native composition, gRPC/mTLS, OpenTelemetry SDK.

**Spec:** [Approved design](../specs/2026-10-13-minikube-signaling-platform-design.md); [master constraints/dependencies](2026-10-13-minikube-signaling-platform.md).

## Global Constraints

All master constraints apply. Source response ≤32,768 bytes, Content-Length/Connection:close, no redirect/chunked/compression. Source batches total events+retiredKeys ≤16, durable monotonic offset/high-water; gateway security cannot advance beyond cell-committed revocation checkpoint. PodUID and fresh JVM processBoot are separate. No TestOnlyNativeCapabilities, fake clock, fake WAL facts or unconditional readiness in deployable image.

## Review Focus

1. Clock proof valid but measurement stale/discontinuous — A1 `signedUnsafeMeasurementIsRejected`.
2. Empty batch/offset rollback falsely claims caught-up — A2 `durableCursorCannotSkipEvents`.
3. Migration retry publishes before installed checkpoint or after physical timeout — A3/A4 `publicationWaitsForExactInstallation`.
4. Enrollment for another Pod/image/DB writer passes parser — A5/A6 `rejectsForeignIncarnationOrWriter`.
5. Health dependency failure restarts healthy JVM or exporter retains unbounded secrets — A7/A8 `readinessFailureDoesNotKillProcess`, `exporterIsBoundedAndRedacted`.

---

## File structure

Create module `signaling-local-platform` (root `pom.xml` registration), package `io.webrtc.signaling.localplatform`; it contains source server, durable repository and host observer, independently executable with explicit local acknowledgement. Shared wire DTOs/proof validation live in `signaling-auth`, not a dependency from Identity to local-only application. Transfer store/orchestration lives in existing `signaling-control-api`; local runtime adapter and health live in `signaling-app/runtime`. Source DB migrations live under `signaling-local-platform/src/main/resources/db/migration`, separate from cell migrations.

### Task A1: Measured signed clock/source transport

**Files:** Create `signaling-local-platform/pom.xml`, `Dockerfile`, `src/main/java/io/webrtc/signaling/localplatform/LocalPlatformApplication.java`, `SourceHttpServer.java`, `HostClockObserver.java`, `ClockReportSource.java`, `src/test/java/io/webrtc/signaling/localplatform/ClockReportSourceTest.java` (all relative to new module); modify root `pom.xml`; create `config/local-minikube-sources.md`.

**Interfaces:** `HostClockObserver.Sample(Instant observedAt,long monotonicNanos,int pairUncertaintyMicros,int relativeRateErrorPpm,boolean continuous)`; `sample(): Sample`; `ClockReportSource.report(String cell,long storageEpoch,UUID podUid,UUID processBoot): CompletionStage<ClockSafetyMonitor.Report>`; `SourceHttpServer.start(InetSocketAddress address,SSLContext tls): CompletionStage<Void>`, `close(): void`. Port9443 requires authenticated enrolled callers, binds existing X-Signaling-Pod-Uid/Process-Boot/Source-Offset headers to certificate/enrollment; clock route preserves existing `ClockSafetyMonitor.Report` and `ClockSafetyMonitor.signingBytes` wire contract.

- [ ] Write `signedUnsafeMeasurementIsRejected`, `noClockReportWithoutMeasuredHostState`, `strictFramingAndCallerBinding`; test source/observer fixture signs ephemeral keys. Assertions:
  ```java
  assertRejected(reportWithUncertaintyMicros(250001));
  assertRejected(reportWithRateErrorPpm(1001));
  assertRejected(reportWithContinuity(false));
  assertRejected(reportWithWindow(Duration.ofSeconds(6)));
  assertRejected(requestWithForeignPodUid);
  assertTrue(encodedResponseBytes <= 32768);
  ```
- [ ] Run `./mvnw -B -pl signaling-local-platform -am -Dtest=ClockReportSourceTest test` after adding module/test skeleton; expected red missing source behavior.
- [ ] Implement observer measurement from host time-sync/error/continuity data, with authenticated observer→source reports and persisted sequence/boot binding. Unavailable measurement produces unsafe/closed status, never constant-safe. Source signs existing report bytes using Secret Ed25519 key, measured validity≤5s; no host clock changes. Exclude native app auto-configuration and require `LOCAL_TEST_ONLY` acknowledgement before startup.
- [ ] Repeat command; expected PASS including replay/nonmonotonic sequence, stale measurement, oversized body, chunked/redirect rejection, certificate/SAN mismatch and shutdown while request in-flight. Host integration gate measures actual host in K5/Q09; fixture measurement is not deployed evidence.
- [ ] Scope review then commit `feat(local-platform): add measured signed clock source`.

### Task A2: Durable security log, epochs and issuance reservations

**Files:** Create `signaling-auth/src/main/java/io/webrtc/signaling/auth/SecuritySourceClient.java`, `SecuritySourceProtocol.java`; create `signaling-local-platform/src/main/java/io/webrtc/signaling/localplatform/SecurityLogRepository.java`, `SecuritySourceHandler.java`; create `signaling-local-platform/src/main/resources/db/migration/V001__security_source.sql`; test `signaling-local-platform/src/test/java/io/webrtc/signaling/localplatform/SecurityLogRepositoryTest.java`, `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/SecuritySourceIT.java` and add local module test dependency in integration `pom.xml`.

**Interfaces:** Nested DTOs on `SecuritySourceClient`: `IssuanceRequest(UUID operationId,String issuer,UserId userId,String kid,long keyRingVersion,Instant issuedAt,int ttlSeconds)`; `IssuanceReservation(UUID operationId,String issuer,UserId userId,long sourceOffset,long securityEpoch,String kid,long keyRingVersion,Instant issuedAt,Instant validUntil,String sourceProof)`; `SecurityCommand(UUID operationId,long expectedHighWater,Kind kind,String issuer,UserId userId,String jti,long revokedThroughEpoch,String kid)` with Kind REVOKE_USER/REVOKE_TOKEN/RETIRE_KEY; `SecurityCommit(long offset,long highWater)`; `KeyStateCommand(UUID operationId,long expectedVersion,Phase phase,String activeKid,Map<String,String> publicFingerprints,Instant eligibleRetirementAt)` with Phase PUBLISHED/SWITCHED; `KeyStateCommit(long version,Instant lastReservedAt)`; `VerifierObservation(UUID podUid,UUID processBoot,String issuer,long version,String publicFingerprint,Set<String> kids,long retirementOffset,boolean fresh)`; `VerifierInventory(long version,Instant validUntil,Set<UUID> podUids)`. Wire observation owns primitive fields independently of I3 cache implementation, avoiding an A2↔I3 dependency cycle.

`SecuritySourceClient.reserveIssuance(IssuanceRequest): CompletionStage<IssuanceReservation>`, `appendSecurity(SecurityCommand): CompletionStage<SecurityCommit>`, `recordKeyState(KeyStateCommand): CompletionStage<KeyStateCommit>`, `observeVerifier(VerifierObservation): CompletionStage<Void>`, `activeVerifierInventory(): CompletionStage<VerifierInventory>`, `updateVerifierInventory(UUID operationId,long expectedVersion,VerifierInventory next): CompletionStage<Void>`, `registerBoot(UUID podUid,UUID processBoot,String cell,String plane): CompletionStage<Void>`. Inventory update is controller-only and includes all current approved verifier Pods from authenticated API; stale/missing inventory blocks rotation, not an empty-set success. Boot registration requires current K2 certificate/PodUID, atomically replaces that Pod's boot, and fences old boot; a request header cannot register or replace identity implicitly. `SecurityLogRepository.read(String cell,String issuer,long fromOffset): CompletionStage<RevocationReconciler.Batch>` preserves existing batch type from storage. `SecuritySourceProtocol.signingBytes(IssuanceReservation): byte[]` encodes domain `WSA_LOCAL_ISSUANCE_V1` and every reservation field except sourceProof in fixed field order/length-prefixed UTF-8; `verify(IssuanceReservation,Map<String,PublicKey>,Instant): boolean` verifies Ed25519 key-id.signature, validity and canonical bytes. Identity additionally checks exact request/response operation/issuer/user/kid/version/time binding. SQL schema stores immutable operation payload hashes, counters, active key versions, verifier observations, last issuance per kid, revocation events and retirement tombstones; no cell authority tables.

- [ ] Write `durableCursorCannotSkipEvents`, `firstIssuanceEpochIsOne`, `reservationAndRetirementSurviveRestart`; assertions:
  ```java
  assertEquals(1L, firstReservation.securityEpoch());
  assertEquals(2L, reserveAfterRevokeThroughEpochOne.securityEpoch());
  assertEquals(firstCommit, exactSameOperationRetry);
  assertRejected(sameOperationIdDifferentPayload);
  assertTrue(batch.events().size() + batch.retiredKeys().size() <= 16);
  assertEquals(persistedHighWater, batch.currentSourceHighWater());
  ```
  Test missing offset, empty-but-not-caught-up batch, rollback/restart, retirement replay and future/expired proof. Revocation watermark initially0; issue max relevant revoked watermark+1, user revoke through current issuance epoch, token revoke through that token's epoch; never issue epoch0.
- [ ] Run `./mvnw -B -pl signaling-local-platform -am -Dtest=SecurityLogRepositoryTest test`; expected red missing durable repository/protocol.
- [ ] Implement tracked durable transactions, expected-version CAS, proof-bound response and append-only high-water. Reserve issuance only for active approved key version, ttl900 and safe clock/source DB authority; validate nonnegative expected epochs and overflow, rejecting Long.MAX_VALUE increments. Reservation validUntil≤issuedAt+5s, sign/emit within reservation validity, token expiry remains issuedAt+900s. Planned retirement uses last reserved issuedAt conservatively even if issuance fails; source computes lastReservation+900s+30s+enrolled propagation bound itself and rejects earlier admin eligibility. Sign revocation batches with existing `RevocationSourceVerifier.signingBytes(cell,issuer,batch)`; registration/admin identities use separate mTLS roles. Replay local persisted cursor before readiness, persist actor revocation application before ack, expose gateway security only from committed cell checkpoint. Existing users after login/revoke follow watermark contract above, without changing RevocationState comparisons. Switching key metadata uses source CAS before local active-key install: mismatch closes issuance until reconciled, so lost acknowledgment cannot issue from stale ring.
- [ ] Repeat unit command and `./mvnw -B -pl signaling-integration-tests -am -Dit.test=SecuritySourceIT verify`; expected PASS with real source/cell PostgreSQL, crash before/after COMMIT, retry and physical receipt assertions. Native source freshness hard5s and emergency2s SLO remain enforced.
- [ ] Scope review then commit `feat(local-platform): persist security source and issuance state`.

### Task A3: Extract a single-cell transfer store

**Files:** Create `signaling-control-api/src/main/java/io/webrtc/signaling/control/CellBucketTransferStore.java`, `ManagedAuthorityTransfer.java`; modify existing `PostgresBucketTransfer.java`; test `signaling-control-api/src/test/java/io/webrtc/signaling/control/CellBucketTransferStoreTest.java`, `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/ScopedBucketTransferStoreIT.java`.

**Interfaces:** `ManagedAuthorityTransfer extends BucketTransferService.AuthorityTransfer` adds `transactionOwners(): List<SqlTransactions>`, `drain(): CompletionStage<Void>`. `CellBucketTransferStore(SqlTransactions sql,String cell,long storageEpoch)`; `prepare(UUID migrationId,HomeRoute source,HomeRoute target): CompletionStage<FreezeReceipt>`; `exportPage(UUID migrationId,Dataset dataset,String cursor): CompletionStage<Page>`; `installPage(UUID migrationId,HomeRoute source,HomeRoute target,Page page): CompletionStage<Void>`; `verifyInstalled(FreezeReceipt receipt,HomeRoute target): CompletionStage<Void>`; `publicationConfirmed(FreezeReceipt receipt,HomeRoute target): CompletionStage<Void>`; `transactionOwners(): List<SqlTransactions>`. Nested Dataset USER_GUARD/SESSION_REGISTRY/HOME_PARTICIPATION/COMMAND_RESULT; `Page(Dataset dataset,String nextCursor,int rowCount,String digest,byte[] canonicalRows,boolean last)` preserves existing dataset canonical serialization/checkpoint and max32 rows/page. FreezeReceipt is existing `BucketTransferService.FreezeReceipt`.

- [ ] Write `eachStoreOwnsOneSqlAuthority`, `publicationWaitsForExactInstallation`, `checkpointRetryDoesNotRecopyOrUnfreeze`:
  ```java
  assertEquals(List.of(sourceSql), sourceStore.transactionOwners());
  assertEquals(List.of(targetSql), targetStore.transactionOwners());
  assertRejected(verifyBeforeAllFourDatasetCheckpoints);
  assertRejected(prepareWhileCallOrParticipationStillActive);
  assertEquals(firstDigest, samePageRetryDigest);
  assertRejected(sameMigrationIdDifferentSourceOrTarget);
  ```
- [ ] Run `./mvnw -B -pl signaling-control-api -am -Dtest=CellBucketTransferStoreTest test`; expected red missing scoped store.
- [ ] Extract existing SQL phases/canonical row encoding into store without changing DRAINING→FROZEN fences, four datasets, digest/count validation, recovery id, 2s SQL bounds or tracked physical completion. `PostgresBucketTransfer` becomes local two-store adapter implementing ManagedAuthorityTransfer for existing production callers; its existing behavior and transactionOwners remain compatible. Page byte ceiling must also satisfy existing retained RPC resource contract; enforce before transmission without inventing larger buffers.
- [ ] Repeat unit gate and `./mvnw -B -pl signaling-integration-tests -am -Dit.test=ScopedBucketTransferStoreIT verify`; expected PASS including unchanged legacy transfer regressions, checkpoint crash/retry, late SQL completion fencing and empty/populated dataset exact readback.
- [ ] Scope review then commit `refactor(control): extract cell-scoped bucket transfer authority`.

### Task A4: Authenticated remote transfer orchestration

**Files:** Create `signaling-control-api/src/main/java/io/webrtc/signaling/control/RemoteBucketTransfer.java`, `BucketTransferRpcServer.java`, `BucketTransferRpcClient.java`; create `signaling-control-api/src/main/proto/bucket_transfer.proto` and protobuf build configuration in its `pom.xml`; modify `signaling-app/src/main/java/io/webrtc/signaling/app/runtime/NativeBucketTransferEnrollment.java`, `NativeBucketTransferConfiguration.java`, `NativeControlProcess.java`, `NativeControlBusinessConfiguration.java`; test `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/RemoteBucketTransferIT.java`.

**Interfaces:** Enrollment provides `ManagedAuthorityTransfer transfer` instead of requiring two DB Stores. `RemoteBucketTransfer` implements ManagedAuthorityTransfer and existing prepare/copyAndInstall/verifyInstalled/publicationConfirmed methods. `BucketTransferRpcClient.call(String ownerCell,Command command): CompletionStage<Reply>`; Command oneof PREPARE/EXPORT_PAGE/INSTALL_PAGE/VERIFY_INSTALLED/PUBLICATION_CONFIRMED contains exact A3 DTO fields plus operation/migration UUID and sender/target scope. Reply uses FreezeReceipt/Page/empty success. gRPC server9553 requires enrolled control identities, independent policy from actor/gateway CellRpc parser; per-command checks cell/storage/bucket epoch/fence/migration binding and finite size/deadline limits.

- [ ] Write real two-control/two-cell SQL fixture `controlHasOnlyItsCellCredential`, `publicationWaitsForExactInstallation`, `wrongIdentityCannotTransfer`; assertions:
  ```java
  assertFalse(controlC001CanConnectToC002Database);
  assertRejected(rpcWithActorCertificate);
  assertRejected(rpcWithMismatchedOwnerCellOrFence);
  assertEquals(oldRoute, directoryWhileInstallationUnproven);
  assertEquals(newRoute, directoryAfterExactInstalledProofAndCas);
  assertEquals(firstMigrationResult, retrySameMigrationResult);
  ```
- [ ] Run `./mvnw -B -pl signaling-integration-tests -am -Dit.test=RemoteBucketTransferIT verify`; expected red missing remote transport/enrollment.
- [ ] Implement source/target phase calls on9553; preserve `BucketTransferService` CAS publication and exact readback. Source control owns only source SQL, target control only target SQL; orchestrator owns Directory SQL and its one local store. NativeControlProcess consumes managed provider, owns its physical RPC/SQL drain, and requires actually installed transfer owner for readiness. Keep production direct adapter valid; never substitute `transferInstalled=true` or grant both credentials. Authenticated retry carries exact immutable payload, not new migration ID on UNKNOWN.
- [ ] Repeat gate; expected PASS including disconnect during page copy, source/target restart, retry after Directory COMMIT before acknowledgment, uninstalled proof rejection and shutdown waiting for physical completion. Compare final SQL digests across all four datasets.
- [ ] Scope review then commit `feat(control): coordinate scoped bucket transfer over mTLS`.

### Task A5: Versioned typed local enrollment

**Files:** Create `signaling-app/src/main/java/io/webrtc/signaling/app/runtime/LocalMinikubeContract.java`, `LocalMinikubeEnrollmentConfiguration.java`; modify `MountedNativeContract.java`, `MountedProductionContract.java`, `signaling-app/src/main/java/io/webrtc/signaling/app/config/RuntimeSecretEnvironment.java`; test `signaling-app/src/test/java/io/webrtc/signaling/app/runtime/LocalMinikubeContractTest.java`; create `config/local-minikube-enrollment.schema.json`.

**Interfaces:** `LocalMinikubeContract.load(Path runtimeYaml,Path enrollmentJson,String mode,String acknowledgement): LocalMinikubeContract`. Enrollment JSON schemaVersion2 replaces static jwtPublicKeys with I3 enrolled JWKS definition and adds typed plane/cell/PodUID/Secret-generation/build+config hashes, storage/routing/schema epochs, PKI/source proof trust, exact DB system/timeline/replica identities, compatibility/call-policy version, formation intent, scoped transfer endpoints and finite quotas. Separate typed records in LocalMinikubeContract; preserve existing native enrollment records as outputs. Runtime YAML remains one `signaling` root, scalar-only≤64KiB; no new general YAML loader.

- [ ] Write `rejectsForeignIncarnationOrWriter`, `productionRejectsLocalContract`, `requiresAllThreeLocalMarkers`; assertions:
  ```java
  assertRejected(loadWithMode("production", "LOCAL_TEST_ONLY"));
  assertRejected(loadWithMode("local-minikube", ""));
  assertRejected(contractForDifferentPodUid);
  assertRejected(contractWithMissingJwksOrTransferOwner);
  assertRejected(nestedOrOversizedRuntimeYaml);
  assertEquals(2, validContract.schemaVersion());
  ```
- [ ] Run `./mvnw -B -pl signaling-app -am -Dtest=LocalMinikubeContractTest test`; expected red missing versioned adapter.
- [ ] Implement strict unknown/duplicate-field validation and cross-field fingerprints/identity/DB binding; paths reference Secret mounts, never inline private key values in ConfigMap. Activation requires mode local-minikube + acknowledgement LOCAL_TEST_ONLY + environment local-test-only. Production constructors/defaults keep rejecting local exceptions; missing schema/source/enrollment leaves startup closed. K2 owns per-incarnation provisioning; processBoot is generated anew inside JVM, not immutable Helm value.
- [ ] Repeat gate; expected PASS plus symlink/path escape, old schema v1 misuse, wrong issuer/audience, duplicate peer identity, 6-contact formation grammar and image/config checksum mismatch tests.
- [ ] Scope review then commit `feat(runtime): add strict local Minikube enrollment adapter`.

### Task A6: Install complete native owners and real observations

**Files:** Create `signaling-app/src/main/java/io/webrtc/signaling/app/runtime/LocalNativeOwners.java`, `LocalCompatibilityCollector.java`, `LocalWalAuthorityCollector.java`; modify `LocalMinikubeEnrollmentConfiguration.java`, `NativeActorProcessConfiguration.java`, `NativeActorCompositionConfiguration.java`, `NativeGatewayBusinessConfiguration.java`, `NativeControlBusinessConfiguration.java` only where adapter hooks require; test `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/LocalNativeCompositionIT.java`.

**Interfaces:** `LocalNativeOwners.install(LocalMinikubeContract contract): CompletionStage<Void>` installs existing Native*Enrollment beans before NativeRuntimeStartup verifies owner installation. `LocalWalAuthorityCollector.observe(): CompletionStage<NativeWalEnrollment>` returns enrollment only after real DB/SQL/WAL observations match pinned system/timeline/writer/replica set; `LocalCompatibilityCollector.observe(): CompletionStage<NativeCompatibilityEnrollment>` requires authenticated live actor/gateway/schema/rollback observations, not constants. JWKS owner created per JVM and wrapped by existing BoundedTokenVerifier; each process owns/drains it.

- [ ] Write `allPlanesRequireConcreteOwners`, `rejectsForeignIncarnationOrWriter`, `clustersCannotShareRemotingMembership`; fixture installs all six members per cell:
  ```java
  assertEquals(6, cellC001UpMembers);
  assertEquals(6, cellC002UpMembers);
  assertEquals(0, foreignCellMembersInEitherCluster);
  assertTrue(actualUserAndCallShardRegionsInstalled);
  assertTrue(actualMaintenanceOutboxRecoveryWorkersInstalled);
  assertFalse(readyWhenWalReplicaIdentityChanged);
  assertFalse(readyWhenRequiredOwnerMissing);
  ```
- [ ] Run `./mvnw -B -pl signaling-integration-tests -am -Dit.test=LocalNativeCompositionIT verify`; expected red missing local owners.
- [ ] Compose actor ingress/business/process/maintenance/scheduling/recovery/WAL/lifecycle, gateway ingress/relay/receiver/security-sweep/boot renewal, control scoped database/directory/bootstrap/sources/transfer/compatibility and calling policy from validated mount. Generate new processBoot each JVM and A2 registerBoot before protected source fetch/admission. Use real `ClusterReadiness.localMinikube("LOCAL_TEST_ONLY")`, separate systemNames, local namespace discovery and actual PodIP UniqueAddress. Authenticated test users are enrolled explicitly; unknown users/denied policy fail closed. Reuse existing NativeCompatibilityCollectors with measured local providers for actual peer features/migration16/rollback receipts; new wire/source features enter compatibility gating, not unconditional declaration.
- [ ] Repeat gate; expected PASS with real TLS/SQL/source components and all NativeRuntimeStartup required owners; expired source/JWKS or mismatched WAL rejects protected admission. Source/Directory DB collectors use their own schema-specific authority facts, never create cell tables there to satisfy a query. Actual deployed PID/owner evidence remains Q18.
- [ ] Scope review then commit `feat(runtime): compose complete local native planes`.

### Task A7: Private mTLS health and authenticated exec probes

**Files:** Modify `signaling-app/src/main/java/io/webrtc/signaling/app/runtime/PrivateHealthServer.java`, `NativeControlIngressEnrollment.java`, `NativeControlIngressConfiguration.java`, `NativeActorSchedulingEnrollment.java`, `NativeActorSchedulingConfiguration.java`, `NativeGatewayLifecycleEnrollment.java`, `NativeGatewaySpringLifecycle.java` (same runtime directory); modify I2 `IdentityTlsConfiguration.java` and A1 `SourceHttpServer.java` for equivalent private health endpoints; create `signaling-app/src/main/java/io/webrtc/signaling/app/runtime/TlsProbe.java`; test `signaling-app/src/test/java/io/webrtc/signaling/app/runtime/PrivateHealthTlsTest.java`.

**Interfaces:** `PrivateHealthServer(InetSocketAddress address,SslContext tls,BooleanSupplier live,BooleanSupplier ready,BooleanSupplier startup,Supplier<String> metrics)` accepts enrolled Netty server TLS requiring trusted probe/monitoring client identity. Add TLS enrollment field to the three plane health-input records listed above. `TlsProbe.main(String[] args): void` accepts `--host --port --authority --ca --cert --key --path --timeout-ms`, emits no response secrets, exit0 only verifiedHTTP200, nonzero on TLS/status/timeout. Port8559; paths `/live`, `/ready`, `/startup`, `/metrics`. Health readiness reflects installed native admission state; per-operation guards still enforce expiry between probe polls.

- [ ] Write `readinessFailureDoesNotKillProcess`, `probeValidatesPeerAndClientIdentity`:
  ```java
  assertEquals(200, liveWithDatabaseOrSourceUnavailable);
  assertEquals(503, readyWithDatabaseOrSourceUnavailable);
  assertNotEquals(0, probeWrongCaExitCode);
  assertNotEquals(0, probeWrongSanExitCode);
  assertNotEquals(0, probeWithoutClientCertificateExitCode);
  assertEquals(0, enrolledReadyProbeExitCode);
  ```
- [ ] Run `./mvnw -B -pl signaling-app -am -Dtest=PrivateHealthTlsTest test`; expected red plaintext/current probe behavior.
- [ ] Implement health TLS, bounded2s exec probe and startup/readiness state distinctions. Keep compatible production constructor wiring through explicit TLS enrollment; no silent plaintext fallback. Identity/source expose equivalent private health via their own TLS listener, reusing probe tool packaged into images by K4. Dependency failure closes readiness/admission but liveness tracks process/event-loop health.
- [ ] Repeat gate; expected PASS including shutdown readiness-first, hung TLS handshake deadline, closed port and dependency outage without JVM restart.
- [ ] Scope review then commit `feat(runtime): secure private health and exec probes`.

### Task A8: Bounded distributed trace export

**Files:** Create `signaling-observability/src/main/java/io/webrtc/signaling/observability/BoundedTraceExporter.java`; modify same package `TraceContext.java`, `OperationObservation.java`, `signaling-observability/pom.xml`, `signaling-app/src/main/java/io/webrtc/signaling/app/runtime/NativeTelemetry.java`, `NativeTelemetryConfiguration.java`; instrument `signaling-control-api/src/main/java/io/webrtc/signaling/control/BootstrapController.java`, `signaling-gateway/src/main/java/io/webrtc/signaling/gateway/NativeGatewayCommands.java`, `signaling-cell-rpc/src/main/java/io/webrtc/signaling/rpc/NativeRpcTelemetry.java`, `NativeOutboxDelivery.java` (same rpc package), `signaling-actors/src/main/java/io/webrtc/signaling/actors/user/UserActor.java`, `signaling-actors/src/main/java/io/webrtc/signaling/actors/call/CallActor.java`, `signaling-storage/src/main/java/io/webrtc/signaling/storage/SqlTransactions.java`. Test `signaling-observability/src/test/java/io/webrtc/signaling/observability/BoundedTraceExporterTest.java`, `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/SignalingTraceIT.java`.

**Interfaces:** `BoundedTraceExporter(int queueCapacity,Duration exportDeadline,SSLContext tls,URI otlpEndpoint)`; nested `SpanRecord(TraceContext context,String parentSpanId,SignalingMetrics.Operation operation,long startedNanos,long endedNanos,Optional<SignalingMetrics.Outcome> logicalOutcome,boolean physicalFinished)`; `offer(SpanRecord span): boolean`, `drain(): CompletionStage<Void>`, `close(): void`. Add backward-compatible `OperationObservation.Receipt.traceContext(): Optional<TraceContext>` default empty; NativeTelemetry supplies full context/parent and emits logical/physical child spans once through existing receipt lifecycle. Export W3C parent linkage and authenticated OTLP gRPC4317, finite queue/retry, no observer authority. Define exporter capacity256, deadline2s for local profile; business/resource contracts remain untouched.

- [ ] Write `exporterIsBoundedAndRedacted` and `crossCellTracePreservesParentChain`:
  ```java
  assertTrue(retainedExportCount <= 256);
  assertFalse(exportedAttributesContainJwtJtiUserIdSdpIceOrPrivateKey);
  assertEquals(originTraceId, destinationActorTraceId);
  assertTrue(hasHttpGatewayRpcActorSqlAndOutboxSpans);
  assertTrue(collectorDownDoesNotChangeBusinessOutcome);
  ```
- [ ] Run `./mvnw -B -pl signaling-observability -am -Dtest=BoundedTraceExporterTest test`; expected red missing exporter.
- [ ] Implement bounded exporter and parent propagation at exact runtime boundaries; reuse existing redactor and finite metrics labels, no user/request/jti label cardinality. Export loss/saturation increments finite metrics, never blocks safety/Netty dispatcher, never claims successful trace delivery after failed collector write. Pin SDK dependency with existing Maven management conventions.
- [ ] Repeat unit gate and `./mvnw -B -pl signaling-integration-tests -am -Dit.test=SignalingTraceIT verify`; expected PASS with actual TLS collector fixture, outage/full queue/late shutdown and parent linkage assertions. Re-run `./mvnw -B clean verify` after all Java tasks and reconcile full reports before K5 candidate build.
- [ ] Scope review then commit `feat(observability): export bounded cross-cell traces`.
