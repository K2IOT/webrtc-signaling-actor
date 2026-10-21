# Local Runtime Adapter (SP1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the unmodified `signaling-app` image install every `Native*Enrollment` owner for the `gateway`, `actor` and `control` planes from a mounted `LOCAL_TEST_ONLY` contract, so a process started with that contract passes `NativeRuntimeStartup.requireInstalled`.

**Architecture:** An `ApplicationContextInitializer` (registered in `spring.factories`) runs only when all three local markers are present, loads `MountedNativeContract` plus a new strict `LocalProcessProfile`, and registers the enrollment singletons the existing `@ConditionalOnBean` auto-configurations already expect. This is the same mechanism the integration tests use (`registerSingleton`), moved into `src/main` behind the fail-closed gate. Owners are built per plane in focused classes; nothing from `src/test` or `TestOnlyNativeCapabilities` enters the image.

**Tech Stack:** Java 21, Spring Boot `ApplicationContextInitializer`, Pekko typed/cluster, Netty `SslContext`, JDK `SSLContext` (PKCS12), Jackson strict parser, JUnit 5 + AssertJ, Testcontainers PostgreSQL 17.6.

**Spec:** `docs/superpowers/specs/2026-10-21-docker-compose-two-cell-design.md` (§4.1–4.3, §4.5, §8). Reference plan: `docs/superpowers/plans/2026-10-13-minikube-authority-runtime.md` (A5/A6).

## Global Constraints

- Activation requires all of: `SIGNALING_DEPLOYMENT_MODE=local-minikube`, `SIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY`, contract `environment="local-test-only"`. Otherwise the initializer registers nothing and startup keeps failing with `NATIVE_RUNTIME_NOT_INSTALLED`.
- `MountedNativeContract` stays schema v1 and unchanged; process-level data goes in a new `LocalProcessProfile` file (`SIGNALING_LOCAL_PROCESS_PROFILE`, default `/run/signaling/process.json`).
- All mounted paths absolute, under the contract directory, ≤ 512 characters; files ≤ 64 KiB; no symlink escaping the directory.
- TLS 1.3 only; RPC leaf certificates carry exactly one URI SAN `spiffe://signaling/local-test-only/cell/<cell>/(actor|gateway)[/<id>]`; JDBC URLs contain `sslmode=verify-full`.
- `podUid` is a fixed UUID per container; `processBoot` is generated fresh in every JVM, never read from a file.
- Actor cell is exactly 6 pods, quorum 4, `required-contact-point-nr=6`; Pekko roles per member include exactly one `az-*` role; at least two distinct `az-*` roles per cell.
- Pekko discovery stays `kubernetes-api`; `seed-nodes` stays empty; `ShardingBootstrap.validateProduction` is not relaxed.
- Only the `control` profile may receive credentials for more than one cell database.
- No `TestOnlyNativeCapabilities`, fake clock, fake WAL facts or unconditional readiness in `src/main`.
- Formatting: match surrounding 4-space code; do not run `spotless:apply` repo-wide; new files may be formatted with `-DspotlessFiles`.
- Status stays `LOCAL_TEST_ONLY` / `NOT_QUALIFIED`.

## Review Focus

1. **Foreign incarnation:** `podUid` in the profile differs from `SIGNALING_POD_UID`, or the contract's cell differs from `SIGNALING_CELL_ID` — startup must be refused before any owner is built (Task 1).
2. **Production image with a local contract:** markers absent or `mode=production` — nothing is registered and the plane fails closed (Task 1).
3. **Path escape:** relative path, `..`, or a symlink pointing outside the mount directory in contract or profile — refused (Task 1).
4. **Credential scope:** an actor or gateway profile that lists a second cell database — refused; only control may (Task 6).
5. **Single-zone or short cell:** fewer than two distinct `az-*` roles, or an actor profile that cannot reach the compatibility window — the process stays not-ready with a stated reason instead of silently hanging (Task 3).

---

## File Structure

New package `io.webrtc.signaling.app.runtime.local` in `signaling-app/src/main/java`:

| File | Responsibility |
|---|---|
| `LocalProcessProfile.java` | Strict JSON record for per-process data (plane, pod, ports, az role, WAL, compatibility, kube-API, transfer cells) |
| `LocalRuntimeContract.java` | Loads and cross-checks contract + profile + environment; owns path-confinement checks |
| `LocalRuntimeInitializer.java` | Activation gate; dispatches to the plane installer; registers singletons |
| `LocalTls.java` | Netty `SslContext` and JDK `SSLContext` from PEM/PKCS12 (TLS 1.3, mTLS) |
| `LocalTrust.java` | `BoundedTokenVerifier`, `HomeAuthorizationProof`, `RelaySessionAuthorizationProof`, source `Endpoint`s, `ClockSafetyMonitor` |
| `LocalDatabases.java` | `SqlTransactions` from a `NativeControlDatabaseEnrollment.Database` |
| `LocalPekkoConfig.java` | Builds the actor Pekko `Config` and the `NativeActorProcessEnrollment` |
| `LocalActorInstaller.java`, `LocalGatewayInstaller.java`, `LocalControlInstaller.java` | One installer per plane |

Modified: `signaling-app/src/main/resources/META-INF/spring.factories` (add initializer), `NativeControlDatabaseResources.java` (expose a reusable open method), `NativeCompatibility.java` and `NativeCompatibilityObservationProducer.java` (mutual-auth key path), `config/native-sources.md` (document the profile), the spec (§4.1 file list).

Tests: unit tests under `signaling-app/src/test/java/io/webrtc/signaling/app/runtime/local/`; the composition integration test in `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/LocalRuntimeCompositionIT.java`, reusing `test-only-pki` fixtures.

---

### Task 1: Process profile, contract loader and activation gate

**Files:**
- Create: `LocalProcessProfile.java`, `LocalRuntimeContract.java`, `LocalRuntimeInitializer.java` (package above)
- Modify: `signaling-app/src/main/resources/META-INF/spring.factories`
- Test: `signaling-app/src/test/java/io/webrtc/signaling/app/runtime/local/LocalRuntimeContractTest.java`, `LocalRuntimeInitializerTest.java`

**Interfaces:**
- Produces: `LocalProcessProfile` record `(int schemaVersion, Plane plane, UUID podUid, String podIp, int businessPort, int healthPort, int relayPort, String azRole, String gatewayId, UUID gatewayBootId, Wal wal, Compatibility compatibility, Kube kube, List<TransferCell> transferCells, String privacyPseudonymKey, List<String> allowedOrigins)`; nested `Wal(String systemIdentifier, long timeline, Set<String> replicas, int synchronousReplicas, long maximumLagBytes, Duration freshness, Duration maximumReplyAge)`, `Compatibility(int major, int minor, String executableId, Set<String> features, int lowerMinor, int upperMinor)`, `Kube(String apiHost, int apiPort, String caPath, String tokenPath, String namespace)`, `TransferCell(String cell, long storageEpoch, NativeControlDatabaseEnrollment.Database database)`. `Plane` reuses `SignalingApplication.Plane`.
- Produces: `LocalRuntimeContract.load(Environment env): LocalRuntimeContract` exposing `contract(): MountedNativeContract`, `profile(): LocalProcessProfile`, `directory(): Path`, `processBoot(): UUID` (fresh), `resolve(String path): Path` (confined).
- Produces: `LocalRuntimeInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext>`; method `static boolean active(Environment)` is the gate.

- [ ] **Step 1: Write failing tests.** `LocalRuntimeContractTest`: `rejectsPodUidDifferentFromEnvironment`, `rejectsCellDifferentFromEnvironment`, `rejectsRelativeEscapingAndSymlinkPaths`, `rejectsUnknownAndDuplicateProfileFields`, `generatesFreshProcessBootEachLoad` (two loads differ). `LocalRuntimeInitializerTest`: `registersNothingWithoutAllThreeMarkers` (mode `production`, missing acknowledgement, wrong contract environment each leave the context without any `Native*Enrollment`), `plainContextStillFailsWithNativeRuntimeNotInstalled`. Use a temp directory with a minimal valid contract produced by the existing `MountedNativeContractTest.valid()` pattern.
- [ ] **Step 2: Run** `./mvnw -B -pl signaling-app -am test -Dtest='LocalRuntime*Test' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL (classes missing).
- [ ] **Step 3: Implement** the three classes. Parse the profile with the same strict Jackson configuration as `MountedNativeContract` (duplicate detection, depth ≤ 8, trailing tokens fail); reuse `MountedNativeContract.load(file, "local-minikube", "LOCAL_TEST_ONLY")`; confine every path with `toRealPath()` startsWith the real contract directory. `LocalRuntimeInitializer.initialize` does nothing unless `active(env)`; when active it loads the contract once and calls the plane installer (stubbed with `UnsupportedOperationException` until Tasks 4–6). Register the initializer in `spring.factories` under `org.springframework.context.ApplicationContextInitializer`.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Commit** `feat(runtime): add local process profile and activation gate`.

---

### Task 2: TLS, trust and database building blocks

**Files:**
- Create: `LocalTls.java`, `LocalTrust.java`, `LocalDatabases.java`
- Modify: `NativeControlDatabaseResources.java` (make its private `create(Database)` a package-visible `static SqlTransactions open(Database)`; behavior unchanged)
- Test: `LocalTlsTest.java`, `LocalTrustTest.java` in the unit-test package above; `LocalDatabasesIT.java` in `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/`

**Interfaces:**
- Produces: `LocalTls.nettyServer(Path ca, Path certificate, Path privateKey): SslContext`; `nettyClient(...)`; `jdkContext(Path pkcs12, char[] password, Path trustStore, char[] trustPassword): SSLContext`; `jdkTrustOnly(Path ca): SSLContext`. All force `TLSv1.3`.
- Produces: `LocalTrust.tokens(MountedNativeContract c, Resolver r): BoundedTokenVerifier` (uses `TrustedRsaKeys(Map<String,RSAPublicKey>, null, Duration)`, `Rs256TokenVerifier(contract.identity(), keys, 8192)`, `new BoundedTokenVerifier(verifier, 1, 8, Duration.ofSeconds(1))`); `homeProof(...): HomeAuthorizationProof`; `relayProof(...): RelaySessionAuthorizationProof`; `ed25519Keys(Map<String,String> paths): Map<String,PublicKey>`; `clockMonitor(profile, contract, processBoot): ClockSafetyMonitor`; `endpoint(String url, SSLContext tls, Map<String,PublicKey> keys): NativeActorSourceEnrollment.Endpoint`.
- Produces: `LocalDatabases.open(NativeControlDatabaseEnrollment.Database d): SqlTransactions`.

- [ ] **Step 1: Write failing tests.** `LocalTlsTest.rejectsTlsOlderThan13`, `requiresClientCertificateOnServer` (handshake without a client certificate fails), `rejectsCertificateWithoutExactlyOneSpiffeUri` for RPC contexts. `LocalTrustTest.verifiesTokenSignedByConfiguredKidAndRejectsUnknownKid`, `homeProofRoundTripsWithOwnKeyOnly`. `LocalDatabasesIT.opensPoolsAndRunsBoundedTransaction` against a Testcontainers PostgreSQL 17.6 (`transaction_timeout` requires ≥ 17).
- [ ] **Step 2: Run** `./mvnw -B -pl signaling-app -am test -Dtest='LocalTlsTest,LocalTrustTest'`; Expected: FAIL.
- [ ] **Step 3: Implement** the three classes by composing existing constructors listed under Interfaces; keys are PKCS#8 PEM (RSA/Ed25519) and X.509 PEM. No new crypto code.
- [ ] **Step 4: Run** the unit command, then `./mvnw -B -pl signaling-integration-tests -am verify -Dtest=NoSelectedTests -Dit.test=LocalDatabasesIT -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(runtime): add local TLS, trust and database owners`.

---

### Task 3: Actor Pekko configuration and the mutual-auth key path

**Files:**
- Create: `LocalPekkoConfig.java`
- Modify: `NativeCompatibility.java:161`, `NativeCompatibilityObservationProducer.java:77` (read the effective key path)
- Test: `LocalPekkoConfigTest.java`; extend `NativeCompatibilityTest` if present, otherwise add `NativeCompatibilityMutualAuthTest.java` in the same package as `NativeCompatibility`

**Interfaces:**
- Produces: `LocalPekkoConfig.build(LocalRuntimeContract c, Environment env): Config` and `process(LocalRuntimeContract c, Config config, SSLContext server, SSLContext client): NativeActorProcessEnrollment` (system name `signaling-<cell>`).
- The Config contains: `application-actor.conf` + defaults; roles `["signaling-actor", profile.azRole]`; `signaling.cell-id`, `signaling.cluster-fingerprint` (from `SIGNALING_CLUSTER_FINGERPRINT`); `signaling.compatibility.{major,minor,executable-id,features}` and `signaling.compatibility.window.{major,lower-minor,upper-minor}` (`upper-minor = lower-minor + 1`); `pekko.discovery.kubernetes-api.{pod-namespace,pod-label-selector,api-ca-path,api-token-path,api-service-host-env-name,api-service-port-env-name}`; Artery canonical hostname = `profile.podIp`; formation: `signaling.controlled-initial-formation=true` and `new-cluster-enabled=on` when `SIGNALING_FORMATION_PROFILE=initial-six`, otherwise defaults.
- Consumes: Task 1 contract/profile; Task 2 `LocalTls.jdkContext`.

- [ ] **Step 1: Write failing tests.** `LocalPekkoConfigTest.passesProductionValidation` (`ShardingBootstrap.validateProduction(config)` does not throw), `exposesCompatibilityRolesForOwnBuild`, `initialSixEnablesControlledFormationOnly`, `rejectsConfigWithoutAzRole`, `canonicalHostnameEqualsPodIp`. `NativeCompatibilityMutualAuthTest.permitsWhenMutualAuthIsSetUnderConfigSslEngine` (builds the Config with `ssl.config-ssl-engine.require-mutual-authentication=on` and no `ssl.config` key; expects `permits()` true — red today if the latent bug is real; if it is already green, record that in the commit message and keep the test).
- [ ] **Step 2: Run** `./mvnw -B -pl signaling-app -am test -Dtest='LocalPekkoConfigTest,NativeCompatibilityMutualAuthTest'`; Expected: FAIL.
- [ ] **Step 3: Implement** `LocalPekkoConfig` with `ConfigFactory.parseResources("application-actor.conf")` layered under programmatic overrides rendered via `ConfigValueFactory`; fix the two readers to try `...ssl.config-ssl-engine.require-mutual-authentication` first and fall back to `...ssl.config.require-mutual-authentication` so existing fixtures keep passing.
- [ ] **Step 4: Run** the same command plus the existing `NativeCompatibility*` and `ShardingBootstrap*` tests in `signaling-actors`/`signaling-app`; Expected: PASS.
- [ ] **Step 5: Commit** `feat(runtime): build local actor Pekko configuration`.

---

### Task 4: Actor installer

**Files:**
- Create: `LocalActorInstaller.java`
- Modify: `LocalRuntimeInitializer.java` (dispatch ACTOR)
- Test: `LocalActorInstallerTest.java`

**Interfaces:**
- Produces: `LocalActorInstaller.install(GenericApplicationContext ctx, LocalRuntimeContract c)`. Registers singletons for: `NativeActorProcessEnrollment`, `NativeActorSourceEnrollment(cell, storageEpoch, podUid, processBoot, contract.identity(), clock Endpoint, revocations Endpoint)`, `NativeActorBusinessEnrollment` (after the `ActorSystem` exists; `homes` is a function returning `TrustedHome(cell, directoryEpoch, storageEpoch)` resolved from the directory snapshot), `NativeActorIngressEnrollment`, `NativeActorSchedulingEnrollment(system, Set.of(azRole), fingerprint, List.of(), events, healthAddress, live, metrics)`, `NativeActorMaintenanceEnrollment(privacyPseudonymKey, detachment)`, `NativeWalEnrollment` from `profile.wal` with `localSingleWriter=false`, `NativeCellRpcEnrollment` from `contract.cells()`, `NativeCompatibilityObservationEnrollment`, `NativeCompatibilityEnrollment`, a `ClusterReadiness.localMinikube("LOCAL_TEST_ONLY")` bean, and the cell `SqlTransactions`.
- Consumes: Tasks 1–3.

- [ ] **Step 1: Write failing tests.** `registersEveryActorEnrollmentExactlyOnce` (asserts the set of bean types listed above, each present once), `buildsNoSecondCellCredential` (the actor never opens `directoryDatabase`), `homesResolveFromSnapshotAndRejectUnknownBucket`, `refusesWhenWalReplicaSetIsEmpty` (profile with empty `replicas` and `synchronousReplicas=0` is rejected because `localSingleWriter` is not allowed here).
- [ ] **Step 2: Run** `./mvnw -B -pl signaling-app -am test -Dtest=LocalActorInstallerTest`; Expected: FAIL.
- [ ] **Step 3: Implement** the installer by calling the existing record constructors; follow `NativeActorProcessIT.enrollment()` and `NativeActorCompositionIT` for argument values (admission sizes `RpcAdmission(8, 1MiB, 8, 1MiB)`, DB quotas from the contract). `homes` reads the directory snapshot declared by `contract.directorySnapshot()`; verify its SHA-256 equals `directorySnapshotSha256`.
- [ ] **Step 4: Run** the test; Expected: PASS.
- [ ] **Step 5: Commit** `feat(runtime): install actor-plane native owners`.

---

### Task 5: Gateway installer

**Files:**
- Create: `LocalGatewayInstaller.java`
- Modify: `LocalRuntimeInitializer.java` (dispatch GATEWAY)
- Test: `LocalGatewayInstallerTest.java`

**Interfaces:**
- Produces: `LocalGatewayInstaller.install(GenericApplicationContext ctx, LocalRuntimeContract c)` registering `NativeGatewayBusinessEnrollment(GatewayIdentity(gatewayId, gatewayBootId, cell, storageEpoch, region), routingEpoch, tokens, cachedSecurity, securityFresh, homes, relayProofs, relayCacheCapacity≤4096, relayPendingLimit≤64)`, `NativeGatewayIngressEnrollment` (WSS address `0.0.0.0:businessPort`, `UpgradePolicy(profile.allowedOrigins, …)`, `rpcEnvironment="local-test-only"`, `rpcPort=relayPort`), `NativeGatewayLifecycleEnrollment(healthAddress=podIp:healthPort, …)`, `NativeCellRpcEnrollment`, plus the `ClockSafetyMonitor`, `NativeClockSource`, `NativeCachedSecurity` and `NativeCachedRevocationSource` beans with a `NativeWorkerScheduler` job (gap 6 in the exploration: no gateway wiring exists today).
- Consumes: Tasks 1–2.

- [ ] **Step 1: Write failing tests.** `registersGatewayEnrollmentsAndSourceBeans`, `wssPathIsExactlyWsAndSubprotocolIsV1` (inspects the built `UpgradePolicy`), `healthAddressIsConcreteNotWildcard`, `refusesProfileWithTransferCells` (Review Focus 4), `gatewayBootIdComesFromProfileNotRandom`.
- [ ] **Step 2: Run** `./mvnw -B -pl signaling-app -am test -Dtest=LocalGatewayInstallerTest`; Expected: FAIL.
- [ ] **Step 3: Implement** using `NativeGatewayCommandIT.mainGateway` as the recipe for argument values; `cachedSecurity` is `NativeCachedSecurity` fed by `NativeCachedRevocationSource`, never `alwaysAllowed`.
- [ ] **Step 4: Run** the test; Expected: PASS.
- [ ] **Step 5: Commit** `feat(runtime): install gateway-plane native owners`.

---

### Task 6: Control installer, bucket-transfer owner and control compatibility

**Files:**
- Create: `LocalControlInstaller.java`
- Modify: `LocalRuntimeInitializer.java` (dispatch CONTROL); `NativeCellRpcConfiguration.java` only if control needs a `CellRpcClient` (spec §8 risk) — otherwise provide a `NativeCompatibilityEnrollment` bean from the installer
- Test: `LocalControlInstallerTest.java`

**Interfaces:**
- Produces: `LocalControlInstaller.install(GenericApplicationContext ctx, LocalRuntimeContract c)` registering `NativeControlDatabaseEnrollment(regional=directoryDatabase, local=cellDatabase, cell, storageEpoch, () -> true)`, `NativeControlSourceEnrollment`, `NativeControlBusinessEnrollment(PostgresDirectoryRepository(regional, local, cell, storageEpoch, admitted), tokens, cachedSecurity, securityFresh, clockMonitor, Duration.ofSeconds(30))`, `NativeControlIngressEnrollment(address, tls, healthAddress, live, metrics)`, `NativeBucketTransferEnrollment(Store(sqlC001,"c001",e1), Store(sqlC002,"c002",e2))` built from `contract.cellDatabase()` and `profile.transferCells()` (distinct `SqlTransactions`, distinct cells), and a compatibility enrollment so `controlReady()` can become true.
- The control `clock` monitor and `NativeClockSource` must monitor the same `ClockSafetyMonitor` instance (`NativeControlProcess.java:59` checks identity).

- [ ] **Step 1: Write failing tests.** `registersControlEnrollmentsWithTwoDistinctTransferStores`, `rejectsTransferCellEqualToOwnCell`, `controlClockMonitorIsTheSameInstanceAsTheSource`, `controlCompatibilityCanOpenWithoutTestOnlyCapabilities` (asserts no class from `src/test` is referenced: reflection over bean class names does not contain `TestOnly`).
- [ ] **Step 2: Run** `./mvnw -B -pl signaling-app -am test -Dtest=LocalControlInstallerTest`; Expected: FAIL.
- [ ] **Step 3: Implement** following `NativeControlMainIT` lines 93–163; the compatibility enrollment uses the profile's `compatibility` block and the `authenticatedRemoteActors` supplier from `NativeCompatibilityCollectors` rather than constants.
- [ ] **Step 4: Run** the test; Expected: PASS.
- [ ] **Step 5: Commit** `feat(runtime): install control-plane native owners`.

---

### Task 7: Composition integration test (one cell, real PostgreSQL)

**Files:**
- Create: `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/LocalRuntimeCompositionIT.java`; test helper `LocalCompositionFixture.java` in the same package (generates contract + profiles into a temp directory using `test-only-pki`; a test-only in-process signed clock/revocation source ported from `NativeGatewayCommandIT.MainGateway.refreshClock`)

**Interfaces:**
- Consumes: Tasks 1–6 through `SignalingApplication.application(...)` with `SIGNALING_DEPLOYMENT_MODE`/`SIGNALING_LOCAL_ACKNOWLEDGEMENT` set and only the mounted files provided — no `registerSingleton` in the test.

- [ ] **Step 1: Write the failing test** `oneCellStartsAllPlanesFromMountedFilesOnly`: six actor contexts on loopback addresses `127.0.0.11`–`127.0.0.16` (distinct Artery canonical hosts, same ports; `az-a`×3, `az-b`×3), one gateway, one control, plus a second empty cell database for the transfer owner. Assertions: every context starts without `MissingRuntime`; six members reach `Up`; each actor `/ready` returns 200 within 60 s; gateway `GET /ready` 200; control `POST /v1/signaling/bootstrap` with a token signed by the fixture key returns a `wssUrl` for a seeded bucket; a second test `readinessStaysFalseWithOneAzRole` (all six actors share one `az-*` role): no context crashes, `/ready` is 503, and the logged reason names the missing zone (Review Focus 5).
- [ ] **Step 2: Run** `./mvnw -B -pl signaling-integration-tests -am verify -Dtest=NoSelectedTests -Dit.test=LocalRuntimeCompositionIT -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false`; Expected: FAIL (before Tasks 1–6 are complete) then iterate until PASS.
- [ ] **Step 3: Fix** only defects revealed in the installers or fixtures; never relax `NativeRuntimeStartup`, fencing, TTL or admission to pass.
- [ ] **Step 4: Run** the full affected set: `./mvnw -B verify -pl signaling-app,signaling-integration-tests -am -Dspotless.check.skip=true -Dsurefire.rerunFailingTestsCount=2`; Expected: BUILD SUCCESS. Record that machine load (see `uptime`) can flake 2 s DB deadlines.
- [ ] **Step 5: Commit** `test(runtime): compose one local cell from mounted files only`.

---

### Task 8: Image contract, documentation and spec alignment

**Files:**
- Modify: `qualification/scenarios/image-contract.sh` (add a case: image with the local markers and a valid mounted contract passes `requireInstalled`; image without markers still exits with `NATIVE_RUNTIME_NOT_INSTALLED`), `config/native-sources.md` (new "Local process profile" section: file name, fields, confinement rules, `LOCAL_TEST_ONLY` only), `docs/superpowers/specs/2026-10-21-docker-compose-two-cell-design.md` (§4.1: list `process.json` and `SIGNALING_LOCAL_PROCESS_PROFILE`)
- Test: `qualification/scenarios/tests` (extend the image-contract regression if it exists)

- [ ] **Step 1: Write the failing check** in `image-contract.sh`: the no-marker case still expects exit 1 with `NATIVE_RUNTIME_NOT_INSTALLED`; the marker case runs the jar in a temp mount directory and expects the process to pass plane installation (it may then wait on dependencies; assert on the absence of `NATIVE_RUNTIME_NOT_INSTALLED` within 20 s).
- [ ] **Step 2: Run** `bash qualification/scenarios/image-contract.sh`; Expected: the new case FAILS before Task 1–6 merge, PASSES after.
- [ ] **Step 3: Update** the docs and spec text; no behavior change.
- [ ] **Step 4: Run** `python3 -m unittest discover -s qualification/scenarios/tests` and the script again; Expected: PASS.
- [ ] **Step 5: Commit** `docs(runtime): document local process profile and image contract`.

---

## Spec coverage

| Spec section | Task |
|---|---|
| §4.1 contract mount, confinement, markers | 1 |
| §4.2 environment variables incl. `SIGNALING_POD_UID`, `SIGNALING_FORMATION_PROFILE` | 1, 3 |
| §4.3 items 1, 3 (enrollments, verifiers) | 4, 5, 6, 2 |
| §4.3 item 2 (actor DB owners) | 2, 4 |
| §4.3 item 4 (Pekko Config, roles, compat keys, discovery) | 3 |
| §4.3 item 5 (`ClusterReadiness.localMinikube`) | 4 |
| §4.3 items 6–7 (gateway/control sources and compatibility) | 5, 6 |
| §4.3 item 8 (transfer owner, two cells) | 6 |
| §4.5 WAL enrollment, `localSingleWriter=false` | 4 |
| §8 mutual-auth path risk | 3 |
| §8 control `CellRpcClient` risk | 6 |
| §9 criterion 4 (reject bad config) and 5 (no test-only classes) | 1, 6, 7 |

Out of this plan (other sub-projects): source service (SP2), PKI generation, migrate/seed, fake kube-API, compose file, smoke script (SP3).
