# Local Platform (SP2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `signaling-local-platform`, a standalone LOCAL_TEST_ONLY process that serves the signed clock report and the signed durable revocation log over mTLS, and accepts admin revocations over a separate mTLS role.

**Architecture:** A plain-Java executable (JDK `HttpsServer`, JDBC, Flyway; no Spring) with three listeners: source (9443, workload mTLS, TLS 1.3), admin (9444, separate CA and role) and a plain-HTTP `/ready` (8559, concrete address). A caller is identified by the single SPIFFE URI SAN of its certificate; headers never establish identity. The clock figures come from a `HostClockObserver` that measures wall-versus-monotonic drift inside this container; the revocation log is append-only in the `source` database of `pg-directory`. Wire shapes reuse `ClockSafetyMonitor.Report` and `RevocationReconciler.Batch`, and both are signed with the existing `signingBytes` functions.

**Tech Stack:** Java 21, JDK `com.sun.net.httpserver.HttpsServer`, PostgreSQL 17.6 (Testcontainers), Flyway, Jackson, Ed25519 (`java.security`), JUnit 5, Maven wrapper (`./mvnw -o … -am`).

**Spec:** `docs/superpowers/specs/2026-10-21-docker-compose-two-cell-design.md` (§4.4 primary; §4.1, §4.5, §4.6, §8 for ports, DB, PKI, risks). Wire contract: `config/native-sources.md`. Reference: `docs/superpowers/plans/2026-10-13-minikube-authority-runtime.md` (A1, reduced A2). SP1 ledger: `docs/superpowers/execution/2026-10-21-local-runtime-adapter.md`.

## Global Constraints

- Status stays `LOCAL_TEST_ONLY` and `NOT_QUALIFIED`; no claim of production readiness. The process refuses to start unless `SIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY`.
- Source wire: GET only; headers `X-Signaling-Pod-Uid`, `X-Signaling-Process-Boot`, `X-Signaling-Source-Offset` (UUID, UUID, non-negative decimal long); paths `/v1/clock-bound` and `/v1/revocations`; response `application/json`, exact `Content-Length`, `Connection: close`, no redirect/chunked/compression, body ≤ 32768 bytes.
- TLS 1.3 only, client certificate required. Caller identity = exactly one URI SAN matching `spiffe://signaling/local-test-only/cell/<cell>/(actor|gateway|control)(/<id>)?`, cell in the configured set; admin identity = exactly one URI SAN `spiffe://signaling/local-test-only/admin/<name>` on the admin listener only.
- Clock report: Ed25519 over `ClockSafetyMonitor.signingBytes(report)`, 86-char unpadded base64url; validity ≤ 5 s (this plan uses 2 s); `pairUncertaintyMicros` ≤ 250000 and `relativeRateErrorPpm` ≤ 1000 only when measured; otherwise `continuous=false` (an unsafe report is signed and returned, never a constant-safe one).
- Revocation page: `RevocationReconciler.Batch` JSON (seven fields), ≤ 16 events, `sourceProof = <keyId>.<86-char b64url>` over `RevocationSourceVerifier.signingBytes(cell, issuer, batch)`; key id matches `[A-Za-z0-9_-]{1,64}` (this plan: `local-source`); offsets and high-water monotonic; the log is append-only.
- Never log or print key material, passwords, JWTs or SQL parameters. Failures print `LOCAL_PLATFORM_REJECTED <category>` only.
- Formatting: google-java-format for new files only; never run `spotless:apply` repo-wide. Build with `./mvnw -o … -am`; failsafe ITs by fully qualified `-Dit.test=io.webrtc.signaling.localplatform.<Name>` and `-Dspotless.check.skip=true`.
- Identity values (issuer, audience) are never defaulted; empty or unresolved values fail closed. Fixtures use reserved addresses and `TEST_ONLY` markers.

## Rulings made while planning

- Flyway location `classpath:db/source/migration` with history table `source_schema_history` (the storage jar already owns `db/migration`; sharing it would run cell migrations against `source`). The platform runs its own migration at startup as the `source` database owner.
- Event epoch semantics: `RevocationState` marks a token revoked when `storeEpoch >= token.securityEpoch`; the identity service issues no `securityEpoch` claim (read as 0), and a store with no row returns −1. A revoke event therefore needs `epoch >= 0` to revoke an unrevoked user. The admin default epoch is `latest(subject)+1`, or `0` when none. Revoking a user is permanent in this environment because identity cannot re-issue a higher epoch. Recorded in `config/native-sources.md` (Task 8).
- Key retirements are not modelled (YAGNI): every page returns `retiredKeys: []`.
- `sequence` for clock reports is `max(previous+1, epochMicros(now))` per pod, so a restarted source never emits a sequence below one the actor's monitor already accepted.

## Review Focus

Each line is pinned by a test in the owning task.

1. Token with `securityEpoch` 0 and a user with no events: not revoked; after an admin revoke (epoch 0) the same token is revoked (Task 5, Task 7).
2. Concurrent appends: offsets stay gapless and a reader never sees offset *n+1* before *n* (Task 5).
3. Source restart with the caller's boot unchanged: new sequences still exceed earlier ones (Task 4).
4. A valid certificate B presenting the pod UID registered to certificate A is rejected, and a stale boot after a newer one is rejected (Task 3).
5. Wall-clock step, NTP-like slew above 1000 ppm, a stall, or a cold start: report is signed with `continuous=false` (Task 2).
6. Malformed requests (duplicate or non-UUID header, negative or oversized offset, offset above high-water, non-GET, unknown path) yield 4xx with no body detail; an admin certificate on the source port and a workload certificate on the admin port are refused (Task 6).

## File Structure

`signaling-local-platform/` (package `io.webrtc.signaling.localplatform`):

| File | Responsibility |
|---|---|
| `pom.xml` | module, repackage with classifier `exec`, `mainClass` |
| `LocalPlatformApplication.java` | `main`, `start(LocalPlatformConfig) -> Running` (listeners, DB, observer), `Running.close()` |
| `LocalPlatformConfig.java` | strict JSON config + env, fail-closed validation |
| `LocalPlatformFailure.java` | category-only startup failure and its stderr rendering |
| `HostClockObserver.java` | sampling and measurement of wall-vs-monotonic drift |
| `PeerIdentity.java` | SPIFFE URI SAN parsing for workload and admin roles |
| `BootRegistry.java` | `register(peer, pod, boot)`: binding rules |
| `SourceSigner.java` | Ed25519 key load and signing (clock and revocation) |
| `ClockReportService.java` | builds and signs a report for a registered caller |
| `RevocationLog.java`, `JdbcRevocationLog.java` | append and page over PostgreSQL |
| `RevocationPageService.java` | page → signed `Batch` JSON |
| `SourceHandler.java`, `AdminHandler.java`, `TlsContexts.java`, `ReadyHandler.java` | HTTP layer |
| `src/main/resources/db/source/migration/V001__security_source.sql` | log, counter, append-only trigger |
| `Dockerfile.local-platform` (repo root) | image, UID 10001 |

Tests mirror under `src/test/java`; integration tests in `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/`.

---

### Task 1: Module skeleton, config, fail-closed start

**Files:**
- Create: `signaling-local-platform/pom.xml`, `LocalPlatformConfig.java`, `LocalPlatformFailure.java`, `LocalPlatformApplication.java` (main only, `start` throws `UnsupportedOperationException` until Task 6)
- Modify: `pom.xml` (add `<module>signaling-local-platform</module>` after `signaling-identity`)
- Test: `LocalPlatformConfigTest.java`

**Interfaces:**
- Produces:
  - `LocalPlatformConfig.load(Map<String,String> env) -> LocalPlatformConfig` (reads `SIGNALING_PLATFORM_CONFIG`, an absolute path to JSON; also `SIGNALING_LOCAL_ACKNOWLEDGEMENT`, `SIGNALING_IDENTITY_ISSUER`).
  - Accessors: `issuer()`, `cells() -> Map<String,Long>` (cell → storageEpoch), `keyId()`, `signingKeyFile()` (Path), `source()`/`admin()` each `Listener(InetAddress bind, int port, Path keystore, Path keystorePasswordFile, Path clientTrust)`, `health() -> (InetAddress, int)`, `database() -> (jdbcUrl, user, Path passwordFile)`, `clockValidity() -> Duration` (fixed 2 s, not configurable).
  - `LocalPlatformFailure(Category) extends RuntimeException` with `enum Category { ACKNOWLEDGEMENT, CONFIG, IDENTITY, KEY, TLS, DATABASE }`; `message()` is the category name only.
- JSON keys (all required, unknown or duplicate keys rejected, paths absolute, bind addresses concrete — `0.0.0.0` and `::` rejected): `cells` (object cell→storageEpoch ≥ 1, 1..8 entries, names `[a-z][a-z0-9-]{0,23}`), `keyId`, `signingKey`, `source`, `admin`, `health`, `database`.

- [ ] **Step 1: Write failing tests** in `LocalPlatformConfigTest`: `missingAcknowledgementIsRejected` (category `ACKNOWLEDGEMENT`), `wrongAcknowledgementValueIsRejected`, `blankIssuerIsRejected` (`IDENTITY`), `relativePathIsRejected`, `wildcardBindIsRejected`, `unknownKeyIsRejected`, `duplicateKeyIsRejected`, `storageEpochZeroIsRejected`, `validConfigLoads` (writes a `TEST_ONLY` JSON into a `@TempDir` using documentation addresses `192.0.2.x`; asserts accessors). Assert `toString()` of config contains no path or password text.
- [ ] **Step 2: Run to verify failure:** `./mvnw -o -pl signaling-local-platform -am test -Dtest=LocalPlatformConfigTest` → FAIL (module missing).
- [ ] **Step 3: Implement** the pom (dependencies: `signaling-auth`, `signaling-storage`, `signaling-protocol`, `jackson-databind`, `postgresql`, `flyway-core`, `flyway-database-postgresql`; test: `junit-jupiter`, `assertj`; repackage config copied from `signaling-identity/pom.xml` plus `<mainClass>io.webrtc.signaling.localplatform.LocalPlatformApplication</mainClass>`), the module entry, and the three classes. Strict parsing: Jackson with `FAIL_ON_UNKNOWN_PROPERTIES`, `STRICT_DUPLICATE_DETECTION`, `FAIL_ON_TRAILING_TOKENS`. `main` catches `LocalPlatformFailure` and prints `LOCAL_PLATFORM_REJECTED <category>` to stderr, exit 1; any other throwable prints `LOCAL_PLATFORM_REJECTED CONFIG` with no detail.
- [ ] **Step 4: Run tests:** same command → PASS.
- [ ] **Step 5: Commit** `feat(local-platform): add module skeleton with fail-closed LOCAL_TEST_ONLY config`.

### Task 2: Measured host clock

**Files:** Create `HostClockObserver.java`; Test `HostClockObserverTest.java`.

**Interfaces:**
- Produces:
  - `HostClockObserver(LongSupplier monotonicNanos, Supplier<Instant> wall, Duration interval)`; `void sample()` (one bracketed reading); `start()`/`close()` run `sample()` on one daemon scheduler thread every `interval` (production interval 100 ms).
  - `Measurement snapshot()` where `record Measurement(Instant observedAt, int pairUncertaintyMicros, int relativeRateErrorPpm, boolean continuous)`.
- Algorithm (not determined by the signature): each sample reads `n0 = mono; w = wall; n1 = mono`; uncertainty = ceil((n1−n0)/2 in µs) + 1; offset = w − midpoint(n0,n1) in ns. Keep samples for the last 5 s. `pairUncertaintyMicros` = max uncertainty in the window; `relativeRateErrorPpm` = ceil(|offsetLast − offsetFirst| / (monoLast − monoFirst) × 10⁶) over the window; `continuous` = window spans ≥ 2 s, newest sample is ≤ 500 ms old, and no consecutive offset jump > 50 ms occurred in the last 5 s. With no qualifying window: `continuous=false`, `pairUncertaintyMicros = 1_000_000`, `relativeRateErrorPpm = 1_000_000`. Cap both values at `Integer.MAX_VALUE`.

- [ ] **Step 1: Write failing tests** with a scripted fake monotonic/wall pair advancing by 100 ms per `sample()`: `coldStartIsNotContinuous` (fewer than 2 s of samples → `continuous=false`); `steadyClockIsContinuousWithinLimits` (after 3 s: `continuous`, uncertainty ≤ 250000, rate ≤ 1000); `wallStepBackwardMakesReportUnsafe` (wall jumps −200 ms → not continuous for the next 5 s of samples, then recovers); `wallStepForwardIsUnsafe`; `slewAbove1000PpmIsReportedAsRateError` (wall runs 0.2 % fast → `relativeRateErrorPpm > 1000`); `slewAt500PpmStaysWithinLimit`; `stallRaisesPairUncertainty` (one reading with n1−n0 = 600 ms → uncertainty > 250000 while that sample is in the window); `staleSnapshotIsNotContinuous` (no sample for 600 ms of fake mono time).
- [ ] **Step 2: Run** `-Dtest=HostClockObserverTest` → FAIL.
- [ ] **Step 3: Implement** `HostClockObserver` with an immutable-snapshot design (a `volatile` reference to the latest `Measurement`; the sample window guarded by one lock). `snapshot()` computes staleness against the injected monotonic supplier.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(local-platform): measure wall-versus-monotonic drift instead of declaring clock quality`.

### Task 3: Caller identity and boot registry

**Files:** Create `PeerIdentity.java`, `BootRegistry.java`; Test `PeerIdentityTest.java`, `BootRegistryTest.java`.

**Interfaces:**
- Produces:
  - `PeerIdentity.fromCertificate(X509Certificate, Set<String> cells) -> PeerIdentity` (`record PeerIdentity(String spiffeId, String cell, String plane, String instance)`, `plane ∈ {actor, gateway, control}`); throws `IllegalArgumentException` when the certificate has zero or several URI SANs, a different trust domain or path shape, or a cell outside `cells`. `PeerIdentity.admin(X509Certificate) -> String` returns `<name>` from `spiffe://signaling/local-test-only/admin/<name>` (`[a-z0-9-]{1,32}`), same strictness.
  - `BootRegistry.register(PeerIdentity peer, UUID pod, UUID boot) -> Registration` with `record Registration(PeerIdentity peer, UUID pod, UUID boot)`; throws `BootRejected` (checked, no detail) when: the pod is already bound to a different `spiffeId`; or `boot` equals an earlier boot of that pod that a newer boot replaced. A new boot for the same pod and same identity replaces the old one. At most 4096 pods are tracked; the next new pod beyond that is rejected.
- Consumes: JDK `X509Certificate.getSubjectAlternativeNames()`.

- [ ] **Step 1: Write failing tests.** `PeerIdentityTest`: commit `TEST_ONLY` PEM fixtures under `src/test/resources/pki/` produced once with `openssl` (workload CA, admin CA, and leaf certificates covering each case below, including zero and two URI SANs), plus a `README` line stating they are `TEST_ONLY`; never use `sun.security.x509`. Cases: `actorUriSanParses`, `gatewayWithInstanceParses`, `controlParses`, `noUriSanRejected`, `twoUriSansRejected`, `foreignTrustDomainRejected`, `unknownCellRejected`, `adminPathOnWorkloadParserRejected`, `workloadPathOnAdminParserRejected`. `BootRegistryTest`: `firstRegistrationSucceeds`, `sameBootAgainSucceeds`, `newBootReplacesOld`, `staleBootAfterNewerIsRejected`, `podBoundToOtherIdentityIsRejected` (Review Focus 4), `registryIsBounded`.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement.** Registry state behind one lock; retired boots kept in a bounded per-pod set (last 8).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(local-platform): bind pod and boot to the certificate identity, not to headers`.

### Task 4: Signer and clock report service

**Files:** Create `SourceSigner.java`, `ClockReportService.java`; Test `ClockReportServiceTest.java`.

**Interfaces:**
- Produces:
  - `SourceSigner.load(Path pkcs8PemFile, String keyId) -> SourceSigner` (sign-only: it holds no public key and probe-signs at load); `SourceSigner.of(KeyPair, String keyId)` for tests and in-process harnesses; `keyId()`; `byte[] sign(byte[] message)`; `Optional<PublicKey> publicKey()` (present only for `of(KeyPair, …)`; the public key reaches the consumers through the contract); rejects non-Ed25519 keys (`KEY`).
  - `ClockReportService(SourceSigner signer, HostClockObserver observer, Map<String,Long> storageEpochs, Clock wallClock)`; `String clockBody(BootRegistry.Registration r)` returns the JSON object `{"report": {…}, "signature": "<86 chars>"}` (serialised as `ClockSafetyMonitor.Report` record fields, ISO instants, same shape the existing test fixture emits).
- Report values: `keyId` = signer key id; `cell` = `r.peer().cell()`; `storageEpoch` from config; `podUid`, `processBoot` from the registration; `observedAt` = measurement time truncated to microseconds; `validUntil` = `observedAt + 2 s`; the figures from `observer.snapshot()`, except that `pairUncertaintyMicros` = measured + the fixed `CONSUMER_OBSERVATION_ALLOWANCE_MICROS` (100000, the consumer's own wall/monotonic read jitter, which the source cannot measure; added by the Task 7 ruling) and `continuous` additionally requires the reported uncertainty ≤ 250000 and the rate error ≤ 1000 ppm; `sequence = max(lastSequence[pod]+1, epochMicros(now))`.

- [ ] **Step 1: Write failing tests:** `reportVerifiesWithEd25519AndSigningBytes`; `reportCarriesCallerCellAndConfiguredStorageEpoch`; `sequenceStrictlyIncreasesAcrossCalls`; `sequenceStaysAboveEarlierOnesAfterServiceRestart` (new service instance, same registration, earlier body's sequence smaller — Review Focus 3); `unsafeMeasurementIsSignedAsNotContinuous` (cold observer → `continuous=false`, still verifies); `safeMeasurementIsAcceptedByClockSafetyMonitor` (feed the body through a real `ClockSafetyMonitor` accept path with the signer's public key — read `ClockSafetyMonitor` for the public accept method and keep the test at that seam); `validityNeverExceedsFiveSeconds`; `bodyIsUnder32768Bytes`.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** both classes; JSON via a shared `ObjectMapper` configured like the fixture (`JavaTimeModule`, ISO instants, no timestamps-as-numbers).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(local-platform): sign measured clock reports bound to the enrolled caller`.

### Task 5: Durable revocation log and signed pages

**Files:**
- Create: `src/main/resources/db/source/migration/V001__security_source.sql`, `RevocationLog.java`, `JdbcRevocationLog.java`, `RevocationPageService.java`
- Test: `JdbcRevocationLogIT.java` (failsafe, Testcontainers PostgreSQL 17.6), `RevocationPageServiceTest.java` (unit, in-memory `RevocationLog` fake)

**Interfaces:**
- Migration: `source_state(singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), high_water bigint NOT NULL DEFAULT 0 CHECK (high_water >= 0))` with its one row inserted; `revocation_log(source_offset bigint PRIMARY KEY, user_id text NOT NULL CHECK (octet_length(user_id) BETWEEN 1 AND 1024), jti text NULL CHECK (jti IS NULL OR octet_length(jti) BETWEEN 1 AND 256), epoch bigint NOT NULL CHECK (epoch >= 0), committed_at timestamptz NOT NULL)`; a `BEFORE UPDATE OR DELETE` trigger on `revocation_log` raising an exception (append-only); `TRUNCATE` revoked from public.
- `RevocationLog`:
  - `Appended append(String userId, String jtiOrNull, OptionalLong epoch)`; `record Appended(long offset, long epoch)`; throws `EpochConflict` when an explicit epoch ≤ the latest for that `(user, jti)` subject. Implementation: in one transaction `UPDATE source_state SET high_water = high_water + 1 RETURNING high_water` (this serialises appenders and makes offsets gapless), compute the default epoch as `coalesce(max(epoch) WHERE user_id=? AND jti IS NOT DISTINCT FROM ?, -1) + 1`, insert with `committed_at = now truncated to microseconds`.
  - `Snapshot read(long from, int limit)` in one `REPEATABLE READ READ ONLY` transaction; `record Snapshot(List<RevocationState.Event> events, long currentHighWater, Instant newestCommittedAt)`; throws `CursorAhead` when `from > currentHighWater`.
- `RevocationPageService(SourceSigner signer, RevocationLog log, String issuer, Clock wallClock)`; `String pageBody(String cell, long from)`:
  `events` = up to 16 with offset > `from`; `highWater` = last returned offset, or `currentHighWater` when none are returned (this also covers a caught-up reader, which gets `from == highWater`); `currentSourceHighWater` = `currentHighWater`; `checkedAt = max(now, newestCommittedAt)` truncated to microseconds; `retiredKeys = []`; `sourceProof = keyId + "." + b64url(sign(RevocationSourceVerifier.signingBytes(cell, issuer, unsignedBatch)))`. Events use `issuer`, `UserId`, `jti`, `epoch`, `sourceOffset`, `committedAt`.

- [ ] **Step 1: Write failing tests.** `JdbcRevocationLogIT`: `firstAppendIsOffsetOne`; `defaultEpochIsZeroThenIncrements`; `explicitLowerEpochConflicts`; `updateAndDeleteAreRejectedByTrigger`; `concurrentAppendsAreGaplessAndOrdered` (32 threads × 10 appends; offsets exactly 1..320, and a concurrent reader polling `read(from, 16)` never observes an offset after a gap — Review Focus 2); `readReturnsAtMostLimitAndReportsCurrentHighWater`; `cursorAheadIsReported`; `committedAtHasMicrosecondPrecision` (the value equals what is signed). `RevocationPageServiceTest`: `emptyCaughtUpPageHasEqualOffsets`; `fullPageStopsAtSixteenAndIsNotCaughtUp`; `proofVerifiesWithRevocationSourceVerifier` using the real verifier and the `Batch` constructor invariants; `proofRejectedForAnotherCell` (page signed for `c001` fails the `c002` verifier); `pageJsonRoundTripsThroughTheSameParserTheActorUses` (locate the `NativeRevocationSource` JSON parser and decode `Event.userId` exactly as it does — if its DTO shape differs from Jackson's default record serialisation, serialise to that shape); `revokedUserMakesEpochZeroTokenRevoked` and `unrevokedUserKeepsEpochZeroTokenAllowed` (feed returned events into `RevocationState` with `TestSecurityStore`-style store semantics and an `AuthPrincipal` of epoch 0 — Review Focus 1).
- [ ] **Step 2: Run** `./mvnw -o -pl signaling-local-platform verify -Dit.test=io.webrtc.signaling.localplatform.JdbcRevocationLogIT -Dspotless.check.skip=true -am` (Docker required) → FAIL.
- [ ] **Step 3: Implement.** `JdbcRevocationLog(DataSource)` (`PGSimpleDataSource`; one connection per call). Provide `static void migrate(DataSource)` using Flyway with location `classpath:db/source/migration`, table `source_schema_history`, `cleanDisabled(true)`.
- [ ] **Step 4: Run** unit and IT commands → PASS.
- [ ] **Step 5: Commit** `feat(local-platform): add append-only revocation log and signed pages`.

### Task 6: HTTPS listeners and application wiring

**Files:** Create `TlsContexts.java`, `SourceHandler.java`, `AdminHandler.java`, `ReadyHandler.java`; complete `LocalPlatformApplication.start`; Test `SourceServerIT.java` (failsafe, uses a PG container).

**Interfaces:**
- `TlsContexts.server(Listener) -> SSLContext` (PKCS12 keystore + password file; client trust from `clientTrust` PEM bundle; `TLSv1.3` only; need-client-auth).
- Source routes (`SourceHandler`): `GET /v1/clock-bound` → `ClockReportService.clockBody(registration)`; `GET /v1/revocations` → `RevocationPageService.pageBody(peer.cell(), offset)`. The peer is derived from the TLS session certificate, never from headers. Responses: 200 JSON; `400` for malformed or duplicate headers, bad UUID, offset not `0|[1-9][0-9]{0,18}`; `403` for `BootRejected` or certificate parse failure; `404` unknown path; `405` non-GET; `409` for `CursorAhead`; `503` when the DB is unreachable. Error bodies are empty. Every response sets `Content-Length` and `Connection: close`.
- Admin route (`AdminHandler`): `POST /v1/admin/revocations` with body `{"userId": string, "jti"?: string, "epoch"?: integer}` (strict JSON, body ≤ 4096 bytes, `Content-Type: application/json`) → `201 {"offset": n, "epoch": e}`; `409` on `EpochConflict`; `400` invalid; admin identity required via `PeerIdentity.admin`. The user id is validated with the protocol `UserId` constructor (NFC, no trimming).
- `ReadyHandler` on plain HTTP, `GET /ready`: 200 only when the DB answers `SELECT 1`, migration applied, and `observer.snapshot().continuous()`; else 503; bound to the configured concrete address.
- `LocalPlatformApplication.start(LocalPlatformConfig) -> Running` (`Running` is `AutoCloseable`, exposes `sourcePort()`, `adminPort()`, `healthPort()` for tests; port `0` allowed in tests only through the test config path).

- [ ] **Step 1: Write failing tests in `SourceServerIT`** with generated `TEST_ONLY` PKI (reuse the Task 3 resources plus an admin CA): `clockEndpointReturnsSignedReportForEnrolledActor`; `revocationEndpointReturnsEmptyCaughtUpPage`; `adminAppendThenSourceReturnsEvent`; `clientWithoutCertificateIsRejected`; `tls12ClientIsRejected`; `adminCertificateOnSourcePortIsRefused` and `workloadCertificateOnAdminPortIsRefused`; `podOfAnotherIdentityIsForbidden` (Review Focus 4 over the wire); `malformedRequestsGet4xxWithEmptyBody` (non-UUID pod, duplicate `X-Signaling-Pod-Uid`, offset `-1`, offset `99999999999999999999`, `POST` on source, unknown path, offset above high-water → 409); `responsesAreFramedWithContentLengthAndClose`; `readyIs503UntilObserverWarmsThen200`; `startupRefusesWithoutAcknowledgement`.
- [ ] **Step 2: Run** `-Dit.test=io.webrtc.signaling.localplatform.SourceServerIT` → FAIL.
- [ ] **Step 3: Implement** the handlers with `HttpsServer` and a bounded fixed executor (4 threads source, 2 admin, 1 health); `HttpsConfigurator` enforces `needClientAuth` and protocol set; read the certificate via `((HttpsExchange) e).getSSLSession().getPeerCertificates()[0]`. `Running.close()` stops listeners (delay 0), the observer and the data source.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(local-platform): serve signed clock and revocation sources over mTLS with a separate admin role`.

### Task 7: Composition proof against the real runtime

**Files:** Create `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/LocalPlatformCompositionIT.java`; Modify `LocalCompositionServices.java` only if a seam is needed to inject an external source (keep its default behaviour).

**Interfaces:**
- Consumes: SP1 composition harness (`LocalRuntimeCompositionIT`, `LocalCompositionServices`: one cell, six actor JVMs, gateway, control, PG primary+standby), `LocalPlatformApplication.start`, admin endpoint.
- Wiring: start `LocalPlatformApplication` in-process with a config whose `cells` = the harness cell and storage epoch, signing key = the harness source key, and source CA = the one the harness actors trust; publish its source URLs and public key in the contract exactly where the fixture's source was published.

- [ ] **Step 1: Write the failing test** `LocalPlatformCompositionIT`: `controlBecomesReadyOnlyAfterMeasuredClockWarmsUp` (503 first, then 200; assert the platform's `/ready` agrees); `authenticatedInviteStillCommits` (same flow as the SP1 IT, now against the real platform); `adminRevocationDeniesTheUserOnTheRunningPlanes` (admin POST for the caller's `userId`, epoch omitted, then the same user's next authenticated request or bootstrap is denied within 10 s while another user is unaffected — Review Focus 1 end to end); `clockStepMakesThePlanesUnsafeAgain` (step the platform's wall clock **back** 300 ms through the injected wall supplier; a forward step would date every later report in the consumers' future and they could never recover; control returns 503 within 5 s and recovers after the platform's own 5 s window clears. The clock-injection seam is the public `LocalPlatformApplication.start(config, mono, wall)`, documented as a TEST seam, not a hook in a `Running` under `src/test`: the IT lives in another module and cannot see test classes of this one).
- [ ] **Step 2: Run** `./mvnw -o -pl signaling-integration-tests verify -Dit.test=io.webrtc.signaling.storage.LocalPlatformCompositionIT -Dspotless.check.skip=true -am` (the class sits in `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/` but declares package `io.webrtc.signaling.storage`, like the harness classes it uses; an `io.webrtc.signaling.it.*` selector matches nothing) (Docker + Chromium not required for this IT; long-running) → FAIL.
- [ ] **Step 3: Implement** the wiring and any minimal seam; keep `LocalRuntimeCompositionIT` unchanged and green.
- [ ] **Step 4: Run** the new IT and `LocalRuntimeCompositionIT` → PASS. Do not delete `signaling-integration-tests/target/test-classes`.
- [ ] **Step 5: Commit** `test(local-platform): prove the real source serves a real cell composition`.

### Task 8: Image, contract check and documentation

**Files:** Create `Dockerfile.local-platform`; Modify `qualification/scenarios/image-contract.sh` (add a local-platform case), `config/native-sources.md` (new "Local platform source" section: ports, config keys, epoch semantics ruling, measurement limits — reported clock figures are this process's wall-versus-monotonic measurement plus the fixed `CONSUMER_OBSERVATION_ALLOWANCE_MICROS` (100000 µs) stated in `ClockReportService` for the consumer's own read jitter, not a host clock-discipline claim; a measured uncertainty above 150000 µs is reported unsafe — permanence of revoke), `CLAUDE.md` (module list and dependency line, plus the build command), `docs/superpowers/specs/2026-10-21-docker-compose-two-cell-design.md` (§4.4 status → implemented; risk rows for epoch 0 and clock measurement → resolved/limited with evidence names).

**Interfaces:** Image: same pinned base digest as `Dockerfile`, `WORKDIR /opt/signaling`, `COPY --chown=10001:10001 signaling-local-platform/target/signaling-local-platform-0.1.0-SNAPSHOT-exec.jar app.jar`, `USER 10001:10001`, `ENTRYPOINT ["java","-jar","/opt/signaling/app.jar"]`, label `io.webrtc.signaling.release.status="NOT_QUALIFIED"`; no config or key baked in.

- [ ] **Step 1: Write the failing check:** extend `image-contract.sh` with a function taking the local-platform image: inspects user, entrypoint, no env enrollment (`SIGNALING_PLATFORM_CONFIG`, `SIGNALING_IDENTITY_ISSUER`, `SIGNALING_LOCAL_ACKNOWLEDGEMENT` absent), and runs it `--network none --read-only` without acknowledgement expecting non-zero exit and `LOCAL_PLATFORM_REJECTED ACKNOWLEDGEMENT` on stderr, with no stack trace. Run it against a missing image → FAIL.
- [ ] **Step 2: Build and run:** `./mvnw -o -pl signaling-local-platform -am package -DskipTests -Dspotless.check.skip=true`, then `docker build -f Dockerfile.local-platform --build-arg SOURCE_COMMIT=$(git rev-parse HEAD) -t signaling-local-platform:local .`, then `bash qualification/scenarios/image-contract.sh --local-platform-only signaling-local-platform:local` for the new case (or `image-contract.sh APP_IMAGE signaling-local-platform:local` for both; the one-argument app mode is unchanged) → PASS.
- [ ] **Step 3: Update documentation** as listed; do not claim production readiness or clock discipline of the host (the observer sees only this container's wall-vs-monotonic drift).
- [ ] **Step 4: Run the full module and the unchanged IT:** `./mvnw -o -pl signaling-local-platform verify -am -Dspotless.check.skip=true`; confirm `git status` shows only intended files.
- [ ] **Step 5: Commit** `feat(local-platform): package the source service and document its contract`.

---

## Self-Review

- **Spec coverage (§4.4):** TLS 1.3 mTLS 9443, enrolled callers, wire framing → Task 6; measured clock with unsafe-by-default and limits documented → Tasks 2, 4, 8; durable append-only log, monotonic offsets, ≤ 16/page, `signingBytes` proofs → Task 5; admin endpoint with its own role → Task 6; `registerBoot` bound to certificate, new boot replaces old → Task 3; `LOCAL_TEST_ONLY` before start and not in `signaling-app` auto-config → Task 1; Dockerfile → Task 8; "epoch 0" risk (§8) → Tasks 5, 7; Identity Dockerfile is SP3 by design.
- **Placeholder scan:** none; the two places where the implementer must read an existing seam (`ClockSafetyMonitor` accept method, `NativeRevocationSource` event DTO shape) say what to check and which test pins it.
- **Type consistency:** `PeerIdentity`, `BootRegistry.Registration`, `SourceSigner`, `HostClockObserver.Measurement`, `RevocationLog.Appended/Snapshot`, `ClockReportService.clockBody`, `RevocationPageService.pageBody(cell, from)` are used with the same names in Tasks 3–7.
- **Proportion:** the plan states interfaces, algorithms the signatures do not fix, and named tests; bodies are left to the implementer.
