# Minikube Identity and JWKS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Login phát JWT RS256 từ Secret, verify qua bounded JWKS cache và rotate/retire key mà không mở authorization gap.

**Architecture:** Identity giữ private key ring; mỗi verifier process giữ public-key snapshot độc lập. Source service lưu durable security epoch, issuance reservation và retirement tombstones; mTLS admin workflow điều phối publication/observation/switch/retirement.

**Tech Stack:** Existing Java 21/Spring MVC/Jackson, JCA RSA, existing `Rs256TokenVerifier`/`BoundedTokenVerifier`, HTTPS/mTLS, JUnit.

**Spec:** [Approved design](../specs/2026-10-13-minikube-signaling-platform-design.md); [master constraints/dependencies](2026-10-13-minikube-signaling-platform.md).

## Global Constraints

All master constraints apply. Issuer `https://identity.wsa.test`, audience `webrtc-signaling`, RS256/RSA≥2048, TTL15m/max15m/skew30s/REQUIRE_IAT. JWKS refresh30s/hard60s/request2s/body64KiB/keys64/unknown-kid5s. Private key chỉ mount Identity, không ở client/verifier/source. No public admin/refresh API.

## Review Focus

1. Packaging giữ old private PEM/sample password — I1 `packagedArtifactsContainNoPrivateKey`.
2. Issuance epoch0 bị default watermark revoke — I2 `firstLoginUsesEpochOne`.
3. Duplicate kid/private JWK/malformed JSON hoặc HTTP redirect — I3 `rejectsUntrustedSnapshot`.
4. Failed refresh, invalid304 hoặc wall-clock rollback kéo dài cache — I3 `freshnessUsesSuccessfulMonotonicFetchOnly`.
5. Restart/reordered rotation phục hồi retired kid — I4 `retirementSurvivesRestartAndRetry`.

---

## File structure

`signaling-identity/src/main/java/io/webrtc/signaling/identity/` owns key loading, login issuance and admin key-ring HTTP. `signaling-auth/src/main/java/io/webrtc/signaling/auth/` owns JWKS parsing/cache and public verifier interfaces. `signaling-identity/src/test/java/io/webrtc/signaling/identity/` and corresponding auth test directory own fixtures. Dockerfiles package only runtime code; generated credentials stay outside build context.

### Task I1: Secret-mounted key ring and clean Identity package

**Files:** Create `signaling-identity/src/main/java/io/webrtc/signaling/identity/IdentityKeyRing.java`, `SecretKeyRingLoader.java`, `signaling-identity/Dockerfile`; modify `IdentityConfiguration.java`, `RsaKeyMaterial.java`, `IdentityProperties.java` in the same main package, `signaling-identity/src/main/resources/application.yml`, `signaling-identity/pom.xml`; remove `signaling-identity/src/main/resources/keys/private.pem` and `signaling-identity/src/main/resources/keys/public.pem`; test `signaling-identity/src/test/java/io/webrtc/signaling/identity/SecretKeyRingLoaderTest.java` and `qualification/scenarios/minikube/tests/test_identity_image.py`.

**Interfaces:** `IdentityKeyRing.SigningKey(String kid, RSAPublicKey publicKey, RSAPrivateKey privateKey)`; `Snapshot(long version,String activeKid,Map<String,SigningKey> keys,Set<String> retiredKids)`; `snapshot(): Snapshot`; `install(long expectedVersion,Snapshot next): void`; `SecretKeyRingLoader.load(Path directory): Snapshot`. Secret contains `keyring.json` metadata and referenced PEM files, exact version/activeKid; filenames must remain under mounted directory. Snapshot exposes private material only inside Identity, never JSON serialization or logging.

- [ ] Write `rejectsMissingWeakMismatchedOrEscapingKey` and `packagedArtifactsContainNoPrivateKey`; test fixtures generate ephemeral JCA keys, not checked-in PEM. Assertions:
  ```java
  assertThrows(IllegalArgumentException.class, () -> loader.load(missingMount));
  assertThrows(IllegalArgumentException.class, () -> loader.load(rsa1024Mount));
  assertThrows(IllegalArgumentException.class, () -> loader.load(mismatchedPairMount));
  assertThrows(IllegalArgumentException.class, () -> loader.load(pathTraversalMount));
  assertEquals("K1", loader.load(validMount).activeKid());
  ```
  Image test asserts no `BEGIN PRIVATE KEY`/`BEGIN RSA PRIVATE KEY`, `keys/private.pem`, or sample account password in extracted JAR, layers and build context.
- [ ] Run `./mvnw -B -pl signaling-identity -am -Dtest=SecretKeyRingLoaderTest test`; expected red missing loader/key-ring behavior. Run image assertion after packaging to expose existing embedded key.
- [ ] Implement interfaces above and fail startup on absent/invalid Secret. Keep pair correspondence, strict metadata/duplicate-key checks, read-only mount and nonroot image; remove deployable sample account/password defaults without rewriting Git history.
- [ ] Run unit command again and `./mvnw -B -pl signaling-identity -am package -DskipTests`; run `python3 -m unittest discover -s qualification/scenarios/minikube/tests -p 'test_identity_image.py' -v` against built image supplied by K1 lock. Expected tests PASS; packaging alone is not test evidence. Before K1 image exists, record image subgate NOT_RUN, not task fully complete.
- [ ] Scope review/impact/detect_changes as master, stage only listed files, commit `feat(identity): load signing keys from mounted secret`; image gate may remain explicitly pending until K1/K5 build, and closes before checkpoint C3.

### Task I2: TLS login, source-bound claims and internal JWKS

**Files:** Create `signaling-identity/src/main/java/io/webrtc/signaling/identity/IdentitySecurityClient.java`, `IdentityTlsConfiguration.java`; modify existing `IdentityController.java`, `JwtTokenIssuer.java`, `IdentityProperties.java`, `IdentityConfiguration.java`; test `signaling-identity/src/test/java/io/webrtc/signaling/identity/IdentitySecurityContractTest.java`, `IdentityTlsTest.java`.

**Interfaces:** Consumes A2 `SecuritySourceClient.reserveIssuance(IssuanceRequest): CompletionStage<IssuanceReservation>`. Implements client using pinned source URL, client certificate, source proof and bounded transport. `JwtTokenIssuer.issue(String userId): CompletionStage<String>` replaces synchronous issuance; login endpoint adapts async result. IssuanceRequest/Reservation types and all fields are defined in A2. Public listener8443 serves login/JWKS; internal listener9443 requires client auth and serves the same public JWKS snapshot; no credentials/public admin on internal JWKS route.

- [ ] Write `firstLoginUsesEpochOne`, `newLoginChangesJti`, `sourceFailureClosesIssuance`, `jwksListenersExposeSamePublicSnapshot`. Fixture owns real HTTPS servers and generated certs:
  ```java
  assertEquals(1L, claims(firstLogin).get("securityEpoch"));
  assertEquals("https://identity.wsa.test", claims(firstLogin).get("iss"));
  assertEquals("webrtc-signaling", claims(firstLogin).get("aud"));
  assertEquals(900L, numericExp(firstLogin) - numericIat(firstLogin));
  assertNotEquals(claims(firstLogin).get("jti"), claims(secondLogin).get("jti"));
  assertEquals(503, loginWhileSourceUnavailable.statusCode());
  assertEquals(publicJwksBody, authenticatedInternalJwksBody);
  ```
- [ ] Run `./mvnw -B -pl signaling-identity -am -Dtest=IdentitySecurityContractTest,IdentityTlsTest test`; expected red missing source/TLS behavior.
- [ ] Implement async source reservation before signing/response, with source-bound `securityEpoch`, integer iat/exp, new UUID jti and ring version binding. No secret/password/token logs. Missing source proof/expired clock or reservation/key-ring mismatch rejects; source reservation is durable before token can escape. Preserve invalid credential401 behavior and finite login admission. Enrolled account config lives in Secret; unknown users denied. JWKS carries authenticated `X-Signaling-Key-Version` and ETag/public fingerprint from the same immutable ring snapshot, shared across both listeners; header/body mismatch rejects at verifier.
- [ ] Repeat command; expected PASS including unauthenticated internal-client rejection, wrong CA/SAN rejection, `iat`/TTL boundary verification using existing verifier, no private fields in JWKS, and post-revoke login at next epoch.
- [ ] Scope review then commit `feat(identity): issue source-bound JWT over TLS`.

### Task I3: Process-owned bounded JWKS cache

**Files:** Create `signaling-auth/src/main/java/io/webrtc/signaling/auth/JwksSnapshotParser.java`, `JwksCacheOwner.java`, `JwksEnrollment.java`; modify `TrustedRsaKeys.java` only for atomic validated snapshot installation if required; test `signaling-auth/src/test/java/io/webrtc/signaling/auth/JwksCacheTest.java`.

**Interfaces:** `JwksEnrollment(URI endpoint,SSLContext tls,String issuer,int maxTokenTtlSeconds,int clockSkewSeconds)` requires pinned HTTPS authority and 900/30 policy. `JwksSnapshotParser.parse(byte[] body): Map<String,RSAPublicKey>`; `JwksCacheOwner.start(): CompletionStage<Void>`, `verifier(): TokenVerifier`, `isFresh(): boolean`, `retire(Set<String> kids,long sourceOffset): void`, `observation(): Observation`, `close(): void`; `Observation(String issuer,long version,String publicFingerprint,Set<String> kids,long retirementOffset,boolean fresh)`. Owner holds monotonic elapsed time, immutable snapshot and finite I/O executor; verifiers reject until successful initial load plus durable retirement replay. `TokenVerifier.validate(String,Instant)` and existing bounded wrapper remain consumer APIs.

- [ ] Write tests `rejectsUntrustedSnapshot`, `freshnessUsesSuccessfulMonotonicFetchOnly`, `unknownKidRefreshIsBounded`, `retiredKidCannotReenter`; fixture owns deterministic monotonic ticker, wall clock and TLS endpoint. Assertions:
  ```java
  assertFalse(owner.isFresh()); // before initial fetch/replay
  assertTrue(afterValidFetchAtSeconds(0).isFresh());
  assertFalse(afterFailedRefreshAndAdvanceToSeconds(60).isFresh());
  assertEquals(1, requestsDuringUnknownKidBurstWithinSeconds(5));
  assertThrows(AuthException.class, () -> verify(retiredK1Token));
  ```
  Parser cases reject duplicate JSON members/kids, >64 keys, >65,536 bytes, non-RS256 signing keys, RSA<2048, private JWK fields, ambiguous n/e and kid material reuse. No token `jku`/`x5u` controls endpoint.
- [ ] Run `./mvnw -B -pl signaling-auth -am -Dtest=JwksCacheTest test`; expected red missing owner/parser.
- [ ] Implement APIs, scheduled30s refresh, hard60s since verified success, 2s request deadline, one in-flight request, unknown-kid5s process throttle. Unknown key rejects immediately while refresh proceeds; no network on Netty/actor dispatcher. Enforce mTLS/hostname validation, reject redirect/chunked-unbounded response; a304 refreshes age only after trusted endpoint and exact cached version/ETag validation. Validate immutable version header against public fingerprint and reject old version or same version/different material. Failed/stale responses never extend freshness; monotonic clock rollback invalidates owner. A6 maps I3 Observation to A2 wire VerifierObservation and reports through authenticated API only when fresh and retirement replay caught up; cache implementation has no dependency on source service.
- [ ] Repeat command; expected PASS including cache hit during bounded outage, stale rejection exactly at60s, overlapping refresh shutdown, queue saturation, malformed response leaving last valid snapshot intact but aging, and retired-key replay before ready.
- [ ] Scope review then commit `feat(auth): add bounded enrolled JWKS cache`.

### Task I4: Durable rotation/retirement administration

**Files:** Create `signaling-identity/src/main/java/io/webrtc/signaling/identity/IdentityKeyAdminController.java`, `IdentityKeyRotation.java`; create `deploy/minikube/admin/rotate-key.py`; test `signaling-identity/src/test/java/io/webrtc/signaling/identity/IdentityKeyRotationTest.java`, `signaling-integration-tests/src/test/java/io/webrtc/signaling/it/JwksRotationIT.java`.

**Interfaces:** Consumes I1 ring, I3 Observation and A2 `appendSecurity(SecurityCommand): CompletionStage<SecurityCommit>` / active-verifier inventory. `IdentityKeyRotation.publish(UUID operationId,long expectedVersion,IdentityKeyRing.Snapshot next): CompletionStage<Long>`; `switchActive(UUID operationId,long expectedVersion,String kid): CompletionStage<Long>`; `retire(UUID operationId,long expectedVersion,String kid,boolean emergency): CompletionStage<Long>`. Private admin listener9444 uses distinct enrolled admin client identity; ordinary JWKS clients cannot call it. Script CLI `rotate-key.py --context NAME --operation-id UUID --expected-version N --key-secret NAME --mode planned|emergency` prints only operation/status/public fingerprint.

- [ ] Write `cannotSwitchBeforeEveryActiveVerifierObserves`, `retirementSurvivesRestartAndRetry`, `rejectsConflictingOperationId`. Assert with fixture admin/source/cache instances:
  ```java
  assertRejected(switchWhileOneActiveVerifierMissing);
  assertEquals(Set.of("K1", "K2"), publishedKids);
  assertEquals(firstCommitVersion, identicalRetryCommitVersion);
  assertRejected(retrySameOperationIdWithDifferentPayload);
  assertRejected(verifyK1AfterRetirementReplay);
  assertTrue(emergencyRetirementElapsed.compareTo(Duration.ofSeconds(5)) <= 0);
  ```
- [ ] Run `./mvnw -B -pl signaling-identity -am -Dtest=IdentityKeyRotationTest test`; expected red missing workflow.
- [ ] Implement publish K1+K2 → signed/enrolled observations from all active verifier incarnations → source/ring CAS switchK2 → planned wait after durable lastK1 reservation +900s+30s+propagation bound → durable tombstone → removeJWKS. Use actual deployment inventory, not hard-coded quorum; absent/stale observation blocks switching. Emergency retires immediately through source; caches/security sweeps close matching sessions within existing5s hard bound, without TTL wait. Never reuse retired kid/material, including restart or rollback.
- [ ] Run unit command and `./mvnw -B -pl signaling-integration-tests -am -Dit.test=JwksRotationIT verify`; expected PASS with real TLS/source DB and injected time, plus source crash between retirement commit and publication, cachedK1 reconnect rejection and K2 login success. Planned wall-clock wait remains deployed Q08 evidence; do not shorten production policy to speed test.
- [ ] Scope review then commit `feat(identity): add durable key rotation and retirement`.
