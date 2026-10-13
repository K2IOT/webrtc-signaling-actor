# Signaling Identity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Provide an independent development/test RS256 JWT issuer compatible with signaling.

**Architecture:** A Spring MVC application loads immutable accounts and a validated RSA pair at startup. A JCA token issuer signs JWTs, while controllers expose login and a precomputed public JWKS. Existing signaling code is used only as a test dependency.

**Tech Stack:** Java 21, parent-managed Spring Boot 3.5.6, Jackson, JCA, JUnit 5/AssertJ, OpenSSL, Bash.

**Spec:** `docs/superpowers/specs/2026-10-13-signaling-identity-design.md` (approved).

## Global Constraints

- Development/test only; plaintext accounts and committed development keys are accepted.
- RSA >=2048 bits, RS256 only, classpath PKCS#8 private and SPKI public PEM.
- Defaults: port 8081, issuer `http://localhost:8081`, audience `webrtc-signaling`, TTL `15m`, key ID `test-key-1`; alice/alice123/alice and bob/bob123/bob.
- No database, refresh token, registration, runtime JWKS enrollment changes, or production IdP features.
- Preserve existing `AGENTS.md`/`CLAUDE.md` edits. New code lives under `io.webrtc.signaling.identity`.
- Run GitNexus impact before modifying existing symbols; report HIGH/CRITICAL findings. New symbols have no preexisting callers; record that boundary. Run detect_changes before each commit and independently inspect staged files because the graph can be stale.
- Execute focused tests before reactor checks; evidence must distinguish failures and unrun gates.

## Review Focus

- Credentials with leading/trailing spaces must retain exact matching; Task 3 tests this.
- Shared user IDs are permitted, duplicate usernames rejected; Task 1 tests this.
- Concurrent sign operations must not share mutable Signature state; Task 3 tests this.
- Java BigInteger sign bytes must not corrupt JWKS reconstruction; Task 2 tests this.
- Packaged JAR must load keys without source-tree paths; Task 4 smoke tests this.

## File map

Modify root `pom.xml` solely to register the module. Create `signaling-identity/pom.xml`, `README.md`, and `src/main/resources/application.yml`, `keys/private.pem`, `keys/public.pem`.

Java main files beneath `signaling-identity/src/main/java/io/webrtc/signaling/identity/`: `SignalingIdentityApplication.java` (boot entry), `IdentityProperties.java` (configuration validation), `IdentityConfiguration.java` (clock/key beans), `RsaKeyMaterial.java` (PEM validation/public projection), `TestAccountRegistry.java` (credential lookup), `JwtTokenIssuer.java` (signing), `IdentityController.java` (HTTP DTOs/routes), `IdentityExceptionHandler.java` (generic errors).

Tests beneath matching `src/test/java/io/webrtc/signaling/identity/`: `IdentityPropertiesTest.java`, `RsaKeyMaterialTest.java`, `IdentityHttpTest.java`, `JwtTokenIssuerTest.java`. Create `scripts/generate-rsa-keys.sh` and `scripts/test-generate-rsa-keys.sh` (isolated generator regression fixture).

## Task 1: Validated application configuration and accounts

**Interfaces:** `IdentityProperties` is an immutable configuration record `(String issuer, String audience, Duration tokenTtl, String keyId, List<Account> accounts)`; nested `Account(String username, String password, String userId)`. `TestAccountRegistry(IdentityProperties properties)` exposes `Optional<String> authenticate(String username, String password)`.

- [x] Register module and minimal Boot web/validation dependencies; add test-scoped `signaling-auth` dependency and executable-JAR plugin following parent conventions.
- [x] Write `IdentityPropertiesTest` with constructor/binding assertions: default TTL equals 900 seconds; duplicate username fails; two distinct usernames sharing `alice` succeed; null/blank required fields, empty accounts, zero/negative/fractional-second TTL fail. Check identifier UTF-8 limits and control characters against source `Identity.SessionKey`/`UserId` contracts without adding runtime dependencies.
- [x] Run `./mvnw -B -pl signaling-identity -am test -Dtest=IdentityPropertiesTest`; require the expected missing implementation failure after scaffolding compiles.
- [x] Implement properties constructor validation, defensive collection copies, registry immutable map, Boot entry and application YAML. Use exact case-sensitive credential matching. Expose `Clock.systemUTC()` through configuration. Reject unresolved identifier placeholders consistently with current signaling configuration.
- [x] Run focused test command again; require zero failures/errors. Apply module Spotless. Inspect staged scope, run GitNexus detect_changes and commit `feat: configure test signaling identity accounts`.

## Task 2: Resource RSA keys, public JWKS and generation

**Interfaces:** `RsaKeyMaterial.load(Resource privatePem, Resource publicPem, String keyId)` returns validated material; `RSAPrivateKey privateKey()`, `RSAPublicKey publicKey()`, `Map<String, Object> jwks()` expose signing key, public key, immutable JWKS respectively. Configuration loads fixed `classpath:keys/private.pem` and `classpath:keys/public.pem` as startup beans.

- [x] Write `RsaKeyMaterialTest`: valid 2048 pair loads; absent/malformed PEM, 1024 pair, mismatched 2048 pairs fail; JWKS has exact public fields, matching kid, exponent AQAB, no private fields. Decode n/e as positive BigInteger and assert reconstructed key equals original, including high-bit modulus/sign-byte case.
- [x] Run `./mvnw -B -pl signaling-identity -am test -Dtest=RsaKeyMaterialTest`; confirm implementation failure.
- [x] Implement stream-based PEM loading with KeyFactory, minimum-bit checks and sign/verify pair probe; build public n/e via unsigned Base64URL. Keep private key out of DTOs. Run impact on existing Task 1 configuration symbols before adding key beans.
- [x] Write generator regression script: copy generator into a temporary project fixture; invoke from a different working directory; assert 2048-bit PKCS#8/SPKI output using OpenSSL; second invocation fails and hashes remain identical; `--force` replaces with a valid matching pair; unknown options fail. Cleanup fixture with trap.
- [x] Run regression script expecting missing generator failure, then implement generator with script-relative paths, dependency check, temporary generation/validation, overwrite guard, `--force`, and cleanup. Generate committed test resources with it.
- [x] Run `bash -n scripts/generate-rsa-keys.sh scripts/test-generate-rsa-keys.sh`, generator regression and focused RSA tests; require all pass. Format, scope-check with detect_changes, commit `feat: provide test RSA resources and JWKS material`.

## Task 3: JWT issuance and HTTP contracts

**Interfaces:** `JwtTokenIssuer(IdentityProperties properties, RsaKeyMaterial keys, ObjectMapper json, Clock clock)` exposes `String issue(String userId)`; allocate Signature per call. Controller consumes registry, issuer, properties and keys. Request record `LoginRequest(String username, String password)` has bean validation; response record `LoginResponse(String access_token, String token_type, long expires_in)`. Error body is `{"error":"invalid_credentials"}` or `{"error":"invalid_request"}`.

- [x] Write `JwtTokenIssuerTest` with fixed clock: header RS256/test-key-1/JWT, issuer/audience defaults, userId alice, unique UUID jti, integral iat, exp-iat exactly 900. Verify signature; issue concurrently (32 calls), verify every signature and unique jti. Validate through `Rs256TokenVerifier` using 15-minute lifetime, zero skew, propagation 2s, safety bound 5s, preserveJtiOnRefresh true and `test://ordered-security-log`; reject tampered payload and token at expiry.
- [x] Write `IdentityHttpTest` using random-port Spring Boot HTTP requests: alice/bob success and identity correctness; unknown/wrong credentials both identical 401; blank/null/missing credentials and malformed JSON 400; unsupported media type 415; valid password with extra whitespace fails 401; token response Cache-Control no-store; JWKS 200 reconstructs key and verifies HTTP-issued token.
- [x] Run `./mvnw -B -pl signaling-identity -am test -Dtest=JwtTokenIssuerTest,IdentityHttpTest`; confirm missing behavior failures.
- [x] Implement issuer compact JSON JWT with required claims, UUID and clock epoch seconds; implement routes and error advice without logging credentials/tokens. Bean-validation errors and JSON read errors use generic 400, registry miss uses generic 401, signing errors propagate as server errors rather than fake credentials rejection. Use precomputed public JWKS.
- [x] Re-run focused tests, format module, scope-check and commit `feat: expose test JWT login and JWKS endpoints`.

## Task 4: Packaging, instructions and final verification

**Interfaces:** Executable JAR exposes both endpoints on configurable port with bundled resources; README describes trusted-key registration without implying automatic JWKS discovery.

- [x] Add startup-context assertions using ApplicationContextRunner/Boot binding for malformed configuration and missing/malformed/weak/mismatched key resources; confirm configuration failures happen before serving requests. Test fixture keys remain temporary/test-only.
- [x] Write module README with purpose, exact defaults/accounts, module reactor build command, executable JAR path/run command, curl login/JWKS, TTL and port configuration, generator --force behavior, rebuild/restart requirements, public key/kid enrollment into TrustedRsaKeys and issuer/audience/lifetime alignment. Document load-client token reuse and separate issuer/signaling measurements.
- [x] Run `./mvnw -B -pl signaling-identity -am verify`; require all selected reactor modules pass tests and packaging/Spotless.
- [x] Smoke executable JAR from temporary cwd on a free port using process cleanup trap, poll readiness with bounded timeout, login and fetch JWKS over HTTP, confirm token signature/public key compatibility. Never print token/private-key material in evidence output.
- [x] Run generator regression, `git diff --check`, then `./mvnw -B verify` for full reactor verification. Do not repeat broad verification without new changes or an unresolved concern.
- [x] Perform final Java/code review of all changes, resolve actionable findings and rerun affected checks. GitNexus stale/unavailable limitations must be recorded; direct staged diff remains scope evidence.
- [x] Run detect_changes, inspect staged scope, commit `docs: document and verify test identity service`. Report test counts, packaging smoke result, remaining gates, commits, and no push unless requested.

## Execution handoff

Execution method: native inline execution selected by the user on 2026-10-13. All four tasks were implemented. Final review was a self-review because this run used inline execution with no independent review agent. The full reactor verify ran; its only failure was the browser integration test expecting `/usr/bin/chromium`. That test passed on rerun with `CHROMIUM_PATH=/usr/bin/google-chrome`; the module-scoped `./mvnw -B -pl signaling-identity -am verify` passed after final changes.
