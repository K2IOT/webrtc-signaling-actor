# Signaling Identity Service for development and tests

Date: 2026-10-13
Status: Written spec awaiting user review; conversational design approved.

## Purpose and scope

Add an independent Spring Boot Maven module named `signaling-identity` to the existing reactor. Its sole purpose is issuing JWTs for local development, integration tests, and load tests of signaling. Plaintext configured accounts and a committed test private key are explicitly accepted for this purpose. This is not a production Identity Provider.

Use the parent project's Java 21 and Spring Boot dependency management. The service listens on port 8081 by default and is packaged as an executable Spring Boot artifact. It has no database, registration, refresh tokens, logout, federation, key rotation, or user administration. Existing signaling runtime authentication behavior remains unchanged.

## Architecture

Use Spring MVC controllers, validated configuration properties, an immutable account lookup built at startup, an RSA key loader, and a token issuer. Use JCA `SHA256withRSA`, Jackson JSON serialization, and Base64URL without padding for the compact JWT. This matches the existing verifier and avoids an authorization-server framework for two endpoints.

The production dependencies of the module do not include signaling runtime modules. A test-scoped dependency on `signaling-auth` verifies compatibility with the actual `Rs256TokenVerifier`.

Alternatives considered: embedding the issuer in `signaling-app` would couple its lifecycle and routing to signaling; Spring Authorization Server would add protocol and configuration scope unnecessary for this test service.

## Configuration

The module's `application.yml` provides:

```yaml
server:
  port: 8081
signaling:
  identity:
    issuer: http://localhost:8081
    audience: webrtc-signaling
    token-ttl: 15m
    key-id: test-key-1
    accounts:
      - username: alice
        password: alice123
        user-id: alice
      - username: bob
        password: bob123
        user-id: bob
```

Issuer, audience, key ID, and account fields must be nonblank. Username is unique and matched exactly, case sensitively, without trimming credentials. Password is compared exactly. Multiple usernames may map to the same user ID. Account lookup is immutable and avoids scanning the account list for each login. TTL must be positive and an integral number of seconds; JWT timestamps use epoch seconds. At least one account is required.

Validate identifiers against the existing signaling contracts: issuer at most 512 UTF-8 bytes, key ID and user ID at most 256 UTF-8 bytes, and compatible identifier character constraints. Validate audience to the same 512-byte bound used by signaling configuration. Invalid configuration fails startup with actionable diagnostics that do not contain passwords or private-key material.

## Login contract

`POST /api/v1/auth/login` accepts `application/json`:

```json
{"username":"alice","password":"alice123"}
```

Success is HTTP 200 with:

```json
{"access_token":"<signed JWT>","token_type":"Bearer","expires_in":900}
```

The response uses `Cache-Control: no-store`. Unknown username and incorrect password both produce HTTP 401 with the same generic error code `invalid_credentials`. Missing, null, or blank credentials and malformed JSON produce HTTP 400 with a generic `invalid_request` code. Errors do not echo credentials. Unsupported media types retain Spring MVC's HTTP 415 behavior. No endpoint logs credentials or tokens.

Each successful login signs a fresh JWT whose header contains `alg: RS256`, configured `kid`, and `typ: JWT`. Claims contain configured `iss`, configured `aud` as a string, account `userId`, a new UUID `jti`, integer `iat`, and integer `exp = iat + TTL seconds`. Use an injectable clock to make timing behavior testable. No additional claims are required; the existing verifier defaults absent `securityEpoch` to zero.

## RSA resources and generation

Commit development-only RSA keys under `signaling-identity/src/main/resources/keys/private.pem` and `public.pem`. Load them once at startup using classpath resource streams so packaged JAR execution works. Private PEM is unencrypted PKCS#8 (`PRIVATE KEY`); public PEM is SubjectPublicKeyInfo (`PUBLIC KEY`). Require RSA keys with modulus size at least 2048 bits and prove the pair matches by signing and verifying a startup probe. Missing, malformed, weak, or mismatched keys fail startup.

The private key is retained solely by the signing component and never exposed by controllers or JWKS. The public key is used for JWKS and compatibility verification.

Add executable `scripts/generate-rsa-keys.sh` using OpenSSL to generate a 2048-bit pair into those resource paths. Resolve paths from the script location so invocation does not depend on the working directory. Refuse replacement when either output already exists unless `--force` is supplied. Generate and validate the pair in temporary files before replacement; clean temporary files on failure. Document that regenerating resources requires rebuilding/restarting the issuer and updating the verifier's trusted key. OpenSSL is required only for generation, not service startup.

## JWKS contract

`GET /.well-known/jwks.json` returns HTTP 200 and JSON:

```json
{"keys":[{"kty":"RSA","kid":"test-key-1","use":"sig","alg":"RS256","n":"<modulus>","e":"AQAB"}]}
```

Encode modulus and exponent as unsigned big-endian integers in Base64URL without padding, dropping Java BigInteger's sign byte when present. Derive the exponent from the actual public key; the generated key uses 65537 (`AQAB`). JWKS contains exactly one public key and no private fields. Precompute the immutable public representation at startup. JWT and JWKS share the same configured key ID.

## Signaling integration boundary

Current `Rs256TokenVerifier` checks RS256, trusted key lookup by `kid`, issuer, audience, user ID, token ID, expiry, clock skew, and the maximum issuance lifetime. Its trusted key holder supports supplied RSA keys and a caller-supplied refresh callback; it does not automatically fetch a JWKS URL.

The module README must explain how to align issuer/audience, set signaling's maximum JWT lifetime to at least the configured issuer TTL, and enroll `public.pem` under the configured key ID in the test harness's `TrustedRsaKeys`. Publishing JWKS does not itself change signaling's runtime key enrollment. Automatic network JWKS fetching and runtime enrollment changes are outside this module's scope.

Document Maven build/run commands, curl login/JWKS examples, configurable port, account customization, key regeneration, and how load-test clients obtain and reuse a token until renewal is needed. Token issuance throughput is measured separately from signaling throughput; no issuer throughput target is introduced.

## Acceptance and verification

1. Build and package the independent module through the reactor and start its executable JAR without source-tree resource access.
2. HTTP integration tests cover both configured accounts, successful login, wrong/unknown credentials, missing/blank credentials, malformed JSON, and cache prevention on token responses.
3. Verify required JWT header/claims, integer timestamps, exact configured TTL, and unique `jti` across logins.
4. Reconstruct the RSA public key from returned JWKS, verify the JWT signature, confirm matching key ID, and confirm absence of private key fields.
5. Validate an issued token through existing `Rs256TokenVerifier` with an aligned `IdentitySecurityContract` and trusted key; reject a tampered token and an expired token.
6. Startup tests cover missing/malformed PEM, RSA below 2048 bits, mismatched pairs, duplicate usernames, and invalid TTL/configuration.
7. Verify generator shell syntax, successful generation in an isolated fixture, refusal to overwrite, and explicit forced replacement.
8. Run focused module tests, applicable Spotless checks, and reactor verification. Report unrelated or environment-blocked failures separately and do not equate local completion with production qualification.

## Change boundaries

Expected implementation changes: parent module registration, the new `signaling-identity` module, the generation script, and the module README/tests. Preserve existing user edits to `AGENTS.md` and `CLAUDE.md`. Run GitNexus impact analysis before modifying existing symbols and `detect_changes` before any commit. No code implementation is authorized by this written spec until its review and the subsequent implementation-plan gate are complete.
