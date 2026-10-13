# Signaling Identity (development and test)

This Spring Boot service issues RS256 access tokens for local signaling development, integration tests, and load tests. It is a test identity issuer, not a production identity provider. Its accounts and private RSA key are deliberately plaintext/project resources for this scope.

## Defaults

The service listens on port `8081`. `src/main/resources/application.yml` configures issuer `http://localhost:8081`, audience `webrtc-signaling`, a 15-minute token lifetime, key ID `test-key-1`, and these accounts:

| Username | Password | User ID |
| --- | --- | --- |
| alice | alice123 | alice |
| bob | bob123 | bob |

For local-only overrides, pass Spring properties on the command line, for example `--server.port=8082 --signaling.identity.token-ttl=10m`. Keep the signaling verifier's maximum JWT lifetime at least as large as the issuer TTL, and configure the same issuer and audience on both sides.

## Build and run

From the repository root:

```bash
./mvnw -B -pl signaling-identity -am package
java -jar signaling-identity/target/signaling-identity-0.1.0-SNAPSHOT-exec.jar
```

The keys are loaded from the module classpath, so the executable JAR does not depend on the source checkout at runtime.

Login and retrieve the public key set:

```bash
curl -sS -X POST http://localhost:8081/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"alice123"}'

curl -sS http://localhost:8081/.well-known/jwks.json
```

Successful login returns `access_token`, `token_type: Bearer`, and `expires_in`. Credential errors return the same generic 401 response. The token response is marked `Cache-Control: no-store`.

## Signaling verifier setup

The JWKS endpoint publishes the public RSA key and configured `kid`; it never publishes private material. `Rs256TokenVerifier` uses `TrustedRsaKeys` supplied by signaling runtime configuration and does not fetch JWKS automatically. A local integration harness can fetch JWKS once, construct an `RSAPublicKey`, and register `Map.of("test-key-1", publicKey)` in `TrustedRsaKeys`. Keep that key ID, issuer, audience, and the verifier's maximum lifetime aligned with this service.

Load clients should obtain a token before a run and reuse it while valid, then request another token before expiry. Measure identity login traffic separately from signaling traffic when collecting load results.

## Development key generation

The committed `keys/private.pem` and `keys/public.pem` are development-only RSA-2048 keys. To replace them, run from any working directory:

```bash
scripts/generate-rsa-keys.sh --force
```

Without `--force`, the script refuses to overwrite either existing key. Regenerating requires rebuilding/restarting this issuer and updating the public key trusted by each test harness. OpenSSL is required only for key generation, not service startup.
