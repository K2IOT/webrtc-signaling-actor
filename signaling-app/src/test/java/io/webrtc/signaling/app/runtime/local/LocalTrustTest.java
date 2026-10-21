package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.rpc.HomeAuthorizationProof;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalTrustTest {
    @TempDir
    Path mount;

    LocalTestKeys keys;
    LocalRuntimeContract c;

    void install() throws Exception {
        var env = LocalRuntimeFixtures.install(mount, "ACTOR");
        keys = LocalTestKeys.install(mount);
        c = LocalRuntimeContract.load(env);
    }

    static String token(KeyPair key, String kid, Instant now) throws Exception {
        var enc = Base64.getUrlEncoder().withoutPadding();
        var header = "{\"alg\":\"RS256\",\"kid\":\"" + kid + "\"}";
        var claims =
                "{\"userId\":\"alice\",\"jti\":\"j-a\",\"iss\":\"LOCAL_TEST_ONLY_ISSUER\","
                        + "\"aud\":[\"LOCAL_TEST_ONLY_AUDIENCE\"],\"iat\":" + now.getEpochSecond()
                        + ",\"exp\":" + now.plusSeconds(600).getEpochSecond() + "}";
        var input =
                enc.encodeToString(header.getBytes(StandardCharsets.UTF_8))
                        + "." + enc.encodeToString(claims.getBytes(StandardCharsets.UTF_8));
        var signer = Signature.getInstance("SHA256withRSA");
        signer.initSign(key.getPrivate());
        signer.update(input.getBytes(StandardCharsets.US_ASCII));
        return input + "." + enc.encodeToString(signer.sign());
    }

    static HomeAuthorizationProof.Claims claims(Instant now) {
        return new HomeAuthorizationProof.Claims(
                1, "COORDINATOR_GRANT", "c001", "c002", UUID.randomUUID(), CallId.create("c001", 1),
                new UserId("TEST_ONLY"), null, null, 0, 1, 1, 1, 1, 1, 1, UUID.randomUUID(), null, 0,
                null, 1, 0, "a".repeat(64), now, now.plusSeconds(5), 1, null);
    }

    @Test
    void verifiesTokenSignedByConfiguredKidAndRejectsUnknownKid() throws Exception {
        install();
        var now = Instant.now();
        try (var verifier = LocalTrust.tokens(c)) {
            var principal = verifier.verify(token(keys.rsa, "rsa", now), now).toCompletableFuture().join();
            assertThat(principal.userId().value()).isEqualTo("alice");
            assertThatThrownBy(() -> verifier.verify(token(keys.rsa, "unknown", now), now).toCompletableFuture().join())
                    .isInstanceOf(CompletionException.class);
            var other = KeyPairGenerator.getInstance("RSA");
            other.initialize(2048);
            assertThatThrownBy(() -> verifier.verify(token(other.generateKeyPair(), "rsa", now), now).toCompletableFuture().join())
                    .isInstanceOf(CompletionException.class);
        }
    }

    @Test
    void homeProofRoundTripsWithOwnKeyOnly() throws Exception {
        install();
        var now = Instant.now();
        var own = LocalTrust.homeProof(c);
        assertThat(own.sourceCell()).isEqualTo("c001");
        var signed = own.issue(claims(now));
        assertThat(own.decode(signed, "c001", now)).isPresent();
        // Same cell and key id, different private key: the pinned public key must not verify it.
        var stranger = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        var forged =
                new HomeAuthorizationProof("c001", "local", stranger.getPrivate(), Map.of("c001/local", stranger.getPublic()))
                        .issue(claims(now));
        assertThat(own.decode(forged, "c001", now)).isEmpty();
        // A key id the mount does not list is unknown.
        var unlisted =
                new HomeAuthorizationProof("c001", "other", keys.proof.getPrivate(), Map.of("c001/other", keys.proof.getPublic()))
                        .issue(claims(now));
        assertThat(own.decode(unlisted, "c001", now)).isEmpty();
    }

    @Test
    void relayProofUsesTheSameMountedKeys() throws Exception {
        install();
        assertThat(LocalTrust.relayProof(c)).isNotNull();
    }

    @Test
    void relayVerifierTrustsThePublicKeysAndNeverReadsTheSigningKey() throws Exception {
        install();
        var verifier = LocalTrust.relayVerifier(c);
        assertThat(field(verifier, "privateKey")).isNull();
        @SuppressWarnings("unchecked")
        var trusted = (Map<String, java.security.PublicKey>) field(verifier, "trusted");
        assertThat(trusted).containsOnlyKeys("c001/local");
        assertThat(trusted.get("c001/local").getEncoded()).isEqualTo(keys.proof.getPublic().getEncoded());

        var signingKey = mount.resolve("proof.key");
        Files.writeString(signingKey, "TEST_ONLY_SECRET_NOT_A_KEY");
        assertThatThrownBy(() -> LocalTrust.relayProof(c)).isInstanceOf(LocalRuntimeFailure.class);
        assertThat(field(LocalTrust.relayVerifier(c), "privateKey")).isNull();
        Files.delete(signingKey);
        assertFailure(() -> LocalTrust.relayProof(c), Role.PATH, Category.MISSING);
        assertThat(field(LocalTrust.relayVerifier(c), "privateKey")).isNull();

        // Its trusted keys are still checked: an RSA key is refused.
        Files.copy(mount.resolve("rsa.pub"), mount.resolve("proof.pub"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        assertFailure(() -> LocalTrust.relayVerifier(c), Role.CONTRACT, Category.INVALID);
    }

    static Object field(Object owner, String name) throws Exception {
        var field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }

    @Test
    void ed25519KeysAreReadByIdAndMustBeEd25519() throws Exception {
        install();
        var parsed = LocalTrust.ed25519Keys(c, c.contract().sources().publicKeys());
        assertThat(parsed).containsOnlyKeys("source");
        assertThat(parsed.get("source")).isEqualTo(keys.source.getPublic());
        // An RSA key is not an acceptable source or proof key.
        assertThatThrownBy(() -> LocalTrust.ed25519Keys(c, Map.of("rsa", c.contract().jwtPublicKeys().get("rsa"))))
                .isInstanceOfSatisfying(
                        LocalRuntimeFailure.class,
                        f -> {
                            assertThat(f.role()).isEqualTo(Role.CONTRACT);
                            assertThat(f.category()).isEqualTo(Category.INVALID);
                        });
    }

    @Test
    void malformedPemFailsWithoutEchoingContent() throws Exception {
        install();
        Files.writeString(mount.resolve("source.pub"), "TEST_ONLY_SECRET_NOT_PEM");
        assertThatThrownBy(() -> LocalTrust.ed25519Keys(c, c.contract().sources().publicKeys()))
                .isInstanceOfSatisfying(
                        LocalRuntimeFailure.class,
                        f -> {
                            assertThat(f.getMessage()).doesNotContain("TEST_ONLY_SECRET");
                            assertThat(f.getCause()).isNull();
                        });
    }

    @Test
    void clockMonitorIsBoundToThisProcessAndStartsInvalid() throws Exception {
        install();
        var monitor = LocalTrust.clockMonitor(c);
        assertThat(monitor.valid()).isFalse();
    }

    @Test
    void endpointValidatesTheCanonicalHttpsSource() throws Exception {
        install();
        var tls = javax.net.ssl.SSLContext.getDefault();
        var keys = LocalTrust.ed25519Keys(c, c.contract().sources().publicKeys());
        var endpoint = LocalTrust.endpoint(c.contract().sources().clockUrl(), tls, keys);
        assertThat(endpoint.uri()).isEqualTo(URI.create("https://source.platform.svc/v1/c001/clock"));
        assertThatThrownBy(() -> LocalTrust.endpoint("http://source.platform.svc/v1/c001/clock", tls, keys))
                .isInstanceOf(IllegalArgumentException.class);
    }

    static void assertFailure(
            org.assertj.core.api.ThrowableAssert.ThrowingCallable call, Role role, Category category) {
        assertThatThrownBy(call)
                .isInstanceOfSatisfying(
                        LocalRuntimeFailure.class,
                        failure -> {
                            assertThat(failure.role()).isEqualTo(role);
                            assertThat(failure.category()).isEqualTo(category);
                            assertThat(failure.getCause()).isNull();
                            assertThat(failure.getMessage()).doesNotContain("TEST_ONLY_SECRET");
                        });
    }

    @Test
    void proofPrivateKeyMustBeOneUnencryptedPkcs8Block() throws Exception {
        install();
        var der = keys.proof.getPrivate().getEncoded();
        var valid = LocalTestPki.pem("PRIVATE KEY", der);
        var cases = new java.util.LinkedHashMap<String, String>();
        cases.put("encrypted PKCS#8", LocalTestPki.pem("ENCRYPTED PRIVATE KEY", der));
        cases.put("legacy RSA label", LocalTestPki.pem("RSA PRIVATE KEY", der));
        cases.put("legacy EC label", LocalTestPki.pem("EC PRIVATE KEY", der));
        cases.put("wrong label", LocalTestPki.pem("PUBLIC KEY", der));
        cases.put("certificate label", LocalTestPki.pem("CERTIFICATE", der));
        cases.put("truncated", valid.substring(0, valid.length() / 2));
        cases.put("no end line", valid.substring(0, valid.indexOf("-----END")));
        cases.put("two blocks", valid + valid);
        cases.put("key then certificate", valid + LocalTestPki.pem("CERTIFICATE", der));
        cases.put("leading text", "TEST_ONLY_SECRET\n" + valid);
        cases.put("trailing text", valid + "TEST_ONLY_SECRET\n");
        cases.put("openssl encapsulated header", valid.replace("\n", "\nProc-Type: 4,ENCRYPTED\n"));
        cases.put("not base64", "-----BEGIN PRIVATE KEY-----\nTEST_ONLY_SECRET!!\n-----END PRIVATE KEY-----\n");
        cases.put("valid base64, not a key", LocalTestPki.pem("PRIVATE KEY", new byte[]{1, 2, 3, 4}));
        cases.put("public key in a private slot", LocalTestPki.pem("PRIVATE KEY", keys.proof.getPublic().getEncoded()));
        cases.put("empty", "");
        for (var entry : cases.entrySet()) {
            Files.writeString(mount.resolve("proof.key"), entry.getValue());
            assertFailure(() -> LocalTrust.homeProof(c), Role.CONTRACT, Category.INVALID);
            assertFailure(() -> LocalTrust.relayProof(c), Role.CONTRACT, Category.INVALID);
        }
        Files.write(mount.resolve("proof.key"), new byte[LocalFiles.KEY_MAX + 1]);
        assertFailure(() -> LocalTrust.homeProof(c), Role.CONTRACT, Category.TOO_LARGE);
        assertFailure(() -> LocalTrust.relayProof(c), Role.CONTRACT, Category.TOO_LARGE);
        Files.delete(mount.resolve("proof.key"));
        assertFailure(() -> LocalTrust.homeProof(c), Role.PATH, Category.MISSING);
        Files.writeString(mount.resolve("proof.key"), valid);
        assertThat(LocalTrust.homeProof(c)).isNotNull();
        assertThat(LocalTrust.relayProof(c)).isNotNull();
    }

    @Test
    void proofPrivateKeyMustBelongToTheTrustedPublicKey() throws Exception {
        install();
        // The constructors accept any Ed25519 private key; a wrong pair would fail every proof later.
        var stranger = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        LocalTestKeys.write(mount.resolve("proof.key"), "PRIVATE KEY", stranger.getPrivate().getEncoded());
        assertFailure(() -> LocalTrust.homeProof(c), Role.CONTRACT, Category.KEY_MISMATCH);
        assertFailure(() -> LocalTrust.relayProof(c), Role.CONTRACT, Category.KEY_MISMATCH);
        // The other direction: the right private key against a replaced public key.
        LocalTestKeys.write(mount.resolve("proof.key"), "PRIVATE KEY", keys.proof.getPrivate().getEncoded());
        LocalTestKeys.write(mount.resolve("proof.pub"), "PUBLIC KEY", stranger.getPublic().getEncoded());
        assertFailure(() -> LocalTrust.homeProof(c), Role.CONTRACT, Category.KEY_MISMATCH);
        assertFailure(() -> LocalTrust.relayProof(c), Role.CONTRACT, Category.KEY_MISMATCH);
        LocalTestKeys.write(mount.resolve("proof.pub"), "PUBLIC KEY", keys.proof.getPublic().getEncoded());
        assertThat(LocalTrust.homeProof(c).sourceCell()).isEqualTo("c001");
    }

    @Test
    void publicKeysMustBeOneX509Block() throws Exception {
        install();
        var rsaDer = keys.rsa.getPublic().getEncoded();
        for (var bad : java.util.List.of(
                LocalTestPki.pem("RSA PUBLIC KEY", rsaDer),
                LocalTestPki.pem("PUBLIC KEY", rsaDer) + LocalTestPki.pem("PUBLIC KEY", rsaDer),
                LocalTestPki.pem("PUBLIC KEY", rsaDer).substring(0, 60))) {
            Files.writeString(mount.resolve("rsa.pub"), bad);
            assertFailure(() -> LocalTrust.tokens(c), Role.CONTRACT, Category.INVALID);
        }
        // An Ed25519 key is not an RSA token key.
        LocalTestKeys.write(mount.resolve("rsa.pub"), "PUBLIC KEY", keys.source.getPublic().getEncoded());
        assertFailure(() -> LocalTrust.tokens(c), Role.CONTRACT, Category.INVALID);
    }
}
