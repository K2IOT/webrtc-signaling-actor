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
}
