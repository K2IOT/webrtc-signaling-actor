package io.webrtc.signaling.storage.worker;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.auth.RevocationState;
import io.webrtc.signaling.protocol.Identity.UserId;
import java.security.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class RevocationSourceVerifierTest {
    static final String ISSUER = "https://identity.example.test";
    static RevocationReconciler.Batch seal(String cell, RevocationReconciler.Batch unsigned, KeyPair key) throws Exception {
        var signer = Signature.getInstance("Ed25519"); signer.initSign(key.getPrivate()); signer.update(RevocationSourceVerifier.signingBytes(cell, ISSUER, unsigned));
        return new RevocationReconciler.Batch(unsigned.fromOffset(), unsigned.highWater(), unsigned.events(), unsigned.checkedAt(),
            "TEST_ONLY." + Base64.getUrlEncoder().withoutPadding().encodeToString(signer.sign()), unsigned.retiredKeys(), unsigned.currentSourceHighWater());
    }
    @Test void authenticatesFullCellIssuerAndSourceCoverageIncludingPartialPagesAndKeyRetirement() throws Exception {
        var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair(); var now = Instant.now();
        var event = new RevocationState.Event(ISSUER, new UserId("TEST_ONLY_SUBJECT"), "TEST_ONLY_JTI", 2, 1, now);
        var retirement = new RevocationReconciler.KeyRetirement(ISSUER, "TEST_ONLY_RSA", 2, now);
        var complete = seal("c001", new RevocationReconciler.Batch(0, 2, List.of(event), now, "", List.of(retirement), 2), key);
        var verifier = new RevocationSourceVerifier("c001", ISSUER, Map.of("TEST_ONLY", key.getPublic()));
        assertThat(verifier.test(complete)).isTrue();
        var partial = seal("c001", new RevocationReconciler.Batch(0, 1, List.of(event), now, "", List.of(), 2), key);
        assertThat(verifier.test(partial)).isTrue(); assertThat(partial.caughtUp()).isFalse();
        assertThat(new RevocationSourceVerifier("c002", ISSUER, Map.of("TEST_ONLY", key.getPublic())).test(complete)).isFalse();
        assertThat(new RevocationSourceVerifier("c001", "https://other.example.test", Map.of("TEST_ONLY", key.getPublic())).test(complete)).isFalse();
        assertThat(verifier.test(new RevocationReconciler.Batch(0, 1, List.of(event), now, partial.sourceProof(), List.of(), 1))).isFalse();
        assertThat(verifier.test(new RevocationReconciler.Batch(0, 2, List.of(event), now, complete.sourceProof(), List.of(), 2))).isFalse();
    }
    @Test void unknownKeyMissingProofTamperedPayloadAndWrongIssuerCannotAuthenticateCoverage() throws Exception {
        var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair(); var outsider = KeyPairGenerator.getInstance("Ed25519").generateKeyPair(); var now = Instant.now();
        var verifier = new RevocationSourceVerifier("c001", ISSUER, Map.of("TEST_ONLY", key.getPublic()));
        var empty = new RevocationReconciler.Batch(0, 0, List.of(), now, "", List.of(), 0);
        assertThat(verifier.test(empty)).isFalse(); assertThat(verifier.test(seal("c001", empty, outsider))).isFalse();
        var authentic = seal("c001", empty, key);
        assertThat(verifier.test(new RevocationReconciler.Batch(0, 0, List.of(), now.plusMillis(1), authentic.sourceProof(), List.of(), 0))).isFalse();
        var alienEvent = new RevocationState.Event("https://other.example.test", new UserId("TEST_ONLY_SUBJECT"), null, 1, 1, now);
        assertThat(verifier.test(seal("c001", new RevocationReconciler.Batch(0, 1, List.of(alienEvent), now, "", List.of(), 1), key))).isFalse();
        assertThatThrownBy(() -> new RevocationSourceVerifier("c001", ISSUER, Map.of())).isInstanceOf(IllegalArgumentException.class);
    }
}
