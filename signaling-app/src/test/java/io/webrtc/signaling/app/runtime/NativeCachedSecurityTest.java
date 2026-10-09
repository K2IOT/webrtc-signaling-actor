package io.webrtc.signaling.app.runtime;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.worker.*;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class NativeCachedSecurityTest {
    final Instant now = Instant.parse("2026-10-09T01:00:00Z");
    final KeyPair key = key();
    final AtomicLong elapsed = new AtomicLong(1_000_000_000L);
    final MutableClock wall = new MutableClock(now);
    final IdentitySecurityContract contract = new IdentitySecurityContract("LOCAL_TEST_ONLY_ISSUER", "LOCAL_TEST_ONLY", Duration.ofMinutes(15), Duration.ofSeconds(30), Duration.ofSeconds(1), Duration.ofSeconds(5), true, "LOCAL_TEST_ONLY_LOG");
    final RevocationSourceVerifier verifier = new RevocationSourceVerifier("c001", contract.issuer(), Map.of("source", key.getPublic()));

    @Test void unknownCacheCannotAuthorizeAnEmptyRevocationState() throws Exception {
        var cache = cache(8);
        assertThat(cache.usable()).isFalse();
        assertThat(cache.check(principal("user-a", "jti-a", "rsa-a", 1), now)).isEqualTo(AuthorizationStatus.FRESHNESS_UNKNOWN);
        assertThat(cache.apply(signed(0, 0, 0, List.of(), List.of()), elapsed.get())).isTrue();
        assertThat(cache.usable()).isTrue();
        assertThat(cache.check(principal("user-a", "jti-a", "rsa-a", 1), now)).isEqualTo(AuthorizationStatus.ALLOWED);
    }

    @Test void verifiedFeedEnforcesSessionUserAndRetiredKeyScopes() throws Exception {
        var cache = cache(8);
        var event = new RevocationState.Event(contract.issuer(), new UserId("user-a"), "jti-a", 1, 1, now);
        assertThat(cache.apply(signed(0, 1, 1, List.of(event), List.of()), elapsed.get())).isTrue();
        assertThat(cache.check(principal("user-a", "jti-a", "rsa-a", 1), now)).isEqualTo(AuthorizationStatus.REVOKED);
        assertThat(cache.check(principal("user-a", "jti-b", "rsa-a", 1), now)).isEqualTo(AuthorizationStatus.ALLOWED);
        wall.advance(Duration.ofMillis(1));
        var user = new RevocationState.Event(contract.issuer(), new UserId("user-a"), null, 2, 2, now);
        var retired = new RevocationReconciler.KeyRetirement(contract.issuer(), "rsa-a", 3, now);
        assertThat(cache.apply(signed(1, 3, 3, List.of(user), List.of(retired)), elapsed.get())).isTrue();
        assertThat(cache.check(principal("user-a", "jti-b", "rsa-b", 2), wall.instant())).isEqualTo(AuthorizationStatus.REVOKED);
        assertThat(cache.check(principal("user-b", "jti-b", "rsa-a", 1), wall.instant())).isEqualTo(AuthorizationStatus.REVOKED);
        assertThat(cache.check(principal("user-b", "jti-b", "rsa-b", 1), wall.instant())).isEqualTo(AuthorizationStatus.ALLOWED);
    }

    @Test void partialCatchupReplayAndCursorMismatchNeverRefreshAdmission() throws Exception {
        var cache = cache(8);
        assertThat(cache.apply(signed(0, 0, 1, List.of(), List.of()), elapsed.get())).isTrue();
        assertThat(cache.usable()).isFalse();
        wall.advance(Duration.ofMillis(1));
        var complete = signed(0, 1, 1, List.of(new RevocationState.Event(contract.issuer(), new UserId("user-a"), null, 1, 1, now)), List.of());
        assertThat(cache.apply(complete, elapsed.get())).isTrue();
        assertThat(cache.usable()).isTrue();
        assertThat(cache.offset()).isEqualTo(1);
        assertThat(cache.apply(complete, elapsed.get())).isFalse();
        assertThat(cache.usable()).isFalse();
        wall.advance(Duration.ofMillis(1));
        assertThat(cache.apply(signed(0, 2, 2, List.of(), List.of()), elapsed.get())).isFalse();
        assertThat(cache.offset()).isEqualTo(1);
    }

    @Test void ageAndOriginalRequestTimeBoundTheCachedDecision() throws Exception {
        var cache = cache(8);
        long started = elapsed.get();
        var page = signed(0, 0, 0, List.of(), List.of());
        elapsed.addAndGet(Duration.ofSeconds(4).toNanos());
        wall.advance(Duration.ofSeconds(4));
        assertThat(cache.apply(page, started)).isTrue();
        assertThat(cache.usable()).isTrue();
        elapsed.addAndGet(Duration.ofSeconds(1).toNanos());
        assertThat(cache.usable()).isFalse();
        assertThat(cache.check(principal("user-a", "jti-a", "rsa-a", 1), wall.instant())).isEqualTo(AuthorizationStatus.FRESHNESS_UNKNOWN);
    }

    @Test void invalidSignatureAndCapacityExhaustionPreserveRevocationsAndCloseAdmission() throws Exception {
        var cache = cache(1);
        var revoke = new RevocationState.Event(contract.issuer(), new UserId("user-a"), null, 1, 1, now);
        assertThat(cache.apply(signed(0, 1, 1, List.of(revoke), List.of()), elapsed.get())).isTrue();
        wall.advance(Duration.ofMillis(1));
        var valid = signed(1, 2, 2, List.of(new RevocationState.Event(contract.issuer(), new UserId("user-b"), null, 1, 2, now)), List.of());
        assertThat(cache.apply(valid, elapsed.get())).isFalse();
        assertThat(cache.offset()).isEqualTo(1);
        assertThat(cache.usable()).isFalse();
        assertThat(cache.retainedEntries()).isEqualTo(1);
        assertThat(cache.check(principal("user-a", "jti-a", "rsa-a", 1), wall.instant())).isEqualTo(AuthorizationStatus.FRESHNESS_UNKNOWN);
        var invalid = new RevocationReconciler.Batch(1, 1, List.of(), wall.instant(), "source." + "a".repeat(86), List.of(), 1);
        assertThat(cache.apply(invalid, elapsed.get())).isFalse();
    }

    NativeCachedSecurity cache(int capacity) { return new NativeCachedSecurity(contract, verifier, capacity, wall, elapsed::get); }
    AuthPrincipal principal(String user, String jti, String kid, long epoch) { return new AuthPrincipal(new UserId(user), new SessionKey(contract.issuer(), jti), now.plusSeconds(600), now, kid, epoch); }
    RevocationReconciler.Batch signed(long from, long high, long sourceHigh, List<RevocationState.Event> events, List<RevocationReconciler.KeyRetirement> retired) throws Exception {
        var unsigned = new RevocationReconciler.Batch(from, high, events, wall.instant(), "", retired, sourceHigh);
        var signer = Signature.getInstance("Ed25519"); signer.initSign(key.getPrivate()); signer.update(RevocationSourceVerifier.signingBytes("c001", contract.issuer(), unsigned));
        return new RevocationReconciler.Batch(from, high, events, wall.instant(), "source." + Base64.getUrlEncoder().withoutPadding().encodeToString(signer.sign()), retired, sourceHigh);
    }
    static KeyPair key() { try { return KeyPairGenerator.getInstance("Ed25519").generateKeyPair(); } catch (Exception e) { throw new AssertionError(e); } }
    static final class MutableClock extends Clock {
        Instant value; MutableClock(Instant value) { this.value = value; }
        void advance(Duration duration) { value = value.plus(duration); }
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return value; }
    }
}
