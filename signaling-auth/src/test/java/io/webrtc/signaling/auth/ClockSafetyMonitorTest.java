package io.webrtc.signaling.auth;

import static org.assertj.core.api.Assertions.*;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

/** Signed test-only monitor reports prove consumer fencing, never infrastructure clock quality. */
class ClockSafetyMonitorTest {
    static final Instant START = Instant.parse("2026-10-04T00:00:00Z");
    static final UUID POD = UUID.randomUUID(), BOOT = UUID.randomUUID();
    static final class Time extends Clock {
        Instant wall = START; final AtomicLong elapsed = new AtomicLong();
        public ZoneId getZone() { return ZoneOffset.UTC; } public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return wall; }
        void advance(Duration duration) { wall = wall.plus(duration); elapsed.addAndGet(duration.toNanos()); }
    }
    static ClockSafetyMonitor.Report report(long sequence, Instant now, int uncertainty, int rate, boolean continuous) {
        return new ClockSafetyMonitor.Report("TEST_ONLY_TIME", "c001", 1, POD, BOOT, sequence, now, now.plusSeconds(5), uncertainty, rate, continuous);
    }
    static String sign(ClockSafetyMonitor.Report report, KeyPair key) throws Exception {
        var signature = Signature.getInstance("Ed25519"); signature.initSign(key.getPrivate()); signature.update(ClockSafetyMonitor.signingBytes(report));
        return Base64.getUrlEncoder().withoutPadding().encodeToString(signature.sign());
    }
    static ClockSafetyMonitor monitor(Time time, KeyPair key) {
        return new ClockSafetyMonitor("c001", 1, POD, BOOT, Map.of("TEST_ONLY_TIME", key.getPublic()), time, time.elapsed::get);
    }
    @Test void requiresPinnedSourceAndFreshSignedBoundWithConservativeExpiryFromRequestStart() throws Exception {
        var time = new Time(); var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair(); var monitor = monitor(time, key);
        assertThat(monitor.valid()).isFalse();
        var report = report(1, START, 250000, 1000, true); long started = time.elapsed.get();
        time.advance(Duration.ofSeconds(2)); assertThat(monitor.observe(report, sign(report, key), started)).isTrue(); assertThat(monitor.valid()).isTrue();
        time.advance(Duration.ofMillis(2700)); assertThat(monitor.valid()).isTrue();
        time.advance(Duration.ofMillis(60)); assertThat(monitor.valid()).isFalse();
        assertThatThrownBy(() -> new ClockSafetyMonitor("c001", 1, POD, BOOT, Map.of(), time, time.elapsed::get)).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void replayCannotExtendTimeTrustAndWrongKeyOrStorageAndProcessBindingAreRejected() throws Exception {
        var time = new Time(); var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair(); var outsider = KeyPairGenerator.getInstance("Ed25519").generateKeyPair(); var monitor = monitor(time, key);
        var original = report(1, START, 200000, 1000, true);
        assertThat(monitor.observe(original, sign(original, outsider), 0)).isFalse();
        assertThat(monitor.observe(original, sign(original, key), 0)).isTrue(); time.advance(Duration.ofSeconds(4));
        assertThat(monitor.observe(original, sign(original, key), time.elapsed.get())).isFalse();
        time.advance(Duration.ofSeconds(1)); assertThat(monitor.valid()).isFalse();
        var wrongStorage = new ClockSafetyMonitor.Report("TEST_ONLY_TIME", "c001", 2, POD, BOOT, 2, time.wall, time.wall.plusSeconds(5), 100000, 1000, true);
        assertThat(monitor.observe(wrongStorage, sign(wrongStorage, key), time.elapsed.get())).isFalse();
        var wrongBoot = new ClockSafetyMonitor.Report("TEST_ONLY_TIME", "c001", 1, POD, UUID.randomUUID(), 2, time.wall, time.wall.plusSeconds(5), 100000, 1000, true);
        assertThat(monitor.observe(wrongBoot, sign(wrongBoot, key), time.elapsed.get())).isFalse();
    }
    @Test void wallStepOrHostPauseInvalidatesCachedTrustUntilAFreshHigherSequenceReport() throws Exception {
        var time = new Time(); var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair(); var monitor = monitor(time, key);
        var report = report(1, START, 200000, 1000, true); assertThat(monitor.observe(report, sign(report, key), 0)).isTrue();
        time.wall = time.wall.minusSeconds(1); assertThat(monitor.valid()).isFalse();
        time.wall = START; assertThat(monitor.valid()).isFalse(); // Returning the wall clock cannot revive the report.
        var fresh = report(2, time.wall, 200000, 1000, true); assertThat(monitor.observe(fresh, sign(fresh, key), time.elapsed.get())).isTrue();
        time.advance(Duration.ofSeconds(16)); assertThat(monitor.valid()).isFalse();
    }
    @Test void staleResponseExcessiveUncertaintyRateOrLostContinuityCannotAuthorizeShortGrants() throws Exception {
        for (var values : List.of(new int[]{250001,1000,1}, new int[]{100000,1001,1}, new int[]{100000,1000,0})) {
            var time = new Time(); var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair(); var monitor = monitor(time, key);
            var report = report(1, START, values[0], values[1], values[2] == 1);
            assertThat(monitor.observe(report, sign(report,key), 0)).isFalse(); assertThat(monitor.valid()).isFalse();
        }
        var time = new Time(); var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair(); var monitor = monitor(time, key);
        var report = report(1, START, 100000, 1000, true); time.advance(Duration.ofSeconds(6));
        assertThat(monitor.observe(report, sign(report,key), 0)).isFalse(); assertThat(monitor.valid()).isFalse();
    }
}
