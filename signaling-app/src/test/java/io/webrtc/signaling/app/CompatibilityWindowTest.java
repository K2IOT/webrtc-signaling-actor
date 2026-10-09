package io.webrtc.signaling.app;

import static org.assertj.core.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

class CompatibilityWindowTest {
  static List<CompatibilityWindow.Peer> peers(CompatibilityWindow.Peer p) {
    return java.util.stream.IntStream.range(0, 4)
        .mapToObj(
            i ->
                new CompatibilityWindow.Peer(
                    UUID.randomUUID(),
                    "az" + (i % 2),
                    p.cell(),
                    p.major(),
                    p.minor(),
                    p.fingerprint(),
                    p.features()))
        .toList();
  }

  @Test
  void compatiblePreviousMinorCanJoinButNewFeaturesWaitForEveryAuthenticatedUpPeer() {
    var current =
        new CompatibilityWindow.Peer(
            UUID.randomUUID(),
            "az1",
            "c001",
            1,
            1,
            "a".repeat(64),
            Set.of(
                CompatibilityWindow.Feature.SESSION_S1,
                CompatibilityWindow.Feature.FULL_CONNECTION_ACTIVE));
    var previous =
        new CompatibilityWindow.Peer(
            UUID.randomUUID(), "az1", "c001", 1, 0, "a".repeat(64), Set.of());
    var window = new CompatibilityWindow("c001", 1, 1, "a".repeat(64));
    assertThat(window.accepts(previous)).isTrue();
    assertThat(window.enable(CompatibilityWindow.Feature.SESSION_S1, List.of(current, previous)))
        .isFalse();
    assertThat(window.enabled(CompatibilityWindow.Feature.SESSION_S1)).isFalse();
    assertThat(window.enable(CompatibilityWindow.Feature.SESSION_S1, peers(current))).isTrue();
    assertThat(window.accepts(previous)).isFalse();
    assertThat(
            window.accepts(
                new CompatibilityWindow.Peer(
                    UUID.randomUUID(), "az1", "c002", 1, 1, "a".repeat(64), current.features())))
        .isFalse();
    assertThat(
            window.accepts(
                new CompatibilityWindow.Peer(
                    UUID.randomUUID(), "az1", "c001", 2, 1, "a".repeat(64), current.features())))
        .isFalse();
    assertThat(
            window.accepts(
                new CompatibilityWindow.Peer(
                    UUID.randomUUID(), "az1", "c001", 1, 1, "b".repeat(64), current.features())))
        .isFalse();
  }

  @Test
  void rollbackRequiresDisabledFeatureAdmissionAndNoIncompatibleDurableOrPhysicalWork() {
    var window = new CompatibilityWindow("c001", 1, 1, "a".repeat(64));
    var current =
        new CompatibilityWindow.Peer(
            UUID.randomUUID(),
            "az1",
            "c001",
            1,
            1,
            "a".repeat(64),
            Set.of(CompatibilityWindow.Feature.SESSION_S1));
    assertThat(window.enable(CompatibilityWindow.Feature.SESSION_S1, peers(current))).isTrue();
    assertThat(window.rollbackAllowed(0, true, true)).isFalse();
    window.disableAll();
    assertThat(window.rollbackAllowed(1, true, true)).isFalse();
    assertThat(window.rollbackAllowed(0, false, true)).isFalse();
    assertThat(window.rollbackAllowed(0, true, false)).isFalse();
    assertThat(window.rollbackAllowed(0, true, true)).isTrue();
    assertThat(window.enable(CompatibilityWindow.Feature.SESSION_S1, List.of())).isFalse();
    assertThat(
            window.enable(
                CompatibilityWindow.Feature.SESSION_S1,
                List.of(current, current, current, current)))
        .isFalse();
  }
}
