package io.webrtc.signaling.app;

import io.webrtc.signaling.actors.cluster.ClusterShape;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

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

    private static List<CompatibilityWindow.Peer> sized(int count, int zones) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(
                        i ->
                                new CompatibilityWindow.Peer(
                                        UUID.randomUUID(),
                                        "az" + (i % zones),
                                        "c001",
                                        1,
                                        1,
                                        "a".repeat(64),
                                        Set.of(CompatibilityWindow.Feature.SESSION_S1)))
                .toList();
    }

    @Test
    void theDefaultWindowStillNeedsFourToSevenPeersInTwoZones() {
        for (int count = 0; count <= 9; count++) {
            var window = new CompatibilityWindow("c001", 1, 1, "a".repeat(64));
            assertThat(window.enable(CompatibilityWindow.Feature.SESSION_S1, sized(count, 2)))
                    .as("%s peers", count)
                    .isEqualTo(count >= 4 && count <= 7);
        }
    }

    @Test
    void aDeclaredThreeMemberCellOpensTheWindowAtItsQuorumThroughOneSurgeMember() {
        for (int count = 0; count <= 6; count++) {
            var window =
                    new CompatibilityWindow(
                            "c001", 1, 1, "a".repeat(64), ClusterShape.localMajority(3));
            assertThat(window.enable(CompatibilityWindow.Feature.SESSION_S1, sized(count, 2)))
                    .as("%s peers", count)
                    .isEqualTo(count >= 2 && count <= 4);
        }
    }

    @Test
    void aSmallerCellKeepsTheTwoZoneUniquenessAndAcceptanceRules() {
        var shape = ClusterShape.localMajority(3);
        var oneZone = new CompatibilityWindow("c001", 1, 1, "a".repeat(64), shape);
        assertThat(oneZone.enable(CompatibilityWindow.Feature.SESSION_S1, sized(3, 1))).isFalse();

        var peers = new java.util.ArrayList<>(sized(2, 2));
        peers.add(peers.getFirst());
        var duplicate = new CompatibilityWindow("c001", 1, 1, "a".repeat(64), shape);
        assertThat(duplicate.enable(CompatibilityWindow.Feature.SESSION_S1, peers)).isFalse();

        var foreign =
                new CompatibilityWindow.Peer(
                        UUID.randomUUID(), "az1", "c002", 1, 1, "a".repeat(64),
                        Set.of(CompatibilityWindow.Feature.SESSION_S1));
        var mixed = new java.util.ArrayList<>(sized(2, 2));
        mixed.add(foreign);
        var wrongCell = new CompatibilityWindow("c001", 1, 1, "a".repeat(64), shape);
        assertThat(wrongCell.enable(CompatibilityWindow.Feature.SESSION_S1, mixed)).isFalse();
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
