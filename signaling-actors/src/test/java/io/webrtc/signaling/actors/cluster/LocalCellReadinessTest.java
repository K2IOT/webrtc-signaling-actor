package io.webrtc.signaling.actors.cluster;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalCellReadinessTest {
    private static ClusterReadiness.Snapshot healthy(int actors, int domains) {
        return new ClusterReadiness.Snapshot(
                true, true, true, true, true, true, actors, domains, false);
    }

    @Test
    void productionStillRequiresTwoActualFailureDomains() {
        var readiness = new ClusterReadiness();
        readiness.update(healthy(6, 1));
        assertThat(readiness.safetyReady()).isTrue();
        assertThat(readiness.businessReady()).isFalse();
        readiness.update(healthy(4, 2));
        assertThat(readiness.businessReady()).isTrue();
    }

    @Test
    void acknowledgedLocalCellRetainsQuorumSourcesAndDrain() {
        var readiness = ClusterReadiness.localMinikube("LOCAL_TEST_ONLY");
        readiness.update(healthy(4, 1));
        assertThat(readiness.businessReady()).isTrue();
        readiness.update(healthy(3, 1));
        assertThat(readiness.businessReady()).isFalse();
        readiness.update(healthy(6, 0));
        assertThat(readiness.businessReady()).isFalse();
        readiness.update(healthy(6, 1));
        readiness.installSafetyGate(() -> false);
        assertThat(readiness.businessReady()).isFalse();
        assertThat(readiness.safetyReady()).isFalse();
        readiness.beginDrain();
        assertThat(readiness.snapshot().draining()).isTrue();
    }

    @Test
    void localExceptionRequiresExplicitTestOnlyAcknowledgement() {
        assertThatThrownBy(() -> ClusterReadiness.localMinikube(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ClusterReadiness.localMinikube("production"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aDeclaredThreeMemberCellIsReadyAtTwoUpMembersAndNotAtOne() {
        var readiness =
                ClusterReadiness.localMinikube("LOCAL_TEST_ONLY", ClusterShape.localMajority(3));
        readiness.update(healthy(3, 1));
        assertThat(readiness.businessReady()).isTrue();
        readiness.update(healthy(2, 1));
        assertThat(readiness.businessReady()).isTrue();
        readiness.update(healthy(1, 1));
        assertThat(readiness.businessReady()).isFalse();
        readiness.update(healthy(0, 1));
        assertThat(readiness.businessReady()).isFalse();
    }

    @Test
    void aSmallerCellKeepsEverySafetyGateAndTheTestOnlyAcknowledgement() {
        var readiness =
                ClusterReadiness.localMinikube("LOCAL_TEST_ONLY", ClusterShape.localMajority(3));
        readiness.update(healthy(3, 0));
        assertThat(readiness.businessReady()).isFalse();
        readiness.update(healthy(3, 1));
        readiness.installSafetyGate(() -> false);
        assertThat(readiness.safetyReady()).isFalse();
        assertThat(readiness.businessReady()).isFalse();
        assertThatThrownBy(
                () -> ClusterReadiness.localMinikube("production", ClusterShape.localMajority(3)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ClusterReadiness.localMinikube(null, ClusterShape.localMajority(3)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theProductionAndDefaultLocalCellsStillNeedFourUpMembers() {
        var production = new ClusterReadiness();
        production.update(healthy(3, 2));
        assertThat(production.businessReady()).isFalse();
        var local = ClusterReadiness.localMinikube("LOCAL_TEST_ONLY");
        local.update(healthy(3, 1));
        assertThat(local.businessReady()).isFalse();
        var sixLocal =
                ClusterReadiness.localMinikube("LOCAL_TEST_ONLY", ClusterShape.PRODUCTION);
        sixLocal.update(healthy(3, 1));
        assertThat(sixLocal.businessReady()).isFalse();
        sixLocal.update(healthy(4, 1));
        assertThat(sixLocal.businessReady()).isTrue();
    }
}
