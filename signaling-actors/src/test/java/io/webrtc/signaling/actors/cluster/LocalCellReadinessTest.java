package io.webrtc.signaling.actors.cluster;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

class LocalCellReadinessTest {
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

  private static ClusterReadiness.Snapshot healthy(int actors, int domains) {
    return new ClusterReadiness.Snapshot(
        true, true, true, true, true, true, actors, domains, false);
  }
}
