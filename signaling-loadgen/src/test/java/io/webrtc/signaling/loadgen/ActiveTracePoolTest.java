package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ActiveTracePoolTest {
  @Test
  void sparseLiveTracesAreSelectedWithoutScanningIdleSockets() {
    var pool = new ActiveTracePool<Long, String>(200000);
    pool.put(199999L, "round-a");
    for (int i = 0; i < 500; i++)
      assertThat(pool.next().orElseThrow())
          .isEqualTo(new ActiveTracePool.Entry<>(199999L, "round-a"));
    assertThat(pool.size()).isEqualTo(1);
    assertThat(pool.remove(199999L, "round-a")).isTrue();
    assertThat(pool.next()).isEmpty();
  }

  @Test
  void staleTraceCleanupCannotRemoveAReplacementRound() {
    var pool = new ActiveTracePool<Long, Object>(2);
    var old = new Object();
    var current = new Object();
    pool.put(1L, old);
    pool.put(1L, current);
    pool.put(2L, new Object());
    assertThat(pool.remove(1L, old)).isFalse();
    assertThat(pool.size()).isEqualTo(2);
    assertThat(pool.next().orElseThrow().value()).isSameAs(current);
    assertThatThrownBy(() -> pool.put(3L, new Object())).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void SelectionRemainsFairAfterRemovalAndUsesBoundedStorage() {
    var pool = new ActiveTracePool<Long, String>(3);
    pool.put(1L, "a");
    pool.put(2L, "b");
    pool.put(3L, "c");
    assertThat(pool.next().orElseThrow().key()).isEqualTo(1L);
    pool.remove(2L, "b");
    var observed = new java.util.HashSet<Long>();
    for (int i = 0; i < 10; i++) observed.add(pool.next().orElseThrow().key());
    assertThat(observed).containsExactlyInAnyOrder(1L, 3L);
    assertThat(pool.size()).isEqualTo(2);
    pool.remove(1L, "a");
    pool.remove(3L, "c");
    pool.put(4L, "d");
    assertThat(pool.next().orElseThrow().key()).isEqualTo(4L);
  }
}
