package io.webrtc.signaling.rpc;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class RpcRetryBackoffTest {
  @Test
  void exponentialFullJitterHasPositiveBoundedDelayAndOneOriginalDeadline() {
    long remaining = Duration.ofSeconds(1).toNanos();
    assertThat(RpcRetryBackoff.delayNanos(0, 0, remaining)).hasValue(1_000_000);
    assertThat(RpcRetryBackoff.delayNanos(0, .5, remaining)).hasValue(12_500_000);
    assertThat(RpcRetryBackoff.delayNanos(1, .5, remaining)).hasValue(25_000_000);
    assertThat(RpcRetryBackoff.delayNanos(2, .5, remaining)).isEmpty();
    assertThat(RpcRetryBackoff.delayNanos(1, .99, 10_000_000)).isEmpty();
    assertThatThrownBy(() -> RpcRetryBackoff.delayNanos(0, Double.NaN, remaining))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
