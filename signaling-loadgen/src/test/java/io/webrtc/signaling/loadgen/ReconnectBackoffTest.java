package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.junit.jupiter.api.Test;

class ReconnectBackoffTest {
  @Test
  void fullJitterUsesHalfSecondInitialCeilingAndThirtySecondMaximum() {
    for (int attempt = 0; attempt < 100; attempt++) {
      long ceiling = Math.min(30_000_000_000L, 500_000_000L << Math.min(attempt, 6));
      for (long socket = 0; socket < 100; socket++)
        assertThat(ReconnectBackoff.delayNanos(42, socket, attempt, 0)).isBetween(0L, ceiling);
    }
  }

  @Test
  void sourceSeedIsReproducibleAndDifferentSocketsSpreadRetries() {
    var values = new HashSet<Long>();
    for (long socket = 0; socket < 100; socket++) {
      long delay = ReconnectBackoff.delayNanos(42, socket, 4, 0);
      assertThat(ReconnectBackoff.delayNanos(42, socket, 4, 0)).isEqualTo(delay);
      values.add(delay);
    }
    assertThat(values).hasSizeGreaterThan(90);
  }

  @Test
  void serverRetryAfterIsAnOriginalFloorEvenAboveTheJitterCap() {
    assertThat(ReconnectBackoff.delayNanos(42, 0, 0, Duration.ofSeconds(90).toNanos()))
        .isEqualTo(Duration.ofSeconds(90).toNanos());
  }

  @Test
  void retryAfterSupportsHttpDeltaAndAbsoluteDateWithoutRenewingIt() {
    var now = Instant.parse("2026-10-05T00:00:00Z");
    assertThat(ReconnectBackoff.retryAfter("12", now)).hasValue(Duration.ofSeconds(12));
    assertThat(
            ReconnectBackoff.retryAfter(
                DateTimeFormatter.RFC_1123_DATE_TIME.format(
                    now.plusSeconds(20).atZone(ZoneOffset.UTC)),
                now))
        .hasValue(Duration.ofSeconds(20));
    assertThat(
            ReconnectBackoff.retryAfter(
                DateTimeFormatter.RFC_1123_DATE_TIME.format(
                    now.minusSeconds(20).atZone(ZoneOffset.UTC)),
                now))
        .hasValue(Duration.ZERO);
  }

  @Test
  void malformedOrUnboundedRetryAfterAndInvalidSourceAreRejected() {
    for (var text : List.of("-1", "1.5", "NaN", "999999999999999999999", "86401", "tomorrow"))
      assertThat(ReconnectBackoff.retryAfter(text, Instant.now())).isEmpty();
    assertThatThrownBy(() -> ReconnectBackoff.delayNanos(1, -1, 0, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ReconnectBackoff.delayNanos(1, 0, -1, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
