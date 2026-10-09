package io.webrtc.signaling.actors.call;

import io.webrtc.signaling.storage.*;
import java.time.*;

/**
 * Scheduling is a hint; the primary transaction checks the stored version and actual database
 * clock.
 */
public final class DurableDeadlineScheduler {
  private DurableDeadlineScheduler() {}

  public static Duration delay(CallSnapshotRepository.Snapshot snapshot, Clock clock) {
    Instant due = DurableDeadlines.due(snapshot);
    if (due == null) return null;
    Duration delay = Duration.between(clock.instant(), due);
    return delay.isNegative() || delay.isZero() ? Duration.ofMillis(1) : delay;
  }
}
