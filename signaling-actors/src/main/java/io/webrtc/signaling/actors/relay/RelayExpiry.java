package io.webrtc.signaling.actors.relay;

import java.util.concurrent.*;

/** One process scheduler; tasks exist only for admitted retained relay payloads. */
final class RelayExpiry {
  private static final ScheduledThreadPoolExecutor TIMER =
      new ScheduledThreadPoolExecutor(
          1,
          r -> {
            var t = new Thread(r, "relay-retention");
            t.setDaemon(true);
            return t;
          });

  static {
    TIMER.setRemoveOnCancelPolicy(true);
  }

  static ScheduledFuture<?> schedule(Runnable action, long nanos) {
    return TIMER.schedule(action, Math.max(0, nanos), TimeUnit.NANOSECONDS);
  }

  private RelayExpiry() {}
}
