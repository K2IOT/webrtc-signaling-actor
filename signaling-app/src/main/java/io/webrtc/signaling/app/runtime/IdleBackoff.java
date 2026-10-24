package io.webrtc.signaling.app.runtime;

import java.util.concurrent.TimeUnit;

/**
 * Pause of a periodic maintenance scan: none while a scan finds work, doubling from the base after
 * each idle scan up to the cap. Monotonic nanos; callers serialise access through their job.
 */
final class IdleBackoff {
    private final long baseMillis, capMillis;
    private long pauseMillis, nextNanos;
    private boolean scheduled;

    IdleBackoff(long baseMillis, long capMillis) {
        if (baseMillis < 1 || capMillis < baseMillis) throw new IllegalArgumentException("Invalid backoff");
        this.baseMillis = baseMillis;
        this.capMillis = capMillis;
        pauseMillis = baseMillis;
    }

    synchronized boolean due(long nowNanos) {
        return !scheduled || nowNanos - nextNanos >= 0;
    }

    synchronized void completed(boolean active, long nowNanos) {
        if (active) {
            pauseMillis = baseMillis;
            scheduled = false;
            return;
        }
        pauseMillis = scheduled ? Math.min(capMillis, pauseMillis * 2) : Math.min(capMillis, baseMillis * 2);
        nextNanos = nowNanos + TimeUnit.MILLISECONDS.toNanos(pauseMillis);
        scheduled = true;
    }
}
