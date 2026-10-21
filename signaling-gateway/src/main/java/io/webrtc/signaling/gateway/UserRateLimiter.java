package io.webrtc.signaling.gateway;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Bounded per-(issuer, user) token bucket. Admission needs no I/O. When the bound is reached the
 * least recently used bucket is dropped, which can at worst restore one user's burst allowance.
 */
public final class UserRateLimiter {
  private final double rate, burst;
  private final LongSupplier nanos;
  private final Map<String, Bucket> buckets;

  public UserRateLimiter(GatewayRateLimits limits, LongSupplier nanos) {
    Objects.requireNonNull(limits);
    rate = limits.userRate();
    burst = limits.userBurst();
    this.nanos = Objects.requireNonNull(nanos);
    int bound = limits.trackedUsers();
    buckets =
        new LinkedHashMap<>(16, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<String, Bucket> eldest) {
            return size() > bound;
          }
        };
  }

  /**
   * Returns 0 when admitted, otherwise the milliseconds after which one token will be available.
   */
  public synchronized long tryAcquire(String key) {
    long now = nanos.getAsLong();
    Bucket bucket = buckets.computeIfAbsent(key, k -> new Bucket(burst, now));
    bucket.tokens =
        Math.min(burst, bucket.tokens + (now - bucket.at) * rate / TimeUnit.SECONDS.toNanos(1));
    bucket.at = now;
    if (bucket.tokens >= 1) {
      bucket.tokens--;
      return 0;
    }
    return Math.max(1, (long) Math.ceil((1 - bucket.tokens) * 1000.0 / rate));
  }

  public synchronized int tracked() {
    return buckets.size();
  }

  private static final class Bucket {
    double tokens;
    long at;

    Bucket(double tokens, long at) {
      this.tokens = tokens;
      this.at = at;
    }
  }
}
