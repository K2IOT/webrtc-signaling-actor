package io.webrtc.signaling.storage;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

public final class DbAdmission {
  private final Map<DbClass, Semaphore> credits = new EnumMap<>(DbClass.class);
  private final Map<DbClass, Integer> limits = new EnumMap<>(DbClass.class);

  public DbAdmission(Map<DbClass, Integer> limits) {
    if (limits.isEmpty()) throw new IllegalArgumentException("Missing DB quotas");
    limits.forEach(
        (c, n) -> {
          if (n == null || n < 1) throw new IllegalArgumentException("Invalid DB quota");
          this.limits.put(c, n);
          credits.put(c, new Semaphore(n));
        });
  }

  boolean acquire(DbClass c) {
    var s = credits.get(c);
    return s != null && s.tryAcquire();
  }

  void release(DbClass c) {
    credits.get(c).release();
  }

  public int running(DbClass c) {
    return limits.getOrDefault(c, 0)
        - (credits.containsKey(c) ? credits.get(c).availablePermits() : 0);
  }

  public int poolCapacity(boolean safety) {
    return limits.entrySet().stream()
        .filter(e -> e.getKey().safety() == safety)
        .mapToInt(Map.Entry::getValue)
        .sum();
  }
}
