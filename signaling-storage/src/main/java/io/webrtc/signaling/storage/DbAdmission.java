package io.webrtc.signaling.storage;

import java.util.*;

/** All resource dimensions are reserved atomically before virtual-thread dispatch. */
public final class DbAdmission {
  private final Map<DbClass, DbResourceProfile> profiles;
  private final Map<DbClass, Usage> usage = new EnumMap<>(DbClass.class);

  private static final class Usage {
    int count, completions;
    long retainedBytes, resultBytes;
  }

  /** Existing fixtures must explicitly upgrade profiles before production enrollment. */
  public DbAdmission(Map<DbClass, Integer> limits) {
    this(limits, DbResourceProfile.localTestOnly(limits));
  }

  public DbAdmission(Map<DbClass, Integer> limits, Map<DbClass, DbResourceProfile> profiles) {
    if (limits == null
        || limits.isEmpty()
        || profiles == null
        || !limits.keySet().equals(profiles.keySet()))
      throw new IllegalArgumentException("Missing DB resource profiles");
    this.profiles = Map.copyOf(profiles);
    limits.forEach(
        (clazz, count) -> {
          if (clazz == null || count == null || count < 1 || profiles.get(clazz).count() != count)
            throw new IllegalArgumentException("DB quotas differ from resource profiles");
          usage.put(clazz, new Usage());
        });
  }

  DbCost maximumCharge(DbClass clazz) {
    var profile = profiles.get(clazz);
    return profile == null ? null : profile.maximumCharge();
  }

  synchronized boolean acquire(DbClass clazz, DbCost cost, long remainingNanos) {
    var p = profiles.get(clazz);
    if (p == null || !p.accepts(cost) || cost.serviceNanos() > remainingNanos) return false;
    var u = usage.get(clazz);
    if (u.count >= p.count()
        || u.completions >= p.completionSlots()
        || cost.retainedBytes() > p.retainedByteLimit() - u.retainedBytes
        || cost.resultBytes() > p.resultByteLimit() - u.resultBytes) return false;
    u.count++;
    u.completions++;
    u.retainedBytes += cost.retainedBytes();
    u.resultBytes += cost.resultBytes();
    return true;
  }

  synchronized void release(DbClass clazz, DbCost cost) {
    var u = usage.get(clazz);
    if (u == null || u.count < 1) throw new IllegalStateException("DB reservation already retired");
    u.count--;
    u.completions--;
    u.retainedBytes -= cost.retainedBytes();
    u.resultBytes -= cost.resultBytes();
  }

  public synchronized int running(DbClass clazz) {
    var u = usage.get(clazz);
    return u == null ? 0 : u.count;
  }

  public record Resources(int operations, int completions, long retainedBytes, long resultBytes) {}

  public synchronized Resources reserved(DbClass clazz) {
    var u = usage.get(clazz);
    return u == null
        ? new Resources(0, 0, 0, 0)
        : new Resources(u.count, u.completions, u.retainedBytes, u.resultBytes);
  }

  public Map<DbClass, DbResourceProfile> profiles() {
    return profiles;
  }

  public void requireProductionProfiles() {
    profiles.values().forEach(DbResourceProfile::requireProductionEvidence);
  }

  public int poolCapacity(boolean safety) {
    return profiles.entrySet().stream()
        .filter(e -> e.getKey().safety() == safety)
        .mapToInt(e -> e.getValue().count())
        .reduce(0, Math::addExact);
  }
}
