package io.webrtc.signaling.storage;

import java.time.Duration;
import java.util.*;

/** Per-class, nonborrowable resources. A supplied service evidence ID is an operator obligation. */
public record DbResourceProfile(
    int count, long inputBytes, long retainedBytes, long resultBytes,
    long retainedByteLimit, long resultByteLimit, int completionSlots,
    long serviceNanos, String serviceEvidence) {
  public DbResourceProfile {
    if (count < 1 || inputBytes < 1 || retainedBytes < inputBytes || resultBytes < 1
        || retainedByteLimit < retainedBytes || resultByteLimit < resultBytes
        || completionSlots < 1 || serviceNanos < 0 || serviceNanos > Duration.ofSeconds(2).toNanos()
        || serviceEvidence == null || serviceEvidence.isBlank() || serviceEvidence.length() > 128)
      throw new IllegalArgumentException("Explicit finite DB resource profile required");
  }

  public DbCost maximumCharge() {
    return new DbCost(inputBytes, retainedBytes, resultBytes, serviceNanos);
  }

  public boolean accepts(DbCost cost) {
    return cost != null && cost.inputBytes() <= inputBytes && cost.retainedBytes() <= retainedBytes
        && cost.resultBytes() <= resultBytes && cost.serviceNanos() >= serviceNanos;
  }

  public void requireProductionEvidence() {
    if (serviceNanos == 0 || "LOCAL_TEST_ONLY".equals(serviceEvidence)
        || "NOT_QUALIFIED".equals(serviceEvidence))
      throw new IllegalArgumentException("Production DB service profile evidence required");
  }

  /** Compatibility fixtures only. These charges do not qualify throughput or captured inputs. */
  public static Map<DbClass, DbResourceProfile> localTestOnly(Map<DbClass, Integer> quotas) {
    var profiles = new EnumMap<DbClass, DbResourceProfile>(DbClass.class);
    quotas.forEach((clazz, count) -> {
      if (clazz == null || count == null || count < 1) throw new IllegalArgumentException();
      // Outbox128 payloads expand to base64 plus destinations; recovery512 projections and
      // transfer pages can exceed1MiB. Include aggregate DTO/object accounting in result bounds.
      long input = 2L * 1024 * 1024;
      long result = switch (clazz) {
        case RECOVERY, MAINTENANCE -> 32L * 1024 * 1024;
        default -> 4L * 1024 * 1024;
      };
      long retained = Math.addExact(Math.multiplyExact(input, 2), Math.multiplyExact(result, 2));
      profiles.put(clazz, new DbResourceProfile(count, input, retained, result,
          Math.multiplyExact(retained, count), Math.multiplyExact(result, count), count, 0,
          "LOCAL_TEST_ONLY"));
    });
    return Map.copyOf(profiles);
  }
}
