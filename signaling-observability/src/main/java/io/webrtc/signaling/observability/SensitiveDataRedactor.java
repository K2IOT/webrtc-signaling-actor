package io.webrtc.signaling.observability;

import java.util.*;

/**
 * Free-form data is excluded by default; approved fields are validated before structured logging.
 */
public final class SensitiveDataRedactor {
  public static final String REDACTED = "[REDACTED]";
  private static final Set<String> NUMERIC =
      Set.of("durationNanos", "queueAgeMillis", "count", "bytes", "eligible", "successful");

  private SensitiveDataRedactor() {}

  public static Map<String, Object> redact(Map<String, ?> input) {
    if (input == null) return Map.of();
    var result = new LinkedHashMap<String, Object>();
    int count = 0;
    for (var entry : input.entrySet()) {
      if (++count > 64) break;
      String key = entry.getKey();
      if (key == null || !key.matches("[A-Za-z][A-Za-z0-9_]{0,63}")) continue;
      Object value = approved(key, entry.getValue());
      result.put(key, value == null ? REDACTED : value);
    }
    return Collections.unmodifiableMap(result);
  }

  static Object approved(String key, Object value) {
    if (value == null) return null;
    if (NUMERIC.contains(key) && value instanceof Number n) {
      if (n instanceof Long || n instanceof Integer)
        return n.longValue() >= 0 ? n.longValue() : null;
      return null;
    }
    if (value instanceof Enum<?> e) value = e.name();
    if (!(value instanceof String s)) return null;
    return switch (key) {
      case "operation" -> member(s, SignalingMetrics.Operation.class);
      case "outcome", "errorCode" -> member(s, SignalingMetrics.Outcome.class);
      case "plane" -> member(s, SignalingMetrics.Plane.class);
      case "resultClass" -> Set.of("BUSINESS", "SERVICE_FAILURE").contains(s) ? s : null;
      case "cell" -> s.matches("c[0-9]{3}") ? s : null;
      case "traceId" -> s.matches("[a-f0-9]{32}") && !s.equals("0".repeat(32)) ? s : null;
      case "spanId" -> s.matches("[a-f0-9]{16}") && !s.equals("0".repeat(16)) ? s : null;
      case "configurationFingerprint" -> s.matches("[a-f0-9]{64}") ? s : null;
      default -> null;
    };
  }

  private static <E extends Enum<E>> String member(String value, Class<E> type) {
    try {
      return Enum.valueOf(type, value).name();
    } catch (IllegalArgumentException invalid) {
      return null;
    }
  }
}
