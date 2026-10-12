package io.webrtc.signaling.app.runtime;

import java.time.Duration;
import java.util.Set;

/**
 * Operator-admitted writer endpoint uses the existing admitted SQL owner; no endpoint discovery.
 */
public record NativeWalEnrollment(
    String cell,
    long storageEpoch,
    String systemIdentifier,
    long timeline,
    Set<String> replicas,
    int synchronousReplicas,
    long maximumLagBytes,
    Duration freshness,
    Duration maximumReplyAge,
    boolean localSingleWriter) {
  public NativeWalEnrollment {
    replicas = Set.copyOf(replicas);
    if (cell == null
        || !cell.matches("[a-z][a-z0-9-]{0,23}")
        || storageEpoch < 1
        || systemIdentifier == null
        || !systemIdentifier.matches("[0-9]{1,20}")
        || timeline < 1
        || timeline > 0xffffffffL
        || replicas.size() > 8
        || replicas.stream().anyMatch(v -> !v.matches("[A-Za-z0-9_.-]{1,63}"))
        || synchronousReplicas < 0
        || synchronousReplicas > replicas.size()
        || maximumLagBytes < 0
        || freshness == null
        || freshness.compareTo(Duration.ofMillis(200)) < 0
        || freshness.compareTo(Duration.ofSeconds(1)) > 0
        || maximumReplyAge == null
        || maximumReplyAge.isNegative()
        || maximumReplyAge.isZero()
        || maximumReplyAge.compareTo(Duration.ofSeconds(5)) > 0
        || (!localSingleWriter && (replicas.isEmpty() || synchronousReplicas < 1))
        || (localSingleWriter && (!replicas.isEmpty() || synchronousReplicas != 0)))
      throw new IllegalArgumentException("Explicit bounded WAL enrollment required");
  }
}
