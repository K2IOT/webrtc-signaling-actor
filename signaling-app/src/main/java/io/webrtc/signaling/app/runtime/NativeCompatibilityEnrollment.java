package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.app.CompatibilityWindow;
import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;

/**
 * Native authenticated collectors must supply current remote facts; no request may enroll these.
 */
public record NativeCompatibilityEnrollment(
    BuildIdentity localBuild,
    ApprovedWindow approvedWindow,
    Supplier<Map<String, ActorPeers>> authenticatedRemoteActors,
    Supplier<Map<GatewayBoot, GatewayPeer>> authenticatedGatewayBoots,
    Supplier<Map<String, SchemaObservation>> appliedSchemas,
    Supplier<RollbackObservation> rollbackObservation) {
  /** Operator-approved adjacent executable pair; independent of this process's executable. */
  public record ApprovedWindow(int major, int lowerMinor, int upperMinor) {
    public ApprovedWindow {
      if (major < 1 || lowerMinor < 0 || upperMinor != (long) lowerMinor + 1)
        throw new IllegalArgumentException("Explicit adjacent executable window required");
    }

    public boolean contains(int candidateMajor, int candidateMinor) {
      return candidateMajor == major
          && (candidateMinor == lowerMinor || candidateMinor == upperMinor);
    }
  }

  /** Legacy enrollment has no approved pair and therefore cannot admit feature wire traffic. */
  public NativeCompatibilityEnrollment(
      BuildIdentity build,
      Supplier<Map<String, ActorPeers>> actors,
      Supplier<Map<GatewayBoot, GatewayPeer>> gateways,
      Supplier<Map<String, SchemaObservation>> schemas,
      Supplier<RollbackObservation> rollback) {
    this(build, null, actors, gateways, schemas, rollback);
  }

  public record BuildIdentity(
      int major,
      int minor,
      String executableId,
      String fingerprint,
      Set<CompatibilityWindow.Feature> implementedFeatures) {
    public BuildIdentity {
      implementedFeatures = Set.copyOf(implementedFeatures);
      if (major < 1
          || minor < 0
          || executableId == null
          || !executableId.matches("[a-f0-9]{64}")
          || fingerprint == null
          || !fingerprint.matches("[a-f0-9]{64}"))
        throw new IllegalArgumentException("Actual enrolled executable descriptor required");
    }
  }

  public record ActorPeers(Instant validUntil, List<CompatibilityWindow.Peer> peers) {
    public ActorPeers {
      Objects.requireNonNull(validUntil);
      peers = List.copyOf(peers);
      if (peers.size() > 7) throw new IllegalArgumentException();
    }
  }

  public record GatewayBoot(String cell, String gatewayId, UUID bootId) {}

  public record GatewayPeer(
      Instant validUntil,
      int major,
      int minor,
      String fingerprint,
      Set<CompatibilityWindow.Feature> features) {
    public GatewayPeer {
      Objects.requireNonNull(validUntil);
      features = Set.copyOf(features);
      if (major < 1 || minor < 0 || fingerprint == null || !fingerprint.matches("[a-f0-9]{64}"))
        throw new IllegalArgumentException();
    }
  }

  /** Collector must read successful native Flyway history on the actual writer authority. */
  public record SchemaObservation(Instant validUntil, boolean exactMigration16Applied) {
    public SchemaObservation {
      Objects.requireNonNull(validUntil);
    }
  }

  public record RollbackObservation(
      Instant validUntil,
      OptionalLong incompatibleDurableWork,
      Optional<Boolean> originalPhysicalOwnersSettled,
      Optional<Boolean> expandContractSchemaRetained) {
    public RollbackObservation {
      Objects.requireNonNull(validUntil);
      Objects.requireNonNull(incompatibleDurableWork);
      Objects.requireNonNull(originalPhysicalOwnersSettled);
      Objects.requireNonNull(expandContractSchemaRetained);
    }
  }

  public NativeCompatibilityEnrollment {
    Objects.requireNonNull(localBuild);
    Objects.requireNonNull(authenticatedRemoteActors);
    Objects.requireNonNull(authenticatedGatewayBoots);
    Objects.requireNonNull(appliedSchemas);
    Objects.requireNonNull(rollbackObservation);
  }
}
