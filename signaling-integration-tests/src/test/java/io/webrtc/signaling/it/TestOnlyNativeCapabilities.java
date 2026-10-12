package io.webrtc.signaling.storage;

import io.webrtc.signaling.protocol.RuntimeCapabilities;
import java.util.Set;
import java.util.UUID;

/** Explicit isolated fixture features; these declarations do not attest deployed peer discovery. */
final class TestOnlyNativeCapabilities {
  private static final Set<String> FEATURES =
      Set.of(
          RuntimeCapabilities.SESSION_S1, RuntimeCapabilities.FULL_CONNECTION_ACTIVE,
          RuntimeCapabilities.CAUSAL_ROUTE_LOSS, RuntimeCapabilities.NATIVE_RENEWAL_V1,
          RuntimeCapabilities.NATIVE_RECOVERY_RECEIPT_V1,
              RuntimeCapabilities.NATIVE_WAKE_RECEIPT_V1,
          RuntimeCapabilities.STORAGE_DOMAIN_ADOPTION_V1,
              RuntimeCapabilities.NATIVE_CONTROL_RECEIPT_V1,
          RuntimeCapabilities.HOME_PROOF_HIGH_WATER_V1, RuntimeCapabilities.NATIVE_ACTOR_TRACE_V1);

  static RuntimeCapabilities cells(String... cells) {
    var enrolled = Set.of(cells);
    return (cell, feature) -> enrolled.contains(cell) && FEATURES.contains(feature);
  }

  static RuntimeCapabilities gateway(String cell, String gateway, UUID boot) {
    return new RuntimeCapabilities() {
      public boolean permits(String candidate, String feature) {
        return cell.equals(candidate) && FEATURES.contains(feature);
      }

      public boolean permitsGateway(String candidate, String id, UUID incarnation, String feature) {
        return permits(candidate, feature) && gateway.equals(id) && boot.equals(incarnation);
      }
    };
  }

  private TestOnlyNativeCapabilities() {}
}
