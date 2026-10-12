package io.webrtc.signaling.rpc;

import io.webrtc.signaling.protocol.RuntimeCapabilities;
import java.util.Set;
import java.util.UUID;

/** Declared TEST_ONLY fixture support; never packaged or used as runtime discovery. */
public final class TestOnlyCapabilities {
  private TestOnlyCapabilities() {}

  public static RuntimeCapabilities cells(String... candidates) {
    var enrolled = Set.of(candidates);
    return (cell, feature) ->
        enrolled.contains(cell)
            && Set.of(
                    RuntimeCapabilities.FULL_CONNECTION_ACTIVE,
                    RuntimeCapabilities.SESSION_S1,
                    RuntimeCapabilities.NATIVE_CONTROL_RECEIPT_V1)
                .contains(feature);
  }

  public static RuntimeCapabilities gateway(String cell, String gateway, UUID boot) {
    return new RuntimeCapabilities() {
      public boolean permits(String candidate, String feature) {
        return cell.equals(candidate)
            && Set.of(FULL_CONNECTION_ACTIVE, SESSION_S1, NATIVE_CONTROL_RECEIPT_V1)
                .contains(feature);
      }

      public boolean permitsGateway(String candidate, String id, UUID incarnation, String feature) {
        return permits(candidate, feature) && gateway.equals(id) && boot.equals(incarnation);
      }
    };
  }
}
