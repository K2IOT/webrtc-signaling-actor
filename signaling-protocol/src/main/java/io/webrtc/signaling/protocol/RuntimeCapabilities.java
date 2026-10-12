package io.webrtc.signaling.protocol;

/** Capability facts come only from native authenticated enrollment, never request payloads. */
@FunctionalInterface
public interface RuntimeCapabilities {
  String SESSION_S1 = "session-s1";
  String FULL_CONNECTION_ACTIVE = "full-connection-active";
  String CAUSAL_ROUTE_LOSS = "causal-route-loss";
  String NATIVE_RENEWAL_V1 = "native-renewal-v1";
  String NATIVE_RECOVERY_RECEIPT_V1 = "native-recovery-receipt-v1";
  String NATIVE_WAKE_RECEIPT_V1 = "native-wake-receipt-v1";
  String STORAGE_DOMAIN_ADOPTION_V1 = "storage-domain-adoption-v1";
  String NATIVE_CONTROL_RECEIPT_V1 = "native-control-receipt-v1";
  String HOME_PROOF_HIGH_WATER_V1 = "home-proof-high-water-v1";
  String NATIVE_ACTOR_TRACE_V1 = "native-actor-trace-v1";
  String NATIVE_INITIAL_ICE_RECEIPT_V1 = "native-initial-ice-receipt-v1";
  RuntimeCapabilities DENY = (cell, feature) -> false;

  boolean permits(String cell, String feature);

  /** New durable feature admission may be disabled while existing original owners settle. */
  default boolean permitsNew(String cell, String feature) {
    return permits(cell, feature);
  }

  default boolean permitsGateway(
      String cell, String gatewayId, java.util.UUID bootId, String feature) {
    return false;
  }

  default void requireGateway(
      String cell, String gatewayId, java.util.UUID bootId, String feature) {
    if (!permitsGateway(cell, gatewayId, bootId, feature))
      throw new IllegalStateException("Native gateway capability unavailable: " + feature);
  }

  default void require(String cell, String feature) {
    if (!permits(cell, feature))
      throw new IllegalStateException("Native peer capability unavailable: " + feature);
  }
}
