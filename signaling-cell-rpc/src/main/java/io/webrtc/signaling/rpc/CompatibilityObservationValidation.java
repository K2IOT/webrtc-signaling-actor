package io.webrtc.signaling.rpc;

import io.webrtc.signaling.protocol.internal.*;
import java.time.Instant;

final class CompatibilityObservationValidation {
  private CompatibilityObservationValidation() {}
  static boolean valid(CompatibilityObservationRequest request, CompatibilityObservationReply reply) {
    long now = Instant.now().toEpochMilli();
    return reply.getSerializedSize() <= 16384 && reply.getActorsCount() <= 7
        && request.getNonce().equals(reply.getNonce())
        && request.getDestinationCell().equals(reply.getSourceCell())
        && request.getGatewayId().equals(reply.getGatewayId())
        && request.getBootId().equals(reply.getBootId())
        && reply.getObservedAtMs() <= now && reply.getObservedAtMs() >= now - 5000
        && reply.getValidUntilMs() > now && reply.getValidUntilMs() <= reply.getObservedAtMs() + 5000;
  }
}
