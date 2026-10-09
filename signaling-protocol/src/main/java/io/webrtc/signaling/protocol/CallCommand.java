package io.webrtc.signaling.protocol;

import static io.webrtc.signaling.protocol.Identity.*;

import java.util.Objects;

/** Server-bound command; route/identity are verified again at their authoritative store. */
public record CallCommand(
    SignalEnvelope.Type type,
    AuthenticatedSession sender,
    RequestId requestId,
    CallId callId,
    CommandScope scope,
    UserId target,
    NegotiationId negotiationId,
    IceGeneration iceGeneration,
    String payloadJson,
    String intentHash) {
  public CallCommand {
    Objects.requireNonNull(type);
    Objects.requireNonNull(sender);
    Objects.requireNonNull(requestId);
    Objects.requireNonNull(scope);
    Objects.requireNonNull(payloadJson);
    Objects.requireNonNull(intentHash);
  }
}
