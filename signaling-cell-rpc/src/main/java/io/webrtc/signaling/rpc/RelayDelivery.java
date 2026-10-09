package io.webrtc.signaling.rpc;

import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.AuthenticatedSession;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Private volatile delivery, constructed only after native coordinator authorization. */
public record RelayDelivery(
    CallCommand command,
    AuthenticatedSession recipient,
    long callVersion,
    java.time.Instant authorizationUntil) {
  public RelayDelivery {
    Objects.requireNonNull(command);
    Objects.requireNonNull(recipient);
    Objects.requireNonNull(authorizationUntil);
    if (!RelaySessionAuthorizationProof.supports(command)
        || callVersion < 1
        || command.sender().equals(recipient)
        || command.payloadJson() == null
        || command.payloadJson().getBytes(StandardCharsets.UTF_8).length > 81920)
      throw new IllegalArgumentException("Invalid bounded relay delivery");
  }

  @Override
  public String toString() {
    return "RelayDelivery[type="
        + command.type()
        + ", negotiationId="
        + command.negotiationId().value()
        + "]";
  }
}
