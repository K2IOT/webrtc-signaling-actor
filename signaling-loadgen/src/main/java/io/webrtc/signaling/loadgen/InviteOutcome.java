package io.webrtc.signaling.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

/**
 * Unknown work retains its original operation; only an authenticated durable result resolves it.
 */
final class InviteOutcome {
  enum Kind {
    UNRESOLVED,
    CREATED,
    REJECTED,
    ENDED
  }

  static Kind classify(UUID original, JsonNode reply) {
    if (reply == null || !original.toString().equals(reply.path("requestId").asText()))
      return Kind.UNRESOLVED;
    String type = reply.path("type").asText();
    boolean committed =
        reply.path("ackCommitted").isBoolean() && reply.path("ackCommitted").booleanValue();
    if (!(type.equals("ACK_COMMITTED") && committed
        || type.equals("COMMAND_RESULT") && reply.path("ackCommitted").isBoolean() && !committed))
      return Kind.UNRESOLVED;
    var result = reply.path("result");
    if (!result.path("status").asText().equals("FINAL")) return Kind.UNRESOLVED;
    if (reply.hasNonNull("callId")) {
      String version = reply.path("callVersion").asText();
      if (reply.path("callId").asText().isEmpty() || !version.matches("[1-9][0-9]{0,18}"))
        throw new IllegalArgumentException("Invalid durable call binding");
      Long.parseLong(version);
      return switch (result.path("state").asText()) {
        case "TERMINAL", "FAILED" -> Kind.ENDED;
        default -> Kind.CREATED;
      };
    }
    return switch (result.path("code").asText()) {
      case "USER_BUSY", "UNREACHABLE" -> Kind.REJECTED;
      default -> Kind.UNRESOLVED;
    };
  }

  private InviteOutcome() {}
}
