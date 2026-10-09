package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class InviteOutcomeTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final UUID original = UUID.randomUUID();

  private ObjectNode result(String type, boolean committed, String status, String code) {
    var reply =
        JSON.createObjectNode()
            .put("type", type)
            .put("ackCommitted", committed)
            .put("requestId", original.toString());
    reply.putObject("result").put("status", status).put("code", code);
    return reply;
  }

  @Test
  void unknownAndPendingRetainOriginalInviteEvenWhenAPreparingCallIdExists() {
    var unknown =
        JSON.createObjectNode().put("type", "ERROR").put("requestId", original.toString());
    unknown.putObject("error").put("code", "OUTCOME_UNKNOWN");
    assertThat(InviteOutcome.classify(original, unknown)).isEqualTo(InviteOutcome.Kind.UNRESOLVED);
    assertThat(InviteOutcome.classify(original, null)).isEqualTo(InviteOutcome.Kind.UNRESOLVED);
    var pending =
        result("COMMAND_RESULT", false, "PENDING", "WORKFLOW_PENDING")
            .put("callId", "c001/preparing");
    assertThat(InviteOutcome.classify(original, pending)).isEqualTo(InviteOutcome.Kind.UNRESOLVED);
  }

  @Test
  void committedBusyOrUnreachableAllowsTheNextIndependentCall() {
    for (String code : new String[] {"USER_BUSY", "UNREACHABLE"})
      assertThat(InviteOutcome.classify(original, result("ACK_COMMITTED", true, "FINAL", code)))
          .isEqualTo(InviteOutcome.Kind.REJECTED);
  }

  @Test
  void originalReadCanResolveLostInviteAckWithoutInventingAnotherOperation() {
    var found =
        result("COMMAND_RESULT", false, "FINAL", "RINGING")
            .put("callId", "c001/committed")
            .put("callVersion", "9007199254740993");
    assertThat(InviteOutcome.classify(original, found)).isEqualTo(InviteOutcome.Kind.CREATED);
    assertThat(InviteOutcome.classify(UUID.randomUUID(), found))
        .isEqualTo(InviteOutcome.Kind.UNRESOLVED);
  }

  @Test
  void uncommittedOrMalformedFinalCannotClearUnknownWork() {
    assertThat(
            InviteOutcome.classify(original, result("ACK_COMMITTED", false, "FINAL", "USER_BUSY")))
        .isEqualTo(InviteOutcome.Kind.UNRESOLVED);
    var found =
        result("COMMAND_RESULT", false, "FINAL", "RINGING")
            .put("callId", "c001/committed")
            .put("callVersion", "0");
    assertThatThrownBy(() -> InviteOutcome.classify(original, found))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void expiredReadDoesNotProveAnInviteNeverCommitted() {
    var expired =
        JSON.createObjectNode().put("type", "ERROR").put("requestId", original.toString());
    expired.putObject("error").put("code", "RESULT_EXPIRED");
    assertThat(InviteOutcome.classify(original, expired)).isEqualTo(InviteOutcome.Kind.UNRESOLVED);
  }

  @Test
  void finalTerminalResultMustNotResumeAnEndedCall() {
    var ended =
        result("COMMAND_RESULT", false, "FINAL", "CANCELLED")
            .put("callId", "c001/ended")
            .put("callVersion", "3");
    ended.withObject("result").put("state", "TERMINAL");
    assertThat(InviteOutcome.classify(original, ended)).isEqualTo(InviteOutcome.Kind.ENDED);
  }
}
