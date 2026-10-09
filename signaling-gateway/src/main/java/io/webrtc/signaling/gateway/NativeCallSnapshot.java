package io.webrtc.signaling.gateway;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.*;
import java.time.Instant;
import java.util.*;

/**
 * Public metadata projection of an independently authorized native snapshot. Never exposes SDP/ICE
 * payloads.
 */
final class NativeCallSnapshot {
  private static final ObjectMapper JSON =
      new ObjectMapper(
              JsonFactory.builder()
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNestingDepth(8)
                          .maxStringLength(8192)
                          .build())
                  .build())
          .findAndRegisterModules();
  private static final List<String> DEADLINES =
      List.of(
          "prepareUntil",
          "ringUntil",
          "activationUntil",
          "negotiationUntil",
          "callerGraceUntil",
          "winnerGraceUntil",
          "mediaRecoveryUntil",
          "mediaRestartAfter");

  private NativeCallSnapshot() {}

  static void write(ObjectNode result, Snapshot snapshot) {
    try {
      if (snapshot.deadlines() == null || snapshot.deadlines().length() > 8192)
        throw new IllegalArgumentException("Invalid bounded snapshot metadata");
      var metadata = JSON.readTree(snapshot.deadlines());
      if (!metadata.isObject()) throw new IllegalArgumentException("Invalid snapshot metadata");
      String text = metadata.path("iceGeneration").asText("0");
      if (!text.matches("0|[1-9][0-9]{0,18}"))
        throw new IllegalArgumentException("Invalid native ICE generation");
      long ice = Long.parseLong(text);
      if (snapshot.negotiationId() < 0 || (snapshot.negotiationId() == 0) != (ice == 0))
        throw new IllegalArgumentException("Invalid native round");
      result
          .put("state", snapshot.state())
          .put(
              "code",
              Set.of("TERMINAL", "FAILED").contains(snapshot.state())
                  ? "TERMINAL"
                  : metadata.path("negotiationState").asText().equals("INVALIDATED")
                      ? "RESYNC_REQUIRED"
                      : "SNAPSHOT")
          .put("resetPeerConnection", false);
      result.set("caller", participant(snapshot.caller(), metadata.get("callerRoute")));
      result.put("calleeUserId", snapshot.callee().value());
      if (snapshot.winner() != null)
        result.set("winner", participant(snapshot.winner(), metadata.get("winnerRoute")));
      else result.putNull("winner");
      if (snapshot.activationId() != null)
        result.put("activationId", snapshot.activationId().toString());
      else result.putNull("activationId");
      var negotiation =
          result
              .putObject("negotiation")
              .put("negotiationId", Long.toString(snapshot.negotiationId()))
              .put("iceGeneration", Long.toString(ice))
              .put("state", metadata.path("negotiationState").asText("NONE"))
              .put("volatileRetention", "UNKNOWN");
      for (String role : List.of("offerer", "answerer"))
        if (metadata.hasNonNull(role))
          negotiation.set(
              role, session(JSON.treeToValue(metadata.get(role), AuthenticatedSession.class)));
      var deadlines = result.putObject("deadlines");
      for (String field : DEADLINES)
        if (metadata.hasNonNull(field)) {
          var value = metadata.get(field);
          if (!value.isTextual()) throw new IllegalArgumentException("Invalid deadline");
          deadlines.put(field, Instant.parse(value.asText()).toString());
        }
      if (snapshot.terminalReason() != null)
        result.put("terminalReason", snapshot.terminalReason());
      if (snapshot.terminalAt() != null) result.put("terminalAt", snapshot.terminalAt().toString());
    } catch (Exception invalid) {
      throw new IllegalArgumentException("Invalid native snapshot projection");
    }
  }

  private static ObjectNode participant(Participant participant, JsonNode route) throws Exception {
    var result =
        JSON.createObjectNode()
            .put("userId", participant.user().value())
            .put("issuer", participant.key().issuer())
            .put("jti", participant.key().jti())
            .put("sessionIncarnation", participant.incarnation().value().toString())
            .put("connectionGeneration", Long.toString(participant.generation()));
    if (route != null && !route.isNull()) {
      var current = JSON.treeToValue(route, AuthenticatedSession.class);
      if (!participant.sameBinding(current))
        throw new IllegalArgumentException("Native route mismatch");
      result.put("connectionId", current.connectionId().toString());
    }
    return result;
  }

  private static ObjectNode session(AuthenticatedSession session) {
    return JSON.createObjectNode()
        .put("userId", session.userId().value())
        .put("issuer", session.key().issuer())
        .put("jti", session.key().jti())
        .put("sessionIncarnation", session.incarnation().value().toString())
        .put("connectionGeneration", Long.toString(session.connectionGeneration()))
        .put("connectionId", session.connectionId().toString());
  }
}
