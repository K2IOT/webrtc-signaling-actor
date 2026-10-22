package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.webrtc.signaling.protocol.Identity.AuthenticatedSession;
import io.webrtc.signaling.protocol.Identity.SessionIncarnation;
import io.webrtc.signaling.protocol.Identity.SessionKey;
import io.webrtc.signaling.protocol.Identity.UserId;
import io.webrtc.signaling.protocol.ProtocolException;
import io.webrtc.signaling.protocol.ProtocolLimits;
import io.webrtc.signaling.protocol.ProtocolValidator;
import io.webrtc.signaling.protocol.SignalEnvelope;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The compose smoke client (deploy/compose/smoke/payloads.mjs, SP3 Task 8) and the real public
 * decoder agree: every frame the JavaScript builders emit into {@code fixtures/frames.json} (by
 * {@code node payloads.mjs --emit-fixtures}; payloads.test.mjs fails when the file is stale) is
 * accepted by {@link ProtocolValidator#decodePublic} and, for commands, bound by {@link
 * ProtocolValidator#bind}. LOCAL_TEST_ONLY / NOT_QUALIFIED; no Docker.
 */
class ComposeSmokeFramesTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Set<String> EXPECTED =
      Set.of(
          "AUTH",
          "INVITE",
          "ACCEPT",
          "NEGOTIATE_REQUEST",
          "OFFER",
          "ANSWER",
          "ICE_CANDIDATES",
          "HANGUP",
          "EVENT_RECEIVED");

  private final ProtocolValidator validator = new ProtocolValidator(ProtocolLimits.v1());
  private final AuthenticatedSession sender =
      new AuthenticatedSession(
          new UserId("TEST_ONLY_smoke_sender"),
          new SessionKey("http://identity.signaling.local:8081", "TEST_ONLY-jti"),
          new SessionIncarnation(UUID.fromString("00000000-0000-4000-8000-0000000000e1")),
          1,
          UUID.fromString("00000000-0000-4000-8000-0000000000e2"));

  @Test
  void everyEmittedFrameDecodesAndBindsWithTheRealValidator() throws Exception {
    var frames = fixtures().path("frames");
    assertThat(frames.isArray()).isTrue();
    var seen = new HashSet<String>();
    for (JsonNode entry : frames) {
      var frame = entry.path("frame");
      var envelope = validator.decodePublic(JSON.writeValueAsBytes(frame));
      var type = envelope.type();
      assertThat(type.name()).as(entry.path("name").asText()).isEqualTo(frame.path("type").asText());
      seen.add(type.name());
      if (type == SignalEnvelope.Type.AUTH) {
        assertThat(envelope.requestId()).isNull();
        continue;
      }
      var command = validator.bind(envelope, sender);
      assertThat(command.sender()).isEqualTo(sender);
      assertThat(command.intentHash()).matches("[a-f0-9]{64}");
      if (type == SignalEnvelope.Type.INVITE) {
        assertThat(command.scope().value()).isEqualTo("INVITE");
        assertThat(command.target().value())
            .isEqualTo(frame.path("payload").path("targetUserId").asText());
      } else {
        assertThat(command.scope().value()).isEqualTo("CALL:" + frame.path("callId").asText());
      }
      if (frame.has("negotiationId")) {
        assertThat(command.negotiationId().decimal()).isEqualTo(frame.path("negotiationId").asText());
        assertThat(command.iceGeneration().decimal()).isEqualTo(frame.path("iceGeneration").asText());
      }
    }
    assertThat(seen).containsExactlyInAnyOrderElementsOf(EXPECTED);
  }

  /** Negative control: the same decoder refuses a numeric counter, so the agreement is not vacuous. */
  @Test
  void numericCountersAreRefusedByTheSameValidator() throws Exception {
    for (JsonNode entry : fixtures().path("frames")) {
      var frame = (ObjectNode) entry.path("frame").deepCopy();
      if (!frame.has("negotiationId")) continue;
      frame.put("negotiationId", Long.parseLong(frame.path("negotiationId").asText()));
      assertThatThrownBy(() -> validator.decodePublic(JSON.writeValueAsBytes(frame)))
          .as(entry.path("name").asText())
          .isInstanceOf(ProtocolException.class);
    }
  }

  private static JsonNode fixtures() throws Exception {
    var file = repoRoot().resolve("deploy/compose/smoke/fixtures/frames.json");
    var root = JSON.readTree(Files.readString(file));
    assertThat(root.path("status").asText()).contains("TEST_ONLY");
    return root;
  }

  private static Path repoRoot() {
    var dir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
    for (var at = dir; at != null; at = at.getParent())
      if (Files.isRegularFile(at.resolve("deploy/compose/topology.env"))) return at;
    throw new IllegalStateException("repository root not found above " + dir);
  }
}
