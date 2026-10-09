package io.webrtc.signaling.protocol;

import static org.assertj.core.api.Assertions.*;

import com.google.protobuf.ByteString;
import io.webrtc.signaling.protocol.compat.PreviousCommand;
import io.webrtc.signaling.protocol.internal.InternalCommand;
import io.webrtc.signaling.protocol.internal.InternalReply;
import io.webrtc.signaling.protocol.internal.SessionIdentity;
import org.junit.jupiter.api.Test;

class CompatibilityTest {
  @Test
  void adjacentReleasesDecodeAndPreserveUnknownFields() throws Exception {
    var old =
        PreviousCommand.newBuilder()
            .setSchemaMajor(1)
            .setSchemaMinor(0)
            .setOperationId("op-1")
            .setType("HANGUP")
            .build();
    var current = InternalCommand.parseFrom(old.toByteArray());
    assertThat(current.getOperationId()).isEqualTo("op-1");
    var newer = current.toBuilder().setSchemaMinor(1).setTraceId("trace-a").build();
    var previous = PreviousCommand.parseFrom(newer.toByteArray());
    assertThat(previous.getOperationId()).isEqualTo("op-1");
    assertThat(InternalCommand.parseFrom(previous.toByteArray()).getTraceId()).isEqualTo("trace-a");
  }

  @Test
  void generatedIdentityUsesFullBindingAnd64BitCounters() throws Exception {
    var binding =
        SessionIdentity.newBuilder()
            .setIssuer("issuer")
            .setJti("jti")
            .setUserId("alice")
            .setIncarnation(ByteString.copyFrom(new byte[16]))
            .setConnectionGeneration(9007199254740993L)
            .build();
    assertThat(SessionIdentity.parseFrom(binding.toByteArray()).getConnectionGeneration())
        .isEqualTo(9007199254740993L);
    assertThat(InternalReply.getDescriptor().findFieldByName("ack_committed")).isNotNull();
  }

  @Test
  void rejectsUnsupportedInternalSchemaAndOversizedEnvelope() {
    var validator = new ProtocolValidator(ProtocolLimits.v1());
    assertThatThrownBy(() -> validator.decodeInternal(new byte[98305]))
        .isInstanceOf(ProtocolException.class);
    var future = InternalCommand.newBuilder().setSchemaMajor(2).setSchemaMinor(0).build();
    assertThatThrownBy(() -> validator.decodeInternal(future.toByteArray()))
        .isInstanceOf(ProtocolException.class);
  }
}
