package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class NativeCallSnapshotTest {
    final ObjectMapper json=new ObjectMapper();
    final CallId call=CallId.create("c001",1);
    final Participant caller=new Participant(new UserId("TEST_ONLY_caller"),new SessionKey("TEST_ONLY","caller-jti"),new SessionIncarnation(UUID.randomUUID()),Long.MAX_VALUE-1);
    final Participant winner=new Participant(new UserId("TEST_ONLY_callee"),new SessionKey("TEST_ONLY","winner-jti"),new SessionIncarnation(UUID.randomUUID()),Long.MAX_VALUE-2);
    Snapshot snapshot(String metadata){return new Snapshot(call,1,1,1,"ESTABLISHED",Long.MAX_VALUE-3,7,caller,winner.user(),winner,UUID.randomUUID(),"ACTIVE",metadata,"[]","[]",1,null,null,null,new RequestId(UUID.randomUUID()));}
    @Test void nativeSnapshotReturnsTypedBindingsNegotiationAndDeadlinesWithoutSdp()throws Exception {
        var result=json.createObjectNode();NativeCallSnapshot.write(result,snapshot("{\"iceGeneration\":\"9\",\"negotiationState\":\"COMPLETE\",\"negotiationUntil\":\"2026-10-04T00:00:20Z\",\"sdp\":\"SHOULD_NEVER_APPEAR\"}"));
        assertThat(result.path("caller").path("connectionGeneration").asText()).isEqualTo(Long.toString(caller.generation()));assertThat(result.path("winner").path("jti").asText()).isEqualTo("winner-jti");assertThat(result.path("winner").path("connectionGeneration").isTextual()).isTrue();assertThat(result.path("activationId").asText()).isNotBlank();assertThat(result.path("negotiation").path("iceGeneration").asText()).isEqualTo("9");assertThat(result.path("deadlines").isObject()).isTrue();assertThat(result.path("deadlines").path("negotiationUntil").asText()).isEqualTo("2026-10-04T00:00:20Z");assertThat(result.toString()).doesNotContain("SHOULD_NEVER_APPEAR","sdp");assertThat(result.path("resetPeerConnection").asBoolean()).isFalse();
    }
    @Test void invalidatedNativeRoundRequiresResyncWithoutClaimingMediaLoss()throws Exception {
        var result=json.createObjectNode();NativeCallSnapshot.write(result,snapshot("{\"iceGeneration\":\"9\",\"negotiationState\":\"INVALIDATED\"}"));
        assertThat(result.path("code").asText()).isEqualTo("RESYNC_REQUIRED");assertThat(result.path("resetPeerConnection").asBoolean()).isFalse();
    }
    @Test void corruptNativeRoundOrDeadlineIsRejectedBeforeEncoding() {
        assertThatThrownBy(()->NativeCallSnapshot.write(json.createObjectNode(),snapshot("{\"iceGeneration\":\"0\"}"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->NativeCallSnapshot.write(json.createObjectNode(),snapshot("{\"iceGeneration\":\"9\",\"negotiationUntil\":\"not-a-date\"}"))).isInstanceOf(IllegalArgumentException.class);
    }
}
