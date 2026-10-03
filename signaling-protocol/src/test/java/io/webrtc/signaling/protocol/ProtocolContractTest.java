package io.webrtc.signaling.protocol;

import static io.webrtc.signaling.protocol.Identity.*;
import static org.assertj.core.api.Assertions.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ProtocolContractTest {
    private final ProtocolValidator validator = new ProtocolValidator(ProtocolLimits.v1());
    private final String request = "00000000-0000-4000-8000-000000000001";
    private final CallId call = new CallId("c017.e3.00000000-0000-4000-8000-000000000002");
    private final AuthenticatedSession sender = new AuthenticatedSession(new UserId("alice"),
        new SessionKey("https://issuer.example.test", "session-a"), new SessionIncarnation(UUID.randomUUID()), 7, UUID.randomUUID());
    private String frame(String type, String payload) {
        return "{\"v\":1,\"type\":\"" + type + "\",\"requestId\":\"" + request + "\","
            + (type.equals("INVITE") ? "" : "\"callId\":\""+call.value()+"\",\"negotiationId\":\"1\",\"iceGeneration\":\"1\",")
            + "\"payload\":"+payload+"}";
    }
    private SignalEnvelope decode(String json) { return validator.decodePublic(json.getBytes(StandardCharsets.UTF_8)); }
    private String ice(int n, String text) {
        return "{\"startSequence\":\"1\",\"candidates\":[" + java.util.stream.IntStream.range(0,n)
            .mapToObj(i -> "{\"candidate\":\"" + text + "\",\"sdpMid\":\"0\",\"sdpMLineIndex\":0,\"usernameFragment\":\"ufrag\"}")
            .collect(java.util.stream.Collectors.joining(",")) + "]}";
    }
    @Test void derivesInviteScopeAndIdentityFromAuthenticatedContext() {
        var command = validator.bind(decode(frame("INVITE", "{\"targetUserId\":\"bob\"}")), sender);
        assertThat(command.scope().value()).isEqualTo("INVITE");
        assertThat(command.sender()).isEqualTo(sender);
        assertThat(command.target()).isEqualTo(new UserId("bob"));
    }
    @Test void derivesCallScopeFromImmutableRoutedCallId() {
        var c = validator.bind(decode(frame("HANGUP", "{}")), sender);
        assertThat(c.scope().value()).isEqualTo("CALL:"+call.value());
        assertThat(call.coordinatorCell()).isEqualTo("c017");
        assertThat(call.routingEpoch()).isEqualTo(3);
    }
    @ParameterizedTest @ValueSource(strings={"userId", "issuer", "jti", "commandScope", "coordinatorCell", "groupEpoch", "ownershipGroupId", "connectionGeneration"})
    void clientCannotSelectIdentityOrAuthority(String field) {
        assertThatThrownBy(() -> decode(frame("INVITE", "{\"targetUserId\":\"bob\"}").replace("\"v\":1", "\"v\":1,\""+field+"\":\"evil\"")))
            .isInstanceOf(ProtocolException.class);
        assertThatThrownBy(() -> decode(frame("INVITE", "{\"targetUserId\":\"bob\",\""+field+"\":\"evil\"}")))
            .isInstanceOf(ProtocolException.class);
    }
    @Test void limitsIdentifiersByUtf8BytesWithoutTruncating() {
        assertThat(new UserId("e\u0301")).isEqualTo(new UserId("é"));
        assertThatThrownBy(() -> new UserId("é".repeat(129))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SessionKey("x".repeat(513), "jti")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SessionKey("issuer", "j".repeat(257))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new UserId(" alice ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new UserId("a\u0000b")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new UserId("\ud800")).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void rejectsMalformedAndOversizedFramesBeforeBinding() {
        assertThatThrownBy(() -> validator.decodePublic(new byte[81921])).isInstanceOf(ProtocolException.class);
        assertThatThrownBy(() -> decode("{}{}")).isInstanceOf(ProtocolException.class);
        assertThatThrownBy(() -> decode(frame("INVITE", "{\"targetUserId\":\"bob\"}").replace("\"v\":1", "\"v\":1,\"v\":1"))).isInstanceOf(ProtocolException.class);
        assertThatThrownBy(() -> validator.decodePublic(new byte[]{(byte)0xc3,(byte)0x28})).isInstanceOf(ProtocolException.class);
    }
    @Test void enforcesSdpBytes() {
        decode(frame("OFFER", "{\"sdp\":\"" + "a".repeat(65536) + "\"}"));
        assertThatThrownBy(() -> decode(frame("OFFER", "{\"sdp\":\"" + "a".repeat(65537) + "\"}"))).isInstanceOf(ProtocolException.class);
    }
    @Test void enforcesCandidateAndBatchCountAndEncodedBytes() {
        decode(frame("ICE_CANDIDATES", ice(20,"candidate")));
        decode(frame("ICE_CANDIDATES", ice(1,"a".repeat(2048))));
        assertThatThrownBy(() -> decode(frame("ICE_CANDIDATES", ice(1,"a".repeat(2049))))).isInstanceOf(ProtocolException.class);
        assertThatThrownBy(() -> decode(frame("ICE_CANDIDATES", ice(21,"candidate")))).isInstanceOf(ProtocolException.class);
        assertThatThrownBy(() -> decode(frame("ICE_CANDIDATES", ice(5,"a".repeat(1800))))).isInstanceOf(ProtocolException.class);
    }
    @Test void hashUsesCanonicalIntentAndExcludesTransportGeneration() {
        var a = validator.bind(decode(frame("INVITE", "{\"targetUserId\":\"bob\"}")), sender);
        var reconnect = new AuthenticatedSession(sender.userId(), sender.key(), new SessionIncarnation(UUID.randomUUID()), 8, UUID.randomUUID());
        var b = validator.bind(decode(frame("INVITE", "{ \"targetUserId\" : \"bob\" }")), reconnect);
        assertThat(a.intentHash()).hasSize(64).isEqualTo(b.intentHash());
        var changed = validator.bind(decode(frame("INVITE", "{\"targetUserId\":\"charlie\"}")), sender);
        assertThatThrownBy(() -> validator.requireSameIntent(a.intentHash(),changed.intentHash()))
            .isInstanceOf(ProtocolException.class).extracting("code").isEqualTo(ErrorCode.IDEMPOTENCY_CONFLICT);
    }
    @Test void preservesFullCounterPrecisionAndRejectsNumericWireCounters() {
        long large = 9007199254740993L;
        assertThat(new CallVersion(large).decimal()).isEqualTo("9007199254740993");
        var f = frame("OFFER", "{\"sdp\":\"v=0\"}").replace("\"negotiationId\":\"1\"", "\"negotiationId\":\""+large+"\"");
        assertThat(decode(f).negotiationId().value()).isEqualTo(large);
        assertThatThrownBy(() -> decode(f.replace("\""+large+"\"", ""+large))).isInstanceOf(ProtocolException.class);
        assertThatThrownBy(() -> new CallVersion(-1)).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void immutableCommandDoesNotExposeMutablePayload() {
        var c = validator.bind(decode(frame("INVITE", "{\"targetUserId\":\"bob\"}")), sender);
        assertThat(c.payloadJson()).isEqualTo("{\"targetUserId\":\"bob\"}");
        assertThat(c).isNotInstanceOf(java.io.Serializable.class);
    }
}
