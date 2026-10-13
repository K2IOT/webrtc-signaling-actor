package io.webrtc.signaling.protocol;

import java.util.Objects;

import static io.webrtc.signaling.protocol.Identity.*;

public record SignalEnvelope(
        int version,
        Type type,
        RequestId requestId,
        CallId callId,
        NegotiationId negotiationId,
        IceGeneration iceGeneration,
        String payloadJson) {
    public SignalEnvelope {
        Objects.requireNonNull(type);
        Objects.requireNonNull(payloadJson);
    }

    public enum Type {
        AUTH,
        AUTH_REFRESH,
        INVITE,
        ACCEPT,
        CANCEL,
        HANGUP,
        REJECT,
        DECLINE_ALL,
        OFFER,
        ANSWER,
        ICE_CANDIDATES,
        END_OF_CANDIDATES,
        INITIAL_ICE_RECEIPT,
        NEGOTIATE_REQUEST,
        RESUME,
        SYNC_CALL,
        GET_COMMAND_RESULT,
        EVENT_RECEIVED,
        MEDIA_CONNECTED,
        MEDIA_DISCONNECTED,
        ICE_RESTARTING,
        MEDIA_FAILED,
        MEDIA_RECOVERED
    }
}
