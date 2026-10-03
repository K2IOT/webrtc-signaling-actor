package io.webrtc.signaling.protocol;

import static io.webrtc.signaling.protocol.Identity.*;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Control metadata; negotiation bodies travel on the separate volatile relay contract. */
public record SignalEvent(UUID eventId, CallId callId, CallVersion version, AuthenticatedSession destination,
        Type type, Instant serverTime, Outcome outcome) {
    public enum Type { RINGING, ACCEPTED_PENDING_ACTIVATION, CALL_READY, ANSWERED_ELSEWHERE,
        ESTABLISHED, TERMINAL, AUTH_OK, AUTH_EXPIRING, RECONNECT, SECURITY_REVOKED }
    public sealed interface Outcome permits Committed, PendingActivation, Failure {}
    public record Committed() implements Outcome {}
    public record PendingActivation() implements Outcome {}
    public record Failure(ErrorCode code) implements Outcome { public Failure { Objects.requireNonNull(code); } }
    public SignalEvent {
        Objects.requireNonNull(eventId); Objects.requireNonNull(destination); Objects.requireNonNull(type);
        Objects.requireNonNull(serverTime); Objects.requireNonNull(outcome);
    }
}
