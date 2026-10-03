package io.webrtc.signaling.protocol;

/** Bounded, redacted protocol error: never includes attacker-controlled payload. */
public final class ProtocolException extends IllegalArgumentException {
    private final ErrorCode code;
    public ProtocolException(ErrorCode code) { super(code.name()); this.code = code; }
    public ErrorCode code() { return code; }
}
