package io.webrtc.signaling.storage;
/** Nonborrowable admission classes; the safety classes use a distinct physical pool. */
public enum DbClass {
    CRITICAL(false), NORMAL(false), OUTBOX(false), MAINTENANCE(false), RENEWAL(true), TERMINATION(true), RECOVERY(true);
    private final boolean safety;
    DbClass(boolean safety) { this.safety=safety; }
    public boolean safety() { return safety; }
}
