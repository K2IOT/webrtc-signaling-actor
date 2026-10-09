package io.webrtc.signaling.storage;

public final class DbOutcomeUnknownException extends RuntimeException {
  public DbOutcomeUnknownException() {
    super("Database outcome requires authoritative reconciliation");
  }
}
