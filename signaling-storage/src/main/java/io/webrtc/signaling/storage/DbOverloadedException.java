package io.webrtc.signaling.storage;

public final class DbOverloadedException extends RuntimeException {
  public DbOverloadedException() {
    super("Database admission unavailable");
  }
}
