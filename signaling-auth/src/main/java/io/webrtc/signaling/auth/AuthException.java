package io.webrtc.signaling.auth;

public final class AuthException extends IllegalArgumentException {
  public AuthException() {
    super("UNAUTHENTICATED");
  }
}
