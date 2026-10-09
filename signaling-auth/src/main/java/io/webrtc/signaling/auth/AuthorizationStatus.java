package io.webrtc.signaling.auth;

public enum AuthorizationStatus {
  ALLOWED,
  REVOKED,
  FRESHNESS_UNKNOWN,
  TOKEN_EXPIRED
}
