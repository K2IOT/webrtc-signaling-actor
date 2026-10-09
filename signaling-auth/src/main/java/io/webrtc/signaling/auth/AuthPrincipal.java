package io.webrtc.signaling.auth;

import io.webrtc.signaling.protocol.Identity.*;
import java.time.Instant;
import java.util.Objects;

public record AuthPrincipal(
    UserId userId,
    SessionKey key,
    Instant expiresAt,
    Instant issuedAt,
    String signingKeyId,
    long securityEpoch) {
  public AuthPrincipal {
    Objects.requireNonNull(userId);
    Objects.requireNonNull(key);
    Objects.requireNonNull(expiresAt);
    Objects.requireNonNull(issuedAt);
    Objects.requireNonNull(signingKeyId);
    if (securityEpoch < 0) throw new IllegalArgumentException("negative security epoch");
  }
}
