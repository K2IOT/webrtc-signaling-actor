package io.webrtc.signaling.auth;

import io.webrtc.signaling.protocol.Identity.SessionKey;
import io.webrtc.signaling.protocol.Identity.UserId;

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
        Objects.requireNonNull(signingKeyId);
        if (securityEpoch < 0) throw new IllegalArgumentException("negative security epoch");
    }

    /**
     * Empty when an enrolled issuer omits iat; never infer an issuance timestamp from validation.
     */
    public java.util.Optional<Instant> knownIssuedAt() {
        return java.util.Optional.ofNullable(issuedAt);
    }
}
