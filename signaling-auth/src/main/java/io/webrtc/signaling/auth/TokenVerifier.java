package io.webrtc.signaling.auth;

import java.time.Instant;

public interface TokenVerifier {
  AuthPrincipal validate(String compactJwt, Instant now);
}
