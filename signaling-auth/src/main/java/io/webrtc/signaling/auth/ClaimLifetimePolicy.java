package io.webrtc.signaling.auth;

import java.util.Objects;

/** Trusted deployment enrollment, never selected by JWT contents. */
public record ClaimLifetimePolicy(int version, Mode mode, String issuerAttestationReference) {
  public enum Mode {
    REQUIRE_IAT,
    OPTIONAL_IAT_ATTESTED_LIFETIME
  }

  public ClaimLifetimePolicy {
    Objects.requireNonNull(mode);
    Objects.requireNonNull(issuerAttestationReference);
    if (version != 1 || issuerAttestationReference.length() > 512)
      throw new IllegalArgumentException("unsupported claim lifetime policy");
    if (mode == Mode.OPTIONAL_IAT_ATTESTED_LIFETIME
        && (issuerAttestationReference.isBlank()
            || issuerAttestationReference.contains("${")
            || issuerAttestationReference.matches("(?i).*(UNSET|UNCONFIGURED|PLACEHOLDER).*")))
      throw new IllegalArgumentException("optional iat requires issuer lifetime attestation");
  }

  public static ClaimLifetimePolicy legacyRequired() {
    return new ClaimLifetimePolicy(1, Mode.REQUIRE_IAT, "");
  }
}
