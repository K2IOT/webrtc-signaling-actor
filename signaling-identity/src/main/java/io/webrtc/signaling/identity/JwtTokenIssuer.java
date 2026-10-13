package io.webrtc.signaling.identity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Signature;
import java.time.Clock;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

public final class JwtTokenIssuer {
  private static final Base64.Encoder BASE64_URL = Base64.getUrlEncoder().withoutPadding();

  private final IdentityProperties properties;
  private final RsaKeyMaterial keys;
  private final ObjectMapper json;
  private final Clock clock;

  public JwtTokenIssuer(
      IdentityProperties properties, RsaKeyMaterial keys, ObjectMapper json, Clock clock) {
    this.properties = properties;
    this.keys = keys;
    this.json = json;
    this.clock = clock;
  }

  public String issue(String userId) {
    long issuedAt = clock.instant().getEpochSecond();
    var header = Map.of("alg", "RS256", "kid", properties.keyId(), "typ", "JWT");
    var claims =
        Map.of(
            "iss",
            properties.issuer(),
            "aud",
            properties.audience(),
            "userId",
            userId,
            "jti",
            UUID.randomUUID().toString(),
            "iat",
            issuedAt,
            "exp",
            issuedAt + properties.tokenTtl().toSeconds());
    try {
      String signingInput = encode(header) + "." + encode(claims);
      Signature signature = Signature.getInstance("SHA256withRSA");
      signature.initSign(keys.privateKey());
      signature.update(signingInput.getBytes(StandardCharsets.US_ASCII));
      return signingInput + "." + BASE64_URL.encodeToString(signature.sign());
    } catch (JsonProcessingException | GeneralSecurityException e) {
      throw new IllegalStateException("Unable to issue test access token", e);
    }
  }

  private String encode(Object value) throws JsonProcessingException {
    return BASE64_URL.encodeToString(json.writeValueAsBytes(value));
  }
}
