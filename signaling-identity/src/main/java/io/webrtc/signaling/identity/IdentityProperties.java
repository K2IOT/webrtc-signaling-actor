package io.webrtc.signaling.identity;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "signaling.identity")
public record IdentityProperties(
    String issuer, String audience, Duration tokenTtl, String keyId, List<Account> accounts) {

  public IdentityProperties {
    issuer = identifier(issuer, 512, "issuer");
    audience = identifier(audience, 512, "audience");
    keyId = identifier(keyId, 256, "key ID");
    if (issuer.contains("${") || audience.contains("${") || keyId.contains("${"))
      throw new IllegalArgumentException("identity configuration contains unresolved placeholders");
    if (tokenTtl == null || tokenTtl.isZero() || tokenTtl.isNegative() || tokenTtl.getNano() != 0) {
      throw new IllegalArgumentException("token TTL must be positive whole seconds");
    }
    if (accounts == null || accounts.isEmpty()) {
      throw new IllegalArgumentException("at least one test account is required");
    }
    accounts = List.copyOf(accounts);
    Set<String> usernames = new HashSet<>();
    for (Account account : accounts) {
      Objects.requireNonNull(account, "account");
      if (!usernames.add(account.username())) {
        throw new IllegalArgumentException("test account usernames must be unique");
      }
    }
  }

  public record Account(String username, String password, String userId) {
    public Account {
      credential(username, 256, "username");
      credential(password, 1024, "password");
      Objects.requireNonNull(userId, "user ID");
      userId = Normalizer.normalize(userId, Normalizer.Form.NFC);
      identifier(userId, 256, "user ID");
    }
  }

  private static String credential(String value, int maxBytes, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank() || value.getBytes(StandardCharsets.UTF_8).length > maxBytes) {
      throw new IllegalArgumentException("invalid test account " + name);
    }
    return value;
  }

  private static String identifier(String value, int maxBytes, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()
        || !value.equals(value.strip())
        || value.getBytes(StandardCharsets.UTF_8).length > maxBytes
        || value.codePoints().anyMatch(Character::isISOControl)) {
      throw new IllegalArgumentException("invalid bounded " + name);
    }
    return value;
  }
}
