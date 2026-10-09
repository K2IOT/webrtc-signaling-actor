package io.webrtc.signaling.protocol;

import com.fasterxml.jackson.annotation.JsonValue;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

public final class Identity {
  private Identity() {}

  // Canonical user IDs are NFC UTF-8, case-sensitive, no trim/case folding.
  public record UserId(String value) {
    public UserId {
      value = text(Normalizer.normalize(Objects.requireNonNull(value), Normalizer.Form.NFC), 256);
    }

    @JsonValue
    public String value() {
      return value;
    }
  }

  public record SessionKey(String issuer, String jti) {
    public SessionKey {
      issuer = text(issuer, 512);
      jti = text(jti, 256);
    }
  }

  public record SessionIncarnation(UUID value) {
    public SessionIncarnation {
      Objects.requireNonNull(value);
    }
  }

  public record RequestId(UUID value) {
    public RequestId {
      Objects.requireNonNull(value);
    }
  }

  public record CallId(String value) {
    private static final Pattern FORMAT =
        Pattern.compile(
            "([a-z][a-z0-9-]{0,23})\\.e([1-9][0-9]*)\\.([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

    public CallId {
      value = text(value, 96);
      var match = FORMAT.matcher(value);
      if (!match.matches()) throw new IllegalArgumentException("invalid routed call ID");
      positive(Long.parseLong(match.group(2)));
      UUID.fromString(match.group(3));
    }

    public static CallId create(String cell, long epoch) {
      positive(epoch);
      return new CallId(cell + ".e" + epoch + "." + UUID.randomUUID());
    }

    public String coordinatorCell() {
      return value.substring(0, value.indexOf('.'));
    }

    public long routingEpoch() {
      return Long.parseLong(value.substring(value.indexOf(".e") + 2, value.lastIndexOf('.')));
    }

    @JsonValue
    public String value() {
      return value;
    }
  }

  public record CallVersion(long value) {
    public CallVersion {
      positive(value);
    }

    @JsonValue
    public String decimal() {
      return Long.toString(value);
    }
  }

  public record NegotiationId(long value) {
    public NegotiationId {
      positive(value);
    }

    @JsonValue
    public String decimal() {
      return Long.toString(value);
    }
  }

  public record IceGeneration(long value) {
    public IceGeneration {
      positive(value);
    }

    @JsonValue
    public String decimal() {
      return Long.toString(value);
    }
  }

  public record CommandScope(String value) {
    public CommandScope {
      value = text(value, 128);
      if (!value.equals("INVITE")) {
        if (!value.startsWith("CALL:")) throw new IllegalArgumentException("invalid command scope");
        new CallId(value.substring(5));
      }
    }

    public static CommandScope invite() {
      return new CommandScope("INVITE");
    }

    public static CommandScope call(CallId id) {
      return new CommandScope("CALL:" + id.value());
    }
  }

  public record AuthenticatedSession(
      UserId userId,
      SessionKey key,
      SessionIncarnation incarnation,
      long connectionGeneration,
      UUID connectionId) {
    public AuthenticatedSession {
      Objects.requireNonNull(userId);
      Objects.requireNonNull(key);
      Objects.requireNonNull(incarnation);
      Objects.requireNonNull(connectionId);
      positive(connectionGeneration);
    }
  }

  static String text(String value, int maxBytes) {
    Objects.requireNonNull(value);
    if (value.isBlank()
        || !value.equals(value.strip())
        || value.getBytes(StandardCharsets.UTF_8).length > maxBytes
        || value
            .codePoints()
            .anyMatch(c -> Character.isISOControl(c) || c >= 0xd800 && c <= 0xdfff))
      throw new IllegalArgumentException("invalid bounded identifier");
    return value;
  }

  static void positive(long value) {
    if (value <= 0) throw new IllegalArgumentException("counter must be positive");
  }
}
