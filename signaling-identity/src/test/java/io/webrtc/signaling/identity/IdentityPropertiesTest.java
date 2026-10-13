package io.webrtc.signaling.identity;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class IdentityPropertiesTest {
  private static IdentityProperties valid() {
    return new IdentityProperties(
        "http://localhost:8081",
        "webrtc-signaling",
        Duration.ofMinutes(15),
        "test-key-1",
        List.of(
            new IdentityProperties.Account("alice", "alice123", "alice"),
            new IdentityProperties.Account("bob", "bob123", "bob")));
  }

  @Test
  void acceptsDefaultsAndSharedUserIdsAndCopiesAccounts() {
    var source = new java.util.ArrayList<>(valid().accounts());
    var properties =
        new IdentityProperties(
            "http://localhost:8081",
            "webrtc-signaling",
            Duration.ofSeconds(900),
            "test-key-1",
            source);
    source.clear();
    assertThat(properties.tokenTtl()).isEqualTo(Duration.ofSeconds(900));
    assertThat(properties.accounts()).hasSize(2);
    assertThat(
            new IdentityProperties(
                    "http://localhost:8081",
                    "webrtc-signaling",
                    Duration.ofMinutes(15),
                    "test-key-1",
                    List.of(
                        new IdentityProperties.Account("alice", "one", "alice"),
                        new IdentityProperties.Account("alice2", "two", "alice")))
                .accounts())
        .hasSize(2);
  }

  @Test
  void rejectsDuplicateUsernamesAndInvalidRequiredValues() {
    var duplicate = new IdentityProperties.Account("alice", "other-password", "alice");
    assertThatThrownBy(
            () ->
                new IdentityProperties(
                    "http://localhost:8081",
                    "webrtc-signaling",
                    Duration.ofMinutes(15),
                    "test-key-1",
                    List.of(valid().accounts().getFirst(), duplicate)))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(
            () ->
                new IdentityProperties(
                    " ", "aud", Duration.ofMinutes(1), "kid", valid().accounts()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new IdentityProperties("iss", "", Duration.ofMinutes(1), "kid", valid().accounts()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new IdentityProperties("iss", "aud", Duration.ofMinutes(1), "kid", List.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsInvalidTtlAndIdentifiers() {
    for (Duration duration : List.of(Duration.ZERO, Duration.ofSeconds(-1), Duration.ofNanos(1))) {
      assertThatThrownBy(
              () ->
                  new IdentityProperties(
                      "issuer", "audience", duration, "key-id", valid().accounts()))
          .isInstanceOf(IllegalArgumentException.class);
    }
    assertThatThrownBy(
            () ->
                new IdentityProperties(
                    "issuer\n", "audience", Duration.ofMinutes(15), "key-id", valid().accounts()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new IdentityProperties(
                    "${issuer}", "audience", Duration.ofMinutes(15), "key-id", valid().accounts()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new IdentityProperties(
                    "issuer",
                    "audience",
                    Duration.ofMinutes(15),
                    "key-id",
                    List.of(new IdentityProperties.Account("alice", " ", "alice"))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new IdentityProperties(
                    "i".repeat(513),
                    "audience",
                    Duration.ofMinutes(15),
                    "key-id",
                    valid().accounts()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new IdentityProperties(
                    "issuer",
                    "audience",
                    Duration.ofMinutes(15),
                    "key-id",
                    List.of(new IdentityProperties.Account("alice", "secret", "u".repeat(257)))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new IdentityProperties.Account("alice", "secret", "invalid-\ud800-user"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void authenticatesExactCredentialsWithoutNormalizingWhitespace() {
    var registry = new TestAccountRegistry(valid());
    assertThat(registry.authenticate("alice", "alice123")).contains("alice");
    assertThat(registry.authenticate("alice", " alice123 ")).isEmpty();
    assertThat(registry.authenticate(" Alice", "alice123")).isEmpty();
    assertThat(registry.authenticate("unknown", "alice123")).isEmpty();
  }
}
