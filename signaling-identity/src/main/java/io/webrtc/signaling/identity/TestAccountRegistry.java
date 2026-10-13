package io.webrtc.signaling.identity;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class TestAccountRegistry {
  private final Map<String, IdentityProperties.Account> accounts;

  public TestAccountRegistry(IdentityProperties properties) {
    accounts =
        properties.accounts().stream()
            .collect(Collectors.toUnmodifiableMap(IdentityProperties.Account::username, a -> a));
  }

  public Optional<String> authenticate(String username, String password) {
    if (username == null || password == null) return Optional.empty();
    var account = accounts.get(username);
    if (account == null || !account.password().equals(password)) return Optional.empty();
    return Optional.of(account.userId());
  }
}
