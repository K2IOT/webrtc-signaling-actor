package io.webrtc.signaling.control;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class BootstrapAuthorizationRaceTest {
  enum Loss {
    REVOKED,
    EXPIRED,
    FRESHNESS_UNKNOWN
  }

  @ParameterizedTest
  @EnumSource(Loss.class)
  void lateDirectoryResultCannotAuthorizeAfterOriginalIdentityLosesSafety(Loss loss) {
    var now = Instant.parse("2026-10-03T00:00:00Z");
    var current = new AtomicReference<>(now);
    var clock =
        new Clock() {
          public ZoneId getZone() {
            return ZoneOffset.UTC;
          }

          public Clock withZone(ZoneId zone) {
            return this;
          }

          public Instant instant() {
            return current.get();
          }
        };
    var route = new CompletableFuture<Optional<HomeRoute>>();
    var repository =
        new DirectoryRepository() {
          public CompletionStage<Optional<HomeRoute>> read(int bucket) {
            return route;
          }

          public CompletionStage<Boolean> compareAndPublish(HomeRoute previous, HomeRoute next) {
            throw new AssertionError();
          }

          public CompletionStage<Boolean> localActive(int bucket, String cell, long epoch) {
            throw new AssertionError();
          }
        };
    var epoch = new AtomicLong();
    var progress = new AtomicReference<>(new RevocationState.Progress(1, now));
    var store =
        new RevocationState.Store() {
          public boolean apply(RevocationState.Event event) {
            epoch.accumulateAndGet(event.epoch(), Math::max);
            return true;
          }

          public long epoch(String issuer, UserId user, String jti) {
            return epoch.get();
          }

          public RevocationState.Progress progress() {
            return progress.get();
          }

          public void reconcile(long offset, Instant checkedAt) {
            progress.set(new RevocationState.Progress(offset, checkedAt));
          }
        };
    var security = new RevocationState(store, Duration.ofSeconds(5));
    var principal =
        new AuthPrincipal(
            new UserId("alice"),
            new SessionKey("TEST_ONLY", "original"),
            now.plusSeconds(2),
            now,
            "TEST_ONLY",
            1);
    try (var tokens = new BoundedTokenVerifier((t, n) -> principal, 1, 1, Duration.ofSeconds(1))) {
      var controller =
          new BootstrapController(
              new DirectoryService(repository, 16384, Duration.ofSeconds(30)),
              tokens,
              security,
              clock);
      var original = controller.bootstrap(principal).toCompletableFuture();
      assertThat(original).isNotDone();
      switch (loss) {
        case REVOKED ->
            security.apply(
                new RevocationState.Event(
                    principal.key().issuer(),
                    principal.userId(),
                    principal.key().jti(),
                    2,
                    2,
                    now));
        case EXPIRED -> current.set(now.plusSeconds(3));
        case FRESHNESS_UNKNOWN -> progress.set(null);
      }
      route.complete(Optional.of(new HomeRoute(175, "c001", 1, "wss://cell.test/ws")));
      assertThatThrownBy(original::join).hasCauseInstanceOf(AuthException.class);
    }
  }
}
