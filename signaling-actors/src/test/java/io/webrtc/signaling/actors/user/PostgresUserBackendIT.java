package io.webrtc.signaling.actors.user;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class PostgresUserBackendIT {
  static <T> T finish(DbOperation<T> operation) {
    var result = operation.logical().toCompletableFuture().join();
    operation.physicalCompletion().toCompletableFuture().join();
    return result;
  }

  @Test
  void realPrimaryBackendHydratesSessionAndParticipationAcrossWinnerAndRelease() throws Exception {
    try (var runtime = new DbTestRuntime()) {
      var sessions = new SessionRegistryService(runtime.sql, "c001", 1);
      var snapshots = new UserSnapshotService(runtime.sql, "c001", 1);
      // This verifier is explicitly a local SQL test fixture, never a runtime authorization bean.
      var home =
          new HomeParticipationService(
              runtime.sql, "c001", 1, r -> r.grant().proof().equals("TEST_ONLY_VERIFIED"));
      var backend =
          new PostgresUserBackend(
              snapshots,
              sessions,
              new UserReservationService(home),
              new AcceptWinnerService(home, (c, r) -> true));
      var user = new UserId("user-backend-" + UUID.randomUUID());
      var principal =
          new AuthPrincipal(
              user,
              new SessionKey("TEST_ONLY", "jti-" + UUID.randomUUID()),
              Instant.now().plusSeconds(600),
              Instant.now(),
              "test-key",
              1);
      var boot =
          sessions
              .startGatewayBoot(
                  "backend-gateway", UUID.randomUUID(), "TEST_ONLY", UUID.randomUUID())
              .toCompletableFuture()
              .join();
      assertThat(finish(backend.load(user, 1, Duration.ofSeconds(2))).routes()).isEmpty();
      var registered =
          finish(
              backend.execute(
                  new UserCommand.Register(principal, boot, UUID.randomUUID(), 1),
                  Duration.ofSeconds(2)));
      assertThat(registered.code()).isEqualTo(UserCommand.Code.REGISTERED);
      var route = registered.route();
      assertThat(finish(backend.load(user, 1, Duration.ofSeconds(2))).routes())
          .containsExactly(route);
      var call = new CallId("c001.e1." + UUID.randomUUID());
      Instant issued = Instant.now();
      var request =
          new Request(
              user,
              call,
              UUID.randomUUID(),
              "a".repeat(64),
              1,
              Phase.PREPARING,
              new Grant(
                  "c001",
                  1,
                  1,
                  HomeParticipationService.group(call),
                  2,
                  1,
                  UUID.randomUUID(),
                  issued,
                  issued.plusSeconds(5),
                  "TEST_ONLY_VERIFIED"));
      var reserved =
          finish(backend.execute(new UserCommand.Reserve(request), Duration.ofSeconds(2)));
      assertThat(reserved.code()).isEqualTo(UserCommand.Code.RESERVED);
      assertThat(finish(backend.load(user, 1, Duration.ofSeconds(2))).participation())
          .isEqualTo(reserved.participation());
      var claimed =
          finish(
              backend.execute(
                  new UserCommand.Accept(request, reserved.participation().reservationId(), route),
                  Duration.ofSeconds(2)));
      assertThat(claimed.code()).isEqualTo(UserCommand.Code.CLAIMED);
      assertThat(claimed.winner().winner().key()).isEqualTo(principal.key());
      assertThat(finish(backend.load(user, 1, Duration.ofSeconds(2))).participation().winner())
          .isEqualTo(claimed.winner().winner());
      var released =
          finish(
              backend.execute(
                  new UserCommand.Release(
                      request, claimed.winner().reservation(), claimed.winner().version()),
                  Duration.ofSeconds(2)));
      assertThat(released.participation().terminal()).isTrue();
      assertThat(finish(backend.load(user, 1, Duration.ofSeconds(2))).participation()).isNull();
      assertThat(
              finish(backend.execute(new UserCommand.Close(route, 1), Duration.ofSeconds(2)))
                  .code())
          .isEqualTo(UserCommand.Code.CLOSED);
      assertThat(finish(backend.load(user, 1, Duration.ofSeconds(2))).routes()).isEmpty();
    }
  }
}
