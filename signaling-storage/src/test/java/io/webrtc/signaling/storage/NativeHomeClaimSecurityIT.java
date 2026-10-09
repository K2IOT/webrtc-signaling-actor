package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.storage.HomeParticipationService.*;
import io.webrtc.signaling.storage.worker.RevocationReconciler;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class NativeHomeClaimSecurityIT {
  @Test
  void keyRetirementBlocksNativeClaimEvenWhenRouteRemainsCurrent() throws Exception {
    claimRejected(true);
  }

  @Test
  void incompleteRevocationRangeBlocksClaimAfterPreviouslyFreshSource() throws Exception {
    claimRejected(false);
  }

  private void claimRejected(boolean retireKey) throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("retire-claim-caller");
      var callee = f.sender("retire-claim-callee");
      var invite = f.invite(caller, callee.userId());
      var call = f.service().executeCallCommand(invite).toCompletableFuture().join().callId();
      var group = f.token(call);
      var home =
          new HomeParticipationService(
              f.runtime.sql, "c001", 1, r -> r.grant().proof().equals("TEST_ONLY_VERIFIED"));
      Instant now = Instant.now();
      var request =
          new Request(
              callee.userId(),
              call,
              invite.requestId().value(),
              invite.intentHash(),
              1,
              Phase.RINGING,
              new Grant(
                  "c001",
                  1,
                  1,
                  group.group(),
                  group.epoch(),
                  1,
                  UUID.randomUUID(),
                  now,
                  now.plusSeconds(5),
                  "TEST_ONLY_VERIFIED"));
      var participation = home.queryParticipation(request).toCompletableFuture().join();
      var route = SessionAuthReadIT.route(f, callee);
      var revocations =
          new RevocationReconciler(
              f.runtime.sql,
              "c001",
              1,
              Duration.ofSeconds(5),
              b -> b.sourceProof().equals("TEST_ONLY_SOURCE"));
      SessionAuthReadIT.done(
          revocations.apply(
              new RevocationReconciler.Batch(0, 0, List.of(), Instant.now(), "TEST_ONLY_SOURCE")));
      SessionAuthReadIT.done(
          revocations.apply(
              new RevocationReconciler.Batch(
                  0,
                  1,
                  List.of(),
                  Instant.now(),
                  "TEST_ONLY_SOURCE",
                  retireKey
                      ? List.of(
                          new RevocationReconciler.KeyRetirement(
                              route.key().issuer(),
                              route.signingKeyId(),
                              1,
                              Instant.now().minusMillis(1)))
                      : List.of(),
                  retireKey ? 1 : 2)));
      try (var c = f.connection()) {
        assertThat(new SessionRepository().requireCurrent(c, route)).isEqualTo(route);
      }
      var claims = new AcceptWinnerService(home, revocations::allowedRoute);
      assertThatThrownBy(
              () ->
                  SessionAuthReadIT.done(
                      claims.claimAcceptTracked(
                          request, participation.reservationId(), route, Duration.ofSeconds(2))))
          .hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
    }
  }
}
