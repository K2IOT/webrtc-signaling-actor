package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

/** Same package as the native fixture; no authority models substitute for PostgreSQL reads. */
class PartitionAndPauseIT {
  @Test
  void nativeExpiredPhaseCannotMintFreshAcquisitionOrRenewalGrantAfterLongPause() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("pause-caller");
      var callee = f.sender("pause-callee");
      var invite = f.invite(caller, callee.userId());
      var call = f.service().executeCallCommand(invite).toCompletableFuture().join().callId();
      var token = f.token(call);
      var accept = AcceptCompletionIT.accept(callee, call);
      CoordinatorGrantIT.done(
          f.service()
              .executeUnderAuthorityTracked(
                  accept,
                  new CallCommandService.Authority(call, token, 1, "TEST_ONLY_VERIFIED", 1),
                  Duration.ofSeconds(2)));
      var route =
          CoordinatorGrantIT.done(
              f.runtime.sql.submitTracked(
                  DbClass.CRITICAL,
                  Duration.ofSeconds(2),
                  c -> new SessionRepository().find(c, callee.key())));
      Instant now = Instant.now();
      var request =
          new HomeParticipationService.Request(
              callee.userId(),
              call,
              invite.requestId().value(),
              invite.intentHash(),
              1,
              HomeParticipationService.Phase.RINGING,
              new HomeParticipationService.Grant(
                  "c001",
                  1,
                  1,
                  token.group(),
                  token.epoch(),
                  1,
                  accept.requestId().value(),
                  now,
                  now.plusSeconds(5),
                  "UNSIGNED"));
      var grants = new CoordinatorGrantService(f.runtime.sql, "c001", 1, "TEST_ONLY_LOCAL_OWNER");
      try (var c = f.connection();
          var q =
              c.prepareStatement(
                  "UPDATE call_state SET deadlines=jsonb_build_object('ringUntil',to_char((clock_timestamp()-interval '25 hours') AT TIME ZONE 'UTC','YYYY-MM-DD') || 'T00:00:00Z') WHERE call_id=?")) {
        q.setString(1, call.value());
        q.executeUpdate();
      }
      var claim =
          new HomeParticipationService.AuthorizationIntent("CLAIM", null, 0, null, 0, null, route);
      assertThatThrownBy(
              () ->
                  CoordinatorGrantIT.done(
                      grants.issue(request, claim, token, 1, 1, Duration.ofSeconds(2))))
          .hasCauseInstanceOf(AuthoritySql.FencedException.class);
      var renew =
          new HomeParticipationService.AuthorizationIntent(
              "RENEW", UUID.randomUUID(), 1, null, 0, null, null);
      assertThatThrownBy(
              () ->
                  CoordinatorGrantIT.done(
                      grants.issue(request, renew, token, 1, 1, Duration.ofSeconds(2))))
          .hasCauseInstanceOf(AuthoritySql.FencedException.class);
      // Read/reconciliation stays possible; it grants no new ownership or renewed participant
      // lease.
      var query =
          new HomeParticipationService.AuthorizationIntent("QUERY", null, 0, null, 0, null, null);
      assertThat(
              CoordinatorGrantIT.done(
                      grants.issue(request, query, token, 1, 1, Duration.ofSeconds(2)))
                  .snapshot()
                  .state())
          .isEqualTo("RINGING");
    }
  }

  @Test
  void freshAcquisitionProofExpiresNoLaterThanTheOriginalNativePhaseDeadline() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("deadline-caller");
      var callee = f.sender("deadline-callee");
      var invite = f.invite(caller, callee.userId());
      var call = f.service().executeCallCommand(invite).toCompletableFuture().join().callId();
      var token = f.token(call);
      var accept = AcceptCompletionIT.accept(callee, call);
      CoordinatorGrantIT.done(
          f.service()
              .executeUnderAuthorityTracked(
                  accept,
                  new CallCommandService.Authority(call, token, 1, "TEST_ONLY_VERIFIED", 1),
                  Duration.ofSeconds(2)));
      var route =
          CoordinatorGrantIT.done(
              f.runtime.sql.submitTracked(
                  DbClass.CRITICAL,
                  Duration.ofSeconds(2),
                  c -> new SessionRepository().find(c, callee.key())));
      try (var c = f.connection();
          var q =
              c.prepareStatement(
                  "UPDATE call_state SET deadlines=jsonb_build_object('ringUntil',to_char((clock_timestamp()+interval '4 seconds') AT TIME ZONE 'UTC','YYYY-MM-DD') || 'T' || to_char((clock_timestamp()+interval '4 seconds') AT TIME ZONE 'UTC','HH24:MI:SS.US') || 'Z') WHERE call_id=?")) {
        q.setString(1, call.value());
        q.executeUpdate();
      }
      Instant now = Instant.now();
      var request =
          new HomeParticipationService.Request(
              callee.userId(),
              call,
              invite.requestId().value(),
              invite.intentHash(),
              1,
              HomeParticipationService.Phase.RINGING,
              new HomeParticipationService.Grant(
                  "c001",
                  1,
                  1,
                  token.group(),
                  token.epoch(),
                  1,
                  accept.requestId().value(),
                  now,
                  now.plusSeconds(5),
                  "UNSIGNED"));
      var action =
          new HomeParticipationService.AuthorizationIntent("CLAIM", null, 0, null, 0, null, route);
      var issued =
          CoordinatorGrantIT.done(
              new CoordinatorGrantService(f.runtime.sql, "c001", 1, "TEST_ONLY_LOCAL_OWNER")
                  .issue(request, action, token, 1, 1, Duration.ofSeconds(2)));
      assertThat(issued.expiresAt()).isBeforeOrEqualTo(DurableDeadlines.due(issued.snapshot()));
    }
  }
}
