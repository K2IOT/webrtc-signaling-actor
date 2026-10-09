package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;

class UserReservationRaceIT {
  static DbTestRuntime runtime;
  static HomeParticipationService home;
  static UserReservationService reservations;
  static AcceptWinnerService accepts;
  static SessionRegistryService sessions;
  static GatewayLeaseRepository.Boot boot;

  @BeforeAll
  static void setup() throws Exception {
    runtime = new DbTestRuntime();
    home =
        new HomeParticipationService(
            runtime.sql,
            "c001",
            1,
            request -> request.grant().proof().equals("TEST_ONLY_VERIFIED"));
    reservations = new UserReservationService(home);
    accepts = new AcceptWinnerService(home, (c, r) -> true);
    sessions = new SessionRegistryService(runtime.sql, "c001", 1);
    boot =
        sessions
            .startGatewayBoot("winner-gateway", UUID.randomUUID(), "test", UUID.randomUUID())
            .toCompletableFuture()
            .join();
  }

  @AfterAll
  static void close() {
    runtime.close();
  }

  static HomeParticipationService.Request request(String user, CallId call) {
    Instant issued = Instant.now();
    return new HomeParticipationService.Request(
        new UserId(user),
        call,
        UUID.randomUUID(),
        "01".repeat(32),
        1,
        HomeParticipationService.Phase.RINGING,
        new HomeParticipationService.Grant(
            "c001",
            1,
            1,
            HomeParticipationService.group(call),
            1,
            1,
            UUID.randomUUID(),
            issued,
            issued.plusSeconds(5),
            "TEST_ONLY_VERIFIED"));
  }

  static HomeParticipationService.Request fresh(
      HomeParticipationService.Request r, long epoch, long sequence) {
    Instant issued = Instant.now();
    return new HomeParticipationService.Request(
        r.user(),
        r.call(),
        r.acquireOperation(),
        r.payloadHash(),
        r.directoryEpoch(),
        r.phase(),
        new HomeParticipationService.Grant(
            "c001",
            1,
            1,
            HomeParticipationService.group(r.call()),
            epoch,
            sequence,
            UUID.randomUUID(),
            issued,
            issued.plusSeconds(5),
            "TEST_ONLY_VERIFIED"));
  }

  @Test
  void releaseBeforeReserveCreatesAbsorbingTombstoneAndNewEpochCannotReopen() {
    var r = request("release-first", CallId.create("c001", 1));
    var released = reservations.releaseIfCallVersion(r, null, 0).toCompletableFuture().join();
    assertThat(released.phase()).isEqualTo("RELEASED");
    assertThat(reservations.reserveUser(r).toCompletableFuture().join().phase())
        .isEqualTo("RELEASED");
    assertThat(reservations.reserveUser(fresh(r, 2, 1)).toCompletableFuture().join().phase())
        .isEqualTo("RELEASED");
  }

  @Test
  void reservationRetryAndPulseReplayNeverExtendOriginalGrant() {
    var r = request("renew-user", CallId.create("c001", 1));
    var first = reservations.reserveUser(r).toCompletableFuture().join();
    assertThat(reservations.reserveUser(r).toCompletableFuture().join()).isEqualTo(first);
    var cycle = fresh(r, 1, 2);
    var renewed =
        reservations
            .renewReservation(cycle, first.reservationId(), first.version())
            .toCompletableFuture()
            .join();
    assertThat(
            reservations
                .renewReservation(cycle, first.reservationId(), first.version())
                .toCompletableFuture()
                .join())
        .isEqualTo(renewed);
    var adopted =
        reservations
            .renewReservation(fresh(r, 2, 1), first.reservationId(), renewed.version())
            .toCompletableFuture()
            .join();
    assertThat(adopted.reservationId()).isEqualTo(first.reservationId());
    assertThatThrownBy(
            () ->
                reservations
                    .renewReservation(fresh(r, 1, 3), first.reservationId(), adopted.version())
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(AuthoritySql.FencedException.class);
  }

  @Test
  void reciprocalInvitesCannotCreateTwoLiveReservations() {
    var call = CallId.create("c001", 1);
    var a = request("pair-a", call);
    var b = request("pair-b", call);
    var acquired = reservations.reservePair(a, b).toCompletableFuture().join();
    assertThat(acquired).hasSize(2);
    var reverse = CallId.create("c001", 1);
    assertThatThrownBy(
            () ->
                reservations
                    .reservePair(request("pair-b", reverse), request("pair-a", reverse))
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(UserReservationService.UserBusy.class);
    assertThat(home.queryParticipation(a).toCompletableFuture().join().reservationId())
        .isEqualTo(acquired.getFirst().reservationId());
  }

  @Test
  void expiryAllowsNewCallButNeverOldCallResurrection() throws Exception {
    var old = request("expiry-user", CallId.create("c001", 1));
    var first = reservations.reserveUser(old).toCompletableFuture().join();
    try (var c = PgFixture.connection();
        var s =
            c.prepareStatement(
                "UPDATE user_reservation SET lease_until=clock_timestamp()-interval '1 second' WHERE user_id=?")) {
      s.setString(1, old.user().value());
      s.executeUpdate();
    }
    var newer = request("expiry-user", CallId.create("c001", 1));
    var replacement = reservations.reserveUser(newer).toCompletableFuture().join();
    assertThat(replacement.reservationId()).isNotEqualTo(first.reservationId());
    assertThat(reservations.reserveUser(old).toCompletableFuture().join().phase())
        .isEqualTo("EXPIRED");
    reservations
        .releaseIfCallVersion(old, first.reservationId(), first.version())
        .toCompletableFuture()
        .join();
    assertThat(home.queryParticipation(newer).toCompletableFuture().join().reservationId())
        .isEqualTo(replacement.reservationId());
  }

  @Test
  void oneHundredConcurrentAcceptsAcrossFiveDevicesCommitOneImmutableWinner() throws Exception {
    var r = request("accept-user", CallId.create("c001", 1));
    var reserved = reservations.reserveUser(r).toCompletableFuture().join();
    var routes = new ArrayList<SessionRepository.Route>();
    for (int i = 0; i < 5; i++) {
      var token =
          new AuthPrincipal(
              r.user(),
              new SessionKey("test-issuer", "winner-" + i),
              Instant.now().plusSeconds(300),
              Instant.now(),
              "test-key",
              0);
      routes.add(
          sessions.registerSession(token, boot, UUID.randomUUID(), 1).toCompletableFuture().join());
    }
    var outcomes = new ArrayList<CompletableFuture<AcceptWinnerService.Claim>>();
    try (var workers = Executors.newFixedThreadPool(16)) {
      for (int i = 0; i < 100; i++) {
        var route = routes.get(i % 5);
        outcomes.add(
            CompletableFuture.supplyAsync(
                () -> {
                  for (int retry = 0; retry < 300; retry++)
                    try {
                      return accepts
                          .claimAccept(r, reserved.reservationId(), route)
                          .toCompletableFuture()
                          .join();
                    } catch (CompletionException e) {
                      if (e.getCause() instanceof DbOverloadedException
                          || e.getCause() instanceof AuthoritySql.RetryableConflict
                          || e.getCause() instanceof SqlTransactions.SqlWorkException
                              && e.getCause().getCause() instanceof java.sql.SQLException sqlError
                              && "55P03".equals(sqlError.getSQLState()))
                        java.util.concurrent.locks.LockSupport.parkNanos(5_000_000);
                      else throw e;
                    }
                  throw new AssertionError("Accept contention did not converge");
                },
                workers));
      }
      CompletableFuture.allOf(outcomes.toArray(CompletableFuture[]::new)).join();
    }
    var winners =
        outcomes.stream()
            .map(CompletableFuture::join)
            .map(AcceptWinnerService.Claim::winner)
            .distinct()
            .toList();
    assertThat(winners).hasSize(1);
    assertThat(
            outcomes.stream()
                .map(CompletableFuture::join)
                .map(AcceptWinnerService.Claim::outcome)
                .distinct())
        .contains("CLAIMED", "ANSWERED_ELSEWHERE");
    reservations.releaseIfCallVersion(r, reserved.reservationId(), 0).toCompletableFuture().join();
    assertThat(
            accepts
                .claimAccept(r, reserved.reservationId(), routes.getFirst())
                .toCompletableFuture()
                .join()
                .outcome())
        .isEqualTo("TERMINAL");
  }

  @Test
  void missingProofAndExpiredGrantFailClosed() {
    var r = request("untrusted-grant", CallId.create("c001", 1));
    var g = r.grant();
    var bad =
        new HomeParticipationService.Request(
            r.user(),
            r.call(),
            r.acquireOperation(),
            r.payloadHash(),
            1,
            r.phase(),
            new HomeParticipationService.Grant(
                g.cell(),
                1,
                1,
                g.group(),
                1,
                1,
                g.operation(),
                g.issuedAt(),
                g.expiresAt(),
                "untrusted"));
    assertThatThrownBy(() -> reservations.reserveUser(bad).toCompletableFuture().join())
        .hasCauseInstanceOf(AuthoritySql.FencedException.class);
    Instant past = Instant.now().minusSeconds(6);
    var expired =
        new HomeParticipationService.Request(
            r.user(),
            r.call(),
            r.acquireOperation(),
            r.payloadHash(),
            1,
            r.phase(),
            new HomeParticipationService.Grant(
                g.cell(),
                1,
                1,
                g.group(),
                1,
                1,
                g.operation(),
                past,
                past.plusSeconds(5),
                "TEST_ONLY_VERIFIED"));
    assertThatThrownBy(() -> reservations.reserveUser(expired).toCompletableFuture().join())
        .hasCauseInstanceOf(AuthoritySql.FencedException.class);
  }

  @Test
  void acceptAndCancellationRacePreservesClaimHistoryAndNeverReopens() throws Exception {
    var r = request("accept-cancel", CallId.create("c001", 1));
    var reserved = reservations.reserveUser(r).toCompletableFuture().join();
    var principal =
        new AuthPrincipal(
            r.user(),
            new SessionKey("test-issuer", "accept-cancel"),
            Instant.now().plusSeconds(300),
            Instant.now(),
            "test-key",
            0);
    var route =
        sessions
            .registerSession(principal, boot, UUID.randomUUID(), 1)
            .toCompletableFuture()
            .join();
    var claim = accepts.claimAccept(r, reserved.reservationId(), route);
    var cancel = reservations.releaseIfCallVersion(r, reserved.reservationId(), 0);
    for (var operation : List.of(claim, cancel))
      try {
        operation.toCompletableFuture().join();
      } catch (CompletionException e) {
        assertThat(e.getCause())
            .isInstanceOfAny(
                SqlTransactions.SqlWorkException.class, AuthoritySql.RetryableConflict.class);
      }
    reservations.releaseIfCallVersion(r, reserved.reservationId(), 0).toCompletableFuture().join();
    var terminal = home.queryParticipation(r).toCompletableFuture().join();
    assertThat(terminal.phase()).isEqualTo("RELEASED");
    assertThat(
            accepts
                .claimAccept(r, reserved.reservationId(), route)
                .toCompletableFuture()
                .join()
                .outcome())
        .isEqualTo("TERMINAL");
    assertThat(reservations.reserveUser(r).toCompletableFuture().join()).isEqualTo(terminal);
  }
}
