package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;

class LocalInviteAtomicIT {
  static final class Fixture implements AutoCloseable {
    final String url;
    final DbTestRuntime runtime;
    final SessionRegistryService sessions;
    final GatewayLeaseRepository.Boot boot;
    final GroupOwnerRepository groups;
    final UUID incarnation = UUID.randomUUID();
    final Map<Integer, AuthoritySql.GroupToken> tokens = new HashMap<>();

    Fixture() throws Exception {
      String schema = "local_invite_" + UUID.randomUUID().toString().replace("-", "");
      url = PgFixture.PG.getJdbcUrl() + "&currentSchema=" + schema;
      Flyway.configure()
          .dataSource(url, PgFixture.PG.getUsername(), PgFixture.PG.getPassword())
          .schemas(schema)
          .defaultSchema(schema)
          .locations("classpath:db/migration")
          .load()
          .migrate();
      runtime = new DbTestRuntime(url, PgFixture.PG.getUsername(), PgFixture.PG.getPassword());
      try (var c = connection();
          var s = c.createStatement()) {
        s.execute(
            "INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,lease_sequence,status) SELECT 'c001',1,n,1,1,0,'IDLE' FROM generate_series(0,1023)n");
      }
      groups = new GroupOwnerRepository(runtime.sql, "c001", 1);
      sessions = new SessionRegistryService(runtime.sql, "c001", 1);
      boot =
          sessions
              .startGatewayBoot(
                  "TEST_ONLY_LOCAL", UUID.randomUUID(), "TEST_ONLY", UUID.randomUUID())
              .toCompletableFuture()
              .join();
    }

    Connection connection() throws SQLException {
      return DriverManager.getConnection(
          url, PgFixture.PG.getUsername(), PgFixture.PG.getPassword());
    }

    AuthenticatedSession sender(String name) {
      var principal =
          new AuthPrincipal(
              new UserId(name),
              new SessionKey("TEST_ONLY", UUID.randomUUID().toString()),
              Instant.now().plusSeconds(600),
              Instant.now(),
              "TEST_ONLY",
              1);
      var route =
          sessions
              .registerSession(principal, boot, UUID.randomUUID(), 1)
              .toCompletableFuture()
              .join();
      return new AuthenticatedSession(
          route.user(),
          route.key(),
          route.incarnation(),
          route.connectionGeneration(),
          route.connectionId());
    }

    AuthoritySql.GroupToken token(CallId call) {
      int group = HomeParticipationService.group(call);
      return tokens.computeIfAbsent(
          group,
          g ->
              groups
                  .acquireTracked(g, "TEST_ONLY_LOCAL_OWNER", incarnation, UUID.randomUUID())
                  .logical()
                  .toCompletableFuture()
                  .join()
                  .orElseThrow()
                  .token());
    }

    CallCommandService service() {
      return new CallCommandService(
              runtime.sql,
              "c001",
              1,
              command -> {
                var candidate = CallId.create("c001", 1);
                return CompletableFuture.completedFuture(
                    new CallCommandService.Authority(
                        candidate,
                        token(candidate),
                        1,
                        "TEST_ONLY_VERIFIED",
                        0,
                        new CallCommandService.TargetHome("c001", 1)));
              },
              (c, s, p) -> p.equals("TEST_ONLY_VERIFIED"))
          .businessAdmission(() -> true);
    }

    CallCommand invite(AuthenticatedSession caller, UserId target) {
      return new CallCommand(
          SignalEnvelope.Type.INVITE,
          caller,
          new RequestId(UUID.randomUUID()),
          null,
          CommandScope.invite(),
          target,
          null,
          null,
          "{}",
          "a".repeat(64));
    }

    public void close() {
      runtime.close();
    }
  }

  @Test
  void localInviteCommitsBothReservationsRingingOriginalResultAndOutboxAtomically()
      throws Exception {
    try (var f = new Fixture()) {
      var caller = f.sender("local-caller");
      var callee = f.sender("local-callee");
      var service = f.service();
      var command = f.invite(caller, callee.userId());
      var outcome = service.executeCallCommand(command).toCompletableFuture().join();
      assertThat(outcome.status()).isEqualTo("FINAL");
      assertThat(outcome.state()).isEqualTo("RINGING");
      assertThat(outcome.version()).isEqualTo(1);
      assertThat(outcome.eventIds()).hasSize(2);
      assertThat(service.executeCallCommand(command).toCompletableFuture().join())
          .isEqualTo(outcome);
      try (var c = f.connection();
          var q =
              c.prepareStatement(
                  "SELECT count(*),bool_and(phase='RINGING'),bool_and(lease_until>clock_timestamp()+interval '20 seconds') FROM user_reservation WHERE call_id=?")) {
        q.setString(1, outcome.callId().value());
        try (var r = q.executeQuery()) {
          r.next();
          assertThat(r.getInt(1)).isEqualTo(2);
          assertThat(r.getBoolean(2)).isTrue();
          assertThat(r.getBoolean(3)).isTrue();
        }
      }
      try (var c = f.connection();
          var q = c.prepareStatement("SELECT count(*) FROM call_state WHERE caller_user=?")) {
        q.setString(1, caller.userId().value());
        try (var r = q.executeQuery()) {
          r.next();
          assertThat(r.getInt(1)).isEqualTo(1);
        }
      }
    }
  }

  @Test
  void busyTargetDoesNotLeaveCallerReservationOrPreparingCall() throws Exception {
    try (var f = new Fixture()) {
      var caller = f.sender("busy-local-caller");
      var callee = f.sender("busy-local-callee");
      var other = CallId.create("c001", 1);
      var token = f.token(other);
      Instant now = Instant.now();
      UUID operation = UUID.randomUUID();
      var home = new HomeParticipationService(f.runtime.sql, "c001", 1, r -> true);
      var request =
          new HomeParticipationService.Request(
              callee.userId(),
              other,
              operation,
              "b".repeat(64),
              1,
              HomeParticipationService.Phase.RINGING,
              new HomeParticipationService.Grant(
                  "c001",
                  1,
                  1,
                  token.group(),
                  token.epoch(),
                  1,
                  operation,
                  now,
                  now.plusSeconds(5),
                  "TEST_ONLY"));
      new UserReservationService(home).reserveUser(request).toCompletableFuture().join();
      var outcome =
          f.service()
              .executeCallCommand(f.invite(caller, callee.userId()))
              .toCompletableFuture()
              .join();
      assertThat(outcome.code()).isEqualTo("USER_BUSY");
      assertThat(outcome.callId()).isNull();
      try (var c = f.connection();
          var q =
              c.prepareStatement(
                  "SELECT (SELECT count(*) FROM user_reservation WHERE user_id=?)+(SELECT count(*) FROM call_state WHERE caller_user=?)")) {
        q.setString(1, caller.userId().value());
        q.setString(2, caller.userId().value());
        try (var r = q.executeQuery()) {
          r.next();
          assertThat(r.getInt(1)).isZero();
        }
      }
    }
  }

  @Test
  void staleSameCellHintNeverBypassesFrozenTargetBucket() throws Exception {
    try (var f = new Fixture()) {
      var caller = f.sender("freeze-local-caller");
      var callee = f.sender("freeze-local-callee");
      try (var c = f.connection();
          var q =
              c.prepareStatement("UPDATE bucket_authority SET status='FROZEN' WHERE bucket_id=?")) {
        q.setInt(1, SessionRegistryService.bucket(callee.userId()));
        q.executeUpdate();
      }
      assertThatThrownBy(
              () ->
                  f.service()
                      .executeCallCommand(f.invite(caller, callee.userId()))
                      .toCompletableFuture()
                      .join())
          .hasCauseInstanceOf(AuthoritySql.FencedException.class);
    }
  }

  @Test
  void candidateCallRoutingEpochCannotDifferFromNativeStorageAuthority() throws Exception {
    try (var f = new Fixture()) {
      var caller = f.sender("wrong-epoch-caller");
      var callee = f.sender("wrong-epoch-callee");
      var wrong = CallId.create("c001", 2);
      var service =
          new CallCommandService(
                  f.runtime.sql,
                  "c001",
                  1,
                  c ->
                      CompletableFuture.completedFuture(
                          new CallCommandService.Authority(
                              wrong,
                              f.token(wrong),
                              1,
                              "TEST_ONLY_VERIFIED",
                              0,
                              new CallCommandService.TargetHome("c001", 1))),
                  (c, s, p) -> true)
              .businessAdmission(() -> true);
      assertThatThrownBy(
              () ->
                  service
                      .executeCallCommand(f.invite(caller, callee.userId()))
                      .toCompletableFuture()
                      .join())
          .hasCauseInstanceOf(CallCommandService.AuthorizationRejected.class);
    }
  }
}
