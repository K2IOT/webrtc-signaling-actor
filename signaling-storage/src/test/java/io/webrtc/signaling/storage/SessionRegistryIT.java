package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.Identity.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;

class SessionRegistryIT {
  static DbBoundary boundary;
  static DbPools pools;
  static SessionRegistryService sessions;
  static GatewayLeaseRepository.Boot boot;

  @BeforeAll
  static void setup() throws Exception {
    try (var c = PgFixture.connection();
        var s = c.createStatement()) {
      s.execute(
          "INSERT INTO cell_authority VALUES(1,'c001',1,'GROUPED',1,'ACTIVE') ON CONFLICT DO NOTHING");
      s.execute(
          "INSERT INTO bucket_authority(bucket_id,directory_epoch,status,recovery_epoch) SELECT n,1,'ACTIVE',1 FROM generate_series(0,16383) n ON CONFLICT DO NOTHING");
    }
    var admission = new DbAdmission(Map.of(DbClass.CRITICAL, 8, DbClass.RENEWAL, 2));
    boundary = new DbBoundary(admission);
    pools =
        new DbPools(
            PgFixture.PG.getJdbcUrl(),
            PgFixture.PG.getUsername(),
            PgFixture.PG.getPassword(),
            admission,
            2,
            8);
    sessions = new SessionRegistryService(new SqlTransactions(boundary, pools), "c001", 1);
    boot =
        sessions
            .startGatewayBoot("session-gateway", UUID.randomUUID(), "test", UUID.randomUUID())
            .toCompletableFuture()
            .join();
  }

  @AfterAll
  static void close() {
    pools.close();
    boundary.close();
  }

  static AuthPrincipal token(String user, String jti) {
    return new AuthPrincipal(
        new UserId(user),
        new SessionKey("test-issuer", jti),
        Instant.now().plusSeconds(600),
        Instant.now(),
        "test-key",
        0);
  }

  @Test
  void reconnectAdvancesFenceAndDelayedCloseCannotCloseNewRoute() {
    var token = token("reconnect-user", UUID.randomUUID().toString());
    var first =
        sessions.registerSession(token, boot, UUID.randomUUID(), 1).toCompletableFuture().join();
    var second =
        sessions.registerSession(token, boot, UUID.randomUUID(), 1).toCompletableFuture().join();
    assertThat(second.connectionGeneration()).isEqualTo(first.connectionGeneration() + 1);
    assertThat(second.incarnation()).isEqualTo(first.incarnation());
    assertThat(sessions.closeSessionIfGeneration(first, 1).toCompletableFuture().join()).isFalse();
    assertThat(sessions.lookupLiveRoutes(token.userId(), 1).toCompletableFuture().join())
        .containsExactly(second);
    assertThatThrownBy(
            () ->
                sessions
                    .registerSession(
                        token("other-user", token.key().jti()), boot, UUID.randomUUID(), 1)
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(SessionRepository.BindingRejected.class);
  }

  @Test
  void sixthSessionRejectedAndClosedSessionRetainsItsIncarnation() {
    String user = "five-user";
    var routes = new ArrayList<SessionRepository.Route>();
    for (int n = 0; n < 5; n++)
      routes.add(
          sessions
              .registerSession(token(user, "device-" + n), boot, UUID.randomUUID(), 1)
              .toCompletableFuture()
              .join());
    assertThatThrownBy(
            () ->
                sessions
                    .registerSession(token(user, "sixth"), boot, UUID.randomUUID(), 1)
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(SessionRepository.SessionLimit.class);
    assertThat(sessions.closeSessionIfGeneration(routes.getFirst(), 1).toCompletableFuture().join())
        .isTrue();
    var reopened =
        sessions
            .registerSession(token(user, "device-0"), boot, UUID.randomUUID(), 1)
            .toCompletableFuture()
            .join();
    assertThat(reopened.incarnation()).isEqualTo(routes.getFirst().incarnation());
    assertThat(reopened.connectionGeneration()).isEqualTo(2);
  }

  @Test
  void concurrentRegistrationIsSerializedOrExplicitlyConflictsWithoutLostGeneration() {
    var token = token("race-user", UUID.randomUUID().toString());
    var first =
        sessions.registerSession(token, boot, UUID.randomUUID(), 1).toCompletableFuture().join();
    var futures = new ArrayList<CompletionStage<SessionRepository.Route>>();
    for (int i = 0; i < 6; i++)
      futures.add(sessions.registerSession(token, boot, UUID.randomUUID(), 1));
    var generations = new HashSet<Long>();
    generations.add(first.connectionGeneration());
    for (var f : futures)
      try {
        assertThat(generations.add(f.toCompletableFuture().join().connectionGeneration())).isTrue();
      } catch (CompletionException e) {
        assertThat(e.getCause())
            .isInstanceOfAny(
                AuthoritySql.RetryableConflict.class, SqlTransactions.SqlWorkException.class);
      }
    var live = sessions.lookupLiveRoutes(token.userId(), 1).toCompletableFuture().join();
    assertThat(live.getFirst().connectionGeneration()).isEqualTo(Collections.max(generations));
  }

  @Test
  void pulseReplayDoesNotExtendAndExpiredBootNeverRevives() throws Exception {
    UUID operation = UUID.randomUUID();
    var renewed = sessions.renewGatewayBoot(boot, 2, operation).toCompletableFuture().join();
    assertThat(
            sessions.renewGatewayBoot(boot, 2, operation).toCompletableFuture().join().leaseUntil())
        .isEqualTo(renewed.leaseUntil());
    var doomed =
        sessions
            .startGatewayBoot("doomed", UUID.randomUUID(), "test", UUID.randomUUID())
            .toCompletableFuture()
            .join();
    // Simulate DB-side expiry without sleeping; authority evaluates the primary clock.
    try (var c = PgFixture.connection();
        var s =
            c.prepareStatement(
                "UPDATE gateway_lease SET expired_at=clock_timestamp() WHERE gateway_id=? AND boot_id=?")) {
      s.setString(1, doomed.gatewayId());
      s.setObject(2, doomed.bootId());
      s.executeUpdate();
    }
    assertThatThrownBy(
            () ->
                sessions
                    .renewGatewayBoot(doomed, 2, UUID.randomUUID())
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(AuthoritySql.FencedException.class);
    assertThatThrownBy(
            () ->
                sessions
                    .registerSession(token("expired-boot", "j"), doomed, UUID.randomUUID(), 1)
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(AuthoritySql.FencedException.class);
  }

  @Test
  void olderRefreshCannotReplaceNewerExpirationOrSigningKey() {
    var principal = token("refresh-user", UUID.randomUUID().toString());
    var route =
        sessions
            .registerSession(principal, boot, UUID.randomUUID(), 1)
            .toCompletableFuture()
            .join();
    var newer =
        new AuthPrincipal(
            principal.userId(),
            principal.key(),
            principal.expiresAt().plusSeconds(60),
            principal.issuedAt().plusSeconds(1),
            "new-key",
            1);
    var updated = sessions.refreshSession(route, newer, 1).toCompletableFuture().join();
    var replay = sessions.refreshSession(updated, principal, 1).toCompletableFuture().join();
    assertThat(replay.tokenExpiresAt()).isEqualTo(updated.tokenExpiresAt());
    assertThat(replay.signingKeyId()).isEqualTo("new-key");
  }

  @Test
  void boundedPulseBatchReturnsOriginalFifteenSecondGrants() {
    var one =
        sessions
            .startGatewayBoot("batch-a", UUID.randomUUID(), "test", UUID.randomUUID())
            .toCompletableFuture()
            .join();
    var two =
        sessions
            .startGatewayBoot("batch-b", UUID.randomUUID(), "test", UUID.randomUUID())
            .toCompletableFuture()
            .join();
    var cycles =
        List.of(
            new GatewayLeaseRepository.Renewal(two, 2, UUID.randomUUID()),
            new GatewayLeaseRepository.Renewal(one, 2, UUID.randomUUID()));
    Instant before = Instant.now();
    var result = sessions.renewGatewayBootBatch(cycles).toCompletableFuture().join();
    assertThat(result).hasSize(2);
    for (var b : result)
      assertThat(Duration.between(before, b.leaseUntil()))
          .isBetween(Duration.ofSeconds(14), Duration.ofSeconds(17));
    assertThat(sessions.renewGatewayBootBatch(cycles).toCompletableFuture().join())
        .isEqualTo(result);
    assertThat(SessionRegistryService.GATEWAY_PULSE_INTERVAL).isEqualTo(Duration.ofSeconds(5));
  }

  @Test
  void nativeSecurityPolicyRejectsRegistrationAndTrackedBootReportsPrimaryRemainingTime() {
    var secured =
        new SessionRegistryService(
            new SqlTransactions(boundary, pools), "c001", 1, (connection, principal) -> false);
    var operation =
        secured.startGatewayBootTracked(
            "native-secure", UUID.randomUUID(), "test", UUID.randomUUID(), Duration.ofSeconds(2));
    var grant = operation.logical().toCompletableFuture().join();
    operation.physicalCompletion().toCompletableFuture().join();
    assertThat(grant.remainingMillis()).isBetween(13000L, 15000L);
    assertThatThrownBy(
            () ->
                secured
                    .registerSession(
                        token("native-denied", UUID.randomUUID().toString()),
                        grant.boot(),
                        UUID.randomUUID(),
                        1)
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(AuthoritySql.FencedException.class);
  }
}
