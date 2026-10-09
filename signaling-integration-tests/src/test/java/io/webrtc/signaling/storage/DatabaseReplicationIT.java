package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.webrtc.signaling.protocol.Identity.*;
import java.nio.file.*;
import java.security.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.*;
import org.testcontainers.containers.wait.strategy.Wait;

/** Actual synchronous WAL, stopped old writer and promoted standby; one host, TEST ONLY. */
class DatabaseReplicationIT {
  static final String PASSWORD = "TEST_ONLY_REPLICA";

  static final class Replicas implements AutoCloseable {
    final Network network = Network.newNetwork();
    final PostgreSQLContainer<?> primary =
        new PostgreSQLContainer<>("postgres:17.6")
            .withDatabaseName("signaling_fault")
            .withUsername("postgres")
            .withPassword(PASSWORD)
            .withNetwork(network)
            .withNetworkAliases("task22-primary")
            .withCommand(
                "postgres",
                "-c",
                "wal_level=replica",
                "-c",
                "max_wal_senders=4",
                "-c",
                "synchronous_commit=on");
    GenericContainer<?> standby;
    LocalInviteAtomicIT.Fixture fixture;
    boolean paused;

    Replicas() throws Exception {
      try {
        primary.start();
        var hba =
            primary.execInContainer(
                "bash",
                "-ceu",
                "printf '%s\\n' 'host replication postgres all scram-sha-256' >> \"$PGDATA/pg_hba.conf\"");
        assertThat(hba.getExitCode()).isZero();
        try (var c = DriverManager.getConnection(primary.getJdbcUrl(), "postgres", PASSWORD);
            var q = c.createStatement()) {
          q.execute("SELECT pg_reload_conf()");
        }
        standby =
            new GenericContainer<>("postgres:17.6")
                .withNetwork(network)
                .withEnv("PGDATA", "/var/lib/postgresql/data/replica")
                .withEnv("PGPASSWORD", PASSWORD)
                .withExposedPorts(5432)
                .withCommand(
                    "bash",
                    "-ceu",
                    "install -d -m 0700 -o postgres -g postgres \"$PGDATA\"; "
                        + "runuser -u postgres -- pg_basebackup -D \"$PGDATA\" -R -X stream --checkpoint=fast "
                        + "-d 'host=task22-primary user=postgres application_name=task22_sync'; "
                        + "exec runuser -u postgres -- postgres -D \"$PGDATA\" -c hot_standby=on")
                .waitingFor(
                    Wait.forLogMessage(
                        ".*database system is ready to accept read-only connections.*\\n", 1))
                .withStartupTimeout(Duration.ofSeconds(45));
        standby.start();
        try (var c = DriverManager.getConnection(primary.getJdbcUrl(), "postgres", PASSWORD);
            var q = c.createStatement()) {
          q.execute("ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (task22_sync)'");
          q.execute("SELECT pg_reload_conf()");
        }
        org.awaitility.Awaitility.await()
            .atMost(Duration.ofSeconds(15))
            .until(
                () -> {
                  try (var c =
                          DriverManager.getConnection(primary.getJdbcUrl(), "postgres", PASSWORD);
                      var q = c.createStatement();
                      var r =
                          q.executeQuery(
                              "SELECT count(*) FROM pg_stat_replication WHERE application_name='task22_sync' AND state='streaming' AND sync_state='sync'")) {
                    r.next();
                    return r.getInt(1) == 1;
                  }
                });
        fixture = new LocalInviteAtomicIT.Fixture(primary.getJdbcUrl(), "postgres", PASSWORD);
      } catch (Exception | AssertionError failure) {
        close();
        throw failure;
      }
    }

    String standbyUrl() {
      return "jdbc:postgresql://"
          + standby.getHost()
          + ":"
          + standby.getMappedPort(5432)
          + "/signaling_fault?currentSchema="
          + fixture.url.substring(fixture.url.indexOf("currentSchema=") + 14);
    }

    Connection standbyConnection() throws SQLException {
      return DriverManager.getConnection(standbyUrl(), "postgres", PASSWORD);
    }

    void pauseStandby() {
      DockerClientFactory.instance().client().pauseContainerCmd(standby.getContainerId()).exec();
      paused = true;
    }

    void resumeStandby() {
      if (paused) {
        DockerClientFactory.instance()
            .client()
            .unpauseContainerCmd(standby.getContainerId())
            .exec();
        paused = false;
      }
    }

    @Override
    public void close() {
      try {
        resumeStandby();
        if (fixture != null) fixture.close();
      } finally {
        try {
          if (standby != null) standby.stop();
        } finally {
          try {
            primary.stop();
          } finally {
            network.close();
          }
        }
      }
    }
  }

  @Test
  void synchronousStandbyLossNeverAcknowledgesControlAndOriginalUnknownResultReconciles()
      throws Exception {
    try (var replicas = new Replicas()) {
      var f = replicas.fixture;
      var caller = f.sender("sync-caller");
      var callee = f.sender("sync-callee");
      var command = f.invite(caller, callee.userId());
      var call = CallId.create("c001", 1);
      var context =
          new CallCommandService.Authority(
              call,
              f.token(call),
              1,
              "TEST_ONLY_VERIFIED",
              0,
              new CallCommandService.TargetHome("c001", 1));
      var commands = f.service();
      replicas.pauseStandby();
      var work = commands.executeUnderAuthorityTracked(command, context, Duration.ofMillis(800));
      try {
        org.awaitility.Awaitility.await()
            .atMost(Duration.ofMillis(700))
            .until(
                () -> {
                  try (var c = f.connection();
                      var q = c.createStatement();
                      var r =
                          q.executeQuery(
                              "SELECT count(*) FROM pg_stat_activity WHERE wait_event='SyncRep' AND application_name='signaling-control'")) {
                    r.next();
                    return r.getInt(1) > 0;
                  }
                });
        assertThatThrownBy(() -> work.logical().toCompletableFuture().get(2, TimeUnit.SECONDS))
            .hasCauseInstanceOf(DbOutcomeUnknownException.class);
        assertThat(work.physicalCompletion().toCompletableFuture().isDone()).isFalse();
      } finally {
        replicas.resumeStandby();
        work.physicalCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
      }
      var recorded =
          commands
              .getCommandResult(caller, command.scope(), command.requestId())
              .toCompletableFuture()
              .get(2, TimeUnit.SECONDS)
              .orElseThrow();
      var replay =
          CoordinatorGrantIT.done(
              commands.executeUnderAuthorityTracked(command, context, Duration.ofSeconds(2)));
      assertThat(replay).isEqualTo(recorded);
      assertThat(replay.callId()).isEqualTo(call);
      assertThat(CrashScheduleIT.count(f, "call_state")).isEqualTo(1);
      assertThat(CrashScheduleIT.count(f, "control_outbox")).isEqualTo(2);
      receipt(
          "sync-standby-loss",
          Map.of(
              "unknownReconciled",
              true,
              "physicalTailRetained",
              true,
              "callCount",
              1,
              "outboxCount",
              2));
    }
  }

  @Test
  void physicallyFencedPrimaryPromotionPreservesAcknowledgedCallResultAndOutboxWal()
      throws Exception {
    try (var replicas = new Replicas()) {
      var f = replicas.fixture;
      var caller = f.sender("ha-caller");
      var callee = f.sender("ha-callee");
      var command = f.invite(caller, callee.userId());
      var committed =
          f.service().executeCallCommand(command).toCompletableFuture().get(2, TimeUnit.SECONDS);
      var oldRoot = f.token(committed.callId());
      var principal = SessionAuthReadIT.principal(SessionAuthReadIT.route(f, caller));
      String acknowledged;
      try (var c = f.connection();
          var q = c.createStatement();
          var r = q.executeQuery("SELECT pg_current_wal_flush_lsn()::text")) {
        r.next();
        acknowledged = r.getString(1);
      }
      org.awaitility.Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(
              () -> {
                try (var c = replicas.standbyConnection();
                    var q = c.prepareStatement("SELECT pg_last_wal_replay_lsn()>=?::pg_lsn")) {
                  q.setString(1, acknowledged);
                  try (var r = q.executeQuery()) {
                    r.next();
                    return r.getBoolean(1);
                  }
                }
              });
      DockerClientFactory.instance()
          .client()
          .killContainerCmd(replicas.primary.getContainerId())
          .withSignal("KILL")
          .exec();
      var inspection =
          DockerClientFactory.instance()
              .client()
              .inspectContainerCmd(replicas.primary.getContainerId())
              .exec();
      assertThat(inspection.getState().getRunning()).isFalse();
      long fencedAt = System.nanoTime();
      assertThatThrownBy(
              () -> DriverManager.getConnection(f.url + "&connectTimeout=1", "postgres", PASSWORD))
          .isInstanceOf(SQLException.class);
      var promote =
          replicas.standby.execInContainer(
              "runuser",
              "-u",
              "postgres",
              "--",
              "pg_ctl",
              "promote",
              "-D",
              "/var/lib/postgresql/data/replica",
              "-w",
              "-t",
              "20");
      assertThat(promote.getExitCode()).as(promote.getStderr()).isZero();
      long promotedAt = System.nanoTime();
      // pg_ctl waits for recovery exit; its promotion checkpoint may still be pending.
      try (var c = replicas.standbyConnection();
          var q = c.createStatement()) {
        q.execute("CHECKPOINT");
      }
      long timeline;
      try (var c = replicas.standbyConnection();
          var q = c.createStatement();
          var r =
              q.executeQuery(
                  "SELECT NOT pg_is_in_recovery(),timeline_id FROM pg_control_checkpoint()")) {
        r.next();
        assertThat(r.getBoolean(1)).isTrue();
        timeline = r.getLong(2);
      }
      assertThat(timeline).isGreaterThan(1);
      try (var runtime = new DbTestRuntime(replicas.standbyUrl(), "postgres", PASSWORD)) {
        var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        var now = Instant.now();
        var unsigned =
            new StoragePromotionService.Permit(
                "c001",
                1,
                2,
                UUID.randomUUID(),
                now,
                now.plusSeconds(5),
                digest(replicas.primary.getContainerId() + ":stopped:" + fencedAt),
                digest(acknowledged + ":replayed:" + timeline),
                "TEST_ONLY_HA",
                "");
        var signer = Signature.getInstance("Ed25519");
        signer.initSign(keys.getPrivate());
        signer.update(StoragePromotionService.signingBytes(unsigned));
        var permit =
            new StoragePromotionService.Permit(
                "c001",
                1,
                2,
                unsigned.operation(),
                now,
                unsigned.validUntil(),
                unsigned.oldPrimaryFenceReceiptSha256(),
                unsigned.acknowledgedWalReceiptSha256(),
                unsigned.keyId(),
                Base64.getUrlEncoder().withoutPadding().encodeToString(signer.sign()));
        var promotion =
            new StoragePromotionService(
                runtime.sql,
                "c001",
                StoragePromotionService.enrolledVerifier(
                    List.of(
                        new StoragePromotionService.EnrolledSource(
                            "TEST_ONLY_HA", "c001", keys.getPublic()))));
        assertThat(CoordinatorGrantIT.done(promotion.promote(permit))).isEqualTo(2);
        assertThat(CoordinatorGrantIT.done(promotion.promote(permit))).isEqualTo(2);
        var roots = new GroupOwnerRepository(runtime.sql, "c001", 2);
        var newRoot =
            CoordinatorGrantIT.done(
                    roots.acquireTracked(
                        oldRoot.group(), "TEST_ONLY_HA_NEW", UUID.randomUUID(), UUID.randomUUID()))
                .orElseThrow();
        assertThat(newRoot.token().epoch()).isGreaterThan(oldRoot.epoch());
        assertThatThrownBy(
                () ->
                    CoordinatorGrantIT.done(
                        new GroupOwnerRepository(runtime.sql, "c001", 1).releaseTracked(oldRoot)))
            .hasCauseInstanceOf(AuthoritySql.FencedException.class);
        var registry = new SessionRegistryService(runtime.sql, "c001", 2);
        var boot =
            registry
                .startGatewayBoot(
                    "TEST_ONLY_HA_NEW", UUID.randomUUID(), "TEST_ONLY", UUID.randomUUID())
                .toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        var route =
            registry
                .registerSession(principal, boot, UUID.randomUUID(), 1)
                .toCompletableFuture()
                .get(2, TimeUnit.SECONDS);
        var current =
            new AuthenticatedSession(
                route.user(),
                route.key(),
                route.incarnation(),
                route.connectionGeneration(),
                route.connectionId());
        var commands =
            new CallCommandService(
                runtime.sql,
                "c001",
                2,
                c -> {
                  throw new AssertionError("Replay must not allocate authority");
                },
                (c, s, p) -> p.equals("TEST_ONLY_VERIFIED"));
        assertThat(
                commands
                    .getCommandResult(current, command.scope(), command.requestId())
                    .toCompletableFuture()
                    .get(2, TimeUnit.SECONDS))
            .contains(committed);
        var workflow =
            new CallWorkflowService(runtime.sql, "c001", 2, "TEST_ONLY_HA_NEW", (t, s) -> false);
        var snapshot =
            CoordinatorGrantIT.done(
                    workflow.loadCold(committed.callId(), newRoot.token(), Duration.ofSeconds(2)))
                .orElseThrow();
        assertThat(snapshot.version()).isEqualTo(1);
        assertThat(snapshot.state()).isEqualTo("RINGING");
        try (var c = replicas.standbyConnection();
            var q = c.createStatement();
            var r =
                q.executeQuery(
                    "SELECT (SELECT count(*) FROM call_state),(SELECT count(*) FROM command_result),(SELECT count(*) FROM control_outbox)")) {
          r.next();
          assertThat(r.getInt(1)).isEqualTo(1);
          assertThat(r.getInt(2)).isEqualTo(1);
          assertThat(r.getInt(3)).isEqualTo(2);
        }
      }
      receipt(
          "postgres-primary-failure-local",
          Map.of(
              "acknowledgedWalHighWater",
              acknowledged,
              "oldWriterContainer",
              replicas.primary.getContainerId(),
              "fencedAtNanos",
              fencedAt,
              "promotedAtNanos",
              promotedAt,
              "promotedTimeline",
              timeline,
              "nativeOperationReconciled",
              true));
    }
  }

  static String digest(String value) throws Exception {
    return HexFormat.of()
        .formatHex(
            MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
  }

  static void receipt(String scenario, Map<String, Object> observations) throws Exception {
    Path output = Path.of("target/fault-receipts/" + scenario + ".json");
    Files.createDirectories(output.getParent());
    Files.writeString(
        output,
        new ObjectMapper()
                .writeValueAsString(
                    Map.of(
                        "testOnly",
                        true,
                        "scope",
                        "LOCAL_SYNCHRONOUS_POSTGRES",
                        "scenario",
                        scenario,
                        "observations",
                        observations))
            + "\n");
  }
}
