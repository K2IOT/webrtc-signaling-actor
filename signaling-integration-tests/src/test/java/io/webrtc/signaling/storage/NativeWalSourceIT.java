package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.app.runtime.NativeWalEnrollment;
import io.webrtc.signaling.app.runtime.NativeWalSource;
import java.sql.*;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/** Two actual standby candidates; one Docker host, no physical AZ qualification. */
class NativeWalSourceIT {
  static void policy(DatabaseReplicationIT.Replicas replicas, String setting) throws Exception {
    try (var c =
            DriverManager.getConnection(
                replicas.primary.getJdbcUrl(), "postgres", DatabaseReplicationIT.PASSWORD);
        var q = c.createStatement()) {
      q.execute("ALTER SYSTEM SET synchronous_standby_names='" + setting + "'");
      q.execute("SELECT pg_reload_conf()");
    }
    org.awaitility.Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .until(
            () -> {
              try (var c = replicas.fixture.connection();
                  var q = c.createStatement();
                  var r =
                      q.executeQuery(
                          "SELECT current_setting('synchronous_standby_names'),(SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND sync_state='quorum')")) {
                r.next();
                return setting.equals(r.getString(1)) && r.getInt(2) == 2;
              }
            });
  }

  @Test
  void twoQuorumCandidatesCannotSubstituteForTwoRequiredWriterAcknowledgements() throws Exception {
    try (var replicas = new DatabaseReplicationIT.Replicas();
        var second =
            new GenericContainer<>("postgres:17.6")
                .withNetwork(replicas.network)
                .withEnv("PGDATA", "/var/lib/postgresql/data/replica")
                .withEnv("PGPASSWORD", DatabaseReplicationIT.PASSWORD)
                .withCommand(
                    "bash",
                    "-ceu",
                    "install -d -m 0700 -o postgres -g postgres \"$PGDATA\"; "
                        + "runuser -u postgres -- pg_basebackup -D \"$PGDATA\" -R -X stream --checkpoint=fast "
                        + "-d 'host=task22-primary user=postgres application_name=task22_second'; "
                        + "exec runuser -u postgres -- postgres -D \"$PGDATA\" -c hot_standby=on")
                .waitingFor(
                    Wait.forLogMessage(
                        ".*database system is ready to accept read-only connections.*\\n", 1))
                .withStartupTimeout(Duration.ofSeconds(45))) {
      second.start();
      String id;
      long timeline;
      try (var c = replicas.fixture.connection();
          var q = c.createStatement();
          var r =
              q.executeQuery(
                  "SELECT system_identifier::text,('x'||substring(pg_walfile_name(pg_current_wal_flush_lsn()),1,8))::bit(32)::bigint FROM pg_control_system()")) {
        r.next();
        id = r.getString(1);
        timeline = r.getLong(2);
      }
      policy(replicas, "ANY 1 (task22_sync,task22_second)");
      var observer = new PostgresWalFacts(replicas.fixture.runtime.sql, "c001", 1, id, timeline);
      var two =
          new NativeWalEnrollment(
              "c001",
              1,
              id,
              timeline,
              Set.of("task22_sync", "task22_second"),
              2,
              1048576,
              Duration.ofSeconds(1),
              Duration.ofSeconds(5),
              false);
      var one =
          new NativeWalEnrollment(
              "c001",
              1,
              id,
              timeline,
              two.replicas(),
              1,
              two.maximumLagBytes(),
              two.freshness(),
              two.maximumReplyAge(),
              false);
      try (var source = new NativeWalSource(observer, two);
          var admittedOne = new NativeWalSource(observer, one)) {
        var op = source.poll(Duration.ofSeconds(2));
        var facts = op.logical().toCompletableFuture().get(3, TimeUnit.SECONDS);
        op.physicalCompletion().toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertThat(facts.replicas()).hasSize(2).allMatch(r -> r.syncState().equals("quorum"));
        assertThat(facts.synchronousPolicy().required()).isEqualTo(1);
        assertThat(source.usable()).isFalse();
        var valid = admittedOne.poll(Duration.ofSeconds(2));
        valid.logical().toCompletableFuture().get(3, TimeUnit.SECONDS);
        valid.physicalCompletion().toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertThat(admittedOne.usable()).isTrue();
        policy(replicas, "ANY 2 (task22_sync,task22_second)");
        var repaired = source.poll(Duration.ofSeconds(2));
        repaired.logical().toCompletableFuture().get(3, TimeUnit.SECONDS);
        repaired.physicalCompletion().toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertThat(source.usable()).isTrue();
      }
    }
  }
}
