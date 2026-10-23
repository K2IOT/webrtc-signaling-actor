package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The source's database access reuses a few physical connections. Opening one per revocation page
 * (TCP, TLS, SCRAM and a PostgreSQL backend each time) cost the compose stack about two and a half
 * cores at idle.
 */
@Testcontainers
class SourcePoolIT {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6");

  private static final AtomicInteger DATABASES = new AtomicInteger();

  private final AtomicInteger physicalOpens = new AtomicInteger();
  private String databaseName;
  private PGSimpleDataSource direct;
  private DataSource counting;

  @BeforeEach
  void freshDatabase() throws SQLException {
    databaseName = "pool_" + DATABASES.incrementAndGet();
    try (Connection admin = source(POSTGRES.getDatabaseName()).getConnection();
        Statement statement = admin.createStatement()) {
      statement.execute("CREATE DATABASE " + databaseName);
    }
    direct = source(databaseName);
    counting = counting(direct, physicalOpens);
  }

  private static PGSimpleDataSource source(String database) {
    PGSimpleDataSource source = new PGSimpleDataSource();
    source.setServerNames(new String[] {POSTGRES.getHost()});
    source.setPortNumbers(new int[] {POSTGRES.getFirstMappedPort()});
    source.setDatabaseName(database);
    source.setUser(POSTGRES.getUsername());
    source.setPassword(POSTGRES.getPassword());
    source.setApplicationName("source-pool-it");
    return source;
  }

  /** A data source that counts every physical connection opened through it. */
  private static DataSource counting(DataSource target, AtomicInteger opens) {
    return (DataSource)
        Proxy.newProxyInstance(
            SourcePoolIT.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getConnection")) {
                opens.incrementAndGet();
              }
              try {
                return method.invoke(target, args);
              } catch (InvocationTargetException failure) {
                throw failure.getCause();
              }
            });
  }

  @Test
  @DisplayName("without a pool every read opens a physical connection (the cost being removed)")
  void unpooledReadsEachOpenAConnection() {
    JdbcRevocationLog.migrate(counting);
    JdbcRevocationLog log = new JdbcRevocationLog(counting);
    physicalOpens.set(0);

    for (int i = 0; i < 50; i++) {
      log.read(0, 16);
    }

    assertThat(physicalOpens).hasValue(50);
  }

  @Test
  @DisplayName("sequential reads through the pool share one or two physical connections")
  void sequentialReads_shareFewConnections() {
    try (var pool = SourcePool.wrap(counting, SourcePool.MAX_CONNECTIONS)) {
      JdbcRevocationLog.migrate(pool);
      JdbcRevocationLog log = new JdbcRevocationLog(pool);
      physicalOpens.set(0);

      for (int i = 0; i < 300; i++) {
        log.read(0, 16);
      }

      // minimumIdle is filled by the pool's housekeeper, which may land after the reset.
      assertThat(physicalOpens.get()).isLessThanOrEqualTo(3);
    }
  }

  @Test
  @DisplayName("concurrent reads never open more physical connections than the pool allows")
  void concurrentReads_stayWithinThePoolBound() throws Exception {
    try (var pool = SourcePool.wrap(counting, SourcePool.MAX_CONNECTIONS)) {
      JdbcRevocationLog.migrate(pool);
      JdbcRevocationLog log = new JdbcRevocationLog(pool);
      physicalOpens.set(0);
      ExecutorService executor = Executors.newFixedThreadPool(SourcePool.MAX_CONNECTIONS);
      try {
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < SourcePool.MAX_CONNECTIONS; t++) {
          futures.add(
              executor.submit(
                  () -> {
                    for (int i = 0; i < 60; i++) {
                      log.read(0, 16);
                    }
                  }));
        }
        for (Future<?> future : futures) {
          future.get(60, TimeUnit.SECONDS);
        }
      } finally {
        executor.shutdownNow();
      }

      assertThat(physicalOpens.get()).isLessThanOrEqualTo(SourcePool.MAX_CONNECTIONS);
    }
  }

  @Test
  @DisplayName("a read's read-only, repeatable-read state does not leak into the next append")
  void aReadDoesNotLeakItsStateIntoTheNextAppend() {
    // Flyway holds two connections at once, so the one-connection pool is built after the schema.
    JdbcRevocationLog.migrate(direct);
    try (var pool = SourcePool.wrap(counting, 1)) {
      JdbcRevocationLog log = new JdbcRevocationLog(pool);

      log.read(0, 16);
      RevocationLog.Appended appended = log.append("alice", null, OptionalLong.empty());
      RevocationLog.Snapshot after = log.read(0, 16);

      assertThat(appended.offset()).isEqualTo(1);
      assertThat(after.currentHighWater()).isEqualTo(1);
      assertThat(after.events()).hasSize(1);
      assertThat(physicalOpens.get())
          .as("one pooled connection served all of it")
          .isLessThanOrEqualTo(2);
    }
  }

  @Test
  @DisplayName("a connection killed on the server side is replaced")
  void aKilledBackendIsReplaced() throws Exception {
    try (var pool = SourcePool.wrap(counting, SourcePool.MAX_CONNECTIONS)) {
      JdbcRevocationLog.migrate(pool);
      JdbcRevocationLog log = new JdbcRevocationLog(pool);
      log.read(0, 16);

      try (Connection admin = source(POSTGRES.getDatabaseName()).getConnection();
          Statement statement = admin.createStatement();
          ResultSet killed =
              statement.executeQuery(
                  "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity"
                      + " WHERE datname = '"
                      + databaseName
                      + "' AND application_name = 'source-pool-it'")) {
        assertThat(killed.next()).isTrue();
        assertThat(killed.getInt(1)).isGreaterThanOrEqualTo(1);
      }
      // Past HikariCP's 500 ms liveness bypass window, so the dead connection is checked on borrow.
      Thread.sleep(800);

      assertThat(log.read(0, 16).currentHighWater()).isZero();
    }
  }

  @Test
  @DisplayName("closing the pool closes its physical connections")
  void closingThePoolClosesItsConnections() throws Exception {
    var pool = SourcePool.wrap(counting, SourcePool.MAX_CONNECTIONS);
    JdbcRevocationLog.migrate(pool);
    new JdbcRevocationLog(pool).read(0, 16);
    assertThat(backends()).isGreaterThanOrEqualTo(1);

    pool.close();

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (backends() > 0 && System.nanoTime() < deadline) {
      Thread.sleep(100);
    }
    assertThat(backends()).isZero();
  }

  private int backends() throws SQLException {
    try (Connection admin = source(POSTGRES.getDatabaseName()).getConnection();
        Statement statement = admin.createStatement();
        ResultSet count =
            statement.executeQuery(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = '"
                    + databaseName
                    + "' AND application_name = 'source-pool-it'")) {
      count.next();
      return count.getInt(1);
    }
  }
}
