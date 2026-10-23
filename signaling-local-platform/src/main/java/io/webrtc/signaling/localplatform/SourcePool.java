package io.webrtc.signaling.localplatform;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Objects;
import javax.sql.DataSource;
import org.slf4j.LoggerFactory;

/**
 * The bounded connection pool in front of the source database. Opening a physical connection per
 * revocation page, admin append and readiness probe meant a TCP connection, a TLS handshake, a
 * SCRAM exchange and a PostgreSQL backend each time; with nine consumers polling every 100 ms that
 * was about two and a half cores at idle. The pool reuses a few connections instead.
 *
 * <p>Nothing about what a request may read or write changes: every {@link JdbcRevocationLog}
 * operation still runs in its own transaction, and the pool resets the read-only flag, isolation
 * level and auto-commit mode a connection was left in. A connection the server has closed is
 * detected when it is next borrowed and replaced; a database that cannot be reached surfaces as the
 * same failure as before once the connection timeout passes.
 *
 * <p>Two consequences of reuse, both acceptable for this test double: a connection used within the
 * last 500 ms is handed out without a liveness check (HikariCP's bypass window), so after a
 * PostgreSQL restart a few requests can fail once before a dead connection is evicted; and a
 * connection that is silently dropped on the network blocks a request for the socket timeout
 * rather than the connect timeout. The readiness probe still runs a real query on a borrowed
 * connection, but it no longer proves that a brand-new login would succeed.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
final class SourcePool {

  /**
   * One connection for each thread that can reach the database at once: four source, two admin and
   * one health listener thread, so no request waits for the pool longer than it waited for a fresh
   * connection.
   */
  static final int MAX_CONNECTIONS = 7;

  private static final int MIN_IDLE = 2;
  private static final long CONNECTION_TIMEOUT_MILLIS = 5_000;
  private static final long VALIDATION_TIMEOUT_MILLIS = 3_000;

  private SourcePool() {}

  /**
   * The process prints nothing but a bare failure category (the Flyway log is dropped for the same
   * reason), and HikariCP would otherwise log its lifecycle and driver details through whatever
   * SLF4J backend is present. A failure still reaches the caller as an exception.
   */
  private static void silenceHikariLog() {
    if (LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext logback) {
      logback.getLogger("com.zaxxer.hikari").setLevel(ch.qos.logback.classic.Level.OFF);
    }
  }

  /**
   * Wraps {@code physical}, which keeps the URL, credentials, timeouts and application name, in a
   * pool of at most {@code maxConnections}. The caller closes the result, which closes every
   * physical connection.
   */
  static HikariDataSource wrap(DataSource physical, int maxConnections) {
    Objects.requireNonNull(physical, "physical");
    if (maxConnections < 1) {
      throw new IllegalArgumentException();
    }
    silenceHikariLog();
    HikariConfig config = new HikariConfig();
    config.setDataSource(physical);
    config.setPoolName("source-database");
    config.setMaximumPoolSize(maxConnections);
    config.setMinimumIdle(Math.min(MIN_IDLE, maxConnections));
    config.setConnectionTimeout(CONNECTION_TIMEOUT_MILLIS);
    config.setValidationTimeout(VALIDATION_TIMEOUT_MILLIS);
    config.setRegisterMbeans(false);
    return new HikariDataSource(config);
  }
}
