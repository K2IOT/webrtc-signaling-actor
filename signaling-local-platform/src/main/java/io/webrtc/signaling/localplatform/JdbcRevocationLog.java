package io.webrtc.signaling.localplatform;

import io.webrtc.signaling.auth.RevocationState;
import io.webrtc.signaling.protocol.Identity.SessionKey;
import io.webrtc.signaling.protocol.Identity.UserId;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;

/**
 * PostgreSQL {@link RevocationLog}; one connection and one transaction per call.
 *
 * <p>An append runs in a single {@code READ COMMITTED} transaction whose first statement is {@code
 * UPDATE source_state SET high_water = high_water + 1 RETURNING high_water}. That row lock is held
 * to commit, so appenders are serialised, offsets are gapless and each commit is visible in offset
 * order. The default epoch is read after the lock is taken (nobody else can append to the subject
 * meanwhile), and the commit time is taken after the lock too, so it never goes backwards along the
 * offsets. A conflict or any failure rolls the whole transaction back, including the counter.
 *
 * <p>A read runs in one {@code REPEATABLE READ READ ONLY} transaction, so the page and the
 * high-water mark come from the same snapshot.
 *
 * <p>Any {@link SQLException} becomes {@link RevocationLog.Unavailable}; nothing of it (message,
 * SQL state, cause) is kept.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
public final class JdbcRevocationLog implements RevocationLog {

  /** Flyway location of the source schema; the storage jar owns {@code db/migration}. */
  static final String MIGRATION_LOCATION = "classpath:db/source/migration";

  /** History table of the source schema, separate from the cell schema's one. */
  static final String HISTORY_TABLE = "source_schema_history";

  private static final int MAX_LIMIT = 1000;

  private static final String BUMP =
      "UPDATE source_state SET high_water = high_water + 1 RETURNING high_water";
  private static final String LATEST_USER_SCOPE =
      "SELECT max(epoch) FROM revocation_log WHERE user_id = ? AND jti IS NULL";
  private static final String LATEST_SESSION_SCOPE =
      "SELECT max(epoch) FROM revocation_log WHERE user_id = ? AND jti = ?";
  // clock_timestamp() is read after the counter lock is held; date_trunc pins the stored value to
  // microseconds (timestamptz has no finer resolution) so that stored and signed values agree.
  private static final String INSERT =
      "INSERT INTO revocation_log (source_offset, user_id, jti, epoch, committed_at) "
          + "VALUES (?, ?, ?, ?, date_trunc('microseconds', clock_timestamp())) "
          + "RETURNING committed_at";
  private static final String HIGH_WATER = "SELECT high_water FROM source_state";
  private static final String PAGE =
      "SELECT source_offset, user_id, jti, epoch, committed_at FROM revocation_log "
          + "WHERE source_offset > ? ORDER BY source_offset LIMIT ?";

  private final DataSource dataSource;

  public JdbcRevocationLog(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
  }

  /**
   * Applies the source schema. Idempotent; {@code clean} is disabled.
   *
   * @throws RevocationLog.Unavailable if the migration cannot be applied
   */
  public static void migrate(DataSource dataSource) {
    Objects.requireNonNull(dataSource, "dataSource");
    try {
      Flyway.configure()
          .dataSource(dataSource)
          .locations(MIGRATION_LOCATION)
          .table(HISTORY_TABLE)
          .cleanDisabled(true)
          // Flyway logs the JDBC URL and the migration progress; the process prints nothing else.
          .loggers(SilentFlywayLog.class.getName())
          .load()
          .migrate();
    } catch (RuntimeException failure) {
      throw new Unavailable();
    }
  }

  @Override
  public Appended append(String userId, String jtiOrNull, OptionalLong epoch) {
    Objects.requireNonNull(userId, "userId");
    Objects.requireNonNull(epoch, "epoch");
    String user = new UserId(userId).value();
    if (jtiOrNull != null) {
      // Same bounds and character rules as the session id inside an event.
      new SessionKey(RevocationLog.UNSCOPED_ISSUER, jtiOrNull);
    }
    if (epoch.isPresent() && epoch.getAsLong() < 0) {
      throw new IllegalArgumentException();
    }
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try {
        Appended appended = appendIn(connection, user, jtiOrNull, epoch);
        connection.commit();
        return appended;
      } catch (EpochConflict conflict) {
        rollbackQuietly(connection);
        throw conflict;
      } catch (SQLException | RuntimeException failure) {
        rollbackQuietly(connection);
        throw new Unavailable();
      }
    } catch (SQLException failure) {
      throw new Unavailable();
    }
  }

  private static Appended appendIn(
      Connection connection, String user, String jti, OptionalLong explicitEpoch)
      throws SQLException {
    long offset;
    try (PreparedStatement bump = connection.prepareStatement(BUMP);
        ResultSet row = bump.executeQuery()) {
      if (!row.next()) {
        throw new SQLException();
      }
      offset = row.getLong(1);
    }
    long latest;
    try (PreparedStatement query =
        connection.prepareStatement(jti == null ? LATEST_USER_SCOPE : LATEST_SESSION_SCOPE)) {
      query.setString(1, user);
      if (jti != null) {
        query.setString(2, jti);
      }
      try (ResultSet row = query.executeQuery()) {
        row.next();
        long max = row.getLong(1);
        latest = row.wasNull() ? -1 : max;
      }
    }
    long epoch;
    if (explicitEpoch.isPresent()) {
      epoch = explicitEpoch.getAsLong();
      if (epoch <= latest) {
        throw new EpochConflict();
      }
    } else {
      if (latest == Long.MAX_VALUE) {
        throw new EpochConflict();
      }
      epoch = latest + 1;
    }
    try (PreparedStatement insert = connection.prepareStatement(INSERT)) {
      insert.setLong(1, offset);
      insert.setString(2, user);
      insert.setString(3, jti);
      insert.setLong(4, epoch);
      try (ResultSet row = insert.executeQuery()) {
        if (!row.next()) {
          throw new SQLException();
        }
      }
    }
    return new Appended(offset, epoch);
  }

  @Override
  public Snapshot read(long from, int limit) {
    if (from < 0 || limit < 1 || limit > MAX_LIMIT) {
      throw new IllegalArgumentException();
    }
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      connection.setReadOnly(true);
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      try {
        Snapshot snapshot = readIn(connection, from, limit);
        connection.commit();
        return snapshot;
      } catch (CursorAhead ahead) {
        rollbackQuietly(connection);
        throw ahead;
      } catch (SQLException | RuntimeException failure) {
        rollbackQuietly(connection);
        throw new Unavailable();
      }
    } catch (SQLException failure) {
      throw new Unavailable();
    }
  }

  private static Snapshot readIn(Connection connection, long from, int limit) throws SQLException {
    long highWater;
    try (PreparedStatement statement = connection.prepareStatement(HIGH_WATER);
        ResultSet row = statement.executeQuery()) {
      if (!row.next()) {
        throw new SQLException();
      }
      highWater = row.getLong(1);
    }
    if (from > highWater) {
      throw new CursorAhead();
    }
    List<RevocationState.Event> events = new ArrayList<>(Math.min(limit, 64));
    Instant newest = Instant.EPOCH;
    try (PreparedStatement statement = connection.prepareStatement(PAGE)) {
      statement.setLong(1, from);
      statement.setInt(2, limit);
      try (ResultSet rows = statement.executeQuery()) {
        while (rows.next()) {
          Instant committedAt =
              rows.getObject(5, OffsetDateTime.class).toInstant().truncatedTo(ChronoUnit.MICROS);
          events.add(
              new RevocationState.Event(
                  RevocationLog.UNSCOPED_ISSUER,
                  new UserId(rows.getString(2)),
                  rows.getString(3),
                  rows.getLong(4),
                  rows.getLong(1),
                  committedAt));
          if (committedAt.isAfter(newest)) {
            newest = committedAt;
          }
        }
      }
    }
    return new Snapshot(events, highWater, newest);
  }

  private static void rollbackQuietly(Connection connection) {
    try {
      connection.rollback();
    } catch (SQLException ignored) {
      // The connection is discarded by the caller's close; the original failure is what matters.
    }
  }
}
