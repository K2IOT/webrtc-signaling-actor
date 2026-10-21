package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.webrtc.signaling.auth.RevocationState;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class JdbcRevocationLogIT {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6");

  private static final AtomicInteger DATABASES = new AtomicInteger();

  private DataSource dataSource;
  private JdbcRevocationLog log;

  @BeforeEach
  void freshDatabase() throws SQLException {
    String name = "revocation_" + DATABASES.incrementAndGet();
    try (Connection admin = source(POSTGRES.getDatabaseName()).getConnection();
        Statement statement = admin.createStatement()) {
      statement.execute("CREATE DATABASE " + name);
    }
    dataSource = source(name);
    JdbcRevocationLog.migrate(dataSource);
    log = new JdbcRevocationLog(dataSource);
  }

  private static PGSimpleDataSource source(String database) {
    PGSimpleDataSource source = new PGSimpleDataSource();
    source.setServerNames(new String[] {POSTGRES.getHost()});
    source.setPortNumbers(new int[] {POSTGRES.getFirstMappedPort()});
    source.setDatabaseName(database);
    source.setUser(POSTGRES.getUsername());
    source.setPassword(POSTGRES.getPassword());
    return source;
  }

  @Test
  void firstAppendIsOffsetOne() {
    RevocationLog.Appended appended = log.append("alice", null, OptionalLong.empty());

    assertThat(appended).isEqualTo(new RevocationLog.Appended(1, 0));
  }

  @Test
  void defaultEpochIsZeroThenIncrements() {
    assertThat(log.append("alice", null, OptionalLong.empty()).epoch()).isZero();
    assertThat(log.append("alice", null, OptionalLong.empty()).epoch()).isEqualTo(1);
    assertThat(log.append("alice", null, OptionalLong.empty()).epoch()).isEqualTo(2);
    // A (user, jti) subject has its own epoch line, independent of the user-scope one.
    assertThat(log.append("alice", "jti-1", OptionalLong.empty()).epoch()).isZero();
    assertThat(log.append("alice", "jti-1", OptionalLong.empty()).epoch()).isEqualTo(1);
    assertThat(log.append("alice", "jti-2", OptionalLong.empty()).epoch()).isZero();
    assertThat(log.append("bob", null, OptionalLong.empty()).epoch()).isZero();
  }

  @Test
  void explicitLowerEpochConflicts() {
    log.append("alice", null, OptionalLong.empty());
    log.append("alice", null, OptionalLong.of(5));

    assertThatThrownBy(() -> log.append("alice", null, OptionalLong.of(5)))
        .isInstanceOf(RevocationLog.EpochConflict.class);
    assertThatThrownBy(() -> log.append("alice", null, OptionalLong.of(0)))
        .isInstanceOf(RevocationLog.EpochConflict.class);
    // The jti subject is separate, so an explicit epoch of 0 is fine there.
    assertThat(log.append("alice", "jti-1", OptionalLong.of(0)).epoch()).isZero();
    // Offsets 1, 2 and 3 are taken; the two conflicts rolled back and did not burn 4 or 5.
    assertThat(log.append("alice", null, OptionalLong.of(6)).offset()).isEqualTo(4);
    assertThat(log.read(0, 16).currentHighWater()).isEqualTo(4);
  }

  @Test
  void invalidInputsAreRejectedWithoutTouchingTheLog() {
    assertThatThrownBy(() -> log.append("", null, OptionalLong.empty()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> log.append(" alice", null, OptionalLong.empty()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> log.append("alice", "", OptionalLong.empty()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> log.append("alice", null, OptionalLong.of(-1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> log.append(null, null, OptionalLong.empty()))
        .isInstanceOf(NullPointerException.class);
    assertThat(log.read(0, 16).currentHighWater()).isZero();
  }

  @Test
  void userIdIsStoredNfcNormalisedAndCaseSensitive() {
    log.append("é", null, OptionalLong.empty());
    log.append("é", null, OptionalLong.empty());
    log.append("Alice", null, OptionalLong.empty());
    log.append("alice", null, OptionalLong.empty());

    List<RevocationState.Event> events = log.read(0, 16).events();

    assertThat(events.get(0).userId().value()).isEqualTo("é");
    // The decomposed and composed spellings are the same subject: the second append is epoch 1.
    assertThat(events.get(1).epoch()).isEqualTo(1);
    assertThat(events.get(2).epoch()).isZero();
    assertThat(events.get(3).epoch()).isZero();
  }

  @Test
  void updateAndDeleteAreRejectedByTrigger() throws SQLException {
    log.append("alice", null, OptionalLong.empty());

    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      assertThatThrownBy(() -> statement.executeUpdate("UPDATE revocation_log SET epoch = 9"))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(() -> statement.executeUpdate("DELETE FROM revocation_log"))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(() -> statement.execute("TRUNCATE revocation_log"))
          .isInstanceOf(SQLException.class);
    }
    assertThat(log.read(0, 16).events()).hasSize(1);
    assertThat(log.read(0, 16).events().get(0).epoch()).isZero();
  }

  @Test
  void highWaterCanOnlyAdvanceByOne() throws SQLException {
    log.append("alice", null, OptionalLong.empty());

    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      assertThatThrownBy(() -> statement.executeUpdate("UPDATE source_state SET high_water = 0"))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(() -> statement.executeUpdate("UPDATE source_state SET high_water = 9"))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(() -> statement.executeUpdate("DELETE FROM source_state"))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(
              () -> statement.executeUpdate("INSERT INTO source_state (singleton) VALUES (false)"))
          .isInstanceOf(SQLException.class);
    }
    assertThat(log.read(0, 16).currentHighWater()).isEqualTo(1);
  }

  @Test
  void migrationIsIdempotentAndUsesItsOwnHistoryTable() throws SQLException {
    JdbcRevocationLog.migrate(dataSource);

    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet tables =
            statement.executeQuery(
                "SELECT count(*) FROM information_schema.tables "
                    + "WHERE table_name = 'source_schema_history'")) {
      tables.next();
      assertThat(tables.getInt(1)).isEqualTo(1);
    }
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet tables =
            statement.executeQuery(
                "SELECT count(*) FROM information_schema.tables "
                    + "WHERE table_name = 'flyway_schema_history'")) {
      tables.next();
      assertThat(tables.getInt(1)).isZero();
    }
  }

  @Test
  void concurrentAppendsAreGaplessAndOrdered() throws Exception {
    int threads = 32;
    int perThread = 10;
    int total = threads * perThread;
    ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
    CountDownLatch start = new CountDownLatch(1);
    AtomicBoolean writersDone = new AtomicBoolean();
    try {
      List<Future<List<Long>>> writers = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        int thread = t;
        writers.add(
            pool.submit(
                () -> {
                  start.await();
                  List<Long> offsets = new ArrayList<>();
                  for (int i = 0; i < perThread; i++) {
                    // A mix of subjects, so default epochs are exercised under contention.
                    String user = "user-" + (thread % 4);
                    offsets.add(log.append(user, null, OptionalLong.empty()).offset());
                  }
                  return offsets;
                }));
      }
      // The reader follows the log while it is being written. Every snapshot it takes must be
      // contiguous from its cursor, and a page that is not full must end exactly at the high
      // water of the same snapshot, so an offset can never be seen ahead of a gap.
      Future<Long> reader =
          pool.submit(
              () -> {
                long cursor = 0;
                while (true) {
                  boolean finishing = writersDone.get();
                  RevocationLog.Snapshot snapshot = log.read(cursor, 16);
                  long expected = cursor;
                  for (RevocationState.Event event : snapshot.events()) {
                    expected++;
                    assertThat(event.sourceOffset()).isEqualTo(expected);
                  }
                  assertThat(snapshot.events().size()).isLessThanOrEqualTo(16);
                  assertThat(snapshot.currentHighWater()).isGreaterThanOrEqualTo(expected);
                  if (snapshot.events().size() < 16) {
                    assertThat(expected).isEqualTo(snapshot.currentHighWater());
                  }
                  cursor = expected;
                  if (finishing && cursor == total) {
                    return cursor;
                  }
                  if (snapshot.events().isEmpty()) {
                    Thread.sleep(1);
                  }
                }
              });
      start.countDown();
      List<Long> all = new ArrayList<>();
      for (Future<List<Long>> writer : writers) {
        List<Long> offsets = writer.get(120, TimeUnit.SECONDS);
        // Each writer sees strictly increasing offsets.
        for (int i = 1; i < offsets.size(); i++) {
          assertThat(offsets.get(i)).isGreaterThan(offsets.get(i - 1));
        }
        all.addAll(offsets);
      }
      writersDone.set(true);
      assertThat(reader.get(120, TimeUnit.SECONDS)).isEqualTo(total);
      assertThat(all).hasSize(total).doesNotHaveDuplicates();
      assertThat(all.stream().mapToLong(Long::longValue).min().orElseThrow()).isEqualTo(1);
      assertThat(all.stream().mapToLong(Long::longValue).max().orElseThrow()).isEqualTo(total);
    } finally {
      pool.shutdownNow();
    }

    // Final state: offsets exactly 1..320 in order, epochs per user are 0..79 in offset order,
    // and committed_at never goes backwards along the offsets.
    List<RevocationState.Event> events = new ArrayList<>();
    long cursor = 0;
    while (cursor < total) {
      RevocationLog.Snapshot page = log.read(cursor, 16);
      events.addAll(page.events());
      cursor = page.events().get(page.events().size() - 1).sourceOffset();
    }
    assertThat(events).hasSize(total);
    java.util.Map<String, Long> lastEpoch = new java.util.HashMap<>();
    Instant previous = Instant.MIN;
    for (int i = 0; i < total; i++) {
      RevocationState.Event event = events.get(i);
      assertThat(event.sourceOffset()).isEqualTo(i + 1L);
      assertThat(event.committedAt()).isAfterOrEqualTo(previous);
      previous = event.committedAt();
      long expectedEpoch = lastEpoch.merge(event.userId().value(), 0L, (a, b) -> a + 1);
      assertThat(event.epoch()).isEqualTo(expectedEpoch);
    }
  }

  @Test
  void readReturnsAtMostLimitAndReportsCurrentHighWater() {
    for (int i = 0; i < 20; i++) {
      log.append("user-" + i, null, OptionalLong.empty());
    }

    RevocationLog.Snapshot first = log.read(0, 16);
    RevocationLog.Snapshot second = log.read(16, 16);
    RevocationLog.Snapshot caughtUp = log.read(20, 16);
    RevocationLog.Snapshot small = log.read(3, 2);

    assertThat(first.events()).hasSize(16);
    assertThat(first.events().get(0).sourceOffset()).isEqualTo(1);
    assertThat(first.events().get(15).sourceOffset()).isEqualTo(16);
    assertThat(first.currentHighWater()).isEqualTo(20);
    assertThat(second.events()).hasSize(4);
    assertThat(second.events().get(0).sourceOffset()).isEqualTo(17);
    assertThat(second.currentHighWater()).isEqualTo(20);
    assertThat(caughtUp.events()).isEmpty();
    assertThat(caughtUp.currentHighWater()).isEqualTo(20);
    assertThat(small.events())
        .extracting(RevocationState.Event::sourceOffset)
        .containsExactly(4L, 5L);
    assertThat(first.newestCommittedAt()).isEqualTo(first.events().get(15).committedAt());
  }

  @Test
  void cursorAheadIsReported() {
    assertThatThrownBy(() -> log.read(1, 16)).isInstanceOf(RevocationLog.CursorAhead.class);
    log.append("alice", null, OptionalLong.empty());
    assertThat(log.read(1, 16).events()).isEmpty();
    assertThatThrownBy(() -> log.read(2, 16)).isInstanceOf(RevocationLog.CursorAhead.class);
    assertThatThrownBy(() -> log.read(Long.MAX_VALUE, 16))
        .isInstanceOf(RevocationLog.CursorAhead.class);
  }

  @Test
  void invalidReadArgumentsAreRejected() {
    assertThatThrownBy(() -> log.read(-1, 16)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> log.read(0, 0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> log.read(0, -3)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void committedAtHasMicrosecondPrecision() throws SQLException {
    for (int i = 0; i < 10; i++) {
      log.append("user-" + i, null, OptionalLong.empty());
    }

    List<RevocationState.Event> events = log.read(0, 16).events();
    List<RevocationState.Event> again = log.read(0, 16).events();

    assertThat(again).isEqualTo(events);
    for (RevocationState.Event event : events) {
      Instant at = event.committedAt();
      assertThat(at).isEqualTo(at.truncatedTo(ChronoUnit.MICROS));
      assertThat(at.getNano() % 1000).isZero();
    }
    // Sub-second precision is really stored (not rounded to a coarser unit).
    assertThat(events).anyMatch(e -> e.committedAt().getNano() % 1_000_000 != 0);
    // The stored value, read back as raw microseconds, is exactly what a page would sign.
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery(
                "SELECT source_offset, (extract(epoch FROM committed_at) * 1000000)::bigint "
                    + "FROM revocation_log ORDER BY source_offset")) {
      int index = 0;
      while (rows.next()) {
        Instant at = events.get(index++).committedAt();
        long micros = at.getEpochSecond() * 1_000_000L + at.getNano() / 1000;
        assertThat(rows.getLong(2)).isEqualTo(micros);
      }
      assertThat(index).isEqualTo(10);
    }
  }

  @Test
  void unreachableDatabaseIsReportedAsDetailFreeUnavailable() {
    PGSimpleDataSource dead = new PGSimpleDataSource();
    dead.setServerNames(new String[] {"127.0.0.1"});
    dead.setPortNumbers(new int[] {1});
    dead.setDatabaseName("secret_database_name");
    dead.setUser("secret_user");
    dead.setPassword("secret-password");
    dead.setConnectTimeout(2);
    JdbcRevocationLog broken = new JdbcRevocationLog(dead);

    for (Runnable call :
        List.<Runnable>of(
            () -> broken.append("alice", null, OptionalLong.empty()), () -> broken.read(0, 16))) {
      assertThatThrownBy(call::run)
          .isInstanceOf(RevocationLog.Unavailable.class)
          .hasMessage(null)
          .hasNoCause()
          .satisfies(e -> assertThat(e.getSuppressed()).isEmpty());
    }
  }
}
