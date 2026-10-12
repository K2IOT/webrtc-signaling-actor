package io.webrtc.signaling.storage.worker;

import io.webrtc.signaling.storage.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Indexed fair due pages are routing hints; every delete reacquires its exact native authority. */
public final class RetentionWorker {
  public record Report(
      int commandResults, int terminalCalls, int homeHistory, int outbox, int transferCopies) {
    public Report(int commandResults, int terminalCalls, int homeHistory, int outbox) {
      this(commandResults, terminalCalls, homeHistory, outbox, 0);
    }

    public int deleted() {
      return commandResults + terminalCalls + homeHistory + outbox + transferCopies;
    }

    private Report plus(Report other) {
      return new Report(
          commandResults + other.commandResults,
          terminalCalls + other.terminalCalls,
          homeHistory + other.homeHistory,
          outbox + other.outbox,
          transferCopies + other.transferCopies);
    }
  }

  private enum Kind {
    COMMAND(
        "command_result r",
        "r.expires_at",
        "r.authority_bucket_id",
        "r.status='FINAL'",
        List.of("r.issuer", "r.jti", "r.command_scope", "r.request_id"),
        List.of("text", "text", "text", "uuid")),
    CALL(
        "call_state r",
        "r.expires_at",
        "r.authority_bucket_id",
        "r.terminal_at IS NOT NULL",
        List.of("r.call_id"),
        List.of("text")),
    HOME(
        "home_participation r",
        "r.expires_at",
        "native_authority_bucket(r.user_id)",
        "r.terminal_at IS NOT NULL",
        List.of("r.call_id", "r.user_id"),
        List.of("text", "text")),
    OUTBOX(
        "control_outbox r",
        "r.expires_at",
        "r.authority_bucket_id",
        "true",
        List.of("r.event_id"),
        List.of("uuid")),
    COPY(
        "bucket_transfer_item r JOIN bucket_transfer t ON t.transfer_id=r.transfer_id",
        "t.updated_at",
        "t.bucket_id",
        "t.state='PUBLISHED' AND octet_length(r.row_key)<=8192",
        List.of("r.transfer_id", "r.dataset", "r.row_key"),
        List.of("uuid", "integer", "text"));
    final String source, time, bucket, predicate;
    final List<String> keys, types;

    Kind(
        String source,
        String time,
        String bucket,
        String predicate,
        List<String> keys,
        List<String> types) {
      this.source = source;
      this.time = time;
      this.bucket = bucket;
      this.predicate = predicate;
      this.keys = keys;
      this.types = types;
    }

    String tuple() {
      return "(" + time + "," + String.join(",", keys) + ")";
    }

    String order() {
      return time + "," + String.join(",", keys);
    }

    String parameterTuple() {
      return "(?::timestamptz,"
          + String.join(",", types.stream().map(t -> "?::" + t).toList())
          + ")";
    }
  }

  private record Cursor(Instant ceiling, Instant after, List<String> key) {}

  private record Candidate(Kind kind, Instant time, int bucket, List<String> key) {}

  private record HintPage(Kind kind, Instant ceiling, List<Candidate> candidates, boolean more) {}

  private static final Report EMPTY = new Report(0, 0, 0, 0, 0);

  private record Retired(Report report, int touched, long bytes) {
    private Retired plus(Retired other) {
      return new Retired(report.plus(other.report), touched + other.touched, bytes + other.bytes);
    }
  }

  private static final Retired NO_RETIREMENT = new Retired(EMPTY, 0, 0);
  private static final int MAX_ROWS = 512, MAX_BUCKETS = 16;
  private static final long MAX_BYTES = 1024 * 1024;
  private final SqlTransactions sql;
  private final String cell;
  private final long epoch;
  private final Map<Kind, Cursor> cursors = new EnumMap<>(Kind.class);
  private final AtomicBoolean busy = new AtomicBoolean();
  private int bucketTurn, kindTurn;

  public RetentionWorker(SqlTransactions sql, String cell, long epoch) {
    this.sql = Objects.requireNonNull(sql);
    if (cell == null || !cell.matches("[a-z][a-z0-9-]{0,23}") || epoch < 1)
      throw new IllegalArgumentException("Invalid retirement authority");
    this.cell = cell;
    this.epoch = epoch;
  }

  /** Native global scan: per-kind quotas, stable horizon/cursors and bounded sorted bucket work. */
  public DbOperation<Report> pruneDue(int limit) {
    return pruneDue(limit, Duration.ofSeconds(2));
  }

  public DbOperation<Report> pruneDue(int limit, Duration budget) {
    return scan(null, limit, budget);
  }

  /** Compatibility caller retains its exact bucket;512 is retirement's limit, not outbox's128. */
  public DbOperation<Report> prune(int bucket, int limit) {
    if (bucket < 0 || bucket > 16383)
      throw new IllegalArgumentException("Invalid retention bucket");
    return scan(bucket, limit, Duration.ofSeconds(2));
  }

  private DbOperation<Report> scan(Integer onlyBucket, int limit, Duration budget) {
    WorkerFence.limit(limit, MAX_ROWS);
    if (budget == null || budget.isNegative() || budget.isZero())
      throw new IllegalArgumentException("Invalid retention budget");
    long end = System.nanoTime() + Math.min(budget.toNanos(), Duration.ofSeconds(2).toNanos());
    if (!busy.compareAndSet(false, true))
      return new DbOperation<>(
          CompletableFuture.failedFuture(new DbOverloadedException()),
          CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.NOT_STARTED));
    var completion = new WorkerCompletion();
    CompletionStage<Report> logical;
    try {
      logical =
          completion
              .track(
                  sql.submitTracked(
                      DbClass.MAINTENANCE,
                      remaining(end),
                      sql.boundedCharge(DbClass.MAINTENANCE, 1024, MAX_BYTES),
                      c -> {
                        WorkerFence.cell(c, cell, epoch);
                        var pages = new ArrayList<HintPage>();
                        int quota = Math.max(1, Math.min(64, limit / 5));
                        int remainingRows = limit;
                        long remainingBytes = MAX_BYTES;
                        var kinds = new ArrayList<>(List.of(Kind.values()));
                        // Even a one-row invocation gives every dataset a discovery turn across
                        // invocations.
                        Collections.rotate(kinds, -Math.floorMod(kindTurn++, kinds.size()));
                        for (Kind kind : kinds) {
                          int count =
                              Math.min(
                                  kind == Kind.CALL ? Math.min(15, quota) : quota, remainingRows);
                          if (count == 0) continue;
                          var hints = hints(c, kind, onlyBucket, count);
                          var candidates = new ArrayList<Candidate>();
                          for (var candidate : hints.candidates()) {
                            // Includes all immutable projection references and conservative
                            // workflow child keys.
                            long bytes = bytes(candidate) + (kind == Kind.CALL ? 16L * 160 : 0);
                            if (bytes > remainingBytes) break;
                            candidates.add(candidate);
                            remainingRows--;
                            remainingBytes -= bytes;
                          }
                          pages.add(
                              new HintPage(
                                  kind,
                                  hints.ceiling(),
                                  List.copyOf(candidates),
                                  hints.more() || candidates.size() < hints.candidates().size()));
                        }
                        return select(pages, onlyBucket == null);
                      }))
              .thenCompose(
                  page -> {
                    var buckets = new TreeMap<Integer, List<Candidate>>();
                    // Selection consumed a contiguous prefix per kind, at most16 buckets. Sort
                    // native work.
                    for (var candidate : page)
                      buckets
                          .computeIfAbsent(candidate.bucket(), ignored -> new ArrayList<>())
                          .add(candidate);
                    var work = new ArrayList<>(buckets.entrySet());
                    // Each transaction owns one bucket and follows100→101. Rotate transactions
                    // across
                    // invocations so a tight original budget cannot repeatedly strand the same
                    // sorted suffix.
                    if (!work.isEmpty())
                      Collections.rotate(work, -Math.floorMod(bucketTurn++, work.size()));
                    CompletionStage<Retired> chain =
                        CompletableFuture.completedFuture(NO_RETIREMENT);
                    for (var entry : work) {
                      var candidates = List.copyOf(entry.getValue());
                      chain =
                          chain.thenCompose(
                              total -> {
                                if (total.touched() >= limit || total.bytes() >= MAX_BYTES)
                                  return CompletableFuture.completedFuture(total);
                                return completion
                                    .track(
                                        sql.submitTracked(
                                            DbClass.MAINTENANCE,
                                            remaining(end),
                                            sql.boundedCharge(DbClass.MAINTENANCE, MAX_BYTES, 1024),
                                            c -> {
                                              WorkerFence.cell(c, cell, epoch);
                                              WorkerFence.cleanupBucket(c, entry.getKey());
                                              return retire(
                                                  c,
                                                  candidates,
                                                  limit - total.touched(),
                                                  MAX_BYTES - total.bytes());
                                            }))
                                    .handle(
                                        (report, failure) -> {
                                          Throwable cause = failure;
                                          while (cause instanceof CompletionException
                                              && cause.getCause() != null) cause = cause.getCause();
                                          // Contention/frozen prefixes advance their hint cursor.
                                          // The failed transaction
                                          // retains physical credit and rolls back all its barriers
                                          // before any next bucket.
                                          if (cause instanceof AuthoritySql.FencedException
                                              || cause instanceof AuthoritySql.RetryableConflict)
                                            return total;
                                          if (failure != null)
                                            throw new CompletionException(failure);
                                          return total.plus(report);
                                        });
                              });
                    }
                    return chain.thenApply(Retired::report);
                  });
    } catch (RuntimeException failure) {
      logical = CompletableFuture.failedFuture(failure);
    }
    return completion.finishTracked(logical, () -> busy.set(false));
  }

  private List<Candidate> select(List<HintPage> pages, boolean advance) {
    var queues = new EnumMap<Kind, ArrayDeque<Candidate>>(Kind.class);
    var consumed = new EnumMap<Kind, Candidate>(Kind.class);
    for (var page : pages) {
      queues.put(page.kind(), new ArrayDeque<>(page.candidates()));
    }
    var buckets = new HashSet<Integer>();
    var selected = new ArrayList<Candidate>();
    boolean progressed;
    do {
      progressed = false;
      // Every nonempty kind gets a turn before another distinct bucket from that kind.
      for (var page : pages) {
        Kind kind = page.kind();
        var queue = queues.get(kind);
        if (queue == null || queue.isEmpty()) continue;
        var next = queue.getFirst();
        if (buckets.size() == MAX_BUCKETS && !buckets.contains(next.bucket())) continue;
        queue.removeFirst();
        buckets.add(next.bucket());
        selected.add(next);
        consumed.put(kind, next);
        progressed = true;
      }
    } while (progressed);
    if (advance) {
      for (var page : pages) {
        var last = consumed.get(page.kind());
        if (queues.get(page.kind()).isEmpty() && !page.more())
          cursors.put(page.kind(), new Cursor(null, null, List.of()));
        else if (last != null)
          cursors.put(page.kind(), new Cursor(page.ceiling(), last.time(), last.key()));
        // An unselected suffix is never skipped permanently. Frozen/busy consumed prefixes
        // advance without granting authority, and exhausted horizons fairly wrap.
      }
    }
    return List.copyOf(selected);
  }

  private HintPage hints(Connection c, Kind kind, Integer onlyBucket, int limit)
      throws SQLException {
    Cursor cursor = onlyBucket == null ? cursors.get(kind) : null;
    Instant ceiling = cursor == null ? null : cursor.ceiling();
    if (ceiling == null) {
      try (var q = c.createStatement();
          var r = q.executeQuery("SELECT clock_timestamp()")) {
        r.next();
        ceiling = r.getTimestamp(1).toInstant();
      }
    }
    boolean after = cursor != null && cursor.after() != null;
    String query =
        "SELECT "
            + kind.time
            + ","
            + kind.bucket
            + ","
            + String.join(",", kind.keys)
            + " FROM "
            + kind.source
            + " WHERE "
            + kind.predicate
            + " AND "
            + kind.time
            + "<=?"
            + (onlyBucket == null ? "" : " AND " + kind.bucket + "=?")
            + (after ? " AND " + kind.tuple() + ">" + kind.parameterTuple() : "")
            + " ORDER BY "
            + kind.order()
            + " LIMIT ?";
    var page = new ArrayList<Candidate>();
    try (var q = c.prepareStatement(query)) {
      int p = 1;
      q.setTimestamp(p++, Timestamp.from(ceiling));
      if (onlyBucket != null) q.setInt(p++, onlyBucket);
      if (after) {
        q.setTimestamp(p++, Timestamp.from(cursor.after()));
        for (String key : cursor.key()) q.setString(p++, key);
      }
      q.setInt(p, limit);
      try (var r = q.executeQuery()) {
        while (r.next()) {
          var keys = new ArrayList<String>();
          for (int i = 0; i < kind.keys.size(); i++) keys.add(r.getString(i + 3));
          page.add(
              new Candidate(kind, r.getTimestamp(1).toInstant(), r.getInt(2), List.copyOf(keys)));
        }
      }
    }
    return new HintPage(kind, ceiling, List.copyOf(page), page.size() == limit);
  }

  private static long bytes(Candidate candidate) {
    long bytes = 128;
    for (String key : candidate.key())
      bytes =
          Math.addExact(bytes, 16L + key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
    return bytes;
  }

  private Retired retire(Connection c, List<Candidate> candidates, int rowBudget, long byteBudget)
      throws SQLException {
    int results = 0, calls = 0, history = 0, outbox = 0, copies = 0, touched = 0;
    long projectionBytes = 0;
    for (var candidate : candidates) {
      long bytes = bytes(candidate);
      if (touched >= rowBudget || bytes > byteBudget - projectionBytes) break;
      var deleted = delete(c, candidate, rowBudget - touched, byteBudget - projectionBytes - bytes);
      touched += deleted.rows() + deleted.cascades();
      projectionBytes += bytes(candidate) + deleted.cascadeBytes();
      switch (candidate.kind()) {
        case COMMAND -> results += deleted.rows();
        case CALL -> calls += deleted.rows();
        case HOME -> history += deleted.rows();
        case OUTBOX -> outbox += deleted.rows();
        case COPY -> copies += deleted.rows();
      }
    }
    return new Retired(
        new Report(results, calls, history, outbox, copies), touched, projectionBytes);
  }

  private record Deleted(int rows, int cascades, long cascadeBytes) {}

  private static Duration remaining(long end) {
    long nanos = end - System.nanoTime();
    if (nanos <= 0) throw new DbOverloadedException();
    return Duration.ofNanos(nanos);
  }

  private static Deleted delete(
      Connection c, Candidate candidate, int rowBudget, long cascadeByteBudget)
      throws SQLException {
    Kind kind = candidate.kind();
    String table =
        switch (kind) {
          case COMMAND -> "command_result";
          case CALL -> "call_state";
          case HOME -> "home_participation";
          case OUTBOX -> "control_outbox";
          case COPY -> "bucket_transfer_item";
        };
    int cascades = 0;
    long cascadeBytes = 0;
    if (kind == Kind.CALL) {
      // Protect the parent before the fresh child count. A prior statement snapshot must not
      // underestimate cascades from an FK insertion that committed before this lock was acquired.
      try (var lock =
          c.prepareStatement(
              "SELECT call_id FROM call_state WHERE call_id=? AND authority_bucket_id=? FOR UPDATE SKIP LOCKED")) {
        lock.setString(1, candidate.key().getFirst());
        lock.setInt(2, candidate.bucket());
        try (var row = lock.executeQuery()) {
          if (!row.next()) return new Deleted(0, 0, 0);
        }
      }
      try (var children =
          c.prepareStatement(
              "SELECT count(*),coalesce(sum(octet_length(call_id)+16+32),0) FROM call_workflow_step WHERE call_id=?")) {
        children.setString(1, candidate.key().getFirst());
        try (var row = children.executeQuery()) {
          row.next();
          long count = row.getLong(1);
          if (count > 16) return new Deleted(0, 0, 0);
          cascades = (int) count;
          cascadeBytes = row.getLong(2);
          if (1L + cascades > rowBudget || cascadeBytes > cascadeByteBudget)
            return new Deleted(0, 0, 0);
        }
      }
    }
    var predicates = new ArrayList<String>();
    for (int i = 0; i < kind.keys.size(); i++)
      predicates.add(kind.keys.get(i) + "=?::" + kind.types.get(i));
    String eligible = kind.predicate + " AND " + kind.time + "<clock_timestamp()";
    if (kind == Kind.COMMAND)
      eligible +=
          " AND NOT EXISTS (SELECT 1 FROM call_state s WHERE s.call_id=r.call_id AND (s.terminal_at IS NULL OR s.expires_at>=clock_timestamp()))";
    if (kind == Kind.CALL)
      eligible +=
          " AND NOT EXISTS (SELECT 1 FROM command_result s WHERE s.call_id=r.call_id) AND (SELECT count(*) FROM call_workflow_step w WHERE w.call_id=r.call_id)<=16";
    if (kind == Kind.COPY)
      eligible +=
          " AND native_transfer_published(t.bucket_id) AND EXISTS(SELECT 1 FROM bucket_authority a WHERE a.bucket_id=t.bucket_id AND a.transfer_id=r.transfer_id)";
    // This is a fresh statement after native100/101, using the complete immutable identity.
    // tableoid prevents ctid collisions between command_result hash partitions.
    String query =
        "WITH due AS (SELECT r.tableoid,r.ctid FROM "
            + kind.source
            + " WHERE "
            + String.join(" AND ", predicates)
            + " AND "
            + kind.bucket
            + "=? AND "
            + eligible
            + " FOR UPDATE OF r SKIP LOCKED) DELETE FROM "
            + table
            + " d USING due h WHERE d.tableoid=h.tableoid AND d.ctid=h.ctid";
    try (var q = c.prepareStatement(query)) {
      int p = 1;
      for (String key : candidate.key()) q.setString(p++, key);
      q.setInt(p, candidate.bucket());
      int rows = q.executeUpdate();
      return new Deleted(rows, rows == 0 ? 0 : cascades, rows == 0 ? 0 : cascadeBytes);
    }
  }
}
