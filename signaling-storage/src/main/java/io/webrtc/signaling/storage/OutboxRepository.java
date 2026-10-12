package io.webrtc.signaling.storage;

import io.webrtc.signaling.protocol.Identity.CallId;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class OutboxRepository {
  public record Claim(
      UUID eventId,
      int bucket,
      CallId callId,
      long version,
      long generation,
      String destination,
      String payloadBase64,
      Instant dispatchUntil) {}

  public record ClaimBatch(List<Claim> claims, boolean pending) {
    public ClaimBatch {
      claims = List.copyOf(claims);
    }
  }

  private final SqlTransactions sql;
  private final String cell;
  private final long epoch;

  private record Cursor(Instant ceiling, Instant after, UUID eventId) {}

  private enum Queue {
    PENDING(
        "next_attempt",
        "delivery_state='PENDING' AND quarantined=false AND expires_at>clock_timestamp()"),
    DISPATCH("dispatch_until", "delivery_state='INFLIGHT'"),
    EXPIRY("expires_at", "quarantined=false");

    final String time;
    final String predicate;

    Queue(String time, String predicate) {
      this.time = time;
      this.predicate = predicate;
    }
  }

  private final AtomicReference<Cursor> pendingCursor =
      new AtomicReference<>(new Cursor(null, null, null));
  private final AtomicReference<Cursor> dispatchCursor =
      new AtomicReference<>(new Cursor(null, null, null));
  private final AtomicReference<Cursor> expiryCursor =
      new AtomicReference<>(new Cursor(null, null, null));
  private final AtomicInteger claimTurn = new AtomicInteger();
  private final AtomicInteger reclaimTurn = new AtomicInteger();

  public OutboxRepository(SqlTransactions sql, String cell, long epoch) {
    this.sql = sql;
    this.cell = cell;
    this.epoch = epoch;
  }

  public UUID insert(
      Connection c, int bucket, CallId call, long version, String destination, String payload)
      throws SQLException {
    UUID event = UUID.randomUUID();
    byte[] bytes = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    if (bytes.length > 8192) throw new IllegalArgumentException("Outbox payload exceeds bound");
    try (var s =
        c.prepareStatement(
            "INSERT INTO"
                + " control_outbox(event_id,authority_bucket_id,call_id,version,destination,payload,delivery_state,next_attempt,expires_at)"
                + " VALUES(?,?,?,?,?::jsonb,?,'PENDING',clock_timestamp(),clock_timestamp()+interval"
                + " '24 hours')")) {
      s.setObject(1, event);
      s.setInt(2, bucket);
      s.setString(3, call.value());
      s.setLong(4, version);
      s.setString(5, destination);
      s.setBytes(6, bytes);
      s.executeUpdate();
      if (org.springframework.transaction.support.TransactionSynchronizationManager
          .isSynchronizationActive()) {
        org.springframework.transaction.support.TransactionSynchronizationManager
            .registerSynchronization(
                new org.springframework.transaction.support.TransactionSynchronization() {
                  @Override
                  public void afterCommit() {
                    sql.outboxHints().offer(event);
                  }
                });
      }
      return event;
    }
  }

  public CompletionStage<List<Claim>> claimOutboxBatch(String worker, UUID incarnation, int limit) {
    if (limit < 1 || limit > 128) throw new IllegalArgumentException("Invalid outbox claim bound");
    return claimOutboxBatchTracked(worker, incarnation, limit).logical();
  }

  public DbOperation<List<Claim>> claimOutboxBatchTracked(
      String worker, UUID incarnation, int limit) {
    var operation = scanOutboxBatchTracked(worker, incarnation, limit, List.of(), Set.of());
    return new DbOperation<>(
        operation.logical().thenApply(ClaimBatch::claims), operation.physicalCompletion());
  }

  public OutboxHints outboxHints() {
    return sql.outboxHints();
  }

  /**
   * Exclusions only suppress this process's still-owned physical delivery; no hint grants
   * authority.
   */
  public DbOperation<ClaimBatch> scanOutboxBatchTracked(
      String worker, UUID incarnation, int limit, List<UUID> preferred, Set<UUID> excluded) {
    if (limit < 1 || limit > 128) throw new IllegalArgumentException("Invalid outbox claim bound");
    if (preferred.size() > 128 || excluded.size() > 512)
      throw new IllegalArgumentException("Outbox scan hints exceed bound");
    var committedHints = List.copyOf(preferred);
    var retained = Set.copyOf(excluded);
    return sql.submitTracked(
        DbClass.OUTBOX,
        Duration.ofSeconds(2),
        c -> {
          AuthoritySql.cellBarrier(c, false);
          AuthoritySql.validateCell(c, cell, epoch);
          // Share one candidate budget. Alternate the odd slot, including legal limit=1,
          // so hints cannot displace durable repair and neither source is permanently excluded.
          boolean hintTurn = (claimTurn.getAndIncrement() & 1) != 0;
          int hintLimit = committedHints.isEmpty() ? 0 : limit / 2 + (hintTurn ? limit % 2 : 0);
          int repairLimit = limit - hintLimit;
          var hints =
              repairLimit == 0
                  ? new LinkedHashMap<UUID, Integer>()
                  : hints(c, Queue.PENDING, pendingCursor, repairLimit);
          if (hintLimit > 0) {
            var hintArray =
                c.createArrayOf(
                    "uuid", committedHints.stream().limit(hintLimit).toArray(UUID[]::new));
            try (var q =
                c.prepareStatement(
                    "SELECT event_id,authority_bucket_id FROM control_outbox WHERE event_id=ANY(?)"
                        + " AND delivery_state='PENDING' AND next_attempt<=clock_timestamp() AND"
                        + " quarantined=false AND expires_at>clock_timestamp()")) {
              q.setArray(1, hintArray);
              try (var r = q.executeQuery()) {
                while (r.next()) hints.put(r.getObject(1, UUID.class), r.getInt(2));
              }
            } finally {
              hintArray.free();
            }
          }
          boolean pending = !hints.isEmpty();
          if (!pending) {
            try (var q =
                    c.prepareStatement(
                        "SELECT EXISTS(SELECT 1 FROM control_outbox WHERE delivery_state='PENDING'"
                            + " AND quarantined=false AND expires_at>clock_timestamp())");
                var r = q.executeQuery()) {
              r.next();
              pending = r.getBoolean(1);
            }
          }
          hints.keySet().removeAll(retained);
          if (hints.isEmpty()) return new ClaimBatch(List.of(), pending);
          var active = activeBuckets(c, hints.values());
          UUID[] ids =
              hints.entrySet().stream()
                  .filter(e -> active.contains(e.getValue()))
                  .map(Map.Entry::getKey)
                  .toArray(UUID[]::new);
          if (ids.length == 0) return new ClaimBatch(List.of(), pending);
          java.sql.Array array = c.createArrayOf("uuid", ids);
          var result = new ArrayList<Claim>();
          try (var s =
              c.prepareStatement(
                  "WITH candidates AS (SELECT event_id FROM control_outbox WHERE"
                      + " delivery_state='PENDING' AND event_id=ANY(?) AND"
                      + " next_attempt<=clock_timestamp() AND expires_at>clock_timestamp() AND"
                      + " quarantined=false ORDER BY next_attempt,event_id"
                      + " LIMIT ? FOR UPDATE SKIP LOCKED) UPDATE"
                      + " control_outbox o SET"
                      + " delivery_state='INFLIGHT',dispatch_owner=?,dispatch_incarnation=?,dispatch_generation=o.dispatch_generation+1,dispatch_until=clock_timestamp()+interval"
                      + " '5 seconds' FROM candidates x WHERE o.event_id=x.event_id RETURNING"
                      + " o.event_id,o.authority_bucket_id,o.call_id,o.version,o.dispatch_generation,o.destination,o.payload,o.dispatch_until")) {
            s.setArray(1, array);
            s.setInt(2, limit);
            s.setString(3, worker);
            s.setObject(4, incarnation);
            try (var r = s.executeQuery()) {
              int bytes = 0;
              while (r.next()) {
                byte[] payload = r.getBytes(7);
                bytes = Math.addExact(bytes, payload.length);
                if (bytes > 1024 * 1024)
                  throw new IllegalArgumentException("Outbox batch exceeds byte bound");
                result.add(
                    new Claim(
                        r.getObject(1, UUID.class),
                        r.getInt(2),
                        new CallId(r.getString(3)),
                        r.getLong(4),
                        r.getLong(5),
                        r.getString(6),
                        Base64.getEncoder().encodeToString(payload),
                        r.getTimestamp(8).toInstant()));
              }
            }
          } finally {
            array.free();
          }
          return new ClaimBatch(result, pending);
        });
  }

  public CompletionStage<Boolean> complete(Claim claim, String worker, UUID incarnation) {
    return completeTracked(claim, worker, incarnation).logical();
  }

  public DbOperation<Boolean> completeTracked(Claim claim, String worker, UUID incarnation) {
    return sql.submitTracked(
        DbClass.OUTBOX,
        Duration.ofSeconds(2),
        c -> {
          queueAuthority(c, claim.bucket());
          try (var s =
              c.prepareStatement(
                  "UPDATE control_outbox SET delivery_state='DELIVERED' WHERE event_id=? AND"
                      + " authority_bucket_id=? AND delivery_state='INFLIGHT' AND dispatch_owner=?"
                      + " AND dispatch_incarnation=? AND dispatch_generation=? AND"
                      + " dispatch_until>clock_timestamp()")) {
            s.setObject(1, claim.eventId());
            s.setInt(2, claim.bucket());
            s.setString(3, worker);
            s.setObject(4, incarnation);
            s.setLong(5, claim.generation());
            return s.executeUpdate() == 1;
          }
        });
  }

  public DbOperation<Integer> reclaimExpired(int limit) {
    if (limit < 1 || limit > 128)
      throw new IllegalArgumentException("Invalid outbox recovery bound");
    return sql.submitTracked(
        DbClass.OUTBOX,
        Duration.ofSeconds(2),
        c -> {
          AuthoritySql.cellBarrier(c, false);
          AuthoritySql.validateCell(c, cell, epoch);
          boolean first = (reclaimTurn.getAndIncrement() & 1) != 0;
          Queue firstQueue = first ? Queue.EXPIRY : Queue.DISPATCH;
          Queue secondQueue = first ? Queue.DISPATCH : Queue.EXPIRY;
          var hints = hints(c, firstQueue, first ? expiryCursor : dispatchCursor, (limit + 1) / 2);
          if (limit > 1)
            hints.putAll(hints(c, secondQueue, first ? dispatchCursor : expiryCursor, limit / 2));
          var active = activeBuckets(c, hints.values());
          UUID[] ids =
              hints.entrySet().stream()
                  .filter(e -> active.contains(e.getValue()))
                  .map(Map.Entry::getKey)
                  .toArray(UUID[]::new);
          if (ids.length == 0) return 0;
          java.sql.Array array = c.createArrayOf("uuid", ids);
          try (var q =
              c.prepareStatement(
                  "WITH due AS (SELECT event_id FROM control_outbox WHERE event_id=ANY(?) AND"
                      + " ((delivery_state='INFLIGHT' AND dispatch_until<=clock_timestamp()) OR"
                      + " (expires_at<=clock_timestamp() AND quarantined=false)) ORDER BY event_id"
                      + " LIMIT ? FOR UPDATE SKIP LOCKED) UPDATE control_outbox o SET"
                      + " delivery_state='PENDING',dispatch_until=NULL,next_attempt=clock_timestamp(),quarantined=(o.expires_at<=clock_timestamp()),terminal_reason=CASE"
                      + " WHEN o.expires_at<=clock_timestamp() THEN 'EXPIRED_UNDELIVERABLE' ELSE"
                      + " NULL END FROM due d WHERE o.event_id=d.event_id")) {
            q.setArray(1, array);
            q.setInt(2, limit);
            return q.executeUpdate();
          } finally {
            array.free();
          }
        });
  }

  /**
   * Hints confer no authority. Advancing before barriers also progresses aborted/deferred pages.
   */
  private static LinkedHashMap<UUID, Integer> hints(
      Connection c, Queue queue, AtomicReference<Cursor> reference, int limit) throws SQLException {
    Cursor previous = reference.get();
    Cursor current = previous;
    if (current.ceiling() == null) {
      try (var q = c.prepareStatement("SELECT clock_timestamp()");
          var r = q.executeQuery()) {
        r.next();
        current = new Cursor(r.getTimestamp(1).toInstant(), null, null);
      }
    }
    var hints = new LinkedHashMap<UUID, Integer>();
    Cursor next = new Cursor(null, null, null);
    try (var q =
        c.prepareStatement(
            "SELECT event_id,authority_bucket_id,"
                + queue.time
                + " FROM control_outbox WHERE "
                + queue.predicate
                + " AND "
                + queue.time
                + "<=?"
                + (current.after() == null ? "" : " AND (" + queue.time + ",event_id)>(?,?)")
                + " ORDER BY "
                + queue.time
                + ",event_id LIMIT ?")) {
      int bind = 1;
      q.setTimestamp(bind++, Timestamp.from(current.ceiling()));
      if (current.after() != null) {
        q.setTimestamp(bind++, Timestamp.from(current.after()));
        q.setObject(bind++, current.eventId());
      }
      q.setInt(bind, limit);
      try (var r = q.executeQuery()) {
        while (r.next()) {
          UUID id = r.getObject(1, UUID.class);
          hints.put(id, r.getInt(2));
          next = new Cursor(current.ceiling(), r.getTimestamp(3).toInstant(), id);
        }
      }
    }
    // The fixed DB-time ceiling guarantees wrap even while new due work is continuously inserted.
    // Every wrap installs a fresh non-null state; identity CAS rejects older pages across wraps.
    reference.compareAndSet(previous, next);
    return hints;
  }

  private static Set<Integer> activeBuckets(Connection c, Collection<Integer> buckets)
      throws SQLException {
    var active = new HashSet<Integer>();
    for (int bucket : new TreeSet<>(buckets)) {
      // A try-lock failure must escape and roll back the partial barrier set (§46.2).
      AuthoritySql.bucketBarrier(c, bucket, false);
      try (var q = c.prepareStatement("SELECT status FROM bucket_authority WHERE bucket_id=?")) {
        q.setInt(1, bucket);
        try (var r = q.executeQuery()) {
          if (r.next() && "ACTIVE".equals(r.getString(1))) active.add(bucket);
        }
      }
    }
    return active;
  }

  public DbOperation<Boolean> defer(Claim claim, String worker, UUID incarnation, boolean poison) {
    return sql.submitTracked(
        DbClass.OUTBOX,
        Duration.ofSeconds(2),
        c -> {
          queueAuthority(c, claim.bucket());
          try (var q =
              c.prepareStatement(
                  "UPDATE control_outbox SET"
                      + " delivery_state='PENDING',dispatch_until=NULL,next_attempt=clock_timestamp()+interval"
                      + " '1 second',quarantined=?,terminal_reason=CASE WHEN ? THEN"
                      + " 'MALFORMED_EVENT' ELSE NULL END WHERE event_id=? AND dispatch_owner=? AND"
                      + " dispatch_incarnation=? AND dispatch_generation=? AND"
                      + " delivery_state='INFLIGHT' AND dispatch_until>clock_timestamp()")) {
            q.setBoolean(1, poison);
            q.setBoolean(2, poison);
            q.setObject(3, claim.eventId());
            q.setString(4, worker);
            q.setObject(5, incarnation);
            q.setLong(6, claim.generation());
            return q.executeUpdate() == 1;
          }
        });
  }

  void queueAuthority(Connection c, int bucket) throws SQLException {
    AuthoritySql.cellBarrier(c, false);
    AuthoritySql.validateCell(c, cell, epoch);
    AuthoritySql.bucketBarrier(c, bucket, false);
    try (var s = c.prepareStatement("SELECT status FROM bucket_authority WHERE bucket_id=?")) {
      s.setInt(1, bucket);
      try (var r = s.executeQuery()) {
        if (!r.next() || !"ACTIVE".equals(r.getString(1))) throw new AuthoritySql.FencedException();
      }
    }
  }
}
