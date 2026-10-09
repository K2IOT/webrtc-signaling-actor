package io.webrtc.signaling.storage;

import io.webrtc.signaling.protocol.Identity.CallId;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletionStage;

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

  private final SqlTransactions sql;
  private final String cell;
  private final long epoch;

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
            "INSERT INTO control_outbox(event_id,authority_bucket_id,call_id,version,destination,payload,delivery_state,next_attempt,expires_at) VALUES(?,?,?,?,?::jsonb,?,'PENDING',clock_timestamp(),clock_timestamp()+interval '24 hours')")) {
      s.setObject(1, event);
      s.setInt(2, bucket);
      s.setString(3, call.value());
      s.setLong(4, version);
      s.setString(5, destination);
      s.setBytes(6, bytes);
      s.executeUpdate();
      return event;
    }
  }

  public CompletionStage<List<Claim>> claimOutboxBatch(String worker, UUID incarnation, int limit) {
    if (limit < 1 || limit > 128) throw new IllegalArgumentException("Invalid outbox claim bound");
    return claimOutboxBatchTracked(worker, incarnation, limit).logical();
  }

  public DbOperation<List<Claim>> claimOutboxBatchTracked(
      String worker, UUID incarnation, int limit) {
    if (limit < 1 || limit > 128) throw new IllegalArgumentException("Invalid outbox claim bound");
    return sql.submitTracked(
        DbClass.OUTBOX,
        Duration.ofSeconds(2),
        c -> {
          AuthoritySql.cellBarrier(c, false);
          AuthoritySql.validateCell(c, cell, epoch);
          var hints = new LinkedHashMap<UUID, Integer>();
          try (var s =
              c.prepareStatement(
                  "SELECT event_id,authority_bucket_id FROM control_outbox WHERE delivery_state='PENDING' AND quarantined=false AND next_attempt<=clock_timestamp() AND expires_at>clock_timestamp() ORDER BY next_attempt,event_id LIMIT ?")) {
            s.setInt(1, limit);
            try (var r = s.executeQuery()) {
              while (r.next()) hints.put(r.getObject(1, UUID.class), r.getInt(2));
            }
          }
          if (hints.isEmpty()) return List.of();
          var active = new HashSet<Integer>();
          for (int bucket : new TreeSet<>(hints.values())) {
            AuthoritySql.bucketBarrier(c, bucket, false);
            try (var s =
                c.prepareStatement("SELECT status FROM bucket_authority WHERE bucket_id=?")) {
              s.setInt(1, bucket);
              try (var r = s.executeQuery()) {
                if (r.next() && "ACTIVE".equals(r.getString(1))) active.add(bucket);
              }
            }
          }
          UUID[] ids =
              hints.entrySet().stream()
                  .filter(e -> active.contains(e.getValue()))
                  .map(Map.Entry::getKey)
                  .toArray(UUID[]::new);
          if (ids.length == 0) return List.of();
          java.sql.Array array = c.createArrayOf("uuid", ids);
          var result = new ArrayList<Claim>();
          try (var s =
              c.prepareStatement(
                  "WITH candidates AS (SELECT event_id FROM control_outbox WHERE delivery_state='PENDING' AND event_id=ANY(?) AND next_attempt<=clock_timestamp() AND expires_at>clock_timestamp() AND quarantined=false ORDER BY next_attempt,event_id LIMIT ? FOR UPDATE SKIP LOCKED) UPDATE control_outbox o SET delivery_state='INFLIGHT',dispatch_owner=?,dispatch_incarnation=?,dispatch_generation=o.dispatch_generation+1,dispatch_until=clock_timestamp()+interval '5 seconds' FROM candidates x WHERE o.event_id=x.event_id RETURNING o.event_id,o.authority_bucket_id,o.call_id,o.version,o.dispatch_generation,o.destination,o.payload,o.dispatch_until")) {
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
          return List.copyOf(result);
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
                  "UPDATE control_outbox SET delivery_state='DELIVERED' WHERE event_id=? AND authority_bucket_id=? AND delivery_state='INFLIGHT' AND dispatch_owner=? AND dispatch_incarnation=? AND dispatch_generation=? AND dispatch_until>clock_timestamp()")) {
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
          var buckets = new TreeSet<Integer>();
          try (var q =
              c.prepareStatement(
                  "SELECT DISTINCT o.authority_bucket_id FROM control_outbox o JOIN bucket_authority b ON b.bucket_id=o.authority_bucket_id AND b.status='ACTIVE' WHERE (o.delivery_state='INFLIGHT' AND o.dispatch_until<=clock_timestamp()) OR (o.expires_at<=clock_timestamp() AND o.quarantined=false) ORDER BY o.authority_bucket_id LIMIT ?")) {
            q.setInt(1, limit);
            try (var r = q.executeQuery()) {
              while (r.next()) buckets.add(r.getInt(1));
            }
          }
          int changed = 0;
          for (int bucket : buckets) {
            queueAuthority(c, bucket);
            try (var q =
                c.prepareStatement(
                    "WITH due AS (SELECT event_id FROM control_outbox WHERE authority_bucket_id=? AND ((delivery_state='INFLIGHT' AND dispatch_until<=clock_timestamp()) OR (expires_at<=clock_timestamp() AND quarantined=false)) ORDER BY event_id LIMIT ? FOR UPDATE SKIP LOCKED) UPDATE control_outbox o SET delivery_state='PENDING',dispatch_until=NULL,next_attempt=clock_timestamp(),quarantined=(o.expires_at<=clock_timestamp()),terminal_reason=CASE WHEN o.expires_at<=clock_timestamp() THEN 'EXPIRED_UNDELIVERABLE' ELSE NULL END FROM due d WHERE o.event_id=d.event_id")) {
              q.setInt(1, bucket);
              q.setInt(2, limit - changed);
              changed += q.executeUpdate();
            }
            if (changed >= limit) break;
          }
          return changed;
        });
  }

  public DbOperation<Boolean> defer(Claim claim, String worker, UUID incarnation, boolean poison) {
    return sql.submitTracked(
        DbClass.OUTBOX,
        Duration.ofSeconds(2),
        c -> {
          queueAuthority(c, claim.bucket());
          try (var q =
              c.prepareStatement(
                  "UPDATE control_outbox SET delivery_state='PENDING',dispatch_until=NULL,next_attempt=clock_timestamp()+interval '1 second',quarantined=?,terminal_reason=CASE WHEN ? THEN 'MALFORMED_EVENT' ELSE NULL END WHERE event_id=? AND dispatch_owner=? AND dispatch_incarnation=? AND dispatch_generation=? AND delivery_state='INFLIGHT' AND dispatch_until>clock_timestamp()")) {
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
