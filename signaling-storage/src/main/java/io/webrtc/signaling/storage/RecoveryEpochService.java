package io.webrtc.signaling.storage;

import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.function.Predicate;

/**
 * Controlled DR repair only. Never called by normal startup; every batch owns exclusive native cell
 * barrier100.
 */
public final class RecoveryEpochService {
  public record Permit(
      String cell,
      long previousEpoch,
      long newEpoch,
      long externalEpochHighWater,
      UUID operation,
      Instant validUntil,
      String signedEvidence) {
    public Permit {
      Objects.requireNonNull(cell);
      Objects.requireNonNull(operation);
      Objects.requireNonNull(validUntil);
      if (previousEpoch < 1
          || newEpoch < 1
          || externalEpochHighWater < 0
          || signedEvidence == null
          || signedEvidence.length() > 8192)
        throw new IllegalArgumentException("Invalid recovery permit");
    }

    public String toString() {
      return "RecoveryPermit[redacted]";
    }
  }

  public record Completion(
      String cell,
      long storageEpoch,
      UUID operation,
      long securityHighWater,
      long privacyHighWater,
      Instant validUntil,
      String signedEvidence) {
    public Completion {
      Objects.requireNonNull(cell);
      Objects.requireNonNull(operation);
      Objects.requireNonNull(validUntil);
      if (storageEpoch < 1
          || securityHighWater < 0
          || privacyHighWater < 0
          || signedEvidence == null
          || signedEvidence.length() > 8192)
        throw new IllegalArgumentException("Invalid recovery completion");
    }

    public String toString() {
      return "RecoveryCompletion[redacted]";
    }
  }

  /** remaining is an existence indicator, avoiding full-table counts on each bounded batch. */
  public record Batch(int changed, int remaining) {}

  private final SqlTransactions sql;
  private final String cell;
  private final Predicate<Permit> fencingAndExternalHighWater;
  private final Predicate<Completion> securityAndPrivacyReplay;

  public RecoveryEpochService(
      SqlTransactions sql, String cell, Predicate<Permit> verifiedRecoverySource) {
    this(sql, cell, verifiedRecoverySource, r -> false);
  }

  public RecoveryEpochService(
      SqlTransactions sql,
      String cell,
      Predicate<Permit> verifiedRecoverySource,
      Predicate<Completion> verifiedReplaySource) {
    this.sql = Objects.requireNonNull(sql);
    this.cell = Objects.requireNonNull(cell);
    fencingAndExternalHighWater = Objects.requireNonNull(verifiedRecoverySource);
    securityAndPrivacyReplay = Objects.requireNonNull(verifiedReplaySource);
  }

  private void authorize(Permit p) {
    if (p == null || !cell.equals(p.cell()) || !fencingAndExternalHighWater.test(p))
      throw new AuthoritySql.FencedException();
  }

  public DbOperation<Long> begin(Permit p) {
    authorize(p);
    return sql.submitTracked(
        DbClass.RECOVERY,
        Duration.ofSeconds(2),
        c -> {
          AuthoritySql.cellBarrier(c, true);
          fresh(c, p.validUntil());
          if (p.newEpoch() <= p.previousEpoch() || p.newEpoch() <= p.externalEpochHighWater())
            throw new AuthoritySql.FencedException();
          var state = state(c);
          if (state.epoch() == p.newEpoch()
              && Set.of("RECOVERING", "ACTIVE").contains(state.status())) {
            journal(c, p, true);
            return p.newEpoch();
          }
          if (state.epoch() != p.previousEpoch()) throw new AuthoritySql.FencedException();
          try (var q =
              c.prepareStatement(
                  "INSERT INTO recovery_epoch_journal(storage_epoch,previous_epoch,external_epoch_high_water,operation_id) VALUES(?,?,?,?)")) {
            q.setLong(1, p.newEpoch());
            q.setLong(2, p.previousEpoch());
            q.setLong(3, p.externalEpochHighWater());
            q.setObject(4, p.operation());
            q.executeUpdate();
          }
          try (var q =
              c.prepareStatement(
                  "UPDATE cell_authority SET storage_epoch=?,status='RECOVERING' WHERE singleton_id=1 AND storage_epoch=?")) {
            q.setLong(1, p.newEpoch());
            q.setLong(2, p.previousEpoch());
            if (q.executeUpdate() != 1) throw new AuthoritySql.FencedException();
          }
          return p.newEpoch();
        });
  }

  public DbOperation<Batch> abortBatch(Permit p, int limit) {
    authorize(p);
    if (limit < 1 || limit > 128) throw new IllegalArgumentException("Recovery batch exceeds 128");
    return sql.submitTracked(
        DbClass.RECOVERY,
        Duration.ofSeconds(2),
        c -> {
          guard(c, p);
          int changed = 0;
          changed +=
              update(
                  c,
                  "WITH candidates AS (SELECT call_id FROM call_state WHERE terminal_at IS NULL ORDER BY call_id FOR UPDATE SKIP LOCKED LIMIT ?) UPDATE call_state s SET state='TERMINAL',version=version+1,saga_phase='COMPENSATE',terminal_reason='DISASTER_RESTORE',terminal_at=clock_timestamp(),expires_at=clock_timestamp()+interval '24 hours 5 seconds',updated_at=clock_timestamp() FROM candidates x WHERE s.call_id=x.call_id",
                  limit);
          changed +=
              update(
                  c,
                  "WITH candidates AS (SELECT call_id,user_id FROM home_participation WHERE terminal_at IS NULL ORDER BY call_id,user_id FOR UPDATE SKIP LOCKED LIMIT ?) UPDATE home_participation h SET phase='EXPIRED',terminal_at=clock_timestamp(),expires_at=clock_timestamp()+interval '24 hours 5 seconds' FROM candidates x WHERE h.call_id=x.call_id AND h.user_id=x.user_id",
                  limit);
          changed +=
              update(
                  c,
                  "WITH candidates AS (SELECT user_id FROM user_reservation ORDER BY user_id FOR UPDATE SKIP LOCKED LIMIT ?) DELETE FROM user_reservation r USING candidates x WHERE r.user_id=x.user_id",
                  limit);
          changed +=
              update(
                  c,
                  "WITH candidates AS (SELECT issuer,jti FROM session_registry WHERE closed_at IS NULL ORDER BY issuer,jti FOR UPDATE SKIP LOCKED LIMIT ?) UPDATE session_registry s SET closed_at=clock_timestamp(),updated_at=clock_timestamp() FROM candidates x WHERE s.issuer=x.issuer AND s.jti=x.jti",
                  limit);
          changed +=
              update(
                  c,
                  "WITH candidates AS (SELECT gateway_id,boot_id FROM gateway_lease WHERE expired_at IS NULL ORDER BY gateway_id,boot_id FOR UPDATE SKIP LOCKED LIMIT ?) UPDATE gateway_lease g SET expired_at=clock_timestamp() FROM candidates x WHERE g.gateway_id=x.gateway_id AND g.boot_id=x.boot_id",
                  limit);
          changed +=
              update(
                  c,
                  "WITH candidates AS (SELECT event_id FROM control_outbox WHERE delivery_state<>'DELIVERED' AND NOT quarantined ORDER BY event_id FOR UPDATE SKIP LOCKED LIMIT ?) UPDATE control_outbox o SET quarantined=true,terminal_reason='DISASTER_RESTORE',delivery_state='PENDING',dispatch_owner=NULL,dispatch_incarnation=NULL,dispatch_until=NULL FROM candidates x WHERE o.event_id=x.event_id",
                  limit);
          changed +=
              update(
                  c,
                  "WITH candidates AS (SELECT r.issuer,r.jti,r.command_scope,r.request_id,r.call_id,s.version FROM command_result r JOIN call_state s ON s.call_id=r.call_id WHERE r.status='PENDING' AND s.terminal_at IS NOT NULL ORDER BY r.issuer,r.jti,r.command_scope,r.request_id FOR UPDATE OF r SKIP LOCKED LIMIT ?) UPDATE command_result r SET status='FINAL',finalized_at=clock_timestamp(),expires_at=clock_timestamp()+interval '24 hours 5 seconds',result=jsonb_build_object('status','FINAL','code','DISASTER_RESTORE','callId',x.call_id,'version',x.version,'state','TERMINAL','eventIds','[]'::jsonb) FROM candidates x WHERE r.issuer=x.issuer AND r.jti=x.jti AND r.command_scope=x.command_scope AND r.request_id=x.request_id",
                  limit);
          return new Batch(changed, outstanding(c) ? 1 : 0);
        });
  }

  public DbOperation<Long> activate(Permit p, Completion completion) {
    authorize(p);
    if (completion == null
        || !cell.equals(completion.cell())
        || completion.storageEpoch() != p.newEpoch()
        || !completion.operation().equals(p.operation())
        || !securityAndPrivacyReplay.test(completion)) throw new AuthoritySql.FencedException();
    return sql.submitTracked(
        DbClass.RECOVERY,
        Duration.ofSeconds(2),
        c -> {
          AuthoritySql.cellBarrier(c, true);
          fresh(c, p.validUntil());
          fresh(c, completion.validUntil());
          var state = state(c);
          if (state.epoch() == p.newEpoch() && state.status().equals("ACTIVE")) {
            journal(c, p, true);
            try (var q =
                c.prepareStatement(
                    "SELECT security_replay_offset,privacy_replay_offset FROM recovery_epoch_journal WHERE storage_epoch=? AND completed_at IS NOT NULL")) {
              q.setLong(1, p.newEpoch());
              try (var r = q.executeQuery()) {
                if (!r.next()
                    || r.getLong(1) != completion.securityHighWater()
                    || r.getLong(2) != completion.privacyHighWater())
                  throw new AuthoritySql.FencedException();
              }
            }
            return p.newEpoch();
          }
          guard(c, p);
          if (outstanding(c)) throw new AuthoritySql.FencedException();
          try (var q =
              c.prepareStatement(
                  "SELECT source_offset=? AND LEAST(checked_at,source_checked_at)>clock_timestamp()-interval '5 seconds' AND GREATEST(checked_at,source_checked_at)<=clock_timestamp() FROM security_progress WHERE singleton_id=1")) {
            q.setLong(1, completion.securityHighWater());
            try (var r = q.executeQuery()) {
              if (!r.next() || !r.getBoolean(1)) throw new AuthoritySql.FencedException();
            }
          }
          try (var q = c.createStatement();
              var r =
                  q.executeQuery(
                      "SELECT EXISTS(SELECT 1 FROM privacy_deletion_request WHERE completed_at IS NULL)")) {
            r.next();
            if (r.getBoolean(1)) throw new AuthoritySql.FencedException();
          }
          try (var q =
              c.prepareStatement(
                  "UPDATE recovery_epoch_journal SET completed_at=clock_timestamp(),security_replay_offset=?,privacy_replay_offset=? WHERE storage_epoch=? AND operation_id=? AND completed_at IS NULL")) {
            q.setLong(1, completion.securityHighWater());
            q.setLong(2, completion.privacyHighWater());
            q.setLong(3, p.newEpoch());
            q.setObject(4, p.operation());
            if (q.executeUpdate() != 1) throw new AuthoritySql.FencedException();
          }
          try (var q =
              c.prepareStatement(
                  "UPDATE cell_authority SET status='ACTIVE' WHERE singleton_id=1 AND storage_epoch=? AND status='RECOVERING'")) {
            q.setLong(1, p.newEpoch());
            if (q.executeUpdate() != 1) throw new AuthoritySql.FencedException();
          }
          return p.newEpoch();
        });
  }

  private record State(long epoch, String status) {}

  private State state(Connection c) throws SQLException {
    try (var q = c.createStatement();
        var r =
            q.executeQuery(
                "SELECT cell_id,storage_epoch,status FROM cell_authority WHERE singleton_id=1")) {
      if (!r.next() || !cell.equals(r.getString(1))) throw new AuthoritySql.FencedException();
      return new State(r.getLong(2), r.getString(3));
    }
  }

  private void guard(Connection c, Permit p) throws SQLException {
    AuthoritySql.cellBarrier(c, true);
    fresh(c, p.validUntil());
    var state = state(c);
    if (state.epoch() != p.newEpoch() || !state.status().equals("RECOVERING"))
      throw new AuthoritySql.FencedException();
    journal(c, p, false);
  }

  private static void journal(Connection c, Permit p, boolean completedAllowed)
      throws SQLException {
    try (var q =
        c.prepareStatement(
            "SELECT previous_epoch,external_epoch_high_water,operation_id FROM recovery_epoch_journal WHERE storage_epoch=?"
                + (completedAllowed ? "" : " AND completed_at IS NULL"))) {
      q.setLong(1, p.newEpoch());
      try (var r = q.executeQuery()) {
        if (!r.next()
            || r.getLong(1) != p.previousEpoch()
            || r.getLong(2) != p.externalEpochHighWater()
            || !p.operation().equals(r.getObject(3, UUID.class)))
          throw new AuthoritySql.FencedException();
      }
    }
  }

  private static void fresh(Connection c, Instant until) throws SQLException {
    try (var q =
        c.prepareStatement(
            "SELECT ?::timestamptz>clock_timestamp() AND ?::timestamptz<=clock_timestamp()+interval '5 seconds'")) {
      q.setTimestamp(1, Timestamp.from(until));
      q.setTimestamp(2, Timestamp.from(until));
      try (var r = q.executeQuery()) {
        r.next();
        if (!r.getBoolean(1)) throw new AuthoritySql.FencedException();
      }
    }
  }

  private static int update(Connection c, String sql, int limit) throws SQLException {
    try (var q = c.prepareStatement(sql)) {
      q.setInt(1, limit);
      return q.executeUpdate();
    }
  }

  private static boolean outstanding(Connection c) throws SQLException {
    try (var q = c.createStatement();
        var r =
            q.executeQuery(
                "SELECT EXISTS(SELECT 1 FROM call_state WHERE terminal_at IS NULL) OR EXISTS(SELECT 1 FROM home_participation WHERE terminal_at IS NULL) OR EXISTS(SELECT 1 FROM user_reservation) OR EXISTS(SELECT 1 FROM session_registry WHERE closed_at IS NULL) OR EXISTS(SELECT 1 FROM gateway_lease WHERE expired_at IS NULL) OR EXISTS(SELECT 1 FROM control_outbox WHERE delivery_state<>'DELIVERED' AND NOT quarantined) OR EXISTS(SELECT 1 FROM command_result WHERE status='PENDING')")) {
      r.next();
      return r.getBoolean(1);
    }
  }
}
