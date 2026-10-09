package io.webrtc.signaling.storage;

import io.webrtc.signaling.protocol.Identity.*;
import java.sql.*;
import java.util.UUID;

public final class CallSnapshotRepository {
  public record Participant(
      UserId user, SessionKey key, SessionIncarnation incarnation, long generation) {
    public boolean samePrincipal(AuthenticatedSession sender) {
      return user.equals(sender.userId()) && key.equals(sender.key());
    }

    public boolean sameBinding(AuthenticatedSession sender) {
      return samePrincipal(sender)
          && incarnation.equals(sender.incarnation())
          && generation == sender.connectionGeneration();
    }
  }

  public record Snapshot(
      CallId callId,
      int bucket,
      long hashVersion,
      int group,
      String state,
      long version,
      long negotiationId,
      Participant caller,
      UserId callee,
      Participant winner,
      UUID activationId,
      String sagaPhase,
      String deadlines,
      String offeredSessions,
      String rejectedSessions,
      long lastGroupEpoch,
      String terminalReason,
      java.time.Instant terminalAt,
      java.time.Instant expiresAt,
      RequestId inviteRequest) {}

  public Snapshot find(Connection c, CallId call) throws SQLException {
    try (var s =
        c.prepareStatement(
            "SELECT authority_bucket_id,ownership_hash_version,ownership_group_id,state,version,negotiation_id,caller_user,caller_issuer,caller_jti,caller_incarnation,caller_generation,callee_user,winner_issuer,winner_jti,winner_incarnation,winner_generation,activation_id,saga_phase,deadlines,offered_sessions,rejected_sessions,last_mutation_group_epoch,terminal_reason,terminal_at,expires_at,invite_request_id FROM call_state WHERE call_id=?")) {
      s.setString(1, call.value());
      try (var r = s.executeQuery()) {
        if (!r.next()) return null;
        var caller =
            new Participant(
                new UserId(r.getString(7)),
                new SessionKey(r.getString(8), r.getString(9)),
                new SessionIncarnation(r.getObject(10, UUID.class)),
                r.getLong(11));
        var callee = new UserId(r.getString(12));
        Participant winner =
            r.getString(13) == null
                ? null
                : new Participant(
                    callee,
                    new SessionKey(r.getString(13), r.getString(14)),
                    new SessionIncarnation(r.getObject(15, UUID.class)),
                    r.getLong(16));
        return new Snapshot(
            call,
            r.getInt(1),
            r.getLong(2),
            r.getInt(3),
            r.getString(4),
            r.getLong(5),
            r.getLong(6),
            caller,
            callee,
            winner,
            r.getObject(17, UUID.class),
            r.getString(18),
            r.getString(19),
            r.getString(20),
            r.getString(21),
            r.getLong(22),
            r.getString(23),
            instant(r, 24),
            instant(r, 25),
            new RequestId(r.getObject(26, UUID.class)));
      }
    }
  }

  private static java.time.Instant instant(ResultSet r, int column) throws SQLException {
    Timestamp value = r.getTimestamp(column);
    return value == null ? null : value.toInstant();
  }
}
