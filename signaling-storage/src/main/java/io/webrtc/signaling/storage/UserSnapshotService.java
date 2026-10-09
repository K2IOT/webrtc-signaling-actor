package io.webrtc.signaling.storage;

import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.sql.*;
import java.time.Duration;
import java.util.*;

/** Primary-authoritative hydration; immutable projections cross the asynchronous DB boundary. */
public final class UserSnapshotService {
  public record Snapshot(
      UserId user, List<SessionRepository.Route> routes, Participation participation) {
    public Snapshot {
      Objects.requireNonNull(user);
      routes = List.copyOf(routes);
      if (routes.size() > 5
          || routes.stream().anyMatch(r -> !user.equals(r.user()))
          || routes.stream().map(SessionRepository.Route::key).distinct().count() != routes.size()
          || participation != null && !user.equals(participation.user()))
        throw new IllegalArgumentException("Invalid user projection");
    }
  }

  private final SqlTransactions sql;
  private final String cell;
  private final long storageEpoch;
  private final SessionRepository sessions = new SessionRepository();

  public UserSnapshotService(SqlTransactions sql, String cell, long epoch) {
    this.sql = Objects.requireNonNull(sql);
    this.cell = cell;
    this.storageEpoch = epoch;
  }

  public DbOperation<Snapshot> load(UserId user, long directoryEpoch, Duration budget) {
    return sql.submitTracked(
        DbClass.CRITICAL,
        budget,
        c -> {
          AuthoritySql.home(
              c,
              cell,
              storageEpoch,
              Map.of(SessionRegistryService.bucket(user), directoryEpoch),
              List.of(user.value()));
          Participation active = null;
          try (var s =
              c.prepareStatement(
                  "SELECT h.call_id,h.acquire_operation_id,h.payload_hash,h.phase,h.reservation_id,r.reservation_version,r.lease_until,h.winner_issuer,h.winner_jti,h.winner_incarnation,h.winner_generation,h.highest_group_epoch FROM user_reservation r JOIN home_participation h ON h.user_id=r.user_id AND h.call_id=r.call_id AND h.reservation_id=r.reservation_id WHERE r.user_id=? AND r.lease_until>clock_timestamp() AND h.phase NOT IN ('RELEASED','EXPIRED') LIMIT 1")) {
            s.setString(1, user.value());
            try (var r = s.executeQuery()) {
              if (r.next()) {
                Winner winner =
                    r.getString(8) == null
                        ? null
                        : new Winner(
                            new SessionKey(r.getString(8), r.getString(9)),
                            new SessionIncarnation(r.getObject(10, UUID.class)),
                            r.getLong(11));
                active =
                    new Participation(
                        new CallId(r.getString(1)),
                        user,
                        r.getObject(2, UUID.class),
                        HexFormat.of().formatHex(r.getBytes(3)),
                        r.getString(4),
                        r.getObject(5, UUID.class),
                        r.getLong(6),
                        r.getTimestamp(7).toInstant(),
                        winner,
                        r.getLong(12));
              }
            }
          }
          return new Snapshot(user, sessions.liveRoutes(c, user), active);
        });
  }
}
