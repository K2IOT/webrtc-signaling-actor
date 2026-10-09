package io.webrtc.signaling.storage;

import io.webrtc.signaling.protocol.Identity.*;
import java.sql.*;
import java.time.*;
import java.util.*;

/**
 * Authenticated private home ingress signs this native projection; missing rows never prove route
 * loss.
 */
public final class NativeRouteLossReadService {
  public enum Cause {
    CLOSED,
    REPLACED,
    TOKEN_EXPIRED,
    BOOT_EXPIRED
  }

  public record View(
      AuthenticatedSession session,
      Cause cause,
      Instant observedAt,
      Instant checkedAt,
      Instant proofUntil,
      String sourceCell,
      long sourceStorageEpoch,
      long directoryEpoch) {}

  private final SqlTransactions sql;
  private final String cell;
  private final long epoch;

  public NativeRouteLossReadService(SqlTransactions sql, String cell, long epoch) {
    this.sql = Objects.requireNonNull(sql);
    this.cell = Objects.requireNonNull(cell);
    this.epoch = epoch;
  }

  public DbOperation<View> observe(
      SessionRepository.Route expected, long directoryEpoch, Duration budget) {
    return sql.submitTracked(
        DbClass.RECOVERY,
        budget,
        c -> {
          AuthoritySql.home(
              c,
              cell,
              epoch,
              Map.of(SessionRegistryService.bucket(expected.user()), directoryEpoch),
              List.of(expected.user().value()));
          try (var q =
              c.prepareStatement(
                  "SELECT s.user_id,s.session_incarnation,s.connection_generation,s.connection_id,s.gateway_id,s.boot_id,s.closed_at,s.updated_at,s.token_exp,g.lease_until,g.expired_at,clock_timestamp() FROM session_registry s JOIN gateway_lease g ON g.gateway_id=s.gateway_id AND g.boot_id=s.boot_id AND g.cell=? AND g.storage_epoch=? WHERE s.issuer=? AND s.jti=?")) {
            q.setString(1, cell);
            q.setLong(2, epoch);
            q.setString(3, expected.key().issuer());
            q.setString(4, expected.key().jti());
            try (var r = q.executeQuery()) {
              if (!r.next()
                  || !expected.user().value().equals(r.getString(1))
                  || !expected.incarnation().value().equals(r.getObject(2, UUID.class))
                  || r.getLong(3) < expected.connectionGeneration())
                throw new AuthoritySql.FencedException();
              Instant now = r.getTimestamp(12).toInstant(), observed;
              Cause cause;
              if (r.getLong(3) > expected.connectionGeneration()) {
                cause = Cause.REPLACED;
                observed = r.getTimestamp(8).toInstant();
              } else {
                if (!Objects.equals(expected.connectionId(), r.getObject(4, UUID.class))
                    || !Objects.equals(expected.gatewayId(), r.getString(5))
                    || !Objects.equals(expected.bootId(), r.getObject(6, UUID.class)))
                  throw new AuthoritySql.FencedException();
                if (r.getTimestamp(7) != null) {
                  cause = Cause.CLOSED;
                  observed = r.getTimestamp(7).toInstant();
                } else if (!r.getTimestamp(9).toInstant().isAfter(now)) {
                  cause = Cause.TOKEN_EXPIRED;
                  observed = r.getTimestamp(9).toInstant();
                } else if (r.getTimestamp(11) != null
                    || !r.getTimestamp(10).toInstant().isAfter(now)) {
                  cause = Cause.BOOT_EXPIRED;
                  observed =
                      r.getTimestamp(11) == null
                          ? r.getTimestamp(10).toInstant()
                          : r.getTimestamp(11).toInstant();
                } else throw new AuthoritySql.FencedException();
              }
              if (observed.isAfter(now)) throw new AuthoritySql.FencedException();
              var sender =
                  new AuthenticatedSession(
                      expected.user(),
                      expected.key(),
                      expected.incarnation(),
                      expected.connectionGeneration(),
                      expected.connectionId());
              return new View(
                  sender, cause, observed, now, now.plusSeconds(5), cell, epoch, directoryEpoch);
            }
          }
        });
  }
}
