package io.webrtc.signaling.storage;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.sql.*;
import java.time.*;
import java.util.*;

/**
 * Fresh guarded primary projection used to issue home proofs; security freshness is a required
 * native policy.
 */
public final class HomeProofReadService {
  @FunctionalInterface
  public interface SecurityPolicy {
    boolean current(Connection connection, UserId user) throws Exception;
  }

  public record View(
      Participation participation,
      List<SessionRepository.Route> currentRoutes,
      UUID activationId,
      long activationCallVersion,
      Instant checkedAt,
      String sourceCell,
      long sourceStorageEpoch,
      long directoryEpoch,
      UUID winnerOperation,
      long winnerCallVersion,
      long coordinatorStorageEpoch,
      long highestLeaseSequence,
      UUID lastRenewOperation,
      @JsonInclude(JsonInclude.Include.NON_NULL) String reservationPhase) {
    public View(
        Participation participation,
        List<SessionRepository.Route> currentRoutes,
        UUID activationId,
        long activationCallVersion,
        Instant checkedAt,
        String sourceCell,
        long sourceStorageEpoch,
        long directoryEpoch,
        UUID winnerOperation,
        long winnerCallVersion,
        long coordinatorStorageEpoch,
        long highestLeaseSequence,
        UUID lastRenewOperation) {
      this(
          participation,
          currentRoutes,
          activationId,
          activationCallVersion,
          checkedAt,
          sourceCell,
          sourceStorageEpoch,
          directoryEpoch,
          winnerOperation,
          winnerCallVersion,
          coordinatorStorageEpoch,
          highestLeaseSequence,
          lastRenewOperation,
          null);
    }

    public View(
        Participation participation,
        List<SessionRepository.Route> currentRoutes,
        UUID activationId,
        long activationCallVersion,
        Instant checkedAt,
        String sourceCell,
        long sourceStorageEpoch,
        long directoryEpoch,
        UUID winnerOperation,
        long winnerCallVersion) {
      this(
          participation,
          currentRoutes,
          activationId,
          activationCallVersion,
          checkedAt,
          sourceCell,
          sourceStorageEpoch,
          directoryEpoch,
          winnerOperation,
          winnerCallVersion,
          0,
          0,
          null);
    }

    public View(
        Participation participation,
        List<SessionRepository.Route> currentRoutes,
        UUID activationId,
        long activationCallVersion,
        Instant checkedAt,
        String sourceCell,
        long sourceStorageEpoch,
        long directoryEpoch) {
      this(
          participation,
          currentRoutes,
          activationId,
          activationCallVersion,
          checkedAt,
          sourceCell,
          sourceStorageEpoch,
          directoryEpoch,
          null,
          0);
    }

    public View {
      Objects.requireNonNull(participation);
      currentRoutes = List.copyOf(currentRoutes);
      if (currentRoutes.size() > 5
          || currentRoutes.stream().anyMatch(r -> !r.user().equals(participation.user())))
        throw new IllegalArgumentException("Invalid home proof projection");
    }
  }

  private final HomeParticipationService home;
  private final SecurityPolicy security;
  private final AcceptWinnerService.RouteSecurityPolicy routesSecurity;
  private final SessionRepository sessions = new SessionRepository();

  public HomeProofReadService(HomeParticipationService home, SecurityPolicy security) {
    this(home, security, (c, r) -> false);
  }

  public HomeProofReadService(
      HomeParticipationService home,
      SecurityPolicy security,
      AcceptWinnerService.RouteSecurityPolicy routesSecurity) {
    this.home = Objects.requireNonNull(home);
    this.security = Objects.requireNonNull(security);
    this.routesSecurity = Objects.requireNonNull(routesSecurity);
  }

  public DbOperation<View> observe(Request request, AuthenticatedSession sender, Duration budget) {
    return observe(request, sender, budget, false);
  }

  /** Guarded LIVE renewal projection; authorized epoch adoption cannot revive an expired lease. */
  public DbOperation<View> observeRenewal(Request request, Duration budget) {
    return observe(request, null, budget, true);
  }

  private DbOperation<View> observe(
      Request request, AuthenticatedSession sender, Duration budget, boolean renewal) {
    return home.submitTracked(
        request,
        renewal ? DbClass.RENEWAL : DbClass.CRITICAL,
        budget,
        new AuthorizationIntent("QUERY", null, 0, null, 0, null, null),
        c -> {
          if (!security.current(c, request.user())) throw new AuthoritySql.FencedException();
          var participation = home.find(c, request, renewal);
          // Only never-acquired history can be absent. Expired, terminal, or conflicting history
          // continues to fail the live projection and can never be skipped as an unresolved
          // reserve.
          if (participation == null && renewal && request.phase() == Phase.PREPARING) return null;
          if (participation == null
              || participation.terminal()
              || !renewal && request.grant().groupEpoch() < participation.highestGroupEpoch())
            throw new AuthoritySql.FencedException();
          Instant now;
          String cell;
          long epoch;
          try (var q = c.createStatement();
              var r =
                  q.executeQuery(
                      "SELECT cell_id,storage_epoch,clock_timestamp() FROM cell_authority WHERE"
                          + " singleton_id=1")) {
            if (!r.next()) throw new AuthoritySql.FencedException();
            cell = r.getString(1);
            epoch = r.getLong(2);
            now = r.getTimestamp(3).toInstant();
          }
          if (participation.leaseUntil() == null
              || !participation.leaseUntil().isAfter(renewal ? now : now.plusSeconds(5)))
            throw new AuthoritySql.FencedException();
          var routes = new ArrayList<SessionRepository.Route>();
          for (var route : sessions.liveRoutes(c, request.user()))
            if (routesSecurity.allowed(c, route)) routes.add(route);
          if (sender != null) {
            if (!sender.userId().equals(request.user())
                || routes.stream()
                    .noneMatch(
                        r ->
                            r.key().equals(sender.key())
                                && r.incarnation().equals(sender.incarnation())
                                && r.connectionGeneration() == sender.connectionGeneration()
                                && r.connectionId().equals(sender.connectionId())))
              throw new AuthoritySql.FencedException();
          }
          UUID activation, winnerOperation;
          long callVersion, winnerCallVersion;
          try (var q =
              c.prepareStatement(
                  "SELECT"
                      + " activation_id,COALESCE(activation_call_version,0),winner_operation,COALESCE(winner_call_version,0)"
                      + " FROM home_participation WHERE call_id=? AND user_id=?")) {
            q.setString(1, request.call().value());
            q.setString(2, request.user().value());
            try (var r = q.executeQuery()) {
              if (!r.next()) throw new AuthoritySql.FencedException();
              activation = r.getObject(1, UUID.class);
              callVersion = r.getLong(2);
              winnerOperation = r.getObject(3, UUID.class);
              winnerCallVersion = r.getLong(4);
            }
          }
          long coordinatorStorage, leaseSequence;
          UUID renewOperation;
          String reservationPhase;
          try (var q =
              c.prepareStatement(
                  "SELECT"
                      + " coordinator_storage_epoch,highest_lease_sequence,last_renew_operation,phase"
                      + " FROM user_reservation WHERE user_id=? AND call_id=? AND reservation_id=?"
                      + " AND lease_until>clock_timestamp()")) {
            q.setString(1, request.user().value());
            q.setString(2, request.call().value());
            q.setObject(3, participation.reservationId());
            try (var r = q.executeQuery()) {
              if (!r.next()) throw new AuthoritySql.FencedException();
              coordinatorStorage = r.getLong(1);
              leaseSequence = r.getLong(2);
              renewOperation = r.getObject(3, UUID.class);
              reservationPhase = r.getString(4);
            }
          }
          if (renewal
              && request.grant().storageEpoch() == coordinatorStorage
              && request.grant().groupEpoch() < participation.highestGroupEpoch())
            throw new AuthoritySql.FencedException();
          return new View(
              participation,
              routes,
              activation,
              callVersion,
              now,
              cell,
              epoch,
              request.directoryEpoch(),
              winnerOperation,
              winnerCallVersion,
              coordinatorStorage,
              leaseSequence,
              renewOperation,
              reservationPhase);
        });
  }
}
