package io.webrtc.signaling.actors.user;

import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

/** All mutation services share the original physical cleanup handle and remaining deadline. */
public final class PostgresUserBackend implements UserActor.Backend {
  private final UserSnapshotService snapshots;
  private final SessionRegistryService sessions;
  private final UserReservationService reservations;
  private final AcceptWinnerService winners;
  private final HomeActivationService activations;
  private final HomeProofReadService proofReads;

  public PostgresUserBackend(
      UserSnapshotService snapshots,
      SessionRegistryService sessions,
      UserReservationService reservations,
      AcceptWinnerService winners) {
    this(snapshots, sessions, reservations, winners, null);
  }

  public PostgresUserBackend(
      UserSnapshotService snapshots,
      SessionRegistryService sessions,
      UserReservationService reservations,
      AcceptWinnerService winners,
      HomeActivationService activations) {
    this(snapshots, sessions, reservations, winners, activations, null);
  }

  public PostgresUserBackend(
      UserSnapshotService snapshots,
      SessionRegistryService sessions,
      UserReservationService reservations,
      AcceptWinnerService winners,
      HomeActivationService activations,
      HomeProofReadService proofReads) {
    this.proofReads = proofReads;
    this.snapshots = Objects.requireNonNull(snapshots);
    this.sessions = Objects.requireNonNull(sessions);
    this.reservations = Objects.requireNonNull(reservations);
    this.winners = Objects.requireNonNull(winners);
    this.activations = activations;
  }

  public DbOperation<UserSnapshotService.Snapshot> load(UserId user, long epoch, Duration budget) {
    return snapshots.load(user, epoch, budget);
  }

  public DbOperation<UserCommand.Result> execute(UserCommand.Operation operation, Duration budget) {
    return switch (operation) {
      case UserCommand.QueryProof r -> {
        if (proofReads == null)
          throw new IllegalStateException("Native home proof reads are required");
        yield map(
            proofReads.observe(r.request(), r.sender(), budget),
            view ->
                new UserCommand.Result(
                    UserCommand.Code.PROOF_READ, null, view.participation(), null, null, view));
      }
      case UserCommand.Register r ->
          map(
              sessions.registerSessionTracked(
                  r.principal(), r.boot(), r.connection(), r.directoryEpoch(), budget),
              route -> new UserCommand.Result(UserCommand.Code.REGISTERED, route, null, null));
      case UserCommand.Refresh r ->
          map(
              sessions.refreshSessionTracked(r.route(), r.principal(), r.directoryEpoch(), budget),
              route -> new UserCommand.Result(UserCommand.Code.REFRESHED, route, null, null));
      case UserCommand.Close r ->
          map(
              sessions.closeSessionIfGenerationTracked(r.route(), r.directoryEpoch(), budget),
              closed ->
                  closed
                      ? new UserCommand.Result(UserCommand.Code.CLOSED, r.route(), null, null)
                      : UserCommand.Result.error(UserCommand.Code.STALE_BINDING));
      case UserCommand.Reserve r ->
          map(
              reservations.reserveUserTracked(r.request(), budget),
              p ->
                  new UserCommand.Result(
                      p.terminal() ? UserCommand.Code.TERMINAL : UserCommand.Code.RESERVED,
                      null,
                      p,
                      null));
      case UserCommand.Renew r ->
          map(
              reservations.renewReservationTracked(
                  r.request(), r.reservation(), r.version(), budget),
              p -> new UserCommand.Result(UserCommand.Code.RENEWED, null, p, null));
      case UserCommand.Release r ->
          map(
              reservations.releaseIfCallVersionTracked(
                  r.request(), r.reservation(), r.version(), budget),
              p -> new UserCommand.Result(UserCommand.Code.RELEASED, null, p, null));
      case UserCommand.Accept r ->
          map(
              winners.claimAcceptTracked(r.request(), r.reservation(), r.route(), budget),
              claim -> {
                var request = r.request();
                var code = UserCommand.Code.valueOf(claim.outcome());
                var p =
                    claim.validUntil() == null
                        ? null
                        : new Participation(
                            request.call(),
                            request.user(),
                            request.acquireOperation(),
                            request.payloadHash(),
                            "CLAIMED",
                            claim.reservation(),
                            claim.version(),
                            claim.validUntil(),
                            claim.winner(),
                            request.grant().groupEpoch());
                return new UserCommand.Result(code, null, p, claim);
              });
      case UserCommand.Activate r -> {
        if (activations == null)
          throw new IllegalStateException("Home activation service is required");
        yield map(
            activations.confirmTracked(
                r.request(),
                r.reservation(),
                r.version(),
                r.activation(),
                r.callVersion(),
                r.winner(),
                r.operation(),
                budget),
            confirmation ->
                new UserCommand.Result(
                    UserCommand.Code.CONFIRMED,
                    null,
                    new Participation(
                        r.request().call(),
                        r.request().user(),
                        r.request().acquireOperation(),
                        r.request().payloadHash(),
                        "ACTIVE",
                        confirmation.reservationId(),
                        confirmation.version(),
                        confirmation.validUntil(),
                        confirmation.winner(),
                        r.request().grant().groupEpoch()),
                    null,
                    confirmation));
      }
    };
  }

  private static <T> DbOperation<UserCommand.Result> map(
      DbOperation<T> operation, Function<T, UserCommand.Result> mapper) {
    return new DbOperation<>(operation.logical().thenApply(mapper), operation.physicalCompletion());
  }
}
