package io.webrtc.signaling.actors.user;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
/** All mutation services share the original physical cleanup handle and remaining deadline. */
public final class PostgresUserBackend implements UserActor.Backend {
    private final UserSnapshotService snapshots;private final SessionRegistryService sessions;private final UserReservationService reservations;private final AcceptWinnerService winners;
    public PostgresUserBackend(UserSnapshotService snapshots,SessionRegistryService sessions,UserReservationService reservations,AcceptWinnerService winners){this.snapshots=Objects.requireNonNull(snapshots);this.sessions=Objects.requireNonNull(sessions);this.reservations=Objects.requireNonNull(reservations);this.winners=Objects.requireNonNull(winners);}
    public DbOperation<UserSnapshotService.Snapshot> load(UserId user,long epoch,Duration budget){return snapshots.load(user,epoch,budget);}
    public DbOperation<UserCommand.Result> execute(UserCommand.Operation operation,Duration budget){return switch(operation){
        case UserCommand.Register r->map(sessions.registerSessionTracked(r.principal(),r.boot(),r.connection(),r.directoryEpoch(),budget),route->new UserCommand.Result(UserCommand.Code.REGISTERED,route,null,null));
        case UserCommand.Refresh r->map(sessions.refreshSessionTracked(r.route(),r.principal(),r.directoryEpoch(),budget),route->new UserCommand.Result(UserCommand.Code.REFRESHED,route,null,null));
        case UserCommand.Close r->map(sessions.closeSessionIfGenerationTracked(r.route(),r.directoryEpoch(),budget),closed->closed?new UserCommand.Result(UserCommand.Code.CLOSED,r.route(),null,null):UserCommand.Result.error(UserCommand.Code.STALE_BINDING));
        case UserCommand.Reserve r->map(reservations.reserveUserTracked(r.request(),budget),p->new UserCommand.Result(p.terminal()?UserCommand.Code.TERMINAL:UserCommand.Code.RESERVED,null,p,null));
        case UserCommand.Renew r->map(reservations.renewReservationTracked(r.request(),r.reservation(),r.version(),budget),p->new UserCommand.Result(UserCommand.Code.RENEWED,null,p,null));
        case UserCommand.Release r->map(reservations.releaseIfCallVersionTracked(r.request(),r.reservation(),r.version(),budget),p->new UserCommand.Result(UserCommand.Code.RELEASED,null,p,null));
        case UserCommand.Accept r->map(winners.claimAcceptTracked(r.request(),r.reservation(),r.route(),budget),claim->{var request=r.request();var code=UserCommand.Code.valueOf(claim.outcome());var p=claim.validUntil()==null?null:new Participation(request.call(),request.user(),request.acquireOperation(),request.payloadHash(),"CLAIMED",claim.reservation(),claim.version(),claim.validUntil(),claim.winner(),request.grant().groupEpoch());return new UserCommand.Result(code,null,p,claim);});
    };}
    private static <T> DbOperation<UserCommand.Result> map(DbOperation<T> operation,Function<T,UserCommand.Result> mapper){return new DbOperation<>(operation.logical().thenApply(mapper),operation.physicalCompletion());}
}
