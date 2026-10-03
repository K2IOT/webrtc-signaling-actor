package io.webrtc.signaling.actors.user;
import io.webrtc.signaling.actors.cluster.CborSerializable;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import java.util.*;
/** Rehydratable cache; repositories always recheck primary authority before mutation. */
public record UserState(UserId user,List<SessionRepository.Route> routes,HomeParticipationService.Participation participation) implements CborSerializable {
    public UserState {new UserSnapshotService.Snapshot(user,routes,participation);routes=List.copyOf(routes);}
    static UserState empty(UserId user){return new UserState(user,List.of(),null);}
    static UserState from(UserSnapshotService.Snapshot snapshot){return new UserState(snapshot.user(),snapshot.routes(),snapshot.participation());}
    boolean current(SessionRepository.Route route){return routes.stream().anyMatch(r->r.user().equals(route.user())&&r.key().equals(route.key())&&r.incarnation().equals(route.incarnation())&&r.connectionGeneration()==route.connectionGeneration()&&r.connectionId().equals(route.connectionId())&&r.bootId().equals(route.bootId())&&r.gatewayId().equals(route.gatewayId()));}
    UserState apply(UserCommand.Result result){var next=new ArrayList<>(routes);if(result.route()!=null){var route=result.route();if(!user.equals(route.user()))throw new IllegalArgumentException("Cross-user result");next.removeIf(r->r.key().equals(route.key()));if(result.code()!=UserCommand.Code.CLOSED)next.add(route);}
        return new UserState(user,next,result.participation()==null?participation:result.participation());}
}
