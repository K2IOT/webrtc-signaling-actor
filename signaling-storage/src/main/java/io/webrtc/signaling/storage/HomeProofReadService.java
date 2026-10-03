package io.webrtc.signaling.storage;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.sql.*;
import java.time.*;
import java.util.*;
/** Fresh guarded primary projection used to issue home proofs; security freshness is a required native policy. */
public final class HomeProofReadService {
    @FunctionalInterface public interface SecurityPolicy {boolean current(Connection connection,UserId user)throws Exception;}
    public record View(Participation participation,List<SessionRepository.Route> currentRoutes,UUID activationId,long activationCallVersion,Instant checkedAt,String sourceCell,long sourceStorageEpoch,long directoryEpoch,UUID winnerOperation,long winnerCallVersion){public View(Participation participation,List<SessionRepository.Route> currentRoutes,UUID activationId,long activationCallVersion,Instant checkedAt,String sourceCell,long sourceStorageEpoch,long directoryEpoch){this(participation,currentRoutes,activationId,activationCallVersion,checkedAt,sourceCell,sourceStorageEpoch,directoryEpoch,null,0);}public View{Objects.requireNonNull(participation);currentRoutes=List.copyOf(currentRoutes);if(currentRoutes.size()>5||currentRoutes.stream().anyMatch(r->!r.user().equals(participation.user())))throw new IllegalArgumentException("Invalid home proof projection");}}
    private final HomeParticipationService home;private final SecurityPolicy security;private final SessionRepository sessions=new SessionRepository();
    public HomeProofReadService(HomeParticipationService home,SecurityPolicy security){this.home=Objects.requireNonNull(home);this.security=Objects.requireNonNull(security);}
    public DbOperation<View> observe(Request request,AuthenticatedSession sender,Duration budget){return home.submitTracked(request,DbClass.CRITICAL,budget,new AuthorizationIntent("QUERY",null,0,null,0,null,null),c->{
        if(!security.current(c,request.user()))throw new AuthoritySql.FencedException();var participation=home.find(c,request);if(participation==null||participation.terminal()||request.grant().groupEpoch()<participation.highestGroupEpoch())throw new AuthoritySql.FencedException();
        Instant now;String cell;long epoch;try(var q=c.createStatement();var r=q.executeQuery("SELECT cell_id,storage_epoch,clock_timestamp() FROM cell_authority WHERE singleton_id=1")){if(!r.next())throw new AuthoritySql.FencedException();cell=r.getString(1);epoch=r.getLong(2);now=r.getTimestamp(3).toInstant();}if(participation.leaseUntil()==null||!participation.leaseUntil().isAfter(now.plusSeconds(5)))throw new AuthoritySql.FencedException();
        var routes=sessions.liveRoutes(c,request.user());if(sender!=null){if(!sender.userId().equals(request.user())||routes.stream().noneMatch(r->r.key().equals(sender.key())&&r.incarnation().equals(sender.incarnation())&&r.connectionGeneration()==sender.connectionGeneration()&&r.connectionId().equals(sender.connectionId())))throw new AuthoritySql.FencedException();}
        UUID activation,winnerOperation;long callVersion,winnerCallVersion;try(var q=c.prepareStatement("SELECT activation_id,COALESCE(activation_call_version,0),winner_operation,COALESCE(winner_call_version,0) FROM home_participation WHERE call_id=? AND user_id=?")){q.setString(1,request.call().value());q.setString(2,request.user().value());try(var r=q.executeQuery()){if(!r.next())throw new AuthoritySql.FencedException();activation=r.getObject(1,UUID.class);callVersion=r.getLong(2);winnerOperation=r.getObject(3,UUID.class);winnerCallVersion=r.getLong(4);}}
        return new View(participation,routes,activation,callVersion,now,cell,epoch,request.directoryEpoch(),winnerOperation,winnerCallVersion);
    });}
}
