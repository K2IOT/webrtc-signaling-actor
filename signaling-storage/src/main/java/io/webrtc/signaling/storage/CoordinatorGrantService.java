package io.webrtc.signaling.storage;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.*;
import io.webrtc.signaling.protocol.Identity.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.*;
import java.time.*;
import java.util.*;
/** Shared call barrier for grant issuance, distinct from exclusive business mutation. The hosting node is fixed. */
public final class CoordinatorGrantService {
    public record Issued(Snapshot snapshot,AuthoritySql.GroupToken token,long sequence,Instant checkedAt,Instant expiresAt,String intentHash) {}
    private static final ObjectMapper JSON=new ObjectMapper().findAndRegisterModules();
    private final SqlTransactions sql;private final String cell,owner;private final long epoch;private final CallSnapshotRepository calls=new CallSnapshotRepository();private final CommandResultRepository results=new CommandResultRepository();
    public CoordinatorGrantService(SqlTransactions sql,String cell,long epoch,String localOwnerNode){this.sql=Objects.requireNonNull(sql);this.cell=Objects.requireNonNull(cell);this.epoch=epoch;owner=Objects.requireNonNull(localOwnerNode);}
    public DbOperation<Issued> issue(Request request,AuthorizationIntent action,AuthoritySql.GroupToken token,long callerDirectoryEpoch,long expectedCallVersion,Duration budget){return sql.submitTracked(DbClass.CRITICAL,budget,c->{
        if(!cell.equals(request.call().coordinatorCell())||request.call().routingEpoch()!=epoch||!cell.equals(token.cell())||token.storageEpoch()!=epoch||token.hashVersion()!=1||token.group()!=HomeParticipationService.group(request.call())||!owner.equals(token.node()))throw new AuthoritySql.FencedException();
        Snapshot hint=calls.find(c,request.call());if(hint==null)throw new AuthoritySql.FencedException();AuthoritySql.coordinatorGrant(c,token,Map.of(hint.bucket(),callerDirectoryEpoch),request.call().value());Snapshot current=calls.find(c,request.call());
        if(current==null||current.version()!=expectedCallVersion||current.hashVersion()!=token.hashVersion()||current.group()!=token.group()||!Set.of(current.caller().user(),current.callee()).contains(request.user()))throw new AuthoritySql.FencedException();
        var original=results.find(c,current.caller().key(),CommandScope.invite(),current.inviteRequest());if(original==null||!current.inviteRequest().value().equals(request.acquireOperation())||!original.hash().equals(request.payloadHash()))throw new HomeParticipationService.IntentConflict();
        boolean allowed=switch(action.action()){case "RESERVE"->current.state().equals("PREPARING");case "CLAIM"->current.state().equals("RINGING")&&current.callee().equals(request.user())&&offered(current,action.route());case "CONFIRM"->current.state().equals("ACTIVATING")&&Objects.equals(current.activationId(),action.activation())&&current.version()==action.callVersion()&&sameWinner(current,request,action.winner());case "RENEW"->current.terminalAt()==null;case "RELEASE"->current.terminalAt()!=null;case "QUERY"->true;default->false;};if(!allowed)throw new AuthoritySql.FencedException();
        try(var q=c.prepareStatement("SELECT lease_sequence,clock_timestamp(),lease_until FROM group_owner WHERE cell_id=? AND ownership_hash_version=1 AND group_id=?")){q.setString(1,cell);q.setInt(2,token.group());try(var r=q.executeQuery()){if(!r.next())throw new AuthoritySql.FencedException();Instant checked=r.getTimestamp(2).toInstant();Instant phaseDeadline=DurableDeadlines.due(current);boolean acquisition=!Set.of("QUERY","RELEASE").contains(action.action());
            if(acquisition&&(phaseDeadline!=null&&!phaseDeadline.isAfter(checked)||phaseDeadline==null&&Set.of("PREPARING","RINGING","ACCEPTED","ACTIVATING","CONNECTING").contains(current.state())))throw new AuthoritySql.FencedException();
            Instant expires=checked.plusSeconds(5);if(acquisition&&phaseDeadline!=null&&expires.isAfter(phaseDeadline))expires=phaseDeadline;Instant ownership=r.getTimestamp(3).toInstant().minusSeconds(5);if(expires.isAfter(ownership))expires=ownership;if(!expires.isAfter(checked))throw new AuthoritySql.FencedException();return new Issued(current,token,r.getLong(1),checked,expires,HomeParticipationService.authorizationHash(request,action));}}
    });}
    private static boolean offered(Snapshot snapshot,SessionRepository.Route route)throws Exception {if(route==null||!route.user().equals(snapshot.callee()))return false;var participant=new Participant(route.user(),route.key(),route.incarnation(),route.connectionGeneration());for(var p:JSON.readTree(snapshot.offeredSessions()))if(JSON.treeToValue(p,Participant.class).equals(participant))return true;return false;}
    private static boolean sameWinner(Snapshot snapshot,Request request,Winner winner){if(request.user().equals(snapshot.caller().user()))return winner==null;return snapshot.winner()!=null&&winner!=null&&snapshot.winner().key().equals(winner.key())&&snapshot.winner().incarnation().equals(winner.incarnation())&&snapshot.winner().generation()==winner.generation();}
}
