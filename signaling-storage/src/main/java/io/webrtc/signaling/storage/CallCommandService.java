package io.webrtc.signaling.storage;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import com.fasterxml.jackson.databind.ObjectMapper;
public final class CallCommandService {
    public record Authority(CallId callId,AuthoritySql.GroupToken group,long directoryEpoch,String proof,long expectedCallVersion) {
        public Authority(CallId callId,AuthoritySql.GroupToken group,long directoryEpoch,String proof){this(callId,group,directoryEpoch,proof,0);}
        public Authority {if(expectedCallVersion<0)throw new IllegalArgumentException("Invalid expected call version");}
    }
    @FunctionalInterface public interface ProofVerifier {boolean verify(CallCommand command,Snapshot snapshot,String proof);}
    public record Outcome(String status,String code,CallId callId,long version,String state,List<UUID> eventIds) {public Outcome{eventIds=List.copyOf(eventIds);}}
    private static final ObjectMapper JSON=new ObjectMapper();
    private final SqlTransactions sql;private final String cell;private final long epoch;
    private final Function<CallCommand,CompletionStage<Authority>> authority;private final ProofVerifier proofs;
    private final CommandResultRepository results=new CommandResultRepository();private final CallSnapshotRepository calls=new CallSnapshotRepository();private final SessionRepository sessions=new SessionRepository();private final OutboxRepository outbox;
    private final HomeParticipationService localHome;private final UserReservationService reservations;
    public CallCommandService(SqlTransactions sql,String cell,long epoch,Function<CallCommand,CompletionStage<Authority>> authority,ProofVerifier proofs){
        this.sql=sql;this.cell=cell;this.epoch=epoch;this.authority=Objects.requireNonNull(authority);this.proofs=Objects.requireNonNull(proofs);outbox=new OutboxRepository(sql,cell,epoch);
        // Used only by the local native transaction after its full primary group/session checks.
        // Remote calls must use the independently supplied request-bound verifier.
        localHome=new HomeParticipationService(sql,cell,epoch,r->false);reservations=new UserReservationService(localHome);
    }
    public CompletionStage<Outcome> executeCallCommand(CallCommand command){return executeCallCommand(command,Duration.ofSeconds(2));}
    public CompletionStage<Outcome> executeCallCommand(CallCommand command,Duration budget){
        if(budget.isNegative()||budget.isZero())return CompletableFuture.<Outcome>failedFuture(new DbOverloadedException()).minimalCompletionStage();
        long start=System.nanoTime();return authority.apply(command).thenCompose(context->{Duration remaining=budget.minusNanos(Math.max(0,System.nanoTime()-start));DbClass clazz=command.type()==SignalEnvelope.Type.CANCEL||command.type()==SignalEnvelope.Type.HANGUP?DbClass.TERMINATION:DbClass.CRITICAL;return sql.submit(clazz,remaining,c->execute(c,command,context));});
    }
    private Outcome execute(Connection c,CallCommand command,Authority context)throws Exception {
        boolean invite=command.type()==SignalEnvelope.Type.INVITE;
        if(!command.scope().equals(invite?CommandScope.invite():CommandScope.call(command.callId()))||!context.callId().coordinatorCell().equals(cell)||!context.group().cell().equals(cell)||context.group().storageEpoch()!=epoch||context.group().group()!=HomeParticipationService.group(context.callId())||!invite&&!context.callId().equals(command.callId()))throw new AuthorizationRejected();
        int bucket;
        if(invite)bucket=SessionRegistryService.bucket(command.sender().userId());
        else {Snapshot hint=calls.find(c,context.callId());if(hint==null)throw new AuthorizationRejected();bucket=hint.bucket();}
        AuthoritySql.coordinator(c,context.group(),Map.of(bucket,context.directoryEpoch()),invite?List.of(command.sender().userId().value()):List.of(),List.of(context.callId().value()));
        Snapshot snapshot=invite?null:calls.find(c,context.callId());
        if(!proofs.verify(command,snapshot,context.proof()))throw new AuthorizationRejected();
        if(invite)requireLocalCurrent(c,command.sender());else requirePrincipal(command.sender(),snapshot);
        var stored=results.find(c,command.sender().key(),command.scope(),command.requestId());
        if(stored!=null){if(!stored.hash().equals(command.intentHash()))throw new CommandResultRepository.IntentConflict();return outcome(stored);}
        if(!results.insertPending(c,command,context.callId(),bucket)){
            stored=results.find(c,command.sender().key(),command.scope(),command.requestId());if(stored==null)throw new AuthoritySql.RetryableConflict();if(!stored.hash().equals(command.intentHash()))throw new CommandResultRepository.IntentConflict();return outcome(stored);
        }
        if(invite)return create(c,command,context,bucket);
        if(snapshot.hashVersion()!=context.group().hashVersion()||snapshot.group()!=context.group().group())throw new AuthoritySql.FencedException();
        if(snapshot.terminalAt()!=null)return finish(c,command,new Outcome("FINAL","ALREADY_TERMINAL",snapshot.callId(),snapshot.version(),snapshot.state(),List.of()));
        requireOperation(command,snapshot);
        if(context.expectedCallVersion()>0&&context.expectedCallVersion()!=snapshot.version())return finish(c,command,new Outcome("FINAL","STALE_VERSION",snapshot.callId(),snapshot.version(),snapshot.state(),List.of()));
        return switch(command.type()) {
            case CANCEL,HANGUP,DECLINE_ALL -> terminal(c,command,context,snapshot);
            case ACCEPT -> pending(snapshot.callId());
            default -> throw new IllegalArgumentException("Command requires its typed actor transition or volatile relay path");
        };
    }
    private Outcome create(Connection c,CallCommand command,Authority context,int bucket)throws Exception {
        if(command.target()==null||command.target().equals(command.sender().userId()))throw new AuthorizationRejected();
        Instant now;long sequence;
        try(var s=c.prepareStatement("SELECT clock_timestamp(),lease_sequence FROM group_owner WHERE cell_id=? AND ownership_hash_version=? AND group_id=?")){s.setString(1,cell);s.setLong(2,context.group().hashVersion());s.setInt(3,context.group().group());try(var r=s.executeQuery()){if(!r.next())throw new AuthoritySql.FencedException();now=r.getTimestamp(1).toInstant();sequence=r.getLong(2);}}
        var grant=new HomeParticipationService.Grant(cell,epoch,context.group().hashVersion(),context.group().group(),context.group().epoch(),sequence,command.requestId().value(),now,now.plusSeconds(5),"LOCAL_PRIMARY_TRANSACTION");
        var request=new HomeParticipationService.Request(command.sender().userId(),context.callId(),command.requestId().value(),command.intentHash(),context.directoryEpoch(),HomeParticipationService.Phase.PREPARING,grant);
        try{reservations.reserve(c,request);}catch(UserReservationService.UserBusy busy){return finish(c,command,new Outcome("FINAL","USER_BUSY",null,0,"NONE",List.of()));}
        try(var s=c.prepareStatement("INSERT INTO call_state(call_id,authority_bucket_id,ownership_hash_version,ownership_group_id,caller_user,caller_issuer,caller_jti,caller_incarnation,caller_generation,callee_user,state,version,negotiation_id,saga_phase,deadlines,last_mutation_group_epoch,invite_request_id) VALUES(?,?,?,?,?,?,?,?,?,?,'PREPARING',1,0,'RESERVE_REMOTE',jsonb_build_object('prepareUntil',clock_timestamp()+interval '15 seconds'),?,?)")){
            s.setString(1,context.callId().value());s.setInt(2,bucket);s.setLong(3,context.group().hashVersion());s.setInt(4,context.group().group());s.setString(5,command.sender().userId().value());s.setString(6,command.sender().key().issuer());s.setString(7,command.sender().key().jti());s.setObject(8,command.sender().incarnation().value());s.setLong(9,command.sender().connectionGeneration());s.setString(10,command.target().value());s.setLong(11,context.group().epoch());s.setObject(12,command.requestId().value());s.executeUpdate();
        }return pending(context.callId());
    }
    private Outcome terminal(Connection c,CallCommand command,Authority context,Snapshot previous)throws Exception {
        long version=Math.addExact(previous.version(),1);String reason=command.type().name();
        try(var s=c.prepareStatement("UPDATE call_state SET state='TERMINAL',version=?,saga_phase='COMPENSATE',terminal_reason=?,terminal_at=clock_timestamp(),expires_at=clock_timestamp()+interval '24 hours 5 seconds',updated_at=clock_timestamp(),last_mutation_group_epoch=? WHERE call_id=? AND version=? AND terminal_at IS NULL")){
            s.setLong(1,version);s.setString(2,reason);s.setLong(3,context.group().epoch());s.setString(4,previous.callId().value());s.setLong(5,previous.version());if(s.executeUpdate()!=1)throw new AuthoritySql.RetryableConflict();
        }
        String destination=JSON.writeValueAsString(Map.of("userId",previous.caller().user().value(),"issuer",previous.caller().key().issuer(),"jti",previous.caller().key().jti()));
        UUID event=outbox.insert(c,previous.bucket(),previous.callId(),version,destination,JSON.writeValueAsString(Map.of("type","CALL_TERMINATED","callId",previous.callId().value(),"callVersion",Long.toString(version),"reason",reason)));
        Outcome finalOutcome=new Outcome("FINAL",reason,previous.callId(),version,"TERMINAL",List.of(event));finish(c,command,finalOutcome);
        var origin=results.find(c,previous.caller().key(),CommandScope.invite(),previous.inviteRequest());if(origin!=null&&origin.status().equals("PENDING"))results.finalizeResult(c,previous.caller().key(),CommandScope.invite(),previous.inviteRequest(),finalOutcome);
        return finalOutcome;
    }
    private Outcome finish(Connection c,CallCommand command,Outcome outcome)throws Exception {results.finalizeResult(c,command.sender().key(),command.scope(),command.requestId(),outcome);return outcome;}
    private static Outcome pending(CallId call){return new Outcome("PENDING","WORKFLOW_PENDING",call,0,"PENDING",List.of());}
    private static Outcome outcome(CommandResultRepository.Stored stored){return stored.outcome()!=null?stored.outcome():pending(stored.callId());}
    private void requireLocalCurrent(Connection c,AuthenticatedSession sender)throws SQLException {
        var route=sessions.find(c,sender.key());if(route==null||!route.user().equals(sender.userId())||!route.incarnation().equals(sender.incarnation())||route.connectionGeneration()!=sender.connectionGeneration()||!Objects.equals(route.connectionId(),sender.connectionId()))throw new AuthorizationRejected();sessions.requireCurrent(c,route);
    }
    private static void requirePrincipal(AuthenticatedSession sender,Snapshot snapshot){if(snapshot==null||!snapshot.caller().samePrincipal(sender)&&(snapshot.winner()==null||!snapshot.winner().samePrincipal(sender))&&!snapshot.callee().equals(sender.userId()))throw new AuthorizationRejected();}
    private static void requireOperation(CallCommand command,Snapshot s)throws Exception {
        boolean caller=s.caller().sameBinding(command.sender()),winner=s.winner()!=null&&s.winner().sameBinding(command.sender());
        boolean offered=false;for(var route:JSON.readTree(s.offeredSessions()))if(route.path("issuer").asText().equals(command.sender().key().issuer())&&route.path("jti").asText().equals(command.sender().key().jti()))offered=true;
        boolean allowed=switch(command.type()){
            case CANCEL -> caller&&Set.of("PREPARING","RINGING","ACCEPTED","ACTIVATING").contains(s.state());
            case HANGUP -> (caller||winner)&&Set.of("ACCEPTED","ACTIVATING","CONNECTING","ESTABLISHED").contains(s.state());
            case ACCEPT,REJECT -> offered&&s.callee().equals(command.sender().userId())&&s.state().equals("RINGING");
            case DECLINE_ALL -> s.callee().equals(command.sender().userId())&&s.state().equals("RINGING");
            default -> false;
        };if(!allowed)throw new AuthorizationRejected();
    }
    public CompletionStage<Optional<Outcome>> getCommandResult(AuthenticatedSession sender,CommandScope scope,RequestId request){return sql.submit(DbClass.RECOVERY,Duration.ofSeconds(2),c->{
        AuthoritySql.cellBarrier(c,false);AuthoritySql.validateCell(c,cell,epoch);var hint=results.find(c,sender.key(),scope,request);
        int bucket=hint==null?SessionRegistryService.bucket(sender.userId()):hint.bucket();AuthoritySql.bucketBarrier(c,bucket,false);AuthoritySql.validateBuckets(c,Map.of(bucket,localBucketEpoch(c,bucket)));requireLocalCurrent(c,sender);
        var result=results.find(c,sender.key(),scope,request);return result==null?Optional.empty():Optional.of(outcome(result));
    });}
    public CompletionStage<Snapshot> loadCallSnapshot(AuthenticatedSession sender,CallId call){return sql.submit(DbClass.RECOVERY,Duration.ofSeconds(2),c->{
        AuthoritySql.cellBarrier(c,false);AuthoritySql.validateCell(c,cell,epoch);Snapshot hint=calls.find(c,call);if(hint==null)throw new AuthorizationRejected();AuthoritySql.bucketBarrier(c,hint.bucket(),false);AuthoritySql.validateBuckets(c,Map.of(hint.bucket(),localBucketEpoch(c,hint.bucket())));requireLocalCurrent(c,sender);Snapshot current=calls.find(c,call);requirePrincipal(sender,current);return current;
    });}
    private static long localBucketEpoch(Connection c,int bucket)throws SQLException {try(var s=c.prepareStatement("SELECT directory_epoch FROM bucket_authority WHERE bucket_id=?")){s.setInt(1,bucket);try(var r=s.executeQuery()){if(!r.next())throw new AuthoritySql.FencedException();return r.getLong(1);}}}
    public static final class AuthorizationRejected extends RuntimeException {public AuthorizationRejected(){super("Operation authorization rejected");}}
}
