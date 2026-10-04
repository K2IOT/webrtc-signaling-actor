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
    public record TargetHome(String cell,long directoryEpoch){public TargetHome{Objects.requireNonNull(cell);if(directoryEpoch<1)throw new IllegalArgumentException("Invalid target home hint");}}
    public record Authority(CallId callId,AuthoritySql.GroupToken group,long directoryEpoch,String proof,long expectedCallVersion,TargetHome targetHome) {
        public Authority(CallId callId,AuthoritySql.GroupToken group,long directoryEpoch,String proof,long expectedCallVersion){this(callId,group,directoryEpoch,proof,expectedCallVersion,null);}
        public Authority(CallId callId,AuthoritySql.GroupToken group,long directoryEpoch,String proof){this(callId,group,directoryEpoch,proof,0);}
        public Authority {if(expectedCallVersion<0)throw new IllegalArgumentException("Invalid expected call version");}
    }
    @FunctionalInterface public interface ProofVerifier {boolean verify(CallCommand command,Snapshot snapshot,String proof);}
    public record Outcome(String status,String code,CallId callId,long version,String state,List<UUID> eventIds) implements io.webrtc.signaling.protocol.ApplicationSerializable {public Outcome{eventIds=List.copyOf(eventIds);}}
    /** Two-home evidence must be independently verified under the native coordinator fence. */
    public record NegotiationEvidence(AuthenticatedSession recipient,Instant proofExpiresAt,Instant participantUntil) {
        public NegotiationEvidence{Objects.requireNonNull(recipient);Objects.requireNonNull(proofExpiresAt);Objects.requireNonNull(participantUntil);}
    }
    @FunctionalInterface public interface NegotiationVerifier {NegotiationEvidence verify(Connection c,CallCommand command,Snapshot snapshot,String proof)throws Exception;}
    private final NegotiationVerifier negotiationVerifier;
    private static final ObjectMapper JSON=new ObjectMapper().findAndRegisterModules();
    private final SqlTransactions sql;private final String cell;private final long epoch;
    private final Function<CallCommand,CompletionStage<Authority>> authority;private final ProofVerifier proofs;
    private final CommandResultRepository results=new CommandResultRepository();private final CallSnapshotRepository calls=new CallSnapshotRepository();private final SessionRepository sessions=new SessionRepository();private final OutboxRepository outbox;
    private final HomeParticipationService localHome;private final UserReservationService reservations;
    public CallCommandService(SqlTransactions sql,String cell,long epoch,Function<CallCommand,CompletionStage<Authority>> authority,ProofVerifier proofs){
        this(sql,cell,epoch,authority,proofs,null);
    }
    public CallCommandService(SqlTransactions sql,String cell,long epoch,Function<CallCommand,CompletionStage<Authority>> authority,ProofVerifier proofs,NegotiationVerifier negotiationVerifier){
        this.negotiationVerifier=negotiationVerifier;
        this.sql=sql;this.cell=cell;this.epoch=epoch;this.authority=Objects.requireNonNull(authority);this.proofs=Objects.requireNonNull(proofs);outbox=new OutboxRepository(sql,cell,epoch);
        // Used only by the local native transaction after its full primary group/session checks.
        // Remote calls must use the independently supplied request-bound verifier.
        localHome=new HomeParticipationService(sql,cell,epoch,r->false);reservations=new UserReservationService(localHome);
    }
    public CompletionStage<Outcome> executeCallCommand(CallCommand command){return executeCallCommand(command,Duration.ofSeconds(2));}
    /** Caller has already resolved its local shard's exact authority. Preserve the physical handle. */
    public DbOperation<Outcome> executeUnderAuthorityTracked(CallCommand command,Authority context,Duration budget){
        DbClass clazz=command.type()==SignalEnvelope.Type.CANCEL||command.type()==SignalEnvelope.Type.HANGUP?DbClass.TERMINATION:DbClass.CRITICAL;
        return sql.submitTracked(clazz,budget,c->execute(c,command,context));
    }
    public CompletionStage<Outcome> executeCallCommand(CallCommand command,Duration budget){
        if(budget.isNegative()||budget.isZero())return CompletableFuture.<Outcome>failedFuture(new DbOverloadedException()).minimalCompletionStage();
        long start=System.nanoTime();return authority.apply(command).thenCompose(context->{Duration remaining=budget.minusNanos(Math.max(0,System.nanoTime()-start));DbClass clazz=command.type()==SignalEnvelope.Type.CANCEL||command.type()==SignalEnvelope.Type.HANGUP?DbClass.TERMINATION:DbClass.CRITICAL;return sql.submit(clazz,remaining,c->execute(c,command,context));});
    }
    private Outcome execute(Connection c,CallCommand command,Authority context)throws Exception {
        boolean invite=command.type()==SignalEnvelope.Type.INVITE;
        if(!command.scope().equals(invite?CommandScope.invite():CommandScope.call(command.callId()))||!context.callId().coordinatorCell().equals(cell)||context.callId().routingEpoch()!=epoch||!context.group().cell().equals(cell)||context.group().storageEpoch()!=epoch||context.group().group()!=HomeParticipationService.group(context.callId())||!invite&&!context.callId().equals(command.callId()))throw new AuthorizationRejected();
        int bucket;
        if(invite)bucket=SessionRegistryService.bucket(command.sender().userId());
        else {Snapshot hint=calls.find(c,context.callId());if(hint==null)throw new AuthorizationRejected();bucket=hint.bucket();}
        var buckets=new TreeMap<Integer,Long>();buckets.put(bucket,context.directoryEpoch());var users=new ArrayList<String>();if(invite)users.add(command.sender().userId().value());
        boolean localPair=invite&&context.targetHome()!=null&&cell.equals(context.targetHome().cell());if(localPair){if(command.target()==null||command.target().equals(command.sender().userId()))throw new AuthorizationRejected();int targetBucket=SessionRegistryService.bucket(command.target());Long existing=buckets.put(targetBucket,context.targetHome().directoryEpoch());if(existing!=null&&existing!=context.targetHome().directoryEpoch())throw new AuthoritySql.FencedException();users.add(command.target().value());}
        AuthoritySql.coordinator(c,context.group(),buckets,users,List.of(context.callId().value()));
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
        if(command.type()==SignalEnvelope.Type.ACCEPT&&snapshot.winner()!=null){
            if(!snapshot.callee().equals(command.sender().userId()))throw new AuthorizationRejected();
            return finish(c,command,new Outcome("FINAL",snapshot.winner().samePrincipal(command.sender())?"ACCEPTED_PENDING_ACTIVATION":"ANSWERED_ELSEWHERE",snapshot.callId(),snapshot.version(),snapshot.state(),List.of()));
        }
        requireOperation(command,snapshot);
        if(context.expectedCallVersion()>0&&context.expectedCallVersion()!=snapshot.version())return finish(c,command,new Outcome("FINAL","STALE_VERSION",snapshot.callId(),snapshot.version(),snapshot.state(),List.of()));
        return switch(command.type()) {
            case CANCEL,HANGUP,DECLINE_ALL -> terminal(c,command,context,snapshot);
            case ACCEPT -> pending(snapshot.callId());
            case NEGOTIATE_REQUEST -> negotiate(c,command,context,snapshot);
            default -> throw new IllegalArgumentException("Command requires its typed actor transition or volatile relay path");
        };
    }
    private Outcome create(Connection c,CallCommand command,Authority context,int bucket)throws Exception {
        if(command.target()==null||command.target().equals(command.sender().userId()))throw new AuthorizationRejected();
        boolean local=context.targetHome()!=null&&cell.equals(context.targetHome().cell());var routes=local?sessions.liveRoutes(c,command.target()):List.<SessionRepository.Route>of();if(local&&routes.isEmpty())return finish(c,command,new Outcome("FINAL","UNREACHABLE",null,0,"NONE",List.of()));
        Instant now;long sequence;
        try(var s=c.prepareStatement("SELECT clock_timestamp(),lease_sequence FROM group_owner WHERE cell_id=? AND ownership_hash_version=? AND group_id=?")){s.setString(1,cell);s.setLong(2,context.group().hashVersion());s.setInt(3,context.group().group());try(var r=s.executeQuery()){if(!r.next())throw new AuthoritySql.FencedException();now=r.getTimestamp(1).toInstant();sequence=r.getLong(2);}}
        var grant=new HomeParticipationService.Grant(cell,epoch,context.group().hashVersion(),context.group().group(),context.group().epoch(),sequence,command.requestId().value(),now,now.plusSeconds(5),"LOCAL_PRIMARY_TRANSACTION");
        var request=new HomeParticipationService.Request(command.sender().userId(),context.callId(),command.requestId().value(),command.intentHash(),context.directoryEpoch(),local?HomeParticipationService.Phase.RINGING:HomeParticipationService.Phase.PREPARING,grant);
        Savepoint reservationPair=c.setSavepoint();try{reservations.reserve(c,request);if(local)reservations.reserve(c,new HomeParticipationService.Request(command.target(),context.callId(),command.requestId().value(),command.intentHash(),context.targetHome().directoryEpoch(),HomeParticipationService.Phase.RINGING,grant));}catch(UserReservationService.UserBusy busy){c.rollback(reservationPair);return finish(c,command,new Outcome("FINAL","USER_BUSY",null,0,"NONE",List.of()));}finally{c.releaseSavepoint(reservationPair);}
        var offered=routes.stream().map(r->new Participant(r.user(),r.key(),r.incarnation(),r.connectionGeneration())).toList();String state=local?"RINGING":"PREPARING";
        try(var s=c.prepareStatement("INSERT INTO call_state(call_id,authority_bucket_id,ownership_hash_version,ownership_group_id,caller_user,caller_issuer,caller_jti,caller_incarnation,caller_generation,callee_user,state,version,negotiation_id,saga_phase,deadlines,offered_sessions,last_mutation_group_epoch,invite_request_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,1,0,?,jsonb_build_object(?,clock_timestamp()+(? * interval '1 second')),?::jsonb,?,?)")){
            s.setString(1,context.callId().value());s.setInt(2,bucket);s.setLong(3,context.group().hashVersion());s.setInt(4,context.group().group());s.setString(5,command.sender().userId().value());s.setString(6,command.sender().key().issuer());s.setString(7,command.sender().key().jti());s.setObject(8,command.sender().incarnation().value());s.setLong(9,command.sender().connectionGeneration());s.setString(10,command.target().value());s.setString(11,state);s.setString(12,local?"WAIT_ACCEPT":"RESERVE_REMOTE");s.setString(13,local?"ringUntil":"prepareUntil");s.setInt(14,local?30:15);s.setString(15,JSON.writeValueAsString(offered));s.setLong(16,context.group().epoch());s.setObject(17,command.requestId().value());s.executeUpdate();
        }
        if(!local)return pending(context.callId());var recipients=new ArrayList<Participant>();recipients.add(new Participant(command.sender().userId(),command.sender().key(),command.sender().incarnation(),command.sender().connectionGeneration()));recipients.addAll(offered);var events=new ArrayList<UUID>();for(var recipient:recipients){String destination=JSON.writeValueAsString(Map.of("userId",recipient.user().value(),"issuer",recipient.key().issuer(),"jti",recipient.key().jti()));String payload=JSON.writeValueAsString(Map.of("type","RINGING","callId",context.callId().value(),"callVersion","1","callerUserId",command.sender().userId().value(),"calleeUserId",command.target().value()));events.add(outbox.insert(c,bucket,context.callId(),1,destination,payload));}
        return finish(c,command,new Outcome("FINAL","RINGING",context.callId(),1,"RINGING",List.copyOf(events)));
    }
    private Outcome negotiate(Connection c,CallCommand command,Authority context,Snapshot previous)throws Exception {
        if(negotiationVerifier==null||previous.activationId()==null||previous.winner()==null)throw new AuthorizationRejected();
        Instant now;try(var q=c.createStatement();var r=q.executeQuery("SELECT clock_timestamp()")){r.next();now=r.getTimestamp(1).toInstant();}
        var evidence=negotiationVerifier.verify(c,command,previous,context.proof());
        var peer=previous.caller().sameBinding(command.sender())?previous.winner():previous.caller();
        if(evidence==null||!peer.sameBinding(evidence.recipient())||!evidence.proofExpiresAt().isAfter(now)
                ||evidence.proofExpiresAt().isAfter(now.plusSeconds(5))||!evidence.participantUntil().isAfter(now.plusSeconds(5)))throw new AuthorizationRejected();
        try(var q=c.prepareStatement("SELECT lease_until>clock_timestamp()+interval '5 seconds' FROM user_reservation WHERE user_id=? AND call_id=?")){
            q.setString(1,previous.caller().user().value());q.setString(2,previous.callId().value());try(var r=q.executeQuery()){if(!r.next()||!r.getBoolean(1))throw new AuthoritySql.FencedException();}
        }
        if(previous.negotiationId()==0&&!previous.caller().sameBinding(command.sender()))throw new AuthorizationRejected();
        var metadata=(com.fasterxml.jackson.databind.node.ObjectNode)JSON.readTree(previous.deadlines());
        Instant previousUntil=metadata.has("negotiationUntil")?Instant.parse(metadata.get("negotiationUntil").asText()):null;
        if(previous.negotiationId()>0&&!"COMPLETE".equals(metadata.path("negotiationState").asText())
                &&previousUntil!=null&&previousUntil.isAfter(now))return finish(c,command,new Outcome("FINAL","NEGOTIATION_BUSY",previous.callId(),previous.version(),previous.state(),List.of()));
        if(previous.state().equals("CONNECTING")&&previousUntil!=null&&!previousUntil.isAfter(now))throw new AuthoritySql.FencedException();
        long round=Math.addExact(previous.negotiationId(),1),ice=Math.addExact(Long.parseLong(metadata.path("iceGeneration").asText("0")),1),version=Math.addExact(previous.version(),1);
        Instant until=now.plusSeconds(20);if(previous.state().equals("CONNECTING")&&previousUntil!=null&&previousUntil.isBefore(until))until=previousUntil;
        metadata.put("iceGeneration",Long.toString(ice));metadata.put("negotiationUntil",until.toString());metadata.put("negotiationState","OFFER_GRANTED");
        metadata.set("offerer",JSON.valueToTree(command.sender()));metadata.set("answerer",JSON.valueToTree(evidence.recipient()));metadata.put("negotiationRequest",command.requestId().value().toString());
        try(var q=c.prepareStatement("UPDATE call_state SET negotiation_id=?,version=?,deadlines=?::jsonb,last_mutation_group_epoch=?,updated_at=clock_timestamp() WHERE call_id=? AND version=? AND terminal_at IS NULL")){
            q.setLong(1,round);q.setLong(2,version);q.setString(3,JSON.writeValueAsString(metadata));q.setLong(4,context.group().epoch());q.setString(5,previous.callId().value());q.setLong(6,previous.version());if(q.executeUpdate()!=1)throw new AuthoritySql.RetryableConflict();
        }
        var events=new ArrayList<UUID>();for(var recipient:List.of(command.sender(),evidence.recipient())){
            String destination=JSON.writeValueAsString(Map.of("userId",recipient.userId().value(),"issuer",recipient.key().issuer(),"jti",recipient.key().jti()));
            String payload=JSON.writeValueAsString(Map.of("type","NEGOTIATION_GRANTED","callId",previous.callId().value(),"callVersion",Long.toString(version),"negotiationId",Long.toString(round),"iceGeneration",Long.toString(ice),"offerer",command.sender().userId().value(),"negotiationUntil",until.toString()));
            events.add(outbox.insert(c,previous.bucket(),previous.callId(),version,destination,payload));
        }
        return finish(c,command,new Outcome("FINAL","NEGOTIATION_GRANTED",previous.callId(),version,previous.state(),events));
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
        boolean offered=false;for(var route:JSON.readTree(s.offeredSessions()))if(JSON.treeToValue(route,Participant.class).samePrincipal(command.sender()))offered=true;
        boolean allowed=switch(command.type()){
            case CANCEL -> caller&&Set.of("PREPARING","RINGING","ACCEPTED","ACTIVATING").contains(s.state());
            case HANGUP -> (caller||winner)&&Set.of("ACCEPTED","ACTIVATING","CONNECTING","ESTABLISHED").contains(s.state());
            case ACCEPT,REJECT -> offered&&s.callee().equals(command.sender().userId())&&s.state().equals("RINGING");
            case NEGOTIATE_REQUEST -> (caller||winner)&&Set.of("CONNECTING","ESTABLISHED").contains(s.state());
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
