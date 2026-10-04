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
        public Authority {if(expectedCallVersion<0||directoryEpoch<0)throw new IllegalArgumentException("Invalid expected call authority");}
    }
    @FunctionalInterface public interface ProofVerifier {boolean verify(CallCommand command,Snapshot snapshot,String proof);}
    public record Outcome(String status,String code,CallId callId,long version,String state,List<UUID> eventIds) implements io.webrtc.signaling.protocol.ApplicationSerializable {public Outcome{eventIds=List.copyOf(eventIds);}}
    /** Two-home evidence must be independently verified under the native coordinator fence. */
    public record NegotiationEvidence(AuthenticatedSession recipient,Instant proofExpiresAt,Instant participantUntil) {
        public NegotiationEvidence{Objects.requireNonNull(recipient);Objects.requireNonNull(proofExpiresAt);Objects.requireNonNull(participantUntil);}
    }
    @FunctionalInterface public interface NegotiationVerifier {NegotiationEvidence verify(Connection c,CallCommand command,Snapshot snapshot,Authority authority)throws Exception;}
    private final NegotiationVerifier negotiationVerifier;
    private static final ObjectMapper JSON=new ObjectMapper().findAndRegisterModules();
    private final SqlTransactions sql;private final String cell;private final long epoch;private final long routingEpoch;
    private final Function<CallCommand,CompletionStage<Authority>> authority;private final ProofVerifier proofs;
    private final CommandResultRepository results=new CommandResultRepository();private final CallSnapshotRepository calls=new CallSnapshotRepository();private final SessionRepository sessions=new SessionRepository();private final OutboxRepository outbox;
    private final HomeParticipationService localHome;private final UserReservationService reservations;
    public CallCommandService(SqlTransactions sql,String cell,long epoch,Function<CallCommand,CompletionStage<Authority>> authority,ProofVerifier proofs){
        this(sql,cell,epoch,authority,proofs,null);
    }
    public CallCommandService(SqlTransactions sql,String cell,long epoch,Function<CallCommand,CompletionStage<Authority>> authority,ProofVerifier proofs,NegotiationVerifier negotiationVerifier){
        this(sql,cell,epoch,epoch,authority,proofs,negotiationVerifier);
    }
    /** Routing allocation is independently configured; storage promotion never rewrites call IDs. */
    public CallCommandService(SqlTransactions sql,String cell,long epoch,long routingEpoch,Function<CallCommand,CompletionStage<Authority>> authority,ProofVerifier proofs,NegotiationVerifier negotiationVerifier){
        if(epoch<1||routingEpoch<1)throw new IllegalArgumentException("Invalid authority epochs");
        this.routingEpoch=routingEpoch;this.negotiationVerifier=negotiationVerifier;
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
        if(invite&&context.directoryEpoch()<1)throw new AuthorizationRejected();
        if(!command.scope().equals(invite?CommandScope.invite():CommandScope.call(command.callId()))||!context.callId().coordinatorCell().equals(cell)||invite&&context.callId().routingEpoch()!=routingEpoch||!context.group().cell().equals(cell)||context.group().storageEpoch()!=epoch||context.group().group()!=HomeParticipationService.group(context.callId())||!invite&&!context.callId().equals(command.callId()))throw new AuthorizationRejected();
        int bucket;
        if(invite)bucket=SessionRegistryService.bucket(command.sender().userId());
        else {Snapshot hint=calls.find(c,context.callId());if(hint==null)throw new AuthorizationRejected();bucket=hint.bucket();}
        var buckets=new TreeMap<Integer,Long>();buckets.put(bucket,context.directoryEpoch());var users=new ArrayList<String>();if(invite)users.add(command.sender().userId().value());
        boolean localPair=invite&&context.targetHome()!=null&&cell.equals(context.targetHome().cell());if(localPair){if(command.target()==null||command.target().equals(command.sender().userId()))throw new AuthorizationRejected();int targetBucket=SessionRegistryService.bucket(command.target());Long existing=buckets.put(targetBucket,context.targetHome().directoryEpoch());if(existing!=null&&existing!=context.targetHome().directoryEpoch())throw new AuthoritySql.FencedException();users.add(command.target().value());}
        if(!invite&&command.sender().userId().equals(calls.find(c,context.callId()).caller().user()))users.add(command.sender().userId().value());
        if(!invite&&context.directoryEpoch()==0)AuthoritySql.coordinatorCurrentBucket(c,context.group(),bucket,users,context.callId().value());
        else AuthoritySql.coordinator(c,context.group(),buckets,users,List.of(context.callId().value()));
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
            case RESUME -> rebind(c,command,context,snapshot);
            case MEDIA_CONNECTED -> mediaConnected(c,command,context,snapshot);
            case MEDIA_DISCONNECTED,ICE_RESTARTING,MEDIA_FAILED,MEDIA_RECOVERED -> mediaObserved(c,command,context,snapshot);
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
        var evidence=negotiationVerifier.verify(c,command,previous,context);
        var peer=previous.caller().sameBinding(command.sender())?previous.winner():previous.caller();
        if(evidence==null||!peer.sameBinding(evidence.recipient())||!evidence.proofExpiresAt().isAfter(now)
                ||evidence.proofExpiresAt().isAfter(now.plusSeconds(5))||!evidence.participantUntil().isAfter(now.plusSeconds(5)))throw new AuthorizationRejected();
        try(var q=c.prepareStatement("SELECT lease_until>clock_timestamp()+interval '5 seconds' FROM user_reservation WHERE user_id=? AND call_id=?")){
            q.setString(1,previous.caller().user().value());q.setString(2,previous.callId().value());try(var r=q.executeQuery()){if(!r.next()||!r.getBoolean(1))throw new AuthoritySql.FencedException();}
        }
        if(previous.negotiationId()==0&&!previous.caller().sameBinding(command.sender()))throw new AuthorizationRejected();
        var metadata=(com.fasterxml.jackson.databind.node.ObjectNode)JSON.readTree(previous.deadlines());
        Instant previousUntil=metadata.has("negotiationUntil")?Instant.parse(metadata.get("negotiationUntil").asText()):null;
        if(previous.negotiationId()>0&&!Set.of("COMPLETE","INVALIDATED").contains(metadata.path("negotiationState").asText())
                &&previousUntil!=null&&previousUntil.isAfter(now))return finish(c,command,new Outcome("FINAL","NEGOTIATION_BUSY",previous.callId(),previous.version(),previous.state(),List.of()));
        if(previous.state().equals("CONNECTING")&&previousUntil!=null&&!previousUntil.isAfter(now))throw new AuthoritySql.FencedException();
        if(metadata.has("mediaRecoveryUntil")){if(!Instant.parse(metadata.get("mediaRecoveryUntil").asText()).isAfter(now)||metadata.path("mediaRestartAttempts").asInt()>=1)return terminal(c,command,context,previous,"MEDIA_RECOVERY_EXHAUSTED");if(Instant.parse(metadata.get("mediaRestartAfter").asText()).isAfter(now))return finish(c,command,new Outcome("FINAL","MEDIA_DEBOUNCE",previous.callId(),previous.version(),previous.state(),List.of()));metadata.put("mediaRestartAttempts",metadata.path("mediaRestartAttempts").asInt()+1);}
        long round=Math.addExact(previous.negotiationId(),1),ice=Math.addExact(Long.parseLong(metadata.path("iceGeneration").asText("0")),1),version=Math.addExact(previous.version(),1);
        Instant until=now.plusSeconds(20);if(previous.state().equals("CONNECTING")&&previousUntil!=null&&previousUntil.isBefore(until))until=previousUntil;
        metadata.put("iceGeneration",Long.toString(ice));metadata.put("negotiationUntil",until.toString());metadata.put("negotiationState","OFFER_GRANTED");
        metadata.set("offerer",JSON.valueToTree(command.sender()));metadata.set("answerer",JSON.valueToTree(evidence.recipient()));metadata.put("negotiationRequest",command.requestId().value().toString());metadata.put("mediaCallerConnected",false);metadata.put("mediaWinnerConnected",false);
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
    private Outcome rebind(Connection c,CallCommand command,Authority context,Snapshot previous)throws Exception {
        boolean caller=previous.caller().samePrincipal(command.sender());var bound=caller?previous.caller():previous.winner();
        if(bound==null||!bound.incarnation().equals(command.sender().incarnation())||command.sender().connectionGeneration()<bound.generation())throw new AuthorizationRejected();
        // The caller is native to this coordinator. Remote winners require the independently verified full session proof.
        if(caller){try{requireLocalCurrent(c,command.sender());}catch(AuthorizationRejected rejected){throw rejected;}catch(AuthoritySql.FencedException stale){throw new AuthorizationRejected();}}
        var metadata=(com.fasterxml.jackson.databind.node.ObjectNode)JSON.readTree(previous.deadlines());String field=caller?"callerRoute":"winnerRoute",grace=caller?"callerGraceUntil":"winnerGraceUntil";
        if(metadata.has(grace)){Instant nativeNow;try(var q=c.createStatement();var r=q.executeQuery("SELECT clock_timestamp()")){r.next();nativeNow=r.getTimestamp(1).toInstant();}if(!Instant.parse(metadata.get(grace).asText()).isAfter(nativeNow))return terminal(c,command,context,previous,"RECONNECT_TIMEOUT");}
        if(metadata.has(field)&&JSON.treeToValue(metadata.get(field),AuthenticatedSession.class).equals(command.sender()))return finish(c,command,new Outcome("FINAL","RESUMED",previous.callId(),previous.version(),previous.state(),List.of()));
        metadata.set(field,JSON.valueToTree(command.sender()));metadata.remove(caller?"callerGraceUntil":"winnerGraceUntil");long version=Math.addExact(previous.version(),1);
        String column=caller?"caller_generation":"winner_generation";
        try(var q=c.prepareStatement("UPDATE call_state SET "+column+"=?,version=?,deadlines=?::jsonb,last_mutation_group_epoch=?,updated_at=clock_timestamp() WHERE call_id=? AND version=? AND terminal_at IS NULL")){
            q.setLong(1,command.sender().connectionGeneration());q.setLong(2,version);q.setString(3,JSON.writeValueAsString(metadata));q.setLong(4,context.group().epoch());q.setString(5,previous.callId().value());q.setLong(6,previous.version());if(q.executeUpdate()!=1)throw new AuthoritySql.RetryableConflict();
        }
        return finish(c,command,new Outcome("FINAL","RESUMED",previous.callId(),version,previous.state(),List.of()));
    }
    private Outcome mediaConnected(Connection c,CallCommand command,Authority context,Snapshot previous)throws Exception {
        if(negotiationVerifier==null||previous.activationId()==null||previous.winner()==null||command.negotiationId()==null||command.iceGeneration()==null||command.negotiationId().value()!=previous.negotiationId())throw new AuthorizationRejected();
        var metadata=(com.fasterxml.jackson.databind.node.ObjectNode)JSON.readTree(previous.deadlines());if(command.iceGeneration().value()!=Long.parseLong(metadata.path("iceGeneration").asText("0"))||"INVALIDATED".equals(metadata.path("negotiationState").asText()))throw new AuthorizationRejected();
        var evidence=negotiationVerifier.verify(c,command,previous,context);var peer=previous.caller().sameBinding(command.sender())?previous.winner():previous.caller();Instant now;
        try(var q=c.createStatement();var r=q.executeQuery("SELECT clock_timestamp()")){r.next();now=r.getTimestamp(1).toInstant();}
        if(evidence==null||!peer.sameBinding(evidence.recipient())||!evidence.proofExpiresAt().isAfter(now)||evidence.proofExpiresAt().isAfter(now.plusSeconds(5))||!evidence.participantUntil().isAfter(now.plusSeconds(5)))throw new AuthorizationRejected();
        boolean caller=previous.caller().sameBinding(command.sender());String flag=caller?"mediaCallerConnected":"mediaWinnerConnected";
        if(metadata.path(flag).asBoolean())return finish(c,command,new Outcome("FINAL","MEDIA_RECORDED",previous.callId(),previous.version(),previous.state(),List.of()));
        metadata.put(flag,true);boolean both=metadata.path("mediaCallerConnected").asBoolean()&&metadata.path("mediaWinnerConnected").asBoolean();if(both){metadata.put("negotiationState","COMPLETE");metadata.remove("mediaRecoveryUntil");}
        long version=Math.addExact(previous.version(),1);String state=both?"ESTABLISHED":previous.state();
        try(var q=c.prepareStatement("UPDATE call_state SET state=?,version=?,deadlines=?::jsonb,last_mutation_group_epoch=?,updated_at=clock_timestamp() WHERE call_id=? AND version=? AND terminal_at IS NULL")){
            q.setString(1,state);q.setLong(2,version);q.setString(3,JSON.writeValueAsString(metadata));q.setLong(4,context.group().epoch());q.setString(5,previous.callId().value());q.setLong(6,previous.version());if(q.executeUpdate()!=1)throw new AuthoritySql.RetryableConflict();
        }
        var events=new ArrayList<UUID>();if(both)for(var recipient:List.of(previous.caller(),previous.winner())){
            String destination=JSON.writeValueAsString(Map.of("userId",recipient.user().value(),"issuer",recipient.key().issuer(),"jti",recipient.key().jti()));String payload=JSON.writeValueAsString(Map.of("type","ESTABLISHED","callId",previous.callId().value(),"callVersion",Long.toString(version),"negotiationId",Long.toString(previous.negotiationId()),"iceGeneration",metadata.get("iceGeneration").asText()));events.add(outbox.insert(c,previous.bucket(),previous.callId(),version,destination,payload));
        }
        return finish(c,command,new Outcome("FINAL",both?"ESTABLISHED":"MEDIA_RECORDED",previous.callId(),version,state,events));
    }
    private Outcome mediaObserved(Connection c,CallCommand command,Authority context,Snapshot previous)throws Exception {
        var metadata=(com.fasterxml.jackson.databind.node.ObjectNode)JSON.readTree(previous.deadlines());if(command.negotiationId()==null||command.iceGeneration()==null||command.negotiationId().value()!=previous.negotiationId()||command.iceGeneration().value()!=Long.parseLong(metadata.path("iceGeneration").asText("0")))throw new AuthorizationRejected();
        Instant now;try(var q=c.createStatement();var r=q.executeQuery("SELECT clock_timestamp()")){r.next();now=r.getTimestamp(1).toInstant();}
        if(metadata.has("mediaRecoveryUntil")&&!Instant.parse(metadata.get("mediaRecoveryUntil").asText()).isAfter(now))return terminal(c,command,context,previous,"MEDIA_RECOVERY_EXHAUSTED");
        boolean caller=previous.caller().sameBinding(command.sender());String connected=caller?"mediaCallerConnected":"mediaWinnerConnected";
        if(command.type()==SignalEnvelope.Type.MEDIA_RECOVERED){metadata.put(connected,true);if(metadata.path("mediaCallerConnected").asBoolean()&&metadata.path("mediaWinnerConnected").asBoolean()){metadata.remove("mediaRecoveryUntil");metadata.remove("mediaRestartAfter");metadata.put("mediaRestartAttempts",0);}}
        else{metadata.put(connected,false);if(!metadata.has("mediaRecoveryUntil")){metadata.put("mediaRecoveryUntil",now.plusSeconds(30).toString());metadata.put("mediaRestartAfter",now.plusSeconds(command.type()==SignalEnvelope.Type.MEDIA_DISCONNECTED?5:0).toString());metadata.put("mediaRestartAttempts",0);}if(command.type()==SignalEnvelope.Type.MEDIA_FAILED&&metadata.path("mediaRestartAttempts").asInt()>=1)return terminal(c,command,context,previous,"MEDIA_RECOVERY_EXHAUSTED");}
        long version=Math.addExact(previous.version(),1);try(var q=c.prepareStatement("UPDATE call_state SET version=?,deadlines=?::jsonb,last_mutation_group_epoch=?,updated_at=clock_timestamp() WHERE call_id=? AND version=? AND terminal_at IS NULL")){q.setLong(1,version);q.setString(2,JSON.writeValueAsString(metadata));q.setLong(3,context.group().epoch());q.setString(4,previous.callId().value());q.setLong(5,previous.version());if(q.executeUpdate()!=1)throw new AuthoritySql.RetryableConflict();}return finish(c,command,new Outcome("FINAL","MEDIA_RECORDED",previous.callId(),version,previous.state(),List.of()));
    }
    private Outcome terminal(Connection c,CallCommand command,Authority context,Snapshot previous)throws Exception {
        return terminal(c,command,context,previous,command.type().name());
    }
    private Outcome terminal(Connection c,CallCommand command,Authority context,Snapshot previous,String reason)throws Exception {
        long version=Math.addExact(previous.version(),1);
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
            case RESUME -> (s.caller().samePrincipal(command.sender())||s.winner()!=null&&s.winner().samePrincipal(command.sender()))&&Set.of("ACCEPTED","ACTIVATING","CONNECTING","ESTABLISHED").contains(s.state());
            case MEDIA_CONNECTED,MEDIA_DISCONNECTED,ICE_RESTARTING,MEDIA_FAILED,MEDIA_RECOVERED -> (caller||winner)&&Set.of("CONNECTING","ESTABLISHED").contains(s.state());
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
    public DbOperation<Snapshot> loadCallSnapshotAuthorized(CallCommand read,String proof,Duration budget){
        if(read.type()!=SignalEnvelope.Type.SYNC_CALL||read.callId()==null||!read.scope().equals(CommandScope.call(read.callId())))throw new IllegalArgumentException("Invalid native snapshot read");
        return sql.submitTracked(DbClass.RECOVERY,budget,c->authorizedRead(c,read,proof));
    }
    public DbOperation<Optional<Outcome>> getCommandResultAuthorized(CallCommand read,String proof,Duration budget){
        if(read.type()!=SignalEnvelope.Type.GET_COMMAND_RESULT||read.callId()==null||!read.scope().equals(CommandScope.call(read.callId())))throw new IllegalArgumentException("Invalid native command result read");
        return sql.submitTracked(DbClass.RECOVERY,budget,c->{authorizedRead(c,read,proof);var stored=results.find(c,read.sender().key(),read.scope(),read.requestId());return stored==null?Optional.empty():Optional.of(outcome(stored));});
    }
    public record CriticalContext(Snapshot snapshot,String originalInviteHash) {}
    /** Internal auth-only projection. Original acquisition identity comes from guarded durable history. */
    public DbOperation<CriticalContext> criticalContextAuthorized(CallCommand command,String proof,Duration budget){
        if(!Set.of(SignalEnvelope.Type.NEGOTIATE_REQUEST,SignalEnvelope.Type.MEDIA_CONNECTED).contains(command.type())||command.callId()==null||!command.scope().equals(CommandScope.call(command.callId())))throw new IllegalArgumentException("Invalid critical command context");
        return sql.submitTracked(DbClass.RECOVERY,budget,c->{
            var snapshot=authorizedRead(c,command,proof);
            var original=results.find(c,snapshot.caller().key(),CommandScope.invite(),snapshot.inviteRequest());
            if(original==null||!snapshot.callId().equals(original.callId()))throw new AuthorizationRejected();
            return new CriticalContext(snapshot,original.hash());
        });
    }
    private Snapshot authorizedRead(Connection c,CallCommand read,String proof)throws Exception {
        AuthoritySql.cellBarrier(c,false);AuthoritySql.validateCell(c,cell,epoch);Snapshot hint=calls.find(c,read.callId());if(hint==null||!cell.equals(read.callId().coordinatorCell()))throw new AuthorizationRejected();
        AuthoritySql.bucketBarrier(c,hint.bucket(),false);AuthoritySql.validateBuckets(c,Map.of(hint.bucket(),localBucketEpoch(c,hint.bucket())));AuthoritySql.callReadBarrier(c,read.callId().value());
        Snapshot snapshot=calls.find(c,read.callId());if(snapshot==null||!proofs.verify(read,snapshot,proof))throw new AuthorizationRejected();requirePrincipal(read.sender(),snapshot);return snapshot;
    }
    private static long localBucketEpoch(Connection c,int bucket)throws SQLException {try(var s=c.prepareStatement("SELECT directory_epoch FROM bucket_authority WHERE bucket_id=?")){s.setInt(1,bucket);try(var r=s.executeQuery()){if(!r.next())throw new AuthoritySql.FencedException();return r.getLong(1);}}}
    public static final class AuthorizationRejected extends RuntimeException {public AuthorizationRejected(){super("Operation authorization rejected");}}
}
