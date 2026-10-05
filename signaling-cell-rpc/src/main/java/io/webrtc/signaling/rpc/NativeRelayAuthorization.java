package io.webrtc.signaling.rpc;

import io.webrtc.signaling.actors.relay.RelayAuthorizationCache;
import io.webrtc.signaling.actors.relay.NegotiationRelay;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Traffic-driven cache-miss producer: native committed round, hosting grant and both signed ACTIVE homes. */
public final class NativeRelayAuthorization {
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON=new com.fasterxml.jackson.databind.ObjectMapper(com.fasterxml.jackson.core.JsonFactory.builder().enable(com.fasterxml.jackson.core.StreamReadFeature.STRICT_DUPLICATE_DETECTION).streamReadConstraints(com.fasterxml.jackson.core.StreamReadConstraints.builder().maxNestingDepth(16).maxStringLength(8192).build()).build()).findAndRegisterModules().enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    public record AuthorizedRound(RelayAuthorizationCache.Snapshot authorization,NegotiationRelay.Grant grant,Instant authorizationUntil) {
        public AuthorizedRound {
            Objects.requireNonNull(authorization);Objects.requireNonNull(grant);Objects.requireNonNull(authorizationUntil);
            if(!authorization.callId().equals(grant.call())||!authorization.activationId().equals(grant.activationId())||authorization.callVersion()!=grant.callVersion()||authorization.negotiationId()!=grant.negotiationId()||authorization.iceGeneration()!=grant.iceGeneration()||!authorization.group().equals(grant.group())
                ||!(authorization.sender().equals(grant.offerer())&&authorization.recipient().equals(grant.answerer())||authorization.sender().equals(grant.answerer())&&authorization.recipient().equals(grant.offerer())))throw new IllegalArgumentException("Round grant differs from native authorization");
        }
    }
    // Maximum enrolled pair uncertainty plus 1,000ppm drift over the five-second cache lifetime.
    private static final Duration CLOCK_MARGIN=Duration.ofMillis(255);
    private final CallCommandService commands;private final NativeHomeProofClient homes;private final HomeAuthorizationProof proofs;
    private final Function<UserId,ProofBindings.TrustedHome> directory;private final Function<CallId,Optional<AuthoritySql.GroupToken>> currentGroup;
    private final Clock clock;private final BooleanSupplier trusted;
    public NativeRelayAuthorization(CallCommandService commands,NativeHomeProofClient homes,HomeAuthorizationProof proofs,
            Function<UserId,ProofBindings.TrustedHome> directory,Function<CallId,Optional<AuthoritySql.GroupToken>> currentGroup,Clock clock,BooleanSupplier trusted){
        this.commands=Objects.requireNonNull(commands);this.homes=Objects.requireNonNull(homes);this.proofs=Objects.requireNonNull(proofs);
        this.directory=Objects.requireNonNull(directory);this.currentGroup=Objects.requireNonNull(currentGroup);this.clock=Objects.requireNonNull(clock);this.trusted=Objects.requireNonNull(trusted);
    }
    public RpcOperation<RelayAuthorizationCache.Snapshot> load(CallCommand command,String sessionProof,Duration budget){
        var original=loadRound(command,sessionProof,budget);return new RpcOperation<>(original.logical().thenApply(AuthorizedRound::authorization),original.physicalCompletion());
    }
    public RpcOperation<AuthorizedRound> loadRound(CallCommand command,String sessionProof,Duration budget){
        var scope=new PhysicalScope();long started=System.nanoTime();var startedWall=clock.instant();
        CompletionStage<AuthorizedRound> logical;
        try{
            if(budget==null||budget.isZero()||budget.isNegative()||budget.compareTo(Duration.ofSeconds(2))>0)throw new IllegalArgumentException("Invalid relay refresh budget");
            var token=local(command.callId());long end=started+budget.toNanos();
            var read=commands.relayContextAuthorized(command,sessionProof,remaining(end));
            logical=scope.track(new RpcOperation<>(read.logical(),read.physicalCompletion())).thenCompose(context->{
                var snapshot=context.snapshot();if(!token.equals(local(command.callId())))throw new AuthoritySql.FencedException();
                // Large SDP/ICE bodies stay in the relay lane. Proof requests carry only their original immutable intent hash.
                var metadata=new CallCommand(command.type(),command.sender(),command.requestId(),command.callId(),command.scope(),null,command.negotiationId(),command.iceGeneration(),"{}",command.intentHash());
                var callerHome=Objects.requireNonNull(directory.apply(snapshot.caller().user()));var winnerHome=Objects.requireNonNull(directory.apply(snapshot.winner().user()));
                return scope.track(homes.proveCommandTracked(template(metadata,context,snapshot.caller(),callerHome),callerHome.cell(),snapshot.caller().sameBinding(command.sender())?command.sender():null,metadata,snapshot.caller(),remaining(end)))
                    .thenCompose(a->scope.track(homes.proveCommandTracked(template(metadata,context,snapshot.winner(),winnerHome),winnerHome.cell(),snapshot.winner().sameBinding(command.sender())?command.sender():null,metadata,snapshot.winner(),remaining(end)))
                    .thenApply(b->{
                        if(!token.equals(local(command.callId())))throw new AuthoritySql.FencedException();
                        var caller=active(a,snapshot.caller(),callerHome,command,snapshot,token);var winner=active(b,snapshot.winner(),winnerHome,command,snapshot,token);
                        var own=snapshot.caller().sameBinding(command.sender())?caller:winner;var peer=own==caller?winner:caller;
                        if(!own.connectionId().equals(command.sender().connectionId()))throw new AuthoritySql.FencedException();
                        var recipient=new AuthenticatedSession(peer.user(),peer.session(),peer.incarnation(),peer.generation(),peer.connectionId());
                        long until=deadline(started,startedWall,min(caller.expiresAt(),winner.expiresAt()));
                        long reservation=deadline(started,startedWall,min(caller.participantUntil(),winner.participantUntil()).minusSeconds(5));
                        var authorization=new RelayAuthorizationCache.Snapshot(snapshot.callId(),snapshot.activationId(),snapshot.version(),command.negotiationId().value(),command.iceGeneration().value(),snapshot.state(),command.sender(),recipient,token,started,until,reservation,until,Math.min(until,started+Duration.ofSeconds(5).minus(CLOCK_MARGIN).toNanos()));
                        return new AuthorizedRound(authorization,committedGrant(snapshot,token,started,startedWall),min(caller.expiresAt(),winner.expiresAt()));
                    }));
            });
        }catch(RuntimeException denied){logical=CompletableFuture.failedFuture(denied);}
        return scope.seal(logical);
    }
    private static NegotiationRelay.Grant committedGrant(CallSnapshotRepository.Snapshot snapshot,AuthoritySql.GroupToken token,long started,Instant startedWall){
        try{
            if(snapshot.deadlines()==null||snapshot.deadlines().getBytes(java.nio.charset.StandardCharsets.UTF_8).length>32768)throw new AuthoritySql.FencedException();
            var metadata=JSON.readTree(snapshot.deadlines());
            var offerer=JSON.treeToValue(metadata.get("offerer"),AuthenticatedSession.class);var answerer=JSON.treeToValue(metadata.get("answerer"),AuthenticatedSession.class);
            long ice=Long.parseLong(metadata.path("iceGeneration").asText());long until=deadline(started,startedWall,Instant.parse(metadata.path("negotiationUntil").asText()));
            if(System.nanoTime()-until>=0)throw new AuthoritySql.FencedException();
            return new NegotiationRelay.Grant(snapshot.callId(),snapshot.activationId(),snapshot.version(),snapshot.negotiationId(),ice,offerer,answerer,until,token);
        }catch(Exception invalid){throw new AuthoritySql.FencedException();}
    }
    private AuthoritySql.GroupToken local(CallId call){if(call==null||!trusted.getAsBoolean())throw new AuthoritySql.FencedException();return currentGroup.apply(call).orElseThrow(AuthoritySql.FencedException::new);}
    private Request template(CallCommand command,CallCommandService.CriticalContext context,CallSnapshotRepository.Participant participant,ProofBindings.TrustedHome home){
        var snapshot=context.snapshot();var now=clock.instant();return new Request(participant.user(),snapshot.callId(),snapshot.inviteRequest().value(),context.originalInviteHash(),home.directoryEpoch(),Phase.RINGING,
            new Grant(snapshot.callId().coordinatorCell(),currentGroup.apply(snapshot.callId()).orElseThrow(AuthoritySql.FencedException::new).storageEpoch(),snapshot.hashVersion(),snapshot.group(),Math.max(1,snapshot.lastGroupEpoch()),1,command.requestId().value(),now,now.plusSeconds(5),"UNSIGNED"));
    }
    private HomeAuthorizationProof.Claims active(RpcBusinessHandler.HomeProofReply reply,CallSnapshotRepository.Participant participant,ProofBindings.TrustedHome home,CallCommand command,CallSnapshotRepository.Snapshot snapshot,AuthoritySql.GroupToken token){
        var now=clock.instant();var p=proofs.decode(reply.signed(),home.cell(),now).orElseThrow(AuthoritySql.FencedException::new);
        if(!p.purpose().equals("ACTIVE")||!p.sourceCell().equals(home.cell())||p.sourceStorageEpoch()!=home.storageEpoch()||p.directoryEpoch()!=home.directoryEpoch()
                ||!p.destinationCell().equals(snapshot.callId().coordinatorCell())||!p.call().equals(snapshot.callId())||!p.operation().equals(command.requestId().value())||!p.intentHash().equals(command.intentHash())
                ||!p.user().equals(participant.user())||!Objects.equals(p.session(),participant.key())||!Objects.equals(p.incarnation(),participant.incarnation())||p.generation()!=participant.generation()
                ||p.connectionId()==null||!snapshot.activationId().equals(p.activationId())||p.callVersion()!=snapshot.version()||p.negotiationId()!=snapshot.negotiationId()
                ||p.storageEpoch()!=token.storageEpoch()||p.hashVersion()!=token.hashVersion()||p.group()!=token.group()||p.groupEpoch()!=token.epoch()||!token.incarnation().equals(p.ownerIncarnation())
                ||p.reservationId()==null||p.reservationVersion()<1||p.participantUntil()==null||!p.participantUntil().isAfter(now.plusSeconds(5).plus(CLOCK_MARGIN))
                ||p.issuedAt().isAfter(now.plusMillis(250))||!p.expiresAt().isAfter(now.plus(CLOCK_MARGIN)))throw new AuthoritySql.FencedException();
        return p;
    }
    private static long deadline(long started,Instant startedWall,Instant until){return started+Duration.between(startedWall,until.minus(CLOCK_MARGIN)).toNanos();}
    private static Instant min(Instant a,Instant b){return a.isBefore(b)?a:b;}
    private static Duration remaining(long end){long left=end-System.nanoTime();if(left<=0)throw new CompletionException(new TimeoutException());return Duration.ofNanos(left);}
}
