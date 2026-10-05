package io.webrtc.signaling.rpc;

import io.webrtc.signaling.actors.relay.RelayAuthorizationCache;
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
        var scope=new PhysicalScope();long started=System.nanoTime();var startedWall=clock.instant();
        CompletionStage<RelayAuthorizationCache.Snapshot> logical;
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
                        return new RelayAuthorizationCache.Snapshot(snapshot.callId(),snapshot.activationId(),snapshot.version(),command.negotiationId().value(),command.iceGeneration().value(),snapshot.state(),command.sender(),recipient,token,started,until,reservation,until,Math.min(until,started+Duration.ofSeconds(5).minus(CLOCK_MARGIN).toNanos()));
                    }));
            });
        }catch(RuntimeException denied){logical=CompletableFuture.failedFuture(denied);}
        return scope.seal(logical);
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
