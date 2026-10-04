package io.webrtc.signaling.rpc;

import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.Participant;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/** Source session proof -> guarded native intent -> both current homes -> hosting actor/native CAS. */
public final class NativeCriticalCommandExecutor {
    private final CallCommandService commands;private final NativeHomeProofClient homes;private final RpcBusinessHandler.ActorIngress actors;
    private final Function<UserId,ProofBindings.TrustedHome> directory;private final String cell;private final long storageEpoch;private final Clock clock;
    public NativeCriticalCommandExecutor(CallCommandService commands,NativeHomeProofClient homes,RpcBusinessHandler.ActorIngress actors,
            Function<UserId,ProofBindings.TrustedHome> directory,String cell,long storageEpoch,Clock clock){
        this.commands=Objects.requireNonNull(commands);this.homes=Objects.requireNonNull(homes);this.actors=Objects.requireNonNull(actors);
        this.directory=Objects.requireNonNull(directory);this.cell=Objects.requireNonNull(cell);if(storageEpoch<1)throw new IllegalArgumentException("Invalid native storage epoch");this.storageEpoch=storageEpoch;this.clock=Objects.requireNonNull(clock);
    }
    public RpcOperation<CallCommandService.Outcome> execute(CallCommand command,String sessionProof,Duration budget){
        if(budget==null||budget.isNegative()||budget.isZero())throw new DbOverloadedException();
        if(command.callId()==null||!cell.equals(command.callId().coordinatorCell()))throw new CallCommandService.AuthorizationRejected();
        long end=System.nanoTime()+Math.min(budget.toNanos(),Duration.ofSeconds(2).toNanos());
        var nativeRead=commands.criticalContextAuthorized(command,sessionProof,remaining(end));
        var scope=new PhysicalScope();
        var logical=scope.track(new RpcOperation<>(nativeRead.logical(),nativeRead.physicalCompletion())).thenCompose(context->{
            var snapshot=context.snapshot();if(snapshot.terminalAt()!=null||snapshot.winner()==null||snapshot.activationId()==null)throw new AuthoritySql.FencedException();
            var callerHome=Objects.requireNonNull(directory.apply(snapshot.caller().user()));var winnerHome=Objects.requireNonNull(directory.apply(snapshot.winner().user()));
            var caller=template(command,context,snapshot.caller(),callerHome);var winner=template(command,context,snapshot.winner(),winnerHome);
            return scope.track(homes.proveCommandTracked(caller,callerHome.cell(),command.sender().userId().equals(snapshot.caller().user())?command.sender():null,command,snapshot.caller(),remaining(end)))
                .thenCompose(a->scope.track(homes.proveCommandTracked(winner,winnerHome.cell(),command.sender().userId().equals(snapshot.winner().user())?command.sender():null,command,snapshot.winner(),remaining(end)))
                .thenCompose(b->{
                    var sealed=new CriticalCommandProof(sessionProof,a.signed(),b.signed()).encode();
                    var mutation=actors.callTracked(command,sealed,clock.instant().plus(remaining(end)),RpcBusinessHandler.encode(new RpcBusinessHandler.CallPayload(command,sealed)).length);
                    return scope.track(mutation);
                }));
        });
        return scope.seal(logical);
    }
    private Request template(CallCommand command,CallCommandService.CriticalContext context,Participant participant,ProofBindings.TrustedHome home){
        var snapshot=context.snapshot();var now=clock.instant();
        // This unsigned template grants nothing. NativeHomeProofClient replaces it with the hosting actor's committed grant.
        return new Request(participant.user(),snapshot.callId(),snapshot.inviteRequest().value(),context.originalInviteHash(),home.directoryEpoch(),Phase.RINGING,
            new Grant(cell,storageEpoch,snapshot.hashVersion(),snapshot.group(),Math.max(1,snapshot.lastGroupEpoch()),1,command.requestId().value(),now,now.plusSeconds(5),"UNSIGNED"));
    }
    private static Duration remaining(long end){long left=end-System.nanoTime();if(left<=0)throw new CompletionException(new TimeoutException());return Duration.ofNanos(left);}
}
