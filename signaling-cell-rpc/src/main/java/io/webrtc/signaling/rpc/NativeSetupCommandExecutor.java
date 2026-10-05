package io.webrtc.signaling.rpc;

import io.webrtc.signaling.actors.user.UserCommand;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/** Public durable consent followed by independently fenced saga effects, all within its original budget. */
public final class NativeSetupCommandExecutor {
    private final CallCommandService commands;
    private final NativeHomeProofClient homes;
    private final RpcBusinessHandler.ActorIngress actors;
    private final NativeSagaEffects.Network network;
    private final Function<UserId,ProofBindings.TrustedHome> directory;
    private final String cell;
    private final long storageEpoch;
    private final Clock clock;
    public NativeSetupCommandExecutor(CallCommandService commands,NativeHomeProofClient homes,RpcBusinessHandler.ActorIngress actors,NativeSagaEffects.Network network,Function<UserId,ProofBindings.TrustedHome> directory,String cell,long storageEpoch,Clock clock){
        this.commands=Objects.requireNonNull(commands);this.homes=Objects.requireNonNull(homes);this.actors=Objects.requireNonNull(actors);this.network=Objects.requireNonNull(network);this.directory=Objects.requireNonNull(directory);this.cell=Objects.requireNonNull(cell);if(storageEpoch<1)throw new IllegalArgumentException("Invalid native epoch");this.storageEpoch=storageEpoch;this.clock=Objects.requireNonNull(clock);
    }
    public RpcOperation<CallCommandService.Outcome> execute(CallCommand command,String sessionProof,Duration budget){
        if(budget==null||budget.isNegative()||budget.isZero())throw new DbOverloadedException();
        if(command.callId()==null||!cell.equals(command.callId().coordinatorCell())||!Set.of(SignalEnvelope.Type.INVITE,SignalEnvelope.Type.ACCEPT).contains(command.type()))throw new CallCommandService.AuthorizationRejected();
        long end=System.nanoTime()+Math.min(budget.toNanos(),Duration.ofSeconds(2).toNanos());var scope=new PhysicalScope();
        var logical=scope.track(actors.callTracked(command,sessionProof,clock.instant().plus(remaining(end)),RpcBusinessHandler.encode(new RpcBusinessHandler.CallPayload(command,sessionProof)).length)).thenCompose(initial->{
            if(initial.status().equals("FINAL")&&(command.type()==SignalEnvelope.Type.INVITE||!initial.code().equals("ACCEPTED_PENDING_ACTIVATION")))return CompletableFuture.completedFuture(initial);
            var read=NativeReadRetry.execute(left->commands.setupContextAuthorized(command,sessionProof,left),remaining(end));
            return scope.track(read).thenCompose(context->run(command,sessionProof,context,end,scope));
        });return scope.seal(logical);
    }
    private CompletionStage<CallCommandService.Outcome> run(CallCommand command,String proof,CallCommandService.SetupContext context,long end,PhysicalScope scope){
        var snapshot=context.snapshot();
        if(snapshot.terminalAt()!=null||Set.of("CONNECTING","ESTABLISHED").contains(snapshot.state()))return CompletableFuture.completedFuture(context.durableOutcome());
        var callerHome=Objects.requireNonNull(directory.apply(snapshot.caller().user()));var calleeHome=Objects.requireNonNull(directory.apply(snapshot.callee()));
        var caller=template(context,snapshot.caller().user(),callerHome,command.requestId().value());var callee=template(context,snapshot.callee(),calleeHome,command.requestId().value());
        CompletionStage<String> reserved=CompletableFuture.completedFuture("RESERVED");
        if(command.type()==SignalEnvelope.Type.INVITE&&snapshot.state().equals("PREPARING")){
            var effects=new NativeSagaEffects(actors,network,phase->new NativeSagaEffects.HomeStep(calleeHome.cell(),new UserCommand.Reserve(callee),null),clock);
            reserved=scope.track(effects.applyTracked(CrossCellSaga.Phase.RESERVE_HOME,command.requestId().value(),remaining(end)));
        }
        return reserved.thenCompose(code->{
            if(!code.equals("RESERVED"))throw new CompletionException(new DbOutcomeUnknownException());
            return scope.track(actors.grantTracked(caller,new AuthorizationIntent("QUERY",null,0,null,0,null,null),callerHome.cell(),clock.instant().plus(remaining(end)),RpcBusinessHandler.encode(caller).length));
        }).thenCompose(granted->{
            if(!granted.code().equals("GRANTED")||granted.issued()==null)throw new CompletionException(new DbOutcomeUnknownException());
            var nativeSnapshot=granted.issued().snapshot();
            return scope.track(homes.observeTracked(caller,callerHome.cell(),command.type()==SignalEnvelope.Type.INVITE?command.sender():null,remaining(end)))
                .thenCompose(a->scope.track(homes.observeTracked(callee,calleeHome.cell(),command.type()==SignalEnvelope.Type.ACCEPT?command.sender():null,remaining(end))).thenCompose(b->{
                    var planner=new NativeSagaPlanner(nativeSnapshot,granted.issued().token(),context.originalInviteHash(),command.requestId().value(),a,b,command.type()==SignalEnvelope.Type.ACCEPT?command.sender():null,proof,clock);
                    var effects=new NativeSagaEffects(actors,network,planner,clock);var saga=new CrossCellSaga(effects);
                    CompletionStage<String> progression;
                    if(command.type()==SignalEnvelope.Type.INVITE)progression=scope.track(effects.applyTracked(CrossCellSaga.Phase.RING_COORDINATOR,command.requestId().value(),remaining(end)));
                    else if(nativeSnapshot.state().equals("RINGING"))progression=scope.track(saga.acceptTracked(command.requestId().value(),remaining(end))).thenCompose(result->{if(!result.code().equals("ACCEPTED_PENDING_ACTIVATION"))throw new CompletionException(new DbOutcomeUnknownException());return scope.track(saga.activateTracked(command.requestId().value(),remaining(end))).thenApply(CrossCellSaga.Outcome::code);});
                    else if(nativeSnapshot.state().equals("ACCEPTED"))progression=scope.track(saga.activateTracked(command.requestId().value(),remaining(end))).thenApply(CrossCellSaga.Outcome::code);
                    else if(nativeSnapshot.state().equals("ACTIVATING"))progression=resumeActivation(effects,command.requestId().value(),end,scope);
                    else throw new AuthoritySql.FencedException();
                    return progression.thenCompose(finalCode->{
                        if(!Set.of("RINGING","CALL_READY").contains(finalCode))throw new CompletionException(new DbOutcomeUnknownException());
                        var original=NativeReadRetry.execute(left->commands.setupContextAuthorized(command,proof,left),remaining(end));
                        return scope.track(original).thenApply(CallCommandService.SetupContext::durableOutcome);
                    });
                }));
        });
    }
    private CompletionStage<String> resumeActivation(NativeSagaEffects effects,UUID operation,long end,PhysicalScope scope){return scope.track(effects.applyTracked(CrossCellSaga.Phase.CONFIRM_CALLER,operation,remaining(end))).thenCompose(a->{if(!a.equals("CONFIRMED"))throw new CompletionException(new DbOutcomeUnknownException());return scope.track(effects.applyTracked(CrossCellSaga.Phase.CONFIRM_CALLEE,operation,remaining(end)));}).thenCompose(b->{if(!b.equals("CONFIRMED"))throw new CompletionException(new DbOutcomeUnknownException());return scope.track(effects.applyTracked(CrossCellSaga.Phase.READY_COORDINATOR,operation,remaining(end)));});}
    private Request template(CallCommandService.SetupContext context,UserId user,ProofBindings.TrustedHome home,UUID operation){var s=context.snapshot();var now=clock.instant();return new Request(user,s.callId(),s.inviteRequest().value(),context.originalInviteHash(),home.directoryEpoch(),s.state().equals("PREPARING")?Phase.PREPARING:Phase.RINGING,new Grant(cell,storageEpoch,s.hashVersion(),s.group(),Math.max(1,s.lastGroupEpoch()),1,operation,now,now.plusSeconds(5),"UNSIGNED"));}
    private static Duration remaining(long end){long left=end-System.nanoTime();if(left<=0)throw new CompletionException(new TimeoutException());return Duration.ofNanos(left);}
}
