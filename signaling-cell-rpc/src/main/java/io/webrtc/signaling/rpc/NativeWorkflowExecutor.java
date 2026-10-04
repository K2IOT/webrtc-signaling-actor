package io.webrtc.signaling.rpc;
import io.webrtc.signaling.protocol.Identity.AuthenticatedSession;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.Participant;
import io.webrtc.signaling.storage.HomeParticipationService.Request;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
/** Fresh proofs from both homes followed by current hosting entity CAS, within one original budget. */
public final class NativeWorkflowExecutor {
    public record ProofSpec(Request request,String home,AuthenticatedSession sender,Participant participant){public ProofSpec{Objects.requireNonNull(request);Objects.requireNonNull(home);}}
    private static final ObjectMapper JSON=new ObjectMapper();
    private final NativeHomeProofClient proofs;private final RpcBusinessHandler.ActorIngress actors;private final Clock clock;
    public NativeWorkflowExecutor(NativeHomeProofClient proofs,RpcBusinessHandler.ActorIngress actors,Clock clock){this.proofs=Objects.requireNonNull(proofs);this.actors=Objects.requireNonNull(actors);this.clock=Objects.requireNonNull(clock);}
    public CompletionStage<CallWorkflowService.Outcome> apply(CallWorkflowService.Transition transition,ProofSpec caller,ProofSpec callee,Duration budget){return applyTracked(transition,caller,callee,budget).logical();}
    public RpcOperation<CallWorkflowService.Outcome> applyTracked(CallWorkflowService.Transition transition,ProofSpec caller,ProofSpec callee,Duration budget){
        if(budget==null||budget.isNegative()||budget.isZero())return new RpcOperation<>(CompletableFuture.failedFuture(new DbOutcomeUnknownException()),CompletableFuture.completedFuture(null));
        if(transition.step()==CallWorkflowService.Step.TERMINATE)throw new IllegalArgumentException("Termination uses scoped command or native timer authority");
        for(var spec:List.of(caller,callee))if(!spec.request().call().equals(transition.call())||!spec.request().grant().operation().equals(transition.operation()))throw new IllegalArgumentException("Workflow proof scope mismatch");
        long end=System.nanoTime()+Math.min(Duration.ofSeconds(2).toNanos(),budget.toNanos());boolean active=Set.of(CallWorkflowService.Step.READY,CallWorkflowService.Step.ESTABLISH).contains(transition.step());String callerPurpose=active?"ACTIVE":"SESSION";String calleePurpose=transition.step()==CallWorkflowService.Step.RING?"TARGET_ROUTE":active?"ACTIVE":"WINNER";
        var scope=new PhysicalScope();var logical=scope.track(proofs.proveTracked(caller.request(),caller.home(),caller.sender(),transition,callerPurpose,caller.participant(),remaining(end))).thenCompose(a->scope.track(proofs.proveTracked(callee.request(),callee.home(),callee.sender(),transition,calleePurpose,callee.participant(),remaining(end))).thenCompose(b->{
            try{Instant expiry=min(a.expiresAt(),b.expiresAt());Instant until=min(a.participantUntil(),b.participantUntil());if(!expiry.isAfter(clock.instant())||!until.isAfter(clock.instant().plusSeconds(5)))throw new AuthoritySql.FencedException();
                var sealed=new CallWorkflowService.Transition(transition.call(),transition.group(),transition.directoryEpoch(),transition.expectedVersion(),transition.operation(),transition.step(),transition.winner(),transition.offered(),transition.activationId(),expiry,until,JSON.writeValueAsString(List.of(a.signed(),b.signed())),transition.reason());
                return scope.track(actors.progressTracked(sealed,clock.instant().plus(remaining(end)),RpcBusinessHandler.encode(new RpcBusinessHandler.WorkflowPayload(sealed)).length));
            }catch(Exception invalid){return CompletableFuture.failedFuture(invalid);}
        }));return scope.seal(logical);
    }
    private static Instant min(Instant a,Instant b){return a.isBefore(b)?a:b;}
    private static Duration remaining(long end){long left=end-System.nanoTime();if(left<=0)throw new CompletionException(new TimeoutException());return Duration.ofNanos(left);}
}
