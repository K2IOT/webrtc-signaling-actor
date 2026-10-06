package io.webrtc.signaling.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.*;

/** Correlates native replacement AUTH_OK with the original socket's stale-generation closure. */
public final class StaleGenerationProbe {
    private StaleGenerationProbe(){}
    public enum Outcome { REPLACED, REPLACEMENT_UNKNOWN, BINDING_MISMATCH, CLOSURE_UNKNOWN }
    public record Receipt(Outcome outcome,long originalGeneration,long replacementGeneration,VirtualClient.ProbeReceipt closure){}
    public record Operation(CompletionStage<Receipt> observed,CompletionStage<Void> physicalCompletion){}
    static boolean replacement(JsonNode original,JsonNode newer){
        var before=VirtualClient.AuthBinding.read(original);var after=VirtualClient.AuthBinding.read(newer);
        return before.isPresent()&&after.isPresent()&&replacement(before.get(),after.get());
    }
    private static boolean replacement(VirtualClient.AuthBinding original,VirtualClient.AuthBinding newer){
        return original.incarnation().equals(newer.incarnation())&&newer.connectionGeneration()>original.connectionGeneration();
    }
    static Outcome classify(JsonNode original,JsonNode newer,VirtualClient.ProbeReceipt closure){
        if(newer==null)return Outcome.REPLACEMENT_UNKNOWN;
        if(!replacement(original,newer))return Outcome.BINDING_MISMATCH;
        return stale(closure)?Outcome.REPLACED:Outcome.CLOSURE_UNKNOWN;
    }
    private static boolean stale(VirtualClient.ProbeReceipt closure){
        long elapsed=closure==null?-1:closure.finishedNanos()-closure.intendedNanos();
        return closure!=null&&closure.kind()==VirtualClient.ProbeKind.SECURITY_CLOSURE
            &&elapsed>=0&&elapsed<TimeUnit.SECONDS.toNanos(5)
            &&closure.outcome()==VirtualClient.ProbeOutcome.AUTHORIZATION_REJECTED&&closure.closeCode()==1008
            &&closure.closeReason()==VirtualClient.CloseReason.STALE_CONNECTION;
    }
    /** Both clients must use the approved same-session inventory; native incarnation confirms replacement. */
    public static Operation run(VirtualClient original,VirtualClient newer,long intended){
        if(original==newer||!original.user().equals(newer.user())||!original.cell().equals(newer.cell())||newer.generation()!=0)
            throw new IllegalArgumentException("A fresh same-user replacement client is required");
        var before=original.authBinding().orElseThrow(()->new IllegalArgumentException("Native AUTH binding required"));
        var watch=original.probe(VirtualClient.ProbeKind.SECURITY_CLOSURE,intended);
        // The replacement cannot start before the old socket has physically installed its observer.
        var connected=watch.admission().thenCompose(admitted->admitted?newer.connect(intended):CompletableFuture.<JsonNode>completedFuture(null))
            .handle((ack,error)->error==null?newer.authBinding().orElse(null):null);
        var observed=watch.observed().thenCombine(connected,(closure,after)->new Receipt(
            after==null?Outcome.REPLACEMENT_UNKNOWN:!replacement(before,after)?Outcome.BINDING_MISMATCH:
                stale(closure)?Outcome.REPLACED:Outcome.CLOSURE_UNKNOWN,
            before.connectionGeneration(),after==null?0:after.connectionGeneration(),closure));
        var cleanup=observed.handle((receipt,error)->null).thenCompose(v->newer.drain());
        var physical=CompletableFuture.allOf(watch.physicalCompletion().toCompletableFuture(),cleanup.toCompletableFuture());
        return new Operation(observed.toCompletableFuture().minimalCompletionStage(),physical.minimalCompletionStage());
    }
}
