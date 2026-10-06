package io.webrtc.signaling.loadgen;

import java.util.*;
import java.util.concurrent.*;

/** Owns only replacement sockets admitted inside the declared local socket budget. */
final class SecurityReplacementPool {
    private static final class Owned {VirtualClient client;final CompletableFuture<Void> logical=new CompletableFuture<>(),retired=new CompletableFuture<>();}
    private final int capacity;private int peak;private boolean draining;
    private final Set<Owned> owned=new HashSet<>();
    SecurityReplacementPool(long assigned,int limit){
        if(assigned<0||assigned>limit||limit<1||limit>200000)throw new IllegalArgumentException("Invalid local socket budget");
        capacity=(int)Math.min(16,limit-assigned);
    }
    synchronized Optional<StaleGenerationProbe.Operation> start(SecurityWorkload source,VirtualClient original,long intended){
        if(draining||owned.size()>=capacity){source.unavailable(VirtualClient.ProbeKind.SECURITY_CLOSURE);return Optional.empty();}
        var admission=new Owned();owned.add(admission);peak=Math.max(peak,owned.size());
        admission.client=original.replacement();
        var operation=source.startStale(original,admission.client,intended);
        operation.observed().whenComplete((receipt,error)->admission.logical.complete(null));
        operation.physicalCompletion().whenComplete((v,error)->{
            synchronized(this){if(error==null)owned.remove(admission);}
            if(error==null)admission.retired.complete(null);else admission.retired.completeExceptionally(new IllegalStateException("Replacement cleanup unknown"));
        });
        return Optional.of(new StaleGenerationProbe.Operation(operation.observed(),admission.retired.minimalCompletionStage()));
    }
    synchronized Map<String,Integer> snapshot(){return Map.of("capacity",capacity,"pending",owned.size(),"peak",peak);}
    synchronized CompletionStage<Void> quiesce(){
        draining=true;
        return CompletableFuture.allOf(owned.stream().map(admission->admission.logical).toArray(CompletableFuture[]::new)).minimalCompletionStage();
    }
    synchronized CompletionStage<Void> drain(){
        draining=true;var futures=new ArrayList<CompletableFuture<?>>();
        for(var admission:List.copyOf(owned)){futures.add(admission.client.drain().toCompletableFuture());futures.add(admission.retired);}
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).minimalCompletionStage();
    }
}
