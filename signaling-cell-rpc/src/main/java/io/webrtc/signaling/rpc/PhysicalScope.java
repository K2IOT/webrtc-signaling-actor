package io.webrtc.signaling.rpc;

import java.util.concurrent.*;

/** One composition sentinel plus every child cleanup; unknown physical completion remains quarantined. */
final class PhysicalScope {
    private int retained=1;
    private final CompletableFuture<Void> completion=new CompletableFuture<>();
    <T> CompletionStage<T> track(RpcOperation<T> operation){
        synchronized(this){if(completion.isDone())throw new IllegalStateException("Physical scope already sealed");retained++;}
        operation.physicalCompletion().whenComplete((v,error)->{if(error==null)release();});
        return operation.logical();
    }
    <T> RpcOperation<T> seal(CompletionStage<T> logical){logical.whenComplete((v,e)->release());return new RpcOperation<>(logical,completion);}
    private synchronized void release(){if(--retained==0)completion.complete(null);}
}
