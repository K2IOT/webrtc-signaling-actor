package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.rpc.RpcOperation;
import io.webrtc.signaling.storage.PrimaryCellFacts;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;

/** Process-owned native primary probe. Health reads use only its one bounded cached fact. */
public final class NativeCellHealthSource implements AutoCloseable {
    private final PrimaryCellFacts primary;
    private PrimaryCellFacts.Facts cached;
    private boolean active,draining;
    private CompletableFuture<Void> admitted=CompletableFuture.completedFuture(null);
    private final CompletableFuture<Void> drained=new CompletableFuture<>();
    public NativeCellHealthSource(PrimaryCellFacts primary){this.primary=Objects.requireNonNull(primary);}
    public synchronized boolean usable(){return !draining&&cached!=null&&cached.usable(System.nanoTime());}
    public RpcOperation<PrimaryCellFacts.Facts> poll(Duration budget){
        if(budget==null||budget.isNegative()||budget.isZero()||budget.compareTo(Duration.ofSeconds(2))>0)throw new IllegalArgumentException("Native primary source budget outside bound");
        final CompletableFuture<Void> physical;
        synchronized(this){
            if(draining||active)return new RpcOperation<>(CompletableFuture.failedFuture(new RejectedExecutionException("Native primary source unavailable")),CompletableFuture.completedFuture(null));
            active=true;physical=new CompletableFuture<>();admitted=physical;
        }
        try{
            var operation=primary.poll(budget);
            var logical=operation.logical().handle((facts,error)->{
                synchronized(this){if(error!=null)cached=null;else if(!draining)cached=facts;}
                if(error!=null)throw new CompletionException(error);return facts;
            });
            operation.physicalCompletion().thenCombine(logical.handle((v,e)->null),(a,b)->null).whenComplete((v,error)->{
                if(error==null)synchronized(this){active=false;physical.complete(null);if(draining)drained.complete(null);}
            });
            return new RpcOperation<>(logical,physical);
        }catch(Throwable unknown){
            synchronized(this){cached=null;}
            return new RpcOperation<>(CompletableFuture.failedFuture(unknown),physical);
        }
    }
    public NativeWorkerScheduler.Job job(Duration period){
        if(period==null||period.compareTo(Duration.ofMillis(500))>0)throw new IllegalArgumentException("Native primary probe period exceeds half its freshness window");
        return new NativeWorkerScheduler.Job("cell_primary",NativeWorkerScheduler.Priority.SAFETY,period,this::poll);
    }
    public synchronized CompletionStage<Void> settleAdmitted(){return admitted.minimalCompletionStage();}
    public synchronized CompletionStage<Void> drain(){draining=true;cached=null;if(!active)drained.complete(null);return drained.minimalCompletionStage();}
    @Override public void close(){drain();}
}
