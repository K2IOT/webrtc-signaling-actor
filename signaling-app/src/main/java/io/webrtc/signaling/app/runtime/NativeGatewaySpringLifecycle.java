package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.app.ShutdownCoordinator;
import io.webrtc.signaling.app.SignalingApplication;
import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.rpc.CellRpcClient;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import org.springframework.context.SmartLifecycle;

/** Original receipt ownership precedes client/boot/probe retirement in Spring stop. */
public final class NativeGatewaySpringLifecycle implements SmartLifecycle {
    private final GatewayBootController boot;
    private final NativeGatewayIngress ingress;
    private final NativeGatewaySafety safety;
    private final PrivateHealthServer health;
    private final ShutdownCoordinator shutdown;
    private final List<NativeWorkerScheduler> workers;
    private volatile boolean running,draining;
    private CompletionStage<Void> completion;
    NativeGatewaySpringLifecycle(NativeGatewayIngress ingress,GatewayBootController boot,
            NativeRelaySessionProofCache cache,CellRpcClient client,
            NativeGatewayLifecycleEnrollment enrollment,BoundedTokenVerifier tokens,NativeGatewaySafety safety,
            List<NativeWorkerScheduler> workers,List<NativeClockSource> clocks,List<NativeCachedRevocationSource> sources)throws Exception {
        this.ingress=ingress;this.boot=boot;this.safety=safety;
        this.workers=List.copyOf(workers);
        health=new PrivateHealthServer(enrollment.healthAddress(),enrollment.live(),this::ready,enrollment.metrics());
        shutdown=new ShutdownCoordinator(new ShutdownCoordinator.Hooks(){
            public void readinessOff(){draining=true;}
            public void shedIngress(){ingress.wss().stopAccepting();}
            public CompletionStage<Integer> reconnectBatch(int maximum){return ingress.wss().reconnectBatch(maximum);}
            public CompletionStage<Void> settleAdmitted(){return ingress.drain().thenCompose(v->cache.drain()).thenCompose(v->tokens.drain()).thenCompose(v->client.drain());}
            public CompletionStage<Void> handoffAndRelease(){boot.close();return CompletableFuture.completedFuture(null);}
            public CompletionStage<Void> leaveCluster(){return CompletableFuture.completedFuture(null);}
            public CompletionStage<Void> closeDatabase(){
                var receipts=new java.util.ArrayList<CompletableFuture<?>>();
                workers.forEach(worker->receipts.add(worker.drain().toCompletableFuture()));
                clocks.forEach(clock->receipts.add(clock.drain().toCompletableFuture()));
                sources.forEach(source->receipts.add(source.drain().toCompletableFuture()));
                return CompletableFuture.allOf(receipts.toArray(CompletableFuture[]::new)).thenCompose(v->health.stop());
            }
        });
        try{health.start().toCompletableFuture().get(2,TimeUnit.SECONDS);}
        catch(Exception failed){health.stop();shutdown.close();throw failed;}
    }
    public PrivateHealthServer health(){return health;}
    public boolean ready(){return running&&!draining&&ingress.wss().accepting()&&boot.current()&&safety.valid();}
    @Override public void start(){workers.forEach(NativeWorkerScheduler::start);running=true;}
    @Override public boolean isRunning(){return running;}
    @Override public int getPhase(){return Integer.MAX_VALUE;}
    public synchronized CompletionStage<Void> drain(){
        if(completion==null){
            completion=shutdown.shutdown(SignalingApplication.Plane.GATEWAY,Duration.ofSeconds(300)).thenApply(status->{
                if(status!=ShutdownCoordinator.Status.COMPLETE)throw new IllegalStateException("Native gateway shutdown unproven");
                running=false;return (Void)null;
            }).toCompletableFuture().minimalCompletionStage();
            completion.whenComplete((v,e)->shutdown.close());
        }
        return completion;
    }
    @Override public void stop(Runnable stopped){drain().thenRun(stopped);}
    @Override public void stop(){try{drain().toCompletableFuture().get(310,TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();throw new IllegalStateException("Native gateway shutdown interrupted",e);}catch(Exception e){throw new IllegalStateException("Native gateway shutdown unproven",e);}}
}
