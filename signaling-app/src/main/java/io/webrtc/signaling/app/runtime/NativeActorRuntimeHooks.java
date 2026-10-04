package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.app.ShutdownCoordinator;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.cluster.Cluster;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/** Concrete actor resources. Pekko owns handoff/leave; native release must prove cleanup before pool closure. */
public final class NativeActorRuntimeHooks implements ShutdownCoordinator.Hooks {
    private final NativeActorComposition actors;
    private final CellRpcServer server;
    private final CellRpcClient client;
    private final NativeWorkerScheduler workers;
    private final DbBoundary database;
    private final DbPools pools;
    private final PrivateHealthServer health;
    private final List<Supplier<CompletionStage<Void>>> sourceDrains;
    private final ExecutorService finish=Executors.newSingleThreadExecutor(Thread.ofPlatform().daemon().name("native-runtime-finish").factory());
    private boolean readyOff,shed,rootReleased,closed;
    private CompletionStage<Void> released,closure;
    public NativeActorRuntimeHooks(NativeActorComposition actors,CellRpcServer server,CellRpcClient client,NativeWorkerScheduler workers,DbBoundary database,DbPools pools,PrivateHealthServer health){this(actors,server,client,workers,database,pools,health,List.of());}
    public NativeActorRuntimeHooks(NativeActorComposition actors,CellRpcServer server,CellRpcClient client,NativeWorkerScheduler workers,DbBoundary database,DbPools pools,PrivateHealthServer health,List<Supplier<CompletionStage<Void>>> sourceDrains){
        this.actors=Objects.requireNonNull(actors);this.server=Objects.requireNonNull(server);this.client=Objects.requireNonNull(client);this.workers=Objects.requireNonNull(workers);this.database=Objects.requireNonNull(database);this.pools=Objects.requireNonNull(pools);this.health=Objects.requireNonNull(health);
        if(sourceDrains==null||sourceDrains.size()>16)throw new IllegalArgumentException("Source transport owners exceed bound");this.sourceDrains=List.copyOf(sourceDrains);
    }
    @Override public synchronized void readinessOff(){actors.readiness().beginDrain();readyOff=true;}
    @Override public synchronized void shedIngress(){if(!readyOff)throw new IllegalStateException("Readiness still open");actors.shedNewAcquisition();workers.shedNormal();shed=true;}
    @Override public CompletionStage<Integer> reconnectBatch(int maximum){if(maximum<1||maximum>128)return CompletableFuture.failedFuture(new IllegalArgumentException("Reconnect batch outside bound"));return CompletableFuture.completedFuture(0);}
    @Override public CompletionStage<Void> settleAdmitted(){
        return all(workers.settleAdmitted(),actors.ingress().settleAdmitted(),server.settleAdmitted(),client.settleAdmitted(),database.settleAdmitted());
    }
    @Override public synchronized CompletionStage<Void> handoffAndRelease(){
        if(released!=null)return released;
        if(!shed||!Cluster.get(Adapter.toClassic(actors.system())).isTerminated())return CompletableFuture.failedFuture(new IllegalStateException("Framework handoff/leave not yet observed"));
        released=all(workers.drain(),actors.ingress().drain()).thenComposeAsync(v->actors.drainRoots(),finish).thenRun(()->{synchronized(this){rootReleased=true;}}).toCompletableFuture().minimalCompletionStage();
        return released;
    }
    @Override public CompletionStage<Void> leaveCluster(){return CompletableFuture.failedFuture(new IllegalStateException("Pekko exclusively owns cluster leave"));}
    @Override public synchronized CompletionStage<Void> closeDatabase(){
        if(closure!=null)return closure;
        if(!rootReleased)return CompletableFuture.failedFuture(new IllegalStateException("Native root cleanup unproven"));
        closure=all(server.drain(),client.drain()).thenComposeAsync(v->drainSources(),finish)
            .thenCompose(v->database.drain()).thenRunAsync(pools::close,finish).thenCompose(v->health.stop())
            .thenRun(()->{synchronized(this){closed=true;}finish.shutdown();}).toCompletableFuture().minimalCompletionStage();
        return closure;
    }
    private CompletionStage<Void> drainSources(){try{return CompletableFuture.allOf(sourceDrains.stream().map(d->Objects.requireNonNull(d.get()).toCompletableFuture()).toArray(CompletableFuture[]::new)).minimalCompletionStage();}catch(RuntimeException unknown){return CompletableFuture.failedFuture(unknown);}}
    public synchronized boolean databaseClosed(){return closed;}
    private static CompletionStage<Void> all(CompletionStage<?>... stages){return CompletableFuture.allOf(Arrays.stream(stages).map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new)).minimalCompletionStage();}
}
