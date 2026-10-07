package io.webrtc.signaling.app.runtime;

import org.apache.pekko.actor.CoordinatedShutdown;
import org.apache.pekko.actor.typed.ActorSystem;
import org.springframework.context.SmartLifecycle;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/** Spring stops before destroying beans; Pekko alone performs native handoff and cleanup. */
public final class NativeActorSpringLifecycle implements SmartLifecycle {
    private final ActorSystem<?> system;
    private final NativeActorRuntimeHooks hooks;
    private volatile boolean running;
    private CompletionStage<Void> shutdown;
    public NativeActorSpringLifecycle(ActorSystem<?> system,NativeActorRuntimeHooks hooks){this.system=Objects.requireNonNull(system);this.hooks=Objects.requireNonNull(hooks);}
    @Override public void start(){running=true;}
    @Override public boolean isRunning(){return running;}
    @Override public int getPhase(){return Integer.MAX_VALUE;}
    private synchronized CompletionStage<Void> shutdown(){
        if(shutdown==null)shutdown=CoordinatedShutdown.get(system).runAll(CoordinatedShutdown.unknownReason()).thenApply(done->{
            if(!hooks.databaseClosed())throw new IllegalStateException("Native database closure unproven");
            running=false;return (Void)null;
        }).toCompletableFuture().minimalCompletionStage();
        return shutdown;
    }
    @Override public void stop(Runnable stopped){
        shutdown().thenRun(Objects.requireNonNull(stopped));
        // Failure is not graceful completion. Native owners must disable inferred bean destruction.
    }
    @Override public void stop(){
        try{shutdown().toCompletableFuture().get(65,TimeUnit.SECONDS);}
        catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IllegalStateException("Native shutdown interrupted",interrupted);}
        catch(Exception unknown){throw new IllegalStateException("Native shutdown unproven",unknown);}
    }
}
