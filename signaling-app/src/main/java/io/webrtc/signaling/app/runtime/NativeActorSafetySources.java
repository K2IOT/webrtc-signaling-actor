package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.actors.cluster.*;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.rpc.RpcOperation;
import org.apache.pekko.actor.typed.ActorSystem;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Fixed native actor source binding. All admission reads are cached or local Pekko state. */
public final class NativeActorSafetySources implements AutoCloseable {
    private final ClusterReadiness readiness;private final NativeClusterMembership membership;
    private final ClockSafetyMonitor clock;private final NativeClockSource clockSource;
    private final NativeCellHealthSource primary;private final NativeRevocationSource revocations;
    private final List<NativeWorkerScheduler.Job> jobs;private volatile boolean draining;
    private CompletionStage<Void> drained;
    public NativeActorSafetySources(ActorSystem<?> system,ClusterReadiness readiness,Set<String> enrolledAzRoles,
            String approvedFingerprint,ClockSafetyMonitor clock,NativeClockSource clockSource,NativeCellHealthSource primary,NativeRevocationSource revocations){
        Objects.requireNonNull(system);
        if(approvedFingerprint==null||!approvedFingerprint.matches("[a-f0-9]{64}")||!approvedFingerprint.equals(system.settings().config().getString("signaling.cluster-fingerprint")))throw new IllegalArgumentException("Native cluster fingerprint differs from enrollment");
        this.readiness=Objects.requireNonNull(readiness);this.clock=Objects.requireNonNull(clock);this.clockSource=Objects.requireNonNull(clockSource);
        this.primary=Objects.requireNonNull(primary);this.revocations=Objects.requireNonNull(revocations);
        membership=new NativeClusterMembership(system,readiness,enrolledAzRoles);
        var period=Duration.ofMillis(100);
        jobs=List.of(clockSource.job(period),primary.job(period),revocations.job(period),new NativeWorkerScheduler.Job("actor_membership",NativeWorkerScheduler.Priority.SAFETY,period,budget->{
            var value=refresh();return new RpcOperation<>(CompletableFuture.completedFuture(value),CompletableFuture.completedFuture(null));
        }));
        readiness.installSafetyGate(this::liveSafety);
    }
    private boolean liveSafety(){return !draining&&clock.valid()&&primary.usable()&&revocations.usable();}
    public ClusterReadiness.Snapshot refresh(){
        membership.refresh();boolean primaryValid=primary.usable();
        readiness.updateSafety(true,primaryValid,primaryValid,clock.valid());return readiness.snapshot();
    }
    public List<NativeWorkerScheduler.Job> jobs(){return jobs;}
    /** Invoke after native root release; sources continue safety work during framework handoff. */
    public synchronized CompletionStage<Void> drain(){
        if(drained!=null)return drained;draining=true;readiness.beginDrain();
        drained=CompletableFuture.allOf(clockSource.drain().toCompletableFuture(),primary.drain().toCompletableFuture(),revocations.drain().toCompletableFuture()).minimalCompletionStage();
        return drained;
    }
    @Override public void close(){drain();}
}
