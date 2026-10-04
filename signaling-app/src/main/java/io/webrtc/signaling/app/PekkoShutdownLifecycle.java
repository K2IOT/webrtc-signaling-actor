package io.webrtc.signaling.app;
import com.typesafe.config.*;
import org.apache.pekko.Done;
import org.apache.pekko.actor.CoordinatedShutdown;
import org.apache.pekko.actor.typed.ActorSystem;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
/** Uses only phases explicitly documented by Pekko for application tasks. Framework owns handoff/leave. */
public final class PekkoShutdownLifecycle {
    private PekkoShutdownLifecycle(){}
    private static final java.util.Map<String,Integer> BUDGETS=java.util.Map.ofEntries(
        java.util.Map.entry("before-service-unbind",2),java.util.Map.entry("service-unbind",2),
        java.util.Map.entry("service-requests-done",10),java.util.Map.entry("service-stop",2),
        java.util.Map.entry("before-cluster-shutdown",2),java.util.Map.entry("cluster-sharding-shutdown-region",10),
        java.util.Map.entry("cluster-leave",5),java.util.Map.entry("cluster-exiting",10),
        java.util.Map.entry("cluster-exiting-done",5),java.util.Map.entry("cluster-shutdown",5),
        java.util.Map.entry("before-actor-system-terminate",5),java.util.Map.entry("actor-system-terminate",5));
    public static Config config(){return config(ConfigFactory.load());}
    public static Config config(Config base){
        Config result=ConfigFactory.empty();
        for(var entry:BUDGETS.entrySet())result=result.withValue("pekko.coordinated-shutdown.phases."+entry.getKey()+".timeout",ConfigValueFactory.fromAnyRef(entry.getValue()+"s"));
        for(String phase:java.util.List.of("before-service-unbind","service-requests-done","before-actor-system-terminate"))result=result.withValue("pekko.coordinated-shutdown.phases."+phase+".recover",ConfigValueFactory.fromAnyRef(false));
        return result.withFallback(java.util.Objects.requireNonNull(base)).withFallback(ConfigFactory.load()).resolve();
    }
    public static Duration totalBudget(Config config){return BUDGETS.keySet().stream().map(p->config.getDuration("pekko.coordinated-shutdown.phases."+p+".timeout")).reduce(Duration.ZERO,Duration::plus);}
    public static void register(ActorSystem<?> system,ShutdownCoordinator.Hooks hooks){
        if(totalBudget(system.settings().config()).compareTo(Duration.ofSeconds(65))>0)throw new IllegalArgumentException("Shutdown graph exceeds 65s");
        var shutdown=CoordinatedShutdown.get(system);
        var shed=new AtomicBoolean();var settled=new AtomicBoolean();
        shutdown.addTask("before-service-unbind","signaling-admission-shed",()->{
            hooks.readinessOff();hooks.shedIngress();shed.set(true);return CompletableFuture.completedFuture(Done.getInstance());
        });
        shutdown.addTask("service-requests-done","signaling-physical-settle",()->{
            if(!shed.get())return CompletableFuture.failedFuture(new IllegalStateException("Admission still open"));
            return hooks.settleAdmitted().thenApply(v->{settled.set(true);return Done.getInstance();});
        });
        shutdown.addTask("before-actor-system-terminate","signaling-native-release-and-db-close",()->{
            if(!shed.get()||!settled.get())return CompletableFuture.failedFuture(new IllegalStateException("Physical settlement unproven"));
            // Cluster phases already ran. Never issue another cluster leave or competing framework handoff.
            return hooks.handoffAndRelease().thenCompose(v->hooks.settleAdmitted())
                .thenCompose(v->hooks.closeDatabase()).thenApply(v->Done.getInstance());
        });
    }
}
