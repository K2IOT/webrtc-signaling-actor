package io.webrtc.signaling.actors.lease;
import io.webrtc.signaling.storage.GroupOwnership;
import org.apache.pekko.actor.*;
import org.apache.pekko.cluster.Cluster;
import org.apache.pekko.coordination.lease.LeaseSettings;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
/** ActorSystem-scoped dependency and controller registry; the public Pekko provider loads the adapter. */
public final class PostgresShardLeaseProvider {
    private PostgresShardLeaseProvider() {}
    private static final class Id extends AbstractExtensionId<State> {
        @Override public State createExtension(ExtendedActorSystem system){return new State(system);}
    }
    private static final Id ID=new Id();
    private static final class State implements Extension {
        private final ExtendedActorSystem system;private GroupOwnership repository;private PostgresShardLease.Namespace namespace;private BooleanSupplier clock;
        private ScheduledThreadPoolExecutor timers;private final Map<Integer,PostgresShardLease.Engine> engines=new HashMap<>();
        State(ExtendedActorSystem system){this.system=system;}
        synchronized void install(GroupOwnership repository,String cell,long epoch,UUID pod,BooleanSupplier clock){
            if(this.repository!=null)throw new IllegalStateException("Lease dependencies already installed");
            var cluster=Cluster.get(system);var address=cluster.selfAddress();if(address.host().isEmpty()||address.port().isEmpty())throw new IllegalArgumentException("Lease requires bound member address");
            String hostPort=address.host().get()+":"+address.port().get();String node=hostPort+"#"+cluster.selfUniqueAddress().longUid()+"#"+Objects.requireNonNull(pod);
            namespace=new PostgresShardLease.Namespace(system.name(),cell,epoch,hostPort,node);this.repository=Objects.requireNonNull(repository);this.clock=Objects.requireNonNull(clock);
            timers=new ScheduledThreadPoolExecutor(2,Thread.ofPlatform().daemon().name("group-lease-timer-",0).factory());timers.setRemoveOnCancelPolicy(true);
            system.registerOnTermination(()->{synchronized(this){engines.values().forEach(engine->engine.invalidate(new IllegalStateException("ActorSystem stopped")));timers.shutdownNow();}});
        }
        synchronized PostgresShardLease.Engine resolve(LeaseSettings settings){
            if(repository==null)throw new IllegalStateException("Bounded database and clock dependencies must be installed before sharding");
            int group=namespace.group(settings);return engines.computeIfAbsent(group,id->new PostgresShardLease.Engine(settings,namespace,repository,timers,clock,System::nanoTime));
        }
    }
    public static void install(ActorSystem system,GroupOwnership repository,String cell,long epoch,UUID podUid,BooleanSupplier clockHealthy){ID.get(system).install(repository,cell,epoch,podUid,clockHealthy);}
    static PostgresShardLease.Engine resolve(LeaseSettings settings,ExtendedActorSystem system){return ID.get(system).resolve(settings);}
}
