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
        private record Placement(ActorRef parent,io.webrtc.signaling.storage.AuthoritySql.GroupToken token) {}
        private record WatchPlacement(int group,Placement placement) {}
        private record LostPlacement(int group,Placement placement) {}
        private boolean draining,acquisitionShed;private CompletionStage<Void> drained;
        private ActorRef monitor;private final Map<Integer,Placement> placements=new HashMap<>();
        private ScheduledThreadPoolExecutor timers;private final Map<Integer,PostgresShardLease.Engine> engines=new HashMap<>();
        State(ExtendedActorSystem system){this.system=system;}
        synchronized void install(GroupOwnership repository,String cell,long epoch,UUID pod,BooleanSupplier clock){
            if(this.repository!=null)throw new IllegalStateException("Lease dependencies already installed");
            var cluster=Cluster.get(system);var address=cluster.selfAddress();if(address.host().isEmpty()||address.port().isEmpty())throw new IllegalArgumentException("Lease requires bound member address");
            String hostPort=address.hostPort();String node=address.host().get()+":"+address.port().get()+"#"+cluster.selfUniqueAddress().longUid()+"#"+Objects.requireNonNull(pod);
            namespace=new PostgresShardLease.Namespace(system.name(),cell,epoch,hostPort,node);this.repository=Objects.requireNonNull(repository);this.clock=Objects.requireNonNull(clock);
            timers=new ScheduledThreadPoolExecutor(2,Thread.ofPlatform().daemon().name("group-lease-timer-",0).factory());timers.setRemoveOnCancelPolicy(true);
            monitor=system.systemActorOf(Props.create(PlacementMonitor.class,()->new PlacementMonitor(this)).withMailbox("signaling.lease-placement-mailbox"),"signaling-lease-placement");
            system.registerOnTermination(()->{synchronized(this){var releases=engines.values().stream().map(engine->{engine.invalidate(new IllegalStateException("ActorSystem stopped"));return engine.release().toCompletableFuture();}).toArray(CompletableFuture[]::new);CompletableFuture.allOf(releases).orTimeout(2,TimeUnit.SECONDS).whenComplete((v,e)->timers.shutdownNow());}});
        }
        synchronized PostgresShardLease.Engine resolve(LeaseSettings settings){
            if(repository==null)throw new IllegalStateException("Bounded database and clock dependencies must be installed before sharding");if(draining)throw new io.webrtc.signaling.storage.AuthoritySql.FencedException();
            int group=namespace.group(settings);return engines.computeIfAbsent(group,id->{var engine=new PostgresShardLease.Engine(settings,namespace,repository,timers,clock,System::nanoTime);if(acquisitionShed)engine.shedNewAcquisition();return engine;});
        }
        synchronized void bindPlacement(int group,ActorRef parent){
            var engine=engines.get(group);if(engine==null)throw new IllegalStateException("No acquired group");
            var grant=engine.currentGrant().orElseThrow(io.webrtc.signaling.storage.AuthoritySql.FencedException::new);var binding=new Placement(parent,grant.token());var old=placements.get(group);
            if(binding.equals(old))return;
            if(old!=null){engine.placementLost(old.token());throw new io.webrtc.signaling.storage.AuthoritySql.FencedException();}
            placements.put(group,binding);monitor.tell(new WatchPlacement(group,binding),ActorRef.noSender());
        }
        synchronized void placementLost(LostPlacement lost){if(placements.remove(lost.group(),lost.placement())){var engine=engines.get(lost.group());if(engine!=null)engine.placementLost(lost.placement().token());}}
        private static final class PlacementMonitor extends AbstractActor {
            private final State state;PlacementMonitor(State state){this.state=state;}
            @Override public Receive createReceive(){return receiveBuilder().match(WatchPlacement.class,w->getContext().watchWith(w.placement().parent(),new LostPlacement(w.group(),w.placement()))).match(LostPlacement.class,state::placementLost).build();}
        }
        synchronized void shedNewAcquisition(){requireInstalled();acquisitionShed=true;engines.values().forEach(PostgresShardLease.Engine::shedNewAcquisition);}
        synchronized CompletionStage<Void> drain(){
            requireInstalled();if(drained!=null)return drained;draining=true;
            var tasks=engines.values().stream().map(engine->engine.drain().toCompletableFuture()).toArray(CompletableFuture[]::new);
            drained=CompletableFuture.allOf(tasks).minimalCompletionStage();return drained;
        }
        synchronized void requireInstalled(){if(repository==null)throw new IllegalStateException("Lease dependencies not installed");}
        synchronized Optional<io.webrtc.signaling.storage.GroupOwnerRepository.Grant> currentGrant(int group){var engine=engines.get(group);return engine==null?Optional.empty():engine.currentGrant();}
    }
    public static void install(ActorSystem system,GroupOwnership repository,String cell,long epoch,UUID podUid,BooleanSupplier clockHealthy){ID.get(system).install(repository,cell,epoch,podUid,clockHealthy);}
    static PostgresShardLease.Engine resolve(LeaseSettings settings,ExtendedActorSystem system){return ID.get(system).resolve(settings);}
    public static void shedNewAcquisition(ActorSystem system){ID.get(system).shedNewAcquisition();}
    public static CompletionStage<Void> drain(ActorSystem system){return ID.get(system).drain();}
    public static void bindPlacement(ActorSystem system,int group,ActorRef shardParent){ID.get(system).bindPlacement(group,shardParent);}
    public static void requireInstalled(ActorSystem system){ID.get(system).requireInstalled();}
    public static String ownerNode(ActorSystem system){var state=ID.get(system);synchronized(state){state.requireInstalled();return state.namespace.ownerNode();}}
    public static Optional<io.webrtc.signaling.storage.GroupOwnerRepository.Grant> currentGrant(ActorSystem system,int group){return ID.get(system).currentGrant(group);}
}
