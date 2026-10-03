package io.webrtc.signaling.actors.cluster;
import static org.assertj.core.api.Assertions.*;
import com.typesafe.config.*;
import io.webrtc.signaling.actors.lease.PostgresShardLeaseProvider;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.protocol.Identity.*;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.typed.*;
import org.apache.pekko.cluster.sharding.typed.javadsl.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
class MultiNodeShardingIT {
    record PingUser(ActorRef<Reply> replyTo) implements UserMessage {}
    record PingCall(ActorRef<Reply> replyTo) implements CallMessage {}
    record Reply(String node,String entity,long epoch) implements CborSerializable {}
    enum StopUser implements UserMessage {INSTANCE}
    enum StopCall implements CallMessage {INSTANCE}
    static final String FINGERPRINT="a".repeat(64),SYSTEM="multi-sharding-c001";
    static DbTestRuntime runtime;static GroupOwnerRepository repository;static final List<ActorSystem<Void>> nodes=new ArrayList<>();static final AtomicInteger ungatedChildren=new AtomicInteger();
    static Config config(String zone,String fingerprint){return ConfigFactory.parseString("""
        pekko.remote.artery.transport=tcp
        pekko.remote.artery.canonical.hostname="127.0.0.1"
        pekko.remote.artery.canonical.port=0
        pekko.cluster.roles=["signaling-actor","az-%s"]
        signaling.cluster-fingerprint="%s"
        pekko.loglevel=WARNING
        pekko.cluster.jmx.multi-mbeans-in-same-jvm=on
        pekko.actor.serialization-bindings {
          "io.webrtc.signaling.actors.cluster.MultiNodeShardingIT$PingUser"=jackson-cbor
          "io.webrtc.signaling.actors.cluster.MultiNodeShardingIT$PingCall"=jackson-cbor
          "io.webrtc.signaling.actors.cluster.CborSerializable"=jackson-cbor
        }
        """.formatted(zone,fingerprint)).withFallback(ShardingConfigurationTest.config()).resolve();}
    static ActorSystem<Void> start(String zone,String fingerprint){var system=ActorSystem.<Void>create(Behaviors.empty(),SYSTEM,config(zone,fingerprint));PostgresShardLeaseProvider.install(Adapter.toClassic(system),repository,"c001",1,UUID.randomUUID(),()->true);return system;}
    @BeforeEach void setup()throws Exception {
        nodes.clear();runtime=new DbTestRuntime();repository=new GroupOwnerRepository(runtime.sql,"c001",1);try(var c=PgFixture.connection();var s=c.createStatement()){s.execute("INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,lease_sequence,status) SELECT 'c001',1,n,1,1,0,'IDLE' FROM generate_series(0,1023) n ON CONFLICT DO NOTHING");}
        for(String zone:List.of("a","a","b","c"))nodes.add(start(zone,FINGERPRINT));var seed=Cluster.get(nodes.getFirst()).selfMember().address();for(var node:nodes)Cluster.get(node).manager().tell(new JoinSeedNodes(List.of(seed)));
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(25)).until(()->nodes.stream().allMatch(n->Cluster.get(n).selfMember().status().equals(MemberStatus.up())));for(var node:nodes)register(node);
    }
    static void register(ActorSystem<Void> system){
        assertThat(system.settings().config().getString("pekko.cluster.sharding.state-store-mode")).isEqualTo("ddata");
        assertThat(ShardingBootstrap.userSettings(system.settings().config()).stateStoreMode().name()).isEqualTo("ddata");
        ShardingBootstrap.registerBoth(system,user->Behaviors.receive(UserMessage.class).onMessage(PingUser.class,p->{p.replyTo().tell(new Reply(node(system),user.getEntityId(),0));return Behaviors.same();}).onMessage(StopUser.class,p->Behaviors.stopped()).build(),StopUser.INSTANCE,call->Behaviors.setup(ctx->{
        int group=HomeParticipationService.group(new CallId(call.getEntityId()));var grant=PostgresShardLeaseProvider.currentGrant(Adapter.toClassic(system),group);if(grant.isEmpty()){ungatedChildren.incrementAndGet();throw new IllegalStateException("Child created before authoritative lease COMMIT");}
        return Behaviors.receive(CallMessage.class).onMessage(PingCall.class,p->{var current=PostgresShardLeaseProvider.currentGrant(Adapter.toClassic(system),group);if(current.isPresent())p.replyTo().tell(new Reply(node(system),call.getEntityId(),current.get().token().epoch()));return Behaviors.same();}).onMessage(StopCall.class,p->Behaviors.stopped()).build();
    }),StopCall.INSTANCE,new ClusterReadiness());}
    static String node(ActorSystem<?> system){return Cluster.get(system).selfMember().address().toString();}
    static Reply pingCall(ActorSystem<?> system,CallId call){return ClusterSharding.get(system).entityRefFor(ShardingBootstrap.CALL_TYPE,call.value()).<Reply>ask(PingCall::new,Duration.ofSeconds(20)).toCompletableFuture().join();}
    @AfterEach void close()throws Exception {for(var node:nodes)node.terminate();for(var node:nodes)node.getWhenTerminated().toCompletableFuture().get(20,TimeUnit.SECONDS);runtime.close();}
    @Test void fourNodeRuntimeGatesColdChildrenKeepsWarmTenureAndRecoversAfterCoordinatorHandoff()throws Exception {
        var source=nodes.getLast();var user=ClusterSharding.get(source).entityRefFor(ShardingBootstrap.USER_TYPE,"alice").<Reply>ask(PingUser::new,Duration.ofSeconds(10)).toCompletableFuture().join();assertThat(user.entity()).isEqualTo("alice");
        var call=new CallId("c001.e1.00000000-0000-0000-0000-000000000001");var cold=pingCall(source,call);var warm=pingCall(source,call);assertThat(warm.node()).isEqualTo(cold.node());assertThat(warm.epoch()).isEqualTo(cold.epoch());assertThat(ungatedChildren.get()).isZero();
        var holder=nodes.stream().filter(n->node(n).equals(cold.node())).findFirst().orElseThrow();holder.terminate();holder.getWhenTerminated().toCompletableFuture().get(20,TimeUnit.SECONDS);nodes.removeIf(n->n==holder);
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(()->{try(var c=PgFixture.connection();var q=c.createStatement();var r=q.executeQuery("SELECT status='RELEASED' OR group_epoch>"+cold.epoch()+" FROM group_owner WHERE group_id=685")){return r.next()&&r.getBoolean(1);}});
        var replacement=start("a",FINGERPRINT);Cluster.get(replacement).manager().tell(new JoinSeedNodes(List.of(Cluster.get(nodes.getFirst()).selfMember().address())));nodes.add(replacement);
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(25)).until(()->Cluster.get(replacement).selfMember().status().equals(MemberStatus.up()));register(replacement);
        Reply recovered=pingCall(replacement,call);assertThat(recovered.epoch()).isGreaterThan(cold.epoch());assertThat(recovered.node()).isNotEqualTo(cold.node());assertThat(ungatedChildren.get()).isZero();
        // Fault injection targets the coordinator alone via public ActorSelection/Kill.
        org.apache.pekko.actor.ActorRef coordinator=null;
        for(var n:nodes){try{coordinator=Adapter.toClassic(n).actorSelection("/system/sharding/SignalingCallV1Coordinator/singleton/coordinator").resolveOne(Duration.ofSeconds(1)).toCompletableFuture().join();break;}catch(CompletionException absent){}}
        assertThat(coordinator).isNotNull();coordinator.tell(org.apache.pekko.actor.Kill.getInstance(),org.apache.pekko.actor.ActorRef.noSender());
        Reply afterCoordinatorRestart=pingCall(replacement,call);assertThat(afterCoordinatorRestart.epoch()).isEqualTo(recovered.epoch());assertThat(afterCoordinatorRestart.node()).isEqualTo(recovered.node());
        assertThat(pingCall(replacement,new CallId("c001.e1.00000000-0000-0000-0000-000000000002")).entity()).endsWith("000000000002");
        // Controlled test-only full reformation retains PostgreSQL authority history.
        for(var n:nodes)n.terminate();for(var n:nodes)n.getWhenTerminated().toCompletableFuture().get(20,TimeUnit.SECONDS);nodes.clear();
        for(String zone:List.of("a","a","b","c"))nodes.add(start(zone,FINGERPRINT));var seed=Cluster.get(nodes.getFirst()).selfMember().address();for(var n:nodes)Cluster.get(n).manager().tell(new JoinSeedNodes(List.of(seed)));
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(25)).until(()->nodes.stream().allMatch(n->Cluster.get(n).selfMember().status().equals(MemberStatus.up())));for(var n:nodes)register(n);
        assertThat(pingCall(nodes.getLast(),call).epoch()).isGreaterThan(recovered.epoch());assertThat(ungatedChildren.get()).isZero();
    }
    @Test void incompatibleFingerprintCannotJoinTheCell()throws Exception {
        var incompatible=start("b","b".repeat(64));try{Cluster.get(incompatible).manager().tell(new JoinSeedNodes(List.of(Cluster.get(nodes.getFirst()).selfMember().address())));org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15)).until(()->incompatible.getWhenTerminated().toCompletableFuture().isDone());assertThat(nodes.stream().anyMatch(n->node(n).equals(node(incompatible)))).isFalse();}
        finally{incompatible.terminate();incompatible.getWhenTerminated().toCompletableFuture().get(10,TimeUnit.SECONDS);}
    }
}
