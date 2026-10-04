package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.app.*;
import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.actors.cluster.*;
import io.webrtc.signaling.actors.lease.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.rpc.*;
import org.apache.pekko.actor.CoordinatedShutdown;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.cluster.typed.*;
import org.apache.pekko.cluster.MemberStatus;
import com.typesafe.config.*;
import java.io.File;
import java.net.*;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

/** Genuine PostgreSQL/TLS/framework cleanup; TEST_ONLY clock/topology assertions, no production qualification. */
class NativeRuntimeDrainIT {
    static File cert(String name){return new File(Objects.requireNonNull(NativeRuntimeDrainIT.class.getResource("/test-only-pki/session/"+name)).getFile());}
    @Test void frameworkDrainRetainsNativeDatabaseAndRootUntilUnknownWorkPhysicallySettles()throws Exception {
        var runtime=new DbTestRuntime();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var first=new AtomicBoolean(true);
        try(var c=PgFixture.connection();var q=c.createStatement()){q.execute("INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,lease_sequence,status) SELECT 'c001',1,n,1,1,0,'IDLE' FROM generate_series(0,1023) n ON CONFLICT DO NOTHING");}
        var config=PekkoShutdownLifecycle.config(ConfigFactory.parseString("""
            pekko.actor.provider=cluster
            pekko.remote.artery.canonical.hostname="127.0.0.1"
            pekko.remote.artery.canonical.port=0
            pekko.cluster.roles=["signaling-actor","az-test"]
            pekko.cluster.min-nr-of-members=1 # TEST_ONLY single-node lifecycle fixture
            pekko.cluster.role.signaling-actor.min-nr-of-members=1 # TEST_ONLY single-node lifecycle fixture
            pekko.coordinated-shutdown.run-by-jvm-shutdown-hook=off
            pekko.loglevel=WARNING
            """).withFallback(ShardingBootstrap.baseConfig()));
        var system=ActorSystem.<Void>create(Behaviors.empty(),"runtime-drain-c001",config);
        try(var verifier=new BoundedTokenVerifier((token,now)->{throw new IllegalArgumentException("TEST_ONLY_UNUSED");},1,8,Duration.ofSeconds(1))){
            var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var proofs=new HomeAuthorizationProof("c001","test",keys.getPrivate(),Map.of("c001/test",keys.getPublic()));
            var readiness=new ClusterReadiness();readiness.update(new ClusterReadiness.Snapshot(false,false,true,true,true,true,4,3,false));
            var inputs=new NativeActorComposition.Inputs(runtime.sql,"c001",1,1,UUID.randomUUID(),proofs,u->new ProofBindings.TrustedHome("c001",1,1),(c,p)->false,(c,u)->false,(c,a,b)->false,verifier,CallAuthorizationPolicy.denyAll(),Clock.systemUTC(),()->true,(c,r)->false);
            var composition=new NativeActorComposition(system,inputs,readiness);
            Cluster.get(system).manager().tell(new Join(Cluster.get(system).selfMember().address()));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).until(()->Cluster.get(system).selfMember().status().equals(MemberStatus.up()));
            readiness.update(new ClusterReadiness.Snapshot(true,false,true,true,true,true,4,3,false));composition.register();
            var address=Cluster.get(system).selfMember().address();
            var lease=org.apache.pekko.coordination.lease.javadsl.LeaseProvider.get(Adapter.toClassic(system)).getLease("runtime-drain-c001-shard-SignalingCallV1-985","signaling.postgres-lease",address.hostPort());
            assertThat(lease.acquire().toCompletableFuture().join()).isTrue();
            var server=new CellRpcServer("c001","test",0,RpcTlsContexts.server("test","c001",cert("ca.crt"),cert("server.crt"),cert("server.key")),new RpcAdmission(2,196608,2,196608),(o,c,p,b)->CompletableFuture.failedFuture(new AssertionError()),e->CompletableFuture.failedFuture(new AssertionError())).start();
            var client=new CellRpcClient("test",Map.of("c001",new CellRpcClient.Endpoint("localhost",server.port(),"localhost")),RpcTlsContexts.clients("test",cert("ca.crt"),cert("server.crt"),cert("server.key")),new RpcAdmission(2,196608,2,196608));
            var health=new PrivateHealthServer(new InetSocketAddress("127.0.0.1",0),()->true,readiness::businessReady,()->"TEST_ONLY 1\n");health.start().toCompletableFuture().get(2,TimeUnit.SECONDS);
            var workers=new NativeWorkerScheduler(List.of(new NativeWorkerScheduler.Job("native_tick",NativeWorkerScheduler.Priority.SAFETY,Duration.ofMillis(100),b->{var work=runtime.sql.submitTracked(DbClass.RENEWAL,Duration.ofMillis(100),c->{if(first.getAndSet(false)){entered.countDown();release.await();}return true;});return new RpcOperation<>(work.logical(),work.physicalCompletion());})),e->{});
            var hooks=new NativeActorRuntimeHooks(composition,server,client,workers,runtime.boundary,runtime.pools,health);
            assertThat(hooks.closeDatabase().toCompletableFuture()).isCompletedExceptionally();assertThat(runtime.pools.closed()).isFalse();
            PekkoShutdownLifecycle.register(system,hooks);workers.start();assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
            var shutdown=CoordinatedShutdown.get(system).runAll(CoordinatedShutdown.unknownReason());
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(()->readiness.snapshot().draining());
            readiness.update(new ClusterReadiness.Snapshot(true,true,true,true,true,true,6,3,false));assertThat(readiness.businessReady()).isFalse();
            assertThat(shutdown.toCompletableFuture()).isNotDone();assertThat(runtime.pools.closed()).isFalse();assertThat(lease.checkLease()).isTrue();
            release.countDown();shutdown.toCompletableFuture().get(25,TimeUnit.SECONDS);
            assertThat(runtime.pools.closed()).isTrue();assertThat(lease.checkLease()).isFalse();assertThat(hooks.databaseClosed()).isTrue();
        }finally{release.countDown();system.terminate();system.getWhenTerminated().toCompletableFuture().get(25,TimeUnit.SECONDS);runtime.close();}
    }
}
