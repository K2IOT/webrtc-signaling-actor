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
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/** Genuine PostgreSQL/TLS/framework cleanup; TEST_ONLY clock/topology assertions, no production qualification. */
class NativeRuntimeDrainIT {
    @org.junit.jupiter.api.io.TempDir java.nio.file.Path temporary;
    static File cert(String name){return new File(Objects.requireNonNull(NativeRuntimeDrainIT.class.getResource("/test-only-pki/session/"+name)).getFile());}
    @Test void frameworkDrainRetainsNativeDatabaseAndRootUntilUnknownWorkPhysicallySettles()throws Exception {
        var runtime=new DbTestRuntime();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var first=new AtomicBoolean(true);
        var cryptoEntered=new CountDownLatch(1);var cryptoRelease=new CountDownLatch(1);
        try(var c=PgFixture.connection();var q=c.createStatement()){q.execute("INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,lease_sequence,status) SELECT 'c001',1,n,1,1,0,'IDLE' FROM generate_series(0,1023) n ON CONFLICT DO NOTHING");}
        var remotingPki=new NativeActorProcessIT();remotingPki.temporary=temporary;
        var config=PekkoShutdownLifecycle.config(ConfigFactory.parseString("""
            pekko.actor.provider=cluster
            pekko.remote.artery.canonical.hostname="127.0.0.1"
            pekko.remote.artery.canonical.port=0
            pekko.cluster.roles=["signaling-actor","az-test"]
            pekko.cluster.min-nr-of-members=1 # TEST_ONLY single-node lifecycle fixture
            pekko.cluster.role.signaling-actor.min-nr-of-members=1 # TEST_ONLY single-node lifecycle fixture
            pekko.coordinated-shutdown.run-by-jvm-shutdown-hook=off
            pekko.loglevel=WARNING
            """).withFallback(remotingPki.enrollment(0).config()));
        var system=ActorSystem.<Void>create(Behaviors.empty(),"runtime-drain-c001",config);
        try(var verifier=new BoundedTokenVerifier((token,now)->{if(token.equals("TEST_ONLY_HELD_VERIFY")){cryptoEntered.countDown();try{cryptoRelease.await();}catch(InterruptedException interrupted){Thread.currentThread().interrupt();}}throw new AuthException();},1,8,Duration.ofSeconds(1))){
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
            var defaults=new org.springframework.boot.env.YamlPropertySourceLoader().load("TEST_ONLY_defaults",new org.springframework.core.io.FileSystemResource("../config/production-defaults.yaml"));
            new ApplicationContextRunner().withUserConfiguration(SignalingApplication.class)
                .withInitializer(context->{defaults.forEach(value->context.getEnvironment().getPropertySources().addLast(value));context.getEnvironment().setActiveProfiles("actor");})
                .withPropertyValues("signaling.identity.issuer=TEST_ONLY_ISSUER","signaling.identity.audience=TEST_ONLY_AUDIENCE")
                .withBean(NativeActorComposition.class,()->composition)
                .withBean(CellRpcServer.class,()->server,NativeRuntimeDrainIT::nativeOwned).withBean(CellRpcClient.class,()->client,NativeRuntimeDrainIT::nativeOwned)
                .withBean(NativeWorkerScheduler.class,()->workers,NativeRuntimeDrainIT::nativeOwned)
                .withBean(DbBoundary.class,()->runtime.boundary,NativeRuntimeDrainIT::nativeOwned).withBean(DbPools.class,()->runtime.pools,NativeRuntimeDrainIT::nativeOwned)
                .withBean(PrivateHealthServer.class,()->health,NativeRuntimeDrainIT::nativeOwned)
                .run(context->{assertThat(context).hasNotFailed().hasSingleBean(NativeActorRuntimeHooks.class);
                    var hooks=context.getBean(NativeActorRuntimeHooks.class);
                    assertThat(hooks.closeDatabase().toCompletableFuture()).isCompletedExceptionally();assertThat(runtime.pools.closed()).isFalse();
                    assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
                    var originalCrypto=verifier.verify("TEST_ONLY_HELD_VERIFY",Instant.now());assertThat(cryptoEntered.await(1,TimeUnit.SECONDS)).isTrue();
                    var shutdown=CompletableFuture.runAsync(context::stop);
                    try{
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(()->assertThat(readiness.snapshot().draining()).isTrue());
                    readiness.update(new ClusterReadiness.Snapshot(true,true,true,true,true,true,6,3,false));assertThat(readiness.businessReady()).isFalse();
                    assertThat(shutdown.toCompletableFuture()).isNotDone();assertThat(runtime.pools.closed()).isFalse();assertThat(lease.checkLease()).isTrue();
                    release.countDown();
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(25)).until(()->!lease.checkLease());
                    assertThatThrownBy(()->shutdown.get(150,TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
                    assertThat(runtime.pools.closed()).isFalse();assertThat(originalCrypto.toCompletableFuture()).isNotDone();
                    cryptoRelease.countDown();assertThatThrownBy(()->originalCrypto.toCompletableFuture().join()).hasCauseInstanceOf(AuthException.class);
                    shutdown.toCompletableFuture().get(25,TimeUnit.SECONDS);
                    assertThat(runtime.pools.closed()).isTrue();assertThat(lease.checkLease()).isFalse();assertThat(hooks.databaseClosed()).isTrue();
                    }finally{release.countDown();cryptoRelease.countDown();}
                });
        }finally{release.countDown();cryptoRelease.countDown();system.terminate();system.getWhenTerminated().toCompletableFuture().get(25,TimeUnit.SECONDS);runtime.close();}
    }
    private static void nativeOwned(org.springframework.beans.factory.config.BeanDefinition definition){((org.springframework.beans.factory.support.AbstractBeanDefinition)definition).setDestroyMethodName("");}
}
