package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.app.*;
import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.actors.cluster.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.rpc.*;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.cluster.typed.*;
import org.apache.pekko.cluster.MemberStatus;
import com.typesafe.config.*;
import java.net.*;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.FileSystemResource;

/** Actual failed Main, native SQL/lease/listener; single-node topology is TEST_ONLY. */
class NativeActorStartupFailureIT {
    @ParameterizedTest @ValueSource(booleans={true,false})
    void partialActorLaunchRetainsLeaseAndPoolsUntilOriginalSqlSettles(boolean refreshFailure)throws Exception {
        var runtime=new DbTestRuntime();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        var failing=new CountDownLatch(1);
        try(var c=PgFixture.connection();var q=c.createStatement()){
            q.execute("INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,lease_sequence,status) SELECT 'c001',1,n,1,1,0,'IDLE' FROM generate_series(0,1023) n ON CONFLICT DO NOTHING");
        }
        var config=PekkoShutdownLifecycle.config(ConfigFactory.parseString("""
            pekko.actor.provider=cluster
            pekko.remote.artery.canonical.hostname="127.0.0.1"
            pekko.remote.artery.canonical.port=0
            pekko.cluster.roles=["signaling-actor","az-test"]
            pekko.cluster.min-nr-of-members=1
            pekko.cluster.role.signaling-actor.min-nr-of-members=1
            pekko.coordinated-shutdown.run-by-jvm-shutdown-hook=off
            pekko.loglevel=WARNING
            """).withFallback(ShardingBootstrap.baseConfig()));
        var system=ActorSystem.<Void>create(Behaviors.empty(),"startup-failure-c001",config);
        var tokens=new BoundedTokenVerifier((token,now)->{throw new AuthException();},1,8,Duration.ofSeconds(1));
        CellRpcServer server=null;PrivateHealthServer health=null;
        var workers=new NativeWorkerScheduler(List.of(new NativeWorkerScheduler.Job("test_only_tick",NativeWorkerScheduler.Priority.SAFETY,Duration.ofMillis(100),b->new RpcOperation<>(CompletableFuture.completedFuture(true),CompletableFuture.completedFuture(null)))),e->{});
        try {
            var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            var proofs=new HomeAuthorizationProof("c001","test",keys.getPrivate(),Map.of("c001/test",keys.getPublic()));
            var readiness=new ClusterReadiness();readiness.update(new ClusterReadiness.Snapshot(false,false,true,true,true,true,4,3,false));
            var composition=new NativeActorComposition(system,new NativeActorComposition.Inputs(runtime.sql,"c001",1,1,UUID.randomUUID(),proofs,u->new ProofBindings.TrustedHome("c001",1,1),(c,p)->false,(c,u)->false,(c,a,b)->false,tokens,CallAuthorizationPolicy.denyAll(),Clock.systemUTC(),()->true,(c,r)->false),readiness);
            Cluster.get(system).manager().tell(new Join(Cluster.get(system).selfMember().address()));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).until(()->Cluster.get(system).selfMember().status().equals(MemberStatus.up()));
            readiness.update(new ClusterReadiness.Snapshot(true,false,true,true,true,true,4,3,false));composition.register();
            var lease=org.apache.pekko.coordination.lease.javadsl.LeaseProvider.get(Adapter.toClassic(system)).getLease("startup-failure-c001-shard-SignalingCallV1-985","signaling.postgres-lease",Cluster.get(system).selfMember().address().hostPort());
            assertThat(lease.acquire().toCompletableFuture().join()).isTrue();
            server=new CellRpcServer("c001","test",0,RpcTlsContexts.server("test","c001",NativeRuntimeDrainIT.cert("ca.crt"),NativeRuntimeDrainIT.cert("server.crt"),NativeRuntimeDrainIT.cert("server.key")),new RpcAdmission(2,196608,2,196608),(o,c,p,b)->CompletableFuture.failedFuture(new AssertionError()),e->CompletableFuture.failedFuture(new AssertionError())).start();
            health=new PrivateHealthServer(new InetSocketAddress("127.0.0.1",0),()->true,readiness::businessReady,()->"TEST_ONLY 1\n");health.start().toCompletableFuture().get(2,TimeUnit.SECONDS);
            int rpcPort=server.port(),healthPort=health.port();
            var original=runtime.sql.submitTracked(DbClass.CRITICAL,Duration.ofMillis(100),c->{entered.countDown();release.await();return true;});
            assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
            var defaults=new YamlPropertySourceLoader().load("TEST_ONLY_defaults",new FileSystemResource("../config/production-defaults.yaml"));
            var application=SignalingApplication.application();application.setWebApplicationType(WebApplicationType.NONE);application.setRegisterShutdownHook(false);
            var actualServer=server;var actualHealth=health;
            application.addInitializers(context->{
                defaults.forEach(s->context.getEnvironment().getPropertySources().addLast(s));
                var registry=(org.springframework.beans.factory.support.BeanDefinitionRegistry)context.getBeanFactory();
                owned(registry,"testOnlyComposition",NativeActorComposition.class,composition);
                owned(registry,"testOnlyServer",CellRpcServer.class,actualServer);
                owned(registry,"testOnlyDatabase",DbBoundary.class,runtime.boundary);
                owned(registry,"testOnlyPools",DbPools.class,runtime.pools);
                owned(registry,"testOnlyWorkers",NativeWorkerScheduler.class,workers);
                owned(registry,"testOnlyHealth",PrivateHealthServer.class,actualHealth);
                context.getBeanFactory().addBeanPostProcessor(new BeanPostProcessor(){
                    @Override public Object postProcessAfterInitialization(Object value,String name){
                        if(name.equals("testOnlyHealth")){failing.countDown();if(refreshFailure)throw new IllegalStateException("TEST_ONLY failure before actor lifecycle installation");}
                        return value;
                    }
                });
            });
            var launch=CompletableFuture.runAsync(()->application.run("--spring.profiles.active=actor","--signaling.identity.issuer=TEST_ONLY_ISSUER","--signaling.identity.audience=TEST_ONLY_AUDIENCE"));
            try {
                assertThat(failing.await(10,TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(()->launch.get(500,TimeUnit.MILLISECONDS)).as("failed startup must retain original native SQL cleanup").isInstanceOf(TimeoutException.class);
                assertThat(runtime.pools.closed()).isFalse();assertThat(lease.checkLease()).isTrue();
                release.countDown();original.physicalCompletion().toCompletableFuture().get(3,TimeUnit.SECONDS);
                var failure=catchThrowable(()->launch.get(25,TimeUnit.SECONDS));
                assertThat(failure).isInstanceOf(ExecutionException.class);
                for(Throwable cause=failure;cause!=null;cause=cause.getCause())
                    assertThat(Arrays.stream(cause.getSuppressed()).map(Throwable::getMessage).toList())
                        .as("original startup cleanup must be idempotent across runner and failure event")
                        .doesNotContain("Native startup cleanup unproven");
                assertThat(runtime.pools.closed()).as("failed Main must close pools after native roots").isTrue();
                assertThat(lease.checkLease()).isFalse();
                assertThat(system.getWhenTerminated().toCompletableFuture()).isCompleted();
                assertThatThrownBy(()->new Socket("127.0.0.1",rpcPort)).isInstanceOf(java.io.IOException.class);
                assertThatThrownBy(()->new Socket("127.0.0.1",healthPort)).isInstanceOf(java.io.IOException.class);
            } finally {release.countDown();}
        } finally {
            release.countDown();workers.drain().toCompletableFuture().get(3,TimeUnit.SECONDS);
            if(server!=null)server.drain().toCompletableFuture().get(5,TimeUnit.SECONDS);
            if(health!=null)health.stop().toCompletableFuture().get(5,TimeUnit.SECONDS);
            system.terminate();system.getWhenTerminated().toCompletableFuture().get(25,TimeUnit.SECONDS);
            tokens.close();runtime.close();
        }
    }
    @Test void unpublishedSqlBoundaryCannotProveNativePoolCleanup()throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var runtime=new DbTestRuntime()){
            var original=runtime.sql.submitTracked(DbClass.CRITICAL,Duration.ofMillis(100),c->{entered.countDown();release.await();return true;});
            try {
                assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
                var defaults=new YamlPropertySourceLoader().load("TEST_ONLY_defaults",new FileSystemResource("../config/production-defaults.yaml"));
                var application=SignalingApplication.application();application.setWebApplicationType(WebApplicationType.NONE);application.setRegisterShutdownHook(false);
                application.addInitializers(context->{
                    defaults.forEach(s->context.getEnvironment().getPropertySources().addLast(s));
                    owned((org.springframework.beans.factory.support.BeanDefinitionRegistry)context.getBeanFactory(),"testOnlyPools",DbPools.class,runtime.pools);
                    context.getBeanFactory().addBeanPostProcessor(new BeanPostProcessor(){
                        @Override public Object postProcessAfterInitialization(Object bean,String name){
                            if(name.equals("testOnlyPools"))throw new IllegalStateException("TEST_ONLY pool without physical boundary");return bean;
                        }
                    });
                });
                var failure=catchThrowable(()->application.run("--spring.profiles.active=actor","--signaling.identity.issuer=TEST_ONLY_ISSUER","--signaling.identity.audience=TEST_ONLY_AUDIENCE"));
                assertThat(failure).isNotNull();
                assertThat(runtime.pools.closed()).as("no original SQL receipt means pool closure is unproven").isFalse();
                assertThat(Arrays.stream(failure.getSuppressed()).map(Throwable::getMessage).toList()).contains("Native startup cleanup unproven");
            } finally {release.countDown();original.physicalCompletion().toCompletableFuture().get(3,TimeUnit.SECONDS);}
        }
    }
    private static <T> void owned(org.springframework.beans.factory.support.BeanDefinitionRegistry registry,String name,Class<T> type,T value){
        var definition=new RootBeanDefinition(type,()->value);definition.setDestroyMethodName("");registry.registerBeanDefinition(name,definition);
    }
}
