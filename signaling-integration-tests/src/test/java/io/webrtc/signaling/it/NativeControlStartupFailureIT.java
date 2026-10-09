package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.app.*;
import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.control.PostgresDirectoryRepository;
import io.webrtc.signaling.rpc.RpcOperation;
import java.net.*;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.*;
import org.springframework.boot.*;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.FileSystemResource;

/** Original Main/PostgreSQL/Netty resources; delayed worker/DB tails are explicitly TEST_ONLY. */
class NativeControlStartupFailureIT {
    @Test void originalScopedSourceClosesWithInstalledControlProcess()throws Exception {
        try(var fixture=new Fixture()){
            var identity=new IdentitySecurityContract("TEST_ONLY_ISSUER","TEST_ONLY_AUDIENCE",Duration.ofMinutes(10),Duration.ofSeconds(30),Duration.ofSeconds(1),Duration.ofSeconds(5),true,"TEST_ONLY_SOURCE");
            var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            var cache=new NativeCachedSecurity(identity,new io.webrtc.signaling.storage.worker.RevocationSourceVerifier("c001",identity.issuer(),java.util.Map.of("source",keys.getPublic())),16,java.time.Clock.systemUTC(),System::nanoTime);
            var source=new NativeCachedRevocationSource(URI.create("https://localhost:1/revocations"),NativeClockSourceIT.clientTls(true),cache,java.util.UUID.randomUUID(),java.util.UUID.randomUUID());
            var app=fixture.application();app.addInitializers(context->context.getBeanFactory().registerSingleton("TEST_ONLY_scoped_source",source));
            try(var context=fixture.run(app)){
                var now=java.time.Instant.now();
                var unsigned=new io.webrtc.signaling.storage.worker.RevocationReconciler.Batch(0,0,List.of(),now,"",List.of(),0);
                var signer=java.security.Signature.getInstance("Ed25519");signer.initSign(keys.getPrivate());signer.update(io.webrtc.signaling.storage.worker.RevocationSourceVerifier.signingBytes("c001",identity.issuer(),unsigned));
                var signed=new io.webrtc.signaling.storage.worker.RevocationReconciler.Batch(0,0,List.of(),now,"source."+java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(signer.sign()),List.of(),0);
                assertThat(cache.apply(signed,System.nanoTime())).isTrue();assertThat(source.usable()).isTrue();
                context.stop();assertThat(source.usable()).isFalse();
                assertThat(source.poll(Duration.ofSeconds(1)).logical().toCompletableFuture()).isCompletedExceptionally();
                assertThat(source.drain().toCompletableFuture()).isCompleted();}
            finally{source.drain().toCompletableFuture().get(2,TimeUnit.SECONDS);}
        }
    }
    @Test void managedSourceCannotPollADifferentClockThanBusinessAuthorization()throws Exception {
        try(var fixture=new Fixture()){
            var app=fixture.application();app.addInitializers(context->context.getBeanFactory().registerSingleton("TEST_ONLY_source",new NativeControlSourceEnrollment(
                "c001",1,fixture.clock.pod,fixture.clock.processBoot,new NativeActorSourceEnrollment.Endpoint(URI.create("https://localhost:1/clock"),NativeControlDatabaseFactoryIT.clientTls(),
                java.util.Map.of("TEST_ONLY",fixture.clock.signing.getPublic())),event->{})));
            var failure=catchThrowable(()->{try(var context=fixture.run(app)){}});
            assertThat(failure).hasStackTraceContaining("Control clock ownership differs");
            assertThat(fixture.database.runtime.pools.closed()).isTrue();
        }
    }
    @Test void mismatchedVerifierOwnershipRetiresBothOriginalCpuOwners()throws Exception {
        try(var fixture=new Fixture();var other=new BoundedTokenVerifier((token,now)->{throw new IllegalArgumentException("TEST_ONLY different verifier");},1,8,Duration.ofSeconds(1))){
            var app=fixture.application();app.addInitializers(context->context.getBeanFactory().registerSingleton("TEST_ONLY_otherVerifier",other));
            var failure=catchThrowable(()->fixture.run(app));
            assertThat(failure).hasStackTraceContaining("Native control verifier ownership differs");
            assertThatThrownBy(()->other.verify("TEST_ONLY",java.time.Instant.now()).toCompletableFuture().join()).hasCauseInstanceOf(RejectedExecutionException.class);
            assertThatThrownBy(()->fixture.tokens.verify("TEST_ONLY",java.time.Instant.now()).toCompletableFuture().join()).hasCauseInstanceOf(RejectedExecutionException.class);
            assertThat(fixture.database.runtime.pools.closed()).isTrue();
        }
    }
    @Test void unregisteredDirectorySqlRejectsBeforeHttpsAndRetiresOnlyPublishedOwners()throws Exception {
        try(var fixture=new Fixture();var unregistered=new LocalInviteAtomicIT.Fixture()){
            fixture.directory=new PostgresDirectoryRepository(fixture.database.runtime.sql,unregistered.runtime.sql,"c001",1,()->true);
            var failure=catchThrowable(()->fixture.run(fixture.application()));
            assertThat(failure).hasStackTraceContaining("Control directory SQL owners are not enrolled");
            assertThatThrownBy(()->fixture.tokens.verify("TEST_ONLY",java.time.Instant.now()).toCompletableFuture().join())
                .hasCauseInstanceOf(RejectedExecutionException.class);
            assertThat(fixture.database.runtime.pools.closed()).isTrue();
            assertThat(unregistered.runtime.pools.closed()).isFalse();
        }
    }
    @Test void mainStartsEnrolledSourceWorkersAndRetainsBothAuthorityPoolsUntilPhysicalRetirement()throws Exception {
        var invoked=new CountDownLatch(1);var physical=new CompletableFuture<Void>();
        try(var fixture=new Fixture();var local=new LocalInviteAtomicIT.Fixture()){
            var worker=new NativeWorkerScheduler(List.of(new NativeWorkerScheduler.Job("TEST_ONLY_source".toLowerCase(),NativeWorkerScheduler.Priority.SAFETY,
                Duration.ofMillis(100),budget->{invoked.countDown();return new RpcOperation<>(CompletableFuture.completedFuture(true),physical);})),event->{});
            fixture.directory=new PostgresDirectoryRepository(fixture.database.runtime.sql,local.runtime.sql,"c001",1,()->true);
            var app=fixture.application();app.addInitializers(context->{
                context.getBeanFactory().registerSingleton("TEST_ONLY_localSql",local.runtime.sql);
                context.getBeanFactory().registerSingleton("TEST_ONLY_sourceWorkers",worker);
            });
            try(var context=fixture.run(app)){
                try{
                    assertThat(invoked.await(2,TimeUnit.SECONDS)).as("Main must start enrolled safety source jobs").isTrue();
                    var stopped=CompletableFuture.runAsync(context::stop);
                    assertThatThrownBy(()->stopped.get(200,TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
                    assertThat(fixture.database.runtime.pools.closed()).isFalse();assertThat(local.runtime.pools.closed()).isFalse();
                    physical.complete(null);stopped.get(5,TimeUnit.SECONDS);
                    assertThat(fixture.database.runtime.pools.closed()).isTrue();assertThat(local.runtime.pools.closed()).isTrue();
                }finally{physical.complete(null);worker.drain().toCompletableFuture().get(3,TimeUnit.SECONDS);}
            }
        }
    }

    @Test void failureBeforeLifecycleStartWaitsForOriginalDbTailAndClosesPublishedPrivateProbe()throws Exception {
        var admitted=new CountDownLatch(1);var release=new CountDownLatch(1);
        var health=new AtomicReference<PrivateHealthServer>();var original=new AtomicReference<DbOperation<Boolean>>();
        try(var fixture=new Fixture()){
            var app=fixture.application();
            app.addInitializers(context->{
                var fault=new RootBeanDefinition(Object.class,()->{
                    health.set(context.getBean(PrivateHealthServer.class));
                    original.set(fixture.database.runtime.boundary.submitTracked(DbClass.RECOVERY,Duration.ofMillis(100),()->{
                        fixture.database.runtime.sql.submit(DbClass.RECOVERY,Duration.ofSeconds(2),c->{try(var q=c.createStatement();var r=q.executeQuery("SELECT 1")){return r.next();}}).toCompletableFuture().join();
                        admitted.countDown();release.await();return true;
                    }));
                    try{if(!admitted.await(2,TimeUnit.SECONDS))throw new IllegalStateException("TEST_ONLY DB did not start");}
                    catch(InterruptedException e){Thread.currentThread().interrupt();throw new IllegalStateException(e);}
                    throw new IllegalStateException("TEST_ONLY fail before lifecycle start");
                });
                fault.setDependsOn("nativeControlHealth");
                ((BeanDefinitionRegistry)context.getBeanFactory()).registerBeanDefinition("TEST_ONLY_fault",fault);
            });
            var failed=CompletableFuture.supplyAsync(()->catchThrowable(()->fixture.run(app)));
            try{
                assertThat(admitted.await(10,TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(()->original.get().logical().toCompletableFuture().get(1,TimeUnit.SECONDS)).hasCauseInstanceOf(DbOutcomeUnknownException.class);
                assertThatThrownBy(()->failed.get(200,TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
                assertThat(fixture.database.runtime.pools.closed()).isFalse();assertThat(original.get().physicalCompletion().toCompletableFuture().isDone()).isFalse();
                int port=health.get().port();
                release.countDown();
                assertThat(failed.get(5,TimeUnit.SECONDS)).hasRootCauseMessage("TEST_ONLY fail before lifecycle start");
                original.get().physicalCompletion().toCompletableFuture().get(1,TimeUnit.SECONDS);
                assertThat(fixture.database.runtime.pools.closed()).isTrue();
                assertThatThrownBy(()->new Socket("127.0.0.1",port)).isInstanceOf(java.io.IOException.class);
            }finally{release.countDown();failed.get(5,TimeUnit.SECONDS);}
        }
    }

    static final class Fixture implements AutoCloseable {
        final LocalInviteAtomicIT.Fixture database=new LocalInviteAtomicIT.Fixture();
        final NativeGatewayCommandIT.MainGateway clock=new NativeGatewayCommandIT.MainGateway(KeyPairGenerator.getInstance("Ed25519").generateKeyPair());
        final BoundedTokenVerifier tokens=new BoundedTokenVerifier((token,now)->{throw new IllegalArgumentException("TEST_ONLY no tokens");},1,8,Duration.ofSeconds(1));
        PostgresDirectoryRepository directory=new PostgresDirectoryRepository(database.runtime.sql,database.runtime.sql,"c001",1,()->true);
        Fixture()throws Exception{clock.refreshClock();clock.reports.scheduleAtFixedRate(clock::refreshClock,1,1,TimeUnit.SECONDS);}
        SpringApplication application()throws Exception {
            var business=new NativeControlBusinessEnrollment(directory,tokens,(p,n)->AuthorizationStatus.ALLOWED,()->true,clock.clock,Duration.ofSeconds(30));
            var tls=io.netty.handler.ssl.SslContextBuilder.forServer(NativeGatewayCommandIT.cert("gateway.crt"),NativeGatewayCommandIT.cert("gateway.key")).sslProvider(io.netty.handler.ssl.SslProvider.JDK).protocols("TLSv1.3").build();
            var ingress=new NativeControlIngressEnrollment(new InetSocketAddress("127.0.0.1",0),tls,new InetSocketAddress("127.0.0.1",0),()->true,()->"TEST_ONLY 1\n");
            var defaults=new YamlPropertySourceLoader().load("TEST_ONLY_defaults",new FileSystemResource("../config/production-defaults.yaml"));
            var app=SignalingApplication.application();app.setWebApplicationType(WebApplicationType.REACTIVE);app.setRegisterShutdownHook(false);
            app.addInitializers(context->{
                defaults.forEach(source->context.getEnvironment().getPropertySources().addLast(source));
                context.getBeanFactory().registerSingleton("TEST_ONLY_business",business);
                context.getBeanFactory().registerSingleton("TEST_ONLY_ingress",ingress);
                context.getBeanFactory().registerSingleton("TEST_ONLY_sql",database.runtime.sql);
            });return app;
        }
        org.springframework.context.ConfigurableApplicationContext run(SpringApplication app){return app.run("--spring.profiles.active=control","--signaling.identity.issuer=TEST_ONLY","--signaling.identity.audience=TEST_ONLY");}
        @Override public void close()throws Exception{tokens.close();clock.close();database.close();}
    }
}
