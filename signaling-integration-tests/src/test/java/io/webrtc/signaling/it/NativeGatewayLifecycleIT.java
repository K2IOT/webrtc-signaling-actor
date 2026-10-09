package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.app.*;
import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.InternalCommand;
import java.time.*;
import java.net.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.SmartLifecycle;

/** Actual Main listeners, mTLS and original construction ownership; local signed sources are TEST_ONLY. */
class NativeGatewayLifecycleIT {
    @Test void springStopRetainsOriginalNativeFlightAndPrivateHealthUntilPhysicalCleanup()throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var factories=new AtomicInteger();
        var cryptoEntered=new CountDownLatch(1);var cryptoRelease=new CountDownLatch(1);
        var sourceEntered=new CountDownLatch(1);var sourcePhysical=new CompletableFuture<Void>();
        var sourceWorkers=new NativeWorkerScheduler(List.of(new NativeWorkerScheduler.Job("test_only_source",NativeWorkerScheduler.Priority.SAFETY,Duration.ofMillis(100),budget->{sourceEntered.countDown();return new RpcOperation<>(CompletableFuture.completedFuture(true),sourcePhysical);})),event->{});
        var tasks=Executors.newVirtualThreadPerTaskExecutor();
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var now=Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
            var principal=new AuthPrincipal(new UserId("native-drain-caller"),new SessionKey("TEST_ONLY",UUID.randomUUID().toString()),now.plusSeconds(600),now,"TEST_ONLY",1);
            var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            var proofs=new HomeAuthorizationProof("c001","test",keys.getPrivate(),Map.of("c001/test",keys.getPublic()));
            try(var tokens=new BoundedTokenVerifier((t,n)->{if(t.equals("TEST_ONLY_HELD_VERIFY")){cryptoEntered.countDown();try{cryptoRelease.await();}catch(InterruptedException e){Thread.currentThread().interrupt();throw new AuthException();}}return principal;},1,8,Duration.ofSeconds(1))){
                var operations=new NativeSessionOperations(new SessionRegistryService(f.runtime.sql,"c001",1,(c,p)->p.equals(principal)),tokens,Clock.systemUTC(),proofs.sessionProofs(),proofs.relaySessionProofs(),()->true,CallAuthorizationPolicy.denyAll());
                var sessions=new NativeSessionHandler("c001",1,(peer,gateway)->gateway.gatewayId().equals("gw-1"),operations::execute);
                var tls=RpcTlsContexts.clients("test",NativeGatewayCommandIT.cert("ca.crt"),NativeGatewayCommandIT.cert("gateway.crt"),NativeGatewayCommandIT.cert("gateway.key"));
                try(var server=new CellRpcServer("c001","test",0,RpcTlsContexts.server("test","c001",NativeGatewayCommandIT.cert("ca.crt"),NativeGatewayCommandIT.cert("server.crt"),NativeGatewayCommandIT.cert("server.key")),new RpcAdmission(8,1048576,8,1048576),(o,c,p,b)->CompletableFuture.failedFuture(new AssertionError()),e->CompletableFuture.failedFuture(new AssertionError())).sessions(sessions).start();
                    var client=new CellRpcClient("test",Map.of("c001",new CellRpcClient.Endpoint("localhost",server.port(),"localhost")),cell->{if(factories.incrementAndGet()>1){entered.countDown();try{release.await();}catch(InterruptedException e){Thread.currentThread().interrupt();throw new IllegalStateException(e);}}return tls.context(cell);},new RpcAdmission(8,1048576,8,1048576));
                    var nativeMain=NativeGatewayCommandIT.mainGateway(client,tokens,keys)){
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(nativeMain.boot::current);
                    nativeMain.authenticate(principal);
                    var wire=InternalCommand.newBuilder().setDestinationCell("c001").setOperationId(UUID.randomUUID().toString()).setCallId("c001.e1."+UUID.randomUUID()).setRemainingBudgetMs(1000).build();
                    var original=tasks.submit(()->client.callTracked(CellRpcServer.Operation.RELAY,wire,Duration.ofSeconds(1)));
                    assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
                    // Consume the original construction budget independently of shutdown ordering.
                    var expired=new CompletableFuture<Void>();CompletableFuture.delayedExecutor(1100,TimeUnit.MILLISECONDS).execute(()->expired.complete(null));
                    var originalCrypto=tokens.verify("TEST_ONLY_HELD_VERIFY",now);assertThat(cryptoEntered.await(1,TimeUnit.SECONDS)).isTrue();
                    var defaults=new org.springframework.boot.env.YamlPropertySourceLoader().load("TEST_ONLY_defaults",new org.springframework.core.io.FileSystemResource("../config/production-defaults.yaml"));
                    new ApplicationContextRunner().withUserConfiguration(SignalingApplication.class)
                        .withInitializer(c->{defaults.forEach(v->c.getEnvironment().getPropertySources().addLast(v));c.getEnvironment().setActiveProfiles("gateway");})
                        .withPropertyValues("signaling.identity.issuer=TEST_ONLY_ISSUER","signaling.identity.audience=TEST_ONLY_AUDIENCE")
                        .withBean(NativeGatewayLifecycleEnrollment.class,()->new NativeGatewayLifecycleEnrollment(new InetSocketAddress("127.0.0.1",0),()->true,()->"TEST_ONLY 1\n"))
                        .withBean(NativeGatewayIngress.class,()->nativeMain.ingress,NativeGatewayLifecycleIT::owned)
                        .withBean(GatewayServer.class,()->nativeMain.wss,NativeGatewayLifecycleIT::owned)
                        .withBean(GatewayRelayRpcServer.class,()->nativeMain.relay,NativeGatewayLifecycleIT::owned)
                        .withBean(GatewayBootController.class,()->nativeMain.boot,NativeGatewayLifecycleIT::owned)
                        .withBean(NativeRelaySessionProofCache.class,()->nativeMain.cache,NativeGatewayLifecycleIT::owned)
                        .withBean(NativeGatewayServices.class,()->nativeMain.gateway)
                        .withBean(ClockSafetyMonitor.class,()->nativeMain.clock)
                        .withBean(BoundedTokenVerifier.class,()->tokens,NativeGatewayLifecycleIT::owned)
                        .withBean(NativeGatewaySafety.class,()->nativeMain.safety)
                        .withBean(CellRpcClient.class,()->client,NativeGatewayLifecycleIT::owned)
                        .withBean(NativeWorkerScheduler.class,()->sourceWorkers,NativeGatewayLifecycleIT::owned)
                        .run(context->{
                            assertThat(context).hasNotFailed().hasSingleBean(PrivateHealthServer.class);
                            var nativeLifecycles=context.getBeansOfType(SmartLifecycle.class).values().stream().filter(v->v.getClass().getSimpleName().equals("NativeGatewaySpringLifecycle")).toList();
                            assertThat(nativeLifecycles).hasSize(1);
                            var health=context.getBean(PrivateHealthServer.class);assertThat(health.port()).isPositive();
                            var stopped=CompletableFuture.runAsync(context::stop);
                            try{
                                assertThat(sourceEntered.await(2,TimeUnit.SECONDS)).as("Gateway lifecycle must start its enrolled safety source workers").isTrue();
                                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->!nativeMain.wss.getClass().getMethod("accepting").invoke(nativeMain.wss).equals(Boolean.TRUE));
                                assertThat(stopped).isNotDone();assertThat(nativeMain.boot.current()).isTrue();
                                assertThat(original).isNotDone();assertThat(health.port()).isPositive();
                                expired.get(2,TimeUnit.SECONDS);release.countDown();
                                var flight=original.get(3,TimeUnit.SECONDS);assertThat(flight.logical().toCompletableFuture().get(3,TimeUnit.SECONDS).getErrorCode()).isEqualTo("OUTCOME_UNKNOWN");
                                flight.physicalCompletion().toCompletableFuture().get(3,TimeUnit.SECONDS);
                                assertThatThrownBy(()->stopped.get(150,TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
                                assertThat(nativeMain.boot.current()).isTrue();assertThat(originalCrypto.toCompletableFuture()).isNotDone();
                                cryptoRelease.countDown();originalCrypto.toCompletableFuture().get(1,TimeUnit.SECONDS);
                                assertThatThrownBy(()->stopped.get(150,TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
                                sourcePhysical.complete(null);
                                stopped.get(15,TimeUnit.SECONDS);assertThat(nativeLifecycles.getFirst().isRunning()).isFalse();assertThat(nativeMain.boot.current()).isFalse();
                            }finally{release.countDown();cryptoRelease.countDown();sourcePhysical.complete(null);}
                        });
                }
            }
        }finally{release.countDown();cryptoRelease.countDown();sourcePhysical.complete(null);sourceWorkers.drain().toCompletableFuture().get(3,TimeUnit.SECONDS);tasks.close();}
    }
    static void owned(org.springframework.beans.factory.config.BeanDefinition definition){((org.springframework.beans.factory.support.AbstractBeanDefinition)definition).setDestroyMethodName("");}
}
