package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.app.SignalingApplication;
import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.rpc.*;
import java.net.*;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.FileSystemResource;

/** Real SpringApplication failure after native allocation, with explicit TEST_ONLY enrollment. */
class NativeGatewayStartupFailureIT {
    @ParameterizedTest @ValueSource(booleans={false,true})
    void failedLaunchDoesNotLeaveNativeListenersOpen(boolean healthBindFailure)throws Exception {
        var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        var ingress=new AtomicReference<NativeGatewayIngress>();
        var boot=new AtomicReference<GatewayBootController>();
        var cache=new AtomicReference<NativeRelaySessionProofCache>();
        var relayPort=new AtomicInteger();
        var defaults=new YamlPropertySourceLoader().load("TEST_ONLY_defaults",new FileSystemResource("../config/production-defaults.yaml"));
        try(var occupied=new ServerSocket(0,1,InetAddress.getLoopbackAddress());
            var tokens=new BoundedTokenVerifier((t,n)->{throw new IllegalArgumentException();},1,8,Duration.ofSeconds(1));
            var client=new CellRpcClient("test",Map.of("c001",new CellRpcClient.Endpoint("localhost",1,"localhost")),RpcTlsContexts.clients("test",NativeGatewayCommandIT.cert("ca.crt"),NativeGatewayCommandIT.cert("gateway.crt"),NativeGatewayCommandIT.cert("gateway.key")),new RpcAdmission(8,1048576,8,1048576))){
            var clock=new ClockSafetyMonitor("c001",1,UUID.randomUUID(),UUID.randomUUID(),Map.of("TEST_ONLY",keys.getPublic()),Clock.systemUTC(),System::nanoTime);
            var business=new NativeGatewayBusinessEnrollment(new NativeSessionHandler.GatewayIdentity("gw-1",UUID.randomUUID(),"c001",1,"TEST_ONLY_REGION"),1,tokens,(p,n)->AuthorizationStatus.FRESHNESS_UNKNOWN,u->new ProofBindings.TrustedHome("c001",1,1),new RelaySessionAuthorizationProof(Map.of("c001/test",keys.getPublic())),4,2);
            var wssTls=io.netty.handler.ssl.SslContextBuilder.forServer(NativeGatewayCommandIT.cert("gateway.crt"),NativeGatewayCommandIT.cert("gateway.key")).sslProvider(io.netty.handler.ssl.SslProvider.JDK).protocols("TLSv1.3").build();
            var inputs=new NativeGatewayIngressEnrollment(new InetSocketAddress("127.0.0.1",0),wssTls,new GatewayServer.UpgradePolicy(Set.of("https://app.test"),h->false),1,8,16,EdgeAdmission.Limits.candidate(),"test",0,RpcTlsContexts.gatewayServer("test","c001","gw-1",NativeGatewayCommandIT.cert("ca.crt"),NativeGatewayCommandIT.cert("gateway.crt"),NativeGatewayCommandIT.cert("gateway.key")),new RpcAdmission(8,1048576,8,1048576),new RpcAdmission(8,1048576,8,1048576),GatewaySecuritySweep.Settings.candidate());
            var application=SignalingApplication.application();
            application.setWebApplicationType(WebApplicationType.NONE);
            application.setRegisterShutdownHook(false);
            application.addInitializers(context->{
                defaults.forEach(s->context.getEnvironment().getPropertySources().addLast(s));
                var beans=context.getBeanFactory();
                beans.registerSingleton("TEST_ONLY_business",business);beans.registerSingleton("TEST_ONLY_ingress",inputs);
                beans.registerSingleton("TEST_ONLY_clock",clock);beans.registerSingleton("TEST_ONLY_client",client);
                if(healthBindFailure)beans.registerSingleton("TEST_ONLY_lifecycle",new NativeGatewayLifecycleEnrollment(new InetSocketAddress(InetAddress.getLoopbackAddress(),occupied.getLocalPort()),()->true,()->"TEST_ONLY 1\n"));
                beans.addBeanPostProcessor(new BeanPostProcessor(){public Object postProcessAfterInitialization(Object value,String name){
                    if(value instanceof NativeGatewayIngress owner){ingress.set(owner);relayPort.set(owner.relay().port());}
                    if(value instanceof GatewayBootController owner)boot.set(owner);
                    if(value instanceof NativeRelaySessionProofCache owner)cache.set(owner);
                    return value;
                }});
            });
            try {
                assertThatThrownBy(()->application.run("--spring.profiles.active=gateway","--signaling.identity.issuer=TEST_ONLY_ISSUER","--signaling.identity.audience=TEST_ONLY_AUDIENCE")).isInstanceOf(RuntimeException.class);
                assertThat(ingress.get()).isNotNull();
                assertThat(ingress.get().wss().accepting()).as("failed Main must retire its bound native ingress").isFalse();
                assertThatThrownBy(()->new java.net.Socket("127.0.0.1",relayPort.get())).isInstanceOf(java.io.IOException.class);
            } finally {
                if(ingress.get()!=null)ingress.get().drain().toCompletableFuture().get(12,TimeUnit.SECONDS);
                if(cache.get()!=null)cache.get().drain().toCompletableFuture().get(3,TimeUnit.SECONDS);
                client.drain().toCompletableFuture().get(5,TimeUnit.SECONDS);
                if(boot.get()!=null)boot.get().close();
            }
        }
    }
}
