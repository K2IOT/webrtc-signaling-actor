package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.SessionIncarnation;
import io.webrtc.signaling.storage.SessionRepository;
import io.netty.handler.ssl.*;
import java.net.*;
import java.net.http.*;
import java.time.*;
import java.security.*;
import java.security.interfaces.RSAPublicKey;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Actual WSS/RSA; local security changes and stopped heartbeat clock are explicit TEST_ONLY faults. */
class GatewayIdleSecurityIT {
    @ParameterizedTest @EnumSource(value=AuthorizationStatus.class,names={"REVOKED","FRESHNESS_UNKNOWN","TOKEN_EXPIRED"})
    void idleSocketObservesCachedSecurityLossWithoutHeartbeatOrClientTraffic(AuthorizationStatus rejected)throws Exception {
        var now=Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);var clock=Clock.fixed(now,ZoneOffset.UTC);
        var status=new AtomicReference<>(AuthorizationStatus.ALLOWED);
        var generator=KeyPairGenerator.getInstance("RSA");generator.initialize(2048);var key=generator.generateKeyPair();
        var contract=new IdentitySecurityContract("https://issuer.test","gateway-test",Duration.ofMinutes(15),Duration.ofSeconds(30),Duration.ofSeconds(2),Duration.ofSeconds(5),true,"TEST_ONLY_HIGH_WATER");
        var rsa=new Rs256TokenVerifier(contract,new TrustedRsaKeys(Map.of("test",(RSAPublicKey)key.getPublic()),null,Duration.ofSeconds(1)),8192);
        var boot=UUID.randomUUID();var connections=new ConnectionRegistry("TEST_ONLY_IDLE",boot,8,16);
        try(var verifier=new BoundedTokenVerifier(rsa::validate,1,8,Duration.ofMillis(250))){
            var services=new GatewayServices(){
                public boolean currentBoot(){return true;}
                public CompletionStage<AuthPrincipal> verify(String token,Instant at){return verifier.verify(token,at);}
                public AuthorizationStatus cachedSecurity(AuthPrincipal principal,Instant at){return status.get();}
                public CompletionStage<SessionRepository.Route> register(AuthPrincipal p,UUID id,Duration budget){return CompletableFuture.completedFuture(new SessionRepository.Route(p.userId(),p.key(),new SessionIncarnation(UUID.randomUUID()),1,"TEST_ONLY_IDLE",boot,id,p.expiresAt(),p.signingKeyId(),p.securityEpoch()));}
                public CompletionStage<SessionRepository.Route> refresh(SessionRepository.Route r,AuthPrincipal p,Duration budget){throw new AssertionError();}
                public CompletionStage<Void> close(SessionRepository.Route route){return CompletableFuture.completedFuture(null);}
                public CompletionStage<String> command(CallCommand command,Duration budget){throw new AssertionError("Idle source change requires no command");}
            };
            var tls=SslContextBuilder.forServer(GatewayTlsIT.cert("server.crt"),GatewayTlsIT.cert("server.key")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
            try(var gateway=new GatewayServer(tls,new GatewayServer.UpgradePolicy(Set.of("https://app.test"),headers->false),connections,services,clock,1).start(new InetSocketAddress("127.0.0.1",0));
                var client=HttpClient.newBuilder().sslContext(GatewayTlsIT.trustedTls()).connectTimeout(Duration.ofSeconds(2)).build()){
                var auth=new CompletableFuture<String>();var closed=new CompletableFuture<String>();var closeCode=new AtomicInteger();
                var socket=client.newWebSocketBuilder().subprotocols("webrtc-signaling.v1").header("Origin","https://app.test").buildAsync(URI.create("wss://localhost:"+gateway.port()+"/ws"),new WebSocket.Listener(){
                    public void onOpen(WebSocket ws){ws.request(1);}
                    public CompletionStage<?> onText(WebSocket ws,CharSequence value,boolean last){auth.complete(value.toString());ws.request(1);return null;}
                    public CompletionStage<?> onClose(WebSocket ws,int code,String reason){closeCode.set(code);closed.complete(reason);return null;}
                    public void onError(WebSocket ws,Throwable failure){closed.completeExceptionally(failure);}
                }).get(3,TimeUnit.SECONDS);
                try{
                    socket.sendText("{\"v\":1,\"type\":\"AUTH\",\"payload\":{\"token\":\""+GatewayTlsIT.token(key,now)+"\"}}",true).get(2,TimeUnit.SECONDS);
                    assertThat(auth.get(3,TimeUnit.SECONDS)).contains("AUTH_OK");
                    status.set(rejected);
                    assertThatCode(()->assertThat(closed.get(2,TimeUnit.SECONDS)).isEqualTo("AUTH_"+rejected.name())).as("cached security loss closes an idle actual WSS socket").doesNotThrowAnyException();
                    assertThat(closeCode).hasValue(1008);
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(()->connections.connectionCount()==0);
                }finally{socket.abort();}
            }
        }
    }
}
