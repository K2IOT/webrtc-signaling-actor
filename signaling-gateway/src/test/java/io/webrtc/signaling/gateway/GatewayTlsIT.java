package io.webrtc.signaling.gateway;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.netty.handler.ssl.*;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.cert.*;
import java.security.interfaces.RSAPublicKey;
import javax.net.ssl.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class GatewayTlsIT {
    static File cert(String name){return new File(Objects.requireNonNull(GatewayTlsIT.class.getResource("/test-only-pki/"+name)).getFile());}
    static SSLContext trustedTls()throws Exception {var store=KeyStore.getInstance(KeyStore.getDefaultType());store.load(null,null);try(var in=new FileInputStream(cert("ca.crt"))){store.setCertificateEntry("TEST_ONLY",CertificateFactory.getInstance("X.509").generateCertificate(in));}var tm=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());tm.init(store);var context=SSLContext.getInstance("TLSv1.3");context.init(null,tm.getTrustManagers(),null);return context;}
    static String token(KeyPair pair,Instant now)throws Exception {var encoding=Base64.getUrlEncoder().withoutPadding();String header="{\"alg\":\"RS256\",\"kid\":\"test\"}";String claims="{\"iss\":\"https://issuer.test\",\"aud\":\"gateway-test\",\"userId\":\"alice\",\"jti\":\"test-jti\",\"iat\":"+now.getEpochSecond()+",\"exp\":"+now.plusSeconds(600).getEpochSecond()+"}";String input=encoding.encodeToString(header.getBytes(StandardCharsets.UTF_8))+"."+encoding.encodeToString(claims.getBytes(StandardCharsets.UTF_8));var signature=Signature.getInstance("SHA256withRSA");signature.initSign(pair.getPrivate());signature.update(input.getBytes(StandardCharsets.US_ASCII));return input+"."+encoding.encodeToString(signature.sign());}
    @Test void actualWssRunsRs256AndCommandBindingOffEventLoopsAndRejectsPlaintextAndBadOrigins()throws Exception {
        var generator=KeyPairGenerator.getInstance("RSA");generator.initialize(2048);var pair=generator.generateKeyPair();Instant now=Instant.now();var contract=new IdentitySecurityContract("https://issuer.test","gateway-test",Duration.ofMinutes(15),Duration.ofSeconds(30),Duration.ofSeconds(2),Duration.ofSeconds(5),true,"TEST_ONLY_HIGH_WATER");var actual=new Rs256TokenVerifier(contract,new TrustedRsaKeys(Map.of("test",(RSAPublicKey)pair.getPublic()),null,Duration.ofSeconds(1)),8192);var rsaThread=new AtomicReference<String>();var commandThread=new AtomicReference<String>();UUID boot=UUID.randomUUID();
        try(var verifier=new BoundedTokenVerifier((jwt,at)->{rsaThread.set(Thread.currentThread().getName());return actual.validate(jwt,at);},2,16,Duration.ofMillis(250))){
            var registry=new ConnectionRegistry("TEST_ONLY_TLS",boot,1000);GatewayServices services=new GatewayServices(){
                public boolean currentBoot(){return true;}public CompletionStage<AuthPrincipal> verify(String jwt,Instant at){return verifier.verify(jwt,at);}public AuthorizationStatus cachedSecurity(AuthPrincipal p,Instant at){return p.expiresAt().isAfter(at)?AuthorizationStatus.ALLOWED:AuthorizationStatus.TOKEN_EXPIRED;}
                public CompletionStage<SessionRepository.Route> register(AuthPrincipal p,UUID c,Duration b){return CompletableFuture.completedFuture(new SessionRepository.Route(p.userId(),p.key(),new SessionIncarnation(UUID.randomUUID()),1,"TEST_ONLY_TLS",boot,c,p.expiresAt(),p.signingKeyId(),p.securityEpoch()));}
                public CompletionStage<SessionRepository.Route> refresh(SessionRepository.Route r,AuthPrincipal p,Duration b){throw new AssertionError();}public CompletionStage<Void> close(SessionRepository.Route r){return CompletableFuture.completedFuture(null);}
                public CompletionStage<String> command(CallCommand c,Duration b){commandThread.set(Thread.currentThread().getName());assertThat(c.sender().userId().value()).isEqualTo("alice");return CompletableFuture.completedFuture("{\"v\":1,\"type\":\"WORKFLOW_PENDING\"}");}
            };
            var tls=SslContextBuilder.forServer(cert("server.crt"),cert("server.key")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
            try(var gateway=new GatewayServer(tls,new GatewayServer.UpgradePolicy(Set.of("https://app.test"),headers->false),registry,services,Clock.systemUTC(),1).start(new InetSocketAddress("127.0.0.1",0));var client=HttpClient.newBuilder().sslContext(trustedTls()).connectTimeout(Duration.ofSeconds(2)).build()){
                var messages=new LinkedBlockingQueue<String>();var listener=new WebSocket.Listener(){final StringBuilder partial=new StringBuilder();public void onOpen(WebSocket socket){socket.request(1);}public CompletionStage<?> onText(WebSocket socket,CharSequence data,boolean last){partial.append(data);if(last){messages.add(partial.toString());partial.setLength(0);}socket.request(1);return CompletableFuture.completedFuture(null);}};
                var socket=client.newWebSocketBuilder().header("Origin","https://app.test").connectTimeout(Duration.ofSeconds(2)).buildAsync(URI.create("wss://localhost:"+gateway.port()+"/ws"),listener).join();socket.sendText("{\"v\":1,\"type\":\"AUTH\",\"payload\":{\"token\":\""+token(pair,now)+"\"}}",true).join();assertThat(messages.poll(3,TimeUnit.SECONDS)).contains("AUTH_OK");assertThat(rsaThread.get()).startsWith("signal-auth-");
                socket.sendText("{\"v\":1,\"type\":\"INVITE\",\"requestId\":\""+UUID.randomUUID()+"\",\"payload\":{\"targetUserId\":\"bob\"}}",true).join();assertThat(messages.poll(2,TimeUnit.SECONDS)).contains("WORKFLOW_PENDING");assertThat(commandThread.get()).startsWith("gateway-json-");socket.sendClose(WebSocket.NORMAL_CLOSURE,"done").join();
                assertThatThrownBy(()->client.newWebSocketBuilder().header("Origin","https://evil.test").buildAsync(URI.create("wss://localhost:"+gateway.port()+"/ws"),listener).join()).hasCauseInstanceOf(WebSocketHandshakeException.class);
                assertThatThrownBy(()->client.newWebSocketBuilder().header("Origin","https://app.test").buildAsync(URI.create("wss://localhost:"+gateway.port()+"/ws?token=secret"),listener).join()).hasCauseInstanceOf(WebSocketHandshakeException.class);
                assertThatThrownBy(()->client.newWebSocketBuilder().header("Origin","https://app.test").connectTimeout(Duration.ofSeconds(2)).buildAsync(URI.create("ws://localhost:"+gateway.port()+"/ws"),listener).join()).isInstanceOf(CompletionException.class);
            }
        }
    }
    @Test void gatewayCannotStartWithoutTls(){assertThatThrownBy(()->new GatewayServer(null,new GatewayServer.UpgradePolicy(Set.of(),headers->false),new ConnectionRegistry("gw",UUID.randomUUID(),1),null,Clock.systemUTC(),1)).isInstanceOf(IllegalArgumentException.class);}
}
