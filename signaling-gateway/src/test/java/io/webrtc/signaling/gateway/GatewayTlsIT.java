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
        var generator=KeyPairGenerator.getInstance("RSA");generator.initialize(2048);var pair=generator.generateKeyPair();Instant now=Instant.now();var contract=new IdentitySecurityContract("https://issuer.test","gateway-test",Duration.ofMinutes(15),Duration.ofSeconds(30),Duration.ofSeconds(2),Duration.ofSeconds(5),true,"TEST_ONLY_HIGH_WATER");var actual=new Rs256TokenVerifier(contract,new TrustedRsaKeys(Map.of("test",(RSAPublicKey)pair.getPublic()),null,Duration.ofSeconds(1)),8192);var rsaThread=new AtomicReference<String>();var commandThread=new AtomicReference<String>();var holdCommand=new AtomicBoolean();var commandEntered=new CountDownLatch(1);var commandRelease=new CountDownLatch(1);UUID boot=UUID.randomUUID();
        try(var verifier=new BoundedTokenVerifier((jwt,at)->{rsaThread.set(Thread.currentThread().getName());return actual.validate(jwt,at);},2,16,Duration.ofMillis(250))){
            var registry=new ConnectionRegistry("TEST_ONLY_TLS",boot,1000);GatewayServices services=new GatewayServices(){
                public boolean currentBoot(){return true;}public CompletionStage<AuthPrincipal> verify(String jwt,Instant at){return verifier.verify(jwt,at);}public AuthorizationStatus cachedSecurity(AuthPrincipal p,Instant at){return p.expiresAt().isAfter(at)?AuthorizationStatus.ALLOWED:AuthorizationStatus.TOKEN_EXPIRED;}
                public CompletionStage<SessionRepository.Route> register(AuthPrincipal p,UUID c,Duration b){return CompletableFuture.completedFuture(new SessionRepository.Route(p.userId(),p.key(),new SessionIncarnation(UUID.randomUUID()),1,"TEST_ONLY_TLS",boot,c,p.expiresAt(),p.signingKeyId(),p.securityEpoch()));}
                public CompletionStage<SessionRepository.Route> refresh(SessionRepository.Route r,AuthPrincipal p,Duration b){throw new AssertionError();}public CompletionStage<Void> close(SessionRepository.Route r){return CompletableFuture.completedFuture(null);}
                public CompletionStage<String> command(CallCommand c,Duration b){commandThread.set(Thread.currentThread().getName());if(holdCommand.get()){commandEntered.countDown();try{commandRelease.await();}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IllegalStateException(interrupted);}}assertThat(c.sender().userId().value()).isEqualTo("alice");return CompletableFuture.completedFuture("{\"v\":1,\"type\":\"WORKFLOW_PENDING\"}");}
            };
            var tls=SslContextBuilder.forServer(cert("server.crt"),cert("server.key")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
            try(var gateway=new GatewayServer(tls,new GatewayServer.UpgradePolicy(Set.of("https://app.test"),headers->false),registry,services,Clock.systemUTC(),1).start(new InetSocketAddress("127.0.0.1",0));var client=HttpClient.newBuilder().sslContext(trustedTls()).connectTimeout(Duration.ofSeconds(2)).build()){
                try(var upgrade=(SSLSocket)trustedTls().getSocketFactory().createSocket("localhost",gateway.port())){upgrade.setSoTimeout(2000);upgrade.startHandshake();var request="GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: webrtc-signaling.v1\r\nOrigin: https://app.test\r\nSec-WebSocket-Extensions: permessage-deflate\r\n\r\n";upgrade.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));var input=new BufferedReader(new InputStreamReader(upgrade.getInputStream(),StandardCharsets.US_ASCII));var response=new StringBuilder();String line;while((line=input.readLine())!=null&&!line.isEmpty())response.append(line).append('\n');assertThat(response.toString()).contains("101 Switching Protocols").containsIgnoringCase("Sec-WebSocket-Protocol: webrtc-signaling.v1").doesNotContainIgnoringCase("Sec-WebSocket-Extensions");}
                var messages=new LinkedBlockingQueue<String>();var listener=new WebSocket.Listener(){final StringBuilder partial=new StringBuilder();public void onOpen(WebSocket socket){socket.request(1);}public CompletionStage<?> onText(WebSocket socket,CharSequence data,boolean last){partial.append(data);if(last){messages.add(partial.toString());partial.setLength(0);}socket.request(1);return CompletableFuture.completedFuture(null);}};
                var socket=client.newWebSocketBuilder().subprotocols("webrtc-signaling.v1").header("Origin","https://app.test").connectTimeout(Duration.ofSeconds(2)).buildAsync(URI.create("wss://localhost:"+gateway.port()+"/ws"),listener).join();assertThat(socket.getSubprotocol()).isEqualTo("webrtc-signaling.v1");socket.sendText("{\"v\":1,\"type\":\"AUTH\",\"payload\":{\"token\":\""+token(pair,now)+"\"}}",true).join();assertThat(messages.poll(3,TimeUnit.SECONDS)).contains("AUTH_OK");assertThat(rsaThread.get()).startsWith("signal-auth-");
                socket.sendText("{\"v\":1,\"type\":\"INVITE\",\"requestId\":\""+UUID.randomUUID()+"\",\"payload\":{\"targetUserId\":\"bob\"}}",true).join();assertThat(messages.poll(2,TimeUnit.SECONDS)).contains("WORKFLOW_PENDING");assertThat(commandThread.get()).startsWith("gateway-json-");
                assertThatThrownBy(()->client.newWebSocketBuilder().header("Origin","https://app.test").buildAsync(URI.create("wss://localhost:"+gateway.port()+"/ws"),listener).join()).hasCauseInstanceOf(WebSocketHandshakeException.class);
                assertThatThrownBy(()->client.newWebSocketBuilder().subprotocols("unsupported.v2").header("Origin","https://app.test").buildAsync(URI.create("wss://localhost:"+gateway.port()+"/ws"),listener).join()).hasCauseInstanceOf(WebSocketHandshakeException.class);
                assertThatThrownBy(()->client.newWebSocketBuilder().subprotocols("webrtc-signaling.v1").header("Origin","https://evil.test").buildAsync(URI.create("wss://localhost:"+gateway.port()+"/ws"),listener).join()).hasCauseInstanceOf(WebSocketHandshakeException.class);
                assertThatThrownBy(()->client.newWebSocketBuilder().subprotocols("webrtc-signaling.v1").header("Origin","https://app.test").buildAsync(URI.create("wss://localhost:"+gateway.port()+"/ws?token=secret"),listener).join()).hasCauseInstanceOf(WebSocketHandshakeException.class);
                assertThatThrownBy(()->client.newWebSocketBuilder().subprotocols("webrtc-signaling.v1").header("Origin","https://app.test").connectTimeout(Duration.ofSeconds(2)).buildAsync(URI.create("ws://localhost:"+gateway.port()+"/ws"),listener).join()).isInstanceOf(CompletionException.class);
                holdCommand.set(true);
                socket.sendText("{\"v\":1,\"type\":\"INVITE\",\"requestId\":\""+UUID.randomUUID()+"\",\"payload\":{\"targetUserId\":\"bob\"}}",true).join();
                assertThat(commandEntered.await(2,TimeUnit.SECONDS)).isTrue();
                var stopped=CompletableFuture.runAsync(gateway::close);
                try{assertThatThrownBy(()->stopped.get(500,TimeUnit.MILLISECONDS)).as("original gateway CPU work still owns shutdown").isInstanceOf(TimeoutException.class);}
                finally{commandRelease.countDown();stopped.get(5,TimeUnit.SECONDS);socket.abort();}
            }
        }
    }
    @Test void gatewayCannotStartWithoutTls(){assertThatThrownBy(()->new GatewayServer(null,new GatewayServer.UpgradePolicy(Set.of(),headers->false),new ConnectionRegistry("gw",UUID.randomUUID(),1),null,Clock.systemUTC(),1)).isInstanceOf(IllegalArgumentException.class);}
}
