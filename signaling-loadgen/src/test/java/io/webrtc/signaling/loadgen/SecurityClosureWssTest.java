package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.gateway.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.SessionRepository;
import java.net.*;
import java.security.*;
import java.security.interfaces.RSAPublicKey;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

/** Real TLS/RS256 and native handlers; registration/feed are explicit TEST_ONLY fixtures. */
class SecurityClosureWssTest {
    enum Mode { STALE, REVOKED, EXPIRED, FRESHNESS_UNKNOWN, UNKNOWN_REASON, SILENT }
    @Test void observesNativeReplacementWithoutAttributingNormalCounters()throws Exception {run(Mode.STALE);}
    @Test void observesOriginalRevokedReason()throws Exception {run(Mode.REVOKED);}
    @Test void expirationCannotBeReportedAsRevocation()throws Exception {run(Mode.EXPIRED);}
    @Test void unknownSourceCannotBeReportedAsAuthenticationRejection()throws Exception {run(Mode.FRESHNESS_UNKNOWN);}
    @Test void unknownReasonIsDiscardedAndCannotBeReportedAsSecurityRejection()throws Exception {run(Mode.UNKNOWN_REASON);}
    @Test void silenceUsesOriginalFiveSecondWindowWithoutInferringRejection()throws Exception {run(Mode.SILENT);}
    void run(Mode mode)throws Exception {
        var keys=KeyPairGenerator.getInstance("RSA");keys.initialize(2048);var pair=keys.generateKeyPair();String token=LoadGeneratorTlsTest.token(pair);
        var verifier=new Rs256TokenVerifier(new IdentitySecurityContract("TEST_ONLY_ISSUER","TEST_ONLY_AUDIENCE",Duration.ofHours(1),Duration.ZERO,Duration.ofSeconds(4),Duration.ofSeconds(5),true,"TEST_ONLY_SOURCE"),new TrustedRsaKeys(Map.of("test",(RSAPublicKey)pair.getPublic()),null,Duration.ofSeconds(1)),8192);
        var boss=new NioEventLoopGroup(1);var children=new NioEventLoopGroup(1);var loops=new NioEventLoopGroup(1);Channel server=null;VirtualClient old=null,newer=null;
        var boot=UUID.randomUUID();var incarnation=new SessionIncarnation(UUID.randomUUID());var generations=new AtomicLong();var registry=new ConnectionRegistry("TEST_ONLY_gw",boot,8);var status=new AtomicReference<>(AuthorizationStatus.ALLOWED);var peer=new AtomicReference<Channel>();var credits=new VirtualClient.Credits(8,1048576);var evidence=new EvidenceWriter();
        GatewayServices services=new GatewayServices(){
            public boolean currentBoot(){return true;}
            public CompletionStage<AuthPrincipal> verify(String value,Instant at){try{return CompletableFuture.completedFuture(verifier.validate(value,at));}catch(AuthException denied){return CompletableFuture.failedFuture(denied);}}
            public AuthorizationStatus cachedSecurity(AuthPrincipal p,Instant at){return status.get();}
            public CompletionStage<SessionRepository.Route> register(AuthPrincipal p,UUID connection,Duration budget){return CompletableFuture.completedFuture(new SessionRepository.Route(p.userId(),p.key(),incarnation,generations.incrementAndGet(),"TEST_ONLY_gw",boot,connection,p.expiresAt(),p.signingKeyId(),1));}
            public CompletionStage<SessionRepository.Route> refresh(SessionRepository.Route route,AuthPrincipal p,Duration budget){return CompletableFuture.failedFuture(new UnsupportedOperationException("TEST_ONLY"));}
            public CompletionStage<Void> close(SessionRepository.Route route){return CompletableFuture.completedFuture(null);}
            public CompletionStage<String> command(CallCommand command,Duration budget){throw new AssertionError("Passive observation must not send business commands");}
        };
        try {
            var tls=SslContextBuilder.forServer(LoadGeneratorTlsTest.cert("server.crt"),LoadGeneratorTlsTest.cert("server.key")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
            server=new ServerBootstrap().group(boss,children).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<Channel>(){protected void initChannel(Channel channel){peer.set(channel);channel.pipeline().addLast(tls.newHandler(channel.alloc()),new HttpServerCodec(),new HttpObjectAggregator(81920),new WebSocketServerProtocolHandler(WebSocketServerProtocolConfig.newBuilder().websocketPath("/ws").subprotocols("webrtc-signaling.v1").maxFramePayloadLength(81920).closeOnProtocolViolation(false).build()),new FrameAdmissionHandler(new ProtocolValidator(ProtocolLimits.v1()),Runnable::run,new GatewayIngressBudget(8,1048576)),new AuthHandler(registry,services,Clock.systemUTC(),Runnable::run),new HeartbeatHandler(registry,services,Clock.systemUTC()));channel.pipeline().addBefore(channel.pipeline().context(WebSocketServerProtocolHandler.class).name(),"decoder-rejection",new DecoderRejectionHandler());}}).bind("127.0.0.1",0).sync().channel();
            var endpoint=URI.create("wss://localhost:"+((InetSocketAddress)server.localAddress()).getPort()+"/ws");var clientTls=SslContextBuilder.forClient().trustManager(LoadGeneratorTlsTest.cert("ca.crt")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
            old=new VirtualClient(0,"test-user","c001",endpoint,new InetSocketAddress("127.0.0.1",0),clientTls,loops,credits,evidence,()->token,(c,event)->{});
            var originalAuth=old.connect(System.nanoTime()).toCompletableFuture().get(3,TimeUnit.SECONDS);var field=VirtualClient.class.getDeclaredField("channel");field.setAccessible(true);var original=(Channel)field.get(old);original.eventLoop().submit(()->{}).get(3,TimeUnit.SECONDS);long ordinary=evidence.attempts(),intended=System.nanoTime();
            var observation=old.probe(VirtualClient.ProbeKind.SECURITY_CLOSURE,intended);original.eventLoop().submit(()->{}).get(3,TimeUnit.SECONDS);assertThat(observation.observed().toCompletableFuture()).isNotDone();
            if(mode==Mode.STALE){newer=new VirtualClient(1,"test-user","c001",endpoint,new InetSocketAddress("127.0.0.1",0),clientTls,loops,credits,new EvidenceWriter(),()->token,(c,event)->{});var replacement=newer.connect(System.nanoTime()).toCompletableFuture().get(3,TimeUnit.SECONDS);((Channel)field.get(newer)).eventLoop().submit(()->{}).get(3,TimeUnit.SECONDS);assertThat(replacement.path("sessionIncarnation").asText()).isEqualTo(originalAuth.path("sessionIncarnation").asText());assertThat(Long.parseLong(replacement.path("connectionGeneration").asText())).isGreaterThan(Long.parseLong(originalAuth.path("connectionGeneration").asText()));}
            else if(mode==Mode.UNKNOWN_REASON)peer.get().writeAndFlush(new CloseWebSocketFrame(1008,"TEST_ONLY_PRIVATE_SOURCE_REASON")).addListener(ChannelFutureListener.CLOSE);
            else if(mode!=Mode.SILENT){status.set(mode==Mode.REVOKED?AuthorizationStatus.REVOKED:mode==Mode.EXPIRED?AuthorizationStatus.TOKEN_EXPIRED:AuthorizationStatus.FRESHNESS_UNKNOWN);var owned=peer.get();owned.eventLoop().submit(()->owned.pipeline().get(HeartbeatHandler.class).tick(Instant.now())).get(3,TimeUnit.SECONDS);}
            var receipt=observation.observed().toCompletableFuture().get(6,TimeUnit.SECONDS);observation.physicalCompletion().toCompletableFuture().get(3,TimeUnit.SECONDS);
            assertThat(receipt.intendedNanos()).isEqualTo(intended);assertThat(receipt.generation()).isEqualTo(old.generation());assertThat(evidence.attempts()).isEqualTo(ordinary);assertThat(receipt.toString()).doesNotContain("PRIVATE","TEST_ONLY_jti",token);assertThat(credits.count()).isZero();assertThat(credits.bytes()).isZero();
            if(mode==Mode.SILENT){assertThat(receipt.outcome()).isEqualTo(VirtualClient.ProbeOutcome.DEADLINE_UNKNOWN);assertThat(receipt.finishedNanos()-intended).isBetween(TimeUnit.MILLISECONDS.toNanos(4900),TimeUnit.MILLISECONDS.toNanos(5600));}
            else {assertThat(receipt.closeCode()).isEqualTo(1008);assertThat(receipt.outcome()).isEqualTo(mode==Mode.UNKNOWN_REASON?VirtualClient.ProbeOutcome.UNCLASSIFIED_CLOSE:mode==Mode.FRESHNESS_UNKNOWN?VirtualClient.ProbeOutcome.SOURCE_UNKNOWN:VirtualClient.ProbeOutcome.AUTHORIZATION_REJECTED);assertThat(receipt.closeReason()).isEqualTo(switch(mode){case STALE->VirtualClient.CloseReason.STALE_CONNECTION;case REVOKED->VirtualClient.CloseReason.AUTH_REVOKED;case EXPIRED->VirtualClient.CloseReason.AUTH_TOKEN_EXPIRED;case FRESHNESS_UNKNOWN->VirtualClient.CloseReason.AUTH_FRESHNESS_UNKNOWN;case UNKNOWN_REASON->VirtualClient.CloseReason.UNCLASSIFIED;case SILENT->throw new AssertionError();});}
        }finally{if(old!=null)old.close().toCompletableFuture().get(3,TimeUnit.SECONDS);if(newer!=null)newer.close().toCompletableFuture().get(3,TimeUnit.SECONDS);if(server!=null)server.close().sync();loops.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();children.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();boss.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();}
    }
}
