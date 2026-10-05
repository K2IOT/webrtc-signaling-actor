package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.*;
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
import java.net.*;
import java.security.*;
import java.security.interfaces.RSAPublicKey;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/** TEST_ONLY authority; actual TLS/RS256 and production frame validator/WS decoder. */
class SecurityWireProbeWssTest {
    @Test void malformedProbeObservesOriginalNativeProtocolClose()throws Exception {run(false);}
    @org.junit.jupiter.api.RepeatedTest(10) void oversizedProbeObservesOriginalNativeDecoderClose()throws Exception {run(true);}
    enum Mode { NORMAL, SILENT, ABRUPT, WRONG_CLOSE, EARLY_CLOSE, HELD_WRITE, WRITE_FAILURE_THEN_CLOSE, CREDIT_LIMIT }
    @Test void missingCloseCodeIsNeverCountedAsProtocolRejection()throws Exception {run(false,Mode.ABRUPT);}
    @Test void unrelatedCloseCodeIsNeverCountedAsProtocolRejection()throws Exception {run(false,Mode.WRONG_CLOSE);}
    @Test void silenceExpiresAtOriginalDeadlineWithoutClaimingRejection()throws Exception {run(false,Mode.SILENT);}
    @Test void originalWriteUnknownKeepsCreditAfterReceiptAndSocketClose()throws Exception {run(false,Mode.HELD_WRITE);}
    @Test void originalServerRejectionCanStillArriveAfterLocalWriteFailure()throws Exception {run(false,Mode.WRITE_FAILURE_THEN_CLOSE);}
    @Test void closeBeforeProbeDispatchCannotBeAttributedToProbe()throws Exception {run(false,Mode.EARLY_CLOSE);}
    @Test void insufficientCreditDoesNotWriteOrCloseSocket()throws Exception {run(true,Mode.CREDIT_LIMIT);}
    void run(boolean oversized)throws Exception {run(oversized,Mode.NORMAL);}
    void run(boolean oversized,Mode mode)throws Exception {
        var keys=KeyPairGenerator.getInstance("RSA");keys.initialize(2048);var pair=keys.generateKeyPair();String token=LoadGeneratorTlsTest.token(pair);
        var verifier=new Rs256TokenVerifier(new IdentitySecurityContract("TEST_ONLY_ISSUER","TEST_ONLY_AUDIENCE",Duration.ofHours(1),Duration.ZERO,Duration.ofSeconds(4),Duration.ofSeconds(5),true,"TEST_ONLY_SOURCE"),new TrustedRsaKeys(Map.of("test",(RSAPublicKey)pair.getPublic()),null,Duration.ofSeconds(1)),8192);
        var boss=new NioEventLoopGroup(1);var children=new NioEventLoopGroup(1);var loops=new NioEventLoopGroup(1);Channel server=null;VirtualClient client=null;var budget=new GatewayIngressBudget(16,1048576);var credits=new VirtualClient.Credits(8,mode==Mode.CREDIT_LIMIT?81920:1048576);var evidence=new EvidenceWriter();var delayedHandler=new CompletableFuture<ChannelHandlerContext>();var delayedInput=new java.util.concurrent.atomic.AtomicReference<TextWebSocketFrame>();
        try {
            var tls=SslContextBuilder.forServer(LoadGeneratorTlsTest.cert("server.crt"),LoadGeneratorTlsTest.cert("server.key")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
            server=new ServerBootstrap().group(boss,children).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<Channel>(){protected void initChannel(Channel channel){channel.pipeline().addLast(tls.newHandler(channel.alloc()),new HttpServerCodec(),new HttpObjectAggregator(81920),new DecoderRejectionHandler(),new WebSocketServerProtocolHandler(WebSocketServerProtocolConfig.newBuilder().websocketPath("/ws").subprotocols("webrtc-signaling.v1").maxFramePayloadLength(81920).build()),new SimpleChannelInboundHandler<TextWebSocketFrame>(){protected void channelRead0(ChannelHandlerContext ctx,TextWebSocketFrame frame){if(frame.text().equals("{")&&mode==Mode.WRITE_FAILURE_THEN_CLOSE){delayedInput.set(frame.retain());delayedHandler.complete(ctx);}else if(frame.text().equals("{")&&(mode==Mode.SILENT||mode==Mode.ABRUPT||mode==Mode.WRONG_CLOSE)){if(mode==Mode.ABRUPT)ctx.channel().unsafe().close(ctx.newPromise());if(mode==Mode.WRONG_CLOSE)ctx.writeAndFlush(new CloseWebSocketFrame(1000,"TEST_ONLY")).addListener(ChannelFutureListener.CLOSE);}else ctx.fireChannelRead(frame.retain());}},new FrameAdmissionHandler(new ProtocolValidator(ProtocolLimits.v1()),Runnable::run,budget),new SimpleChannelInboundHandler<FrameAdmissionHandler.Admitted>(){protected void channelRead0(ChannelHandlerContext ctx,FrameAdmissionHandler.Admitted admitted){try{assertThat(admitted.envelope().type()).isEqualTo(SignalEnvelope.Type.AUTH);var payload=new ObjectMapper().readTree(admitted.envelope().payloadJson());assertThat(verifier.validate(payload.path("token").asText(),Instant.now()).userId().value()).isEqualTo("test-user");ctx.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"AUTH_OK\",\"connectionGeneration\":\"1\"}"));}catch(Exception failure){throw new AssertionError(failure);}finally{admitted.release().run();}}});}}).bind("127.0.0.1",0).sync().channel();
            int port=((InetSocketAddress)server.localAddress()).getPort();client=new VirtualClient(0,"test-user","c001",URI.create("wss://localhost:"+port+"/ws"),new InetSocketAddress("127.0.0.1",0),SslContextBuilder.forClient().trustManager(LoadGeneratorTlsTest.cert("ca.crt")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build(),loops,credits,evidence,()->token,(c,event)->{});
            client.connect(System.nanoTime()).toCompletableFuture().get(3,TimeUnit.SECONDS);long ordinary;
            var field=VirtualClient.class.getDeclaredField("channel");field.setAccessible(true);var original=(Channel)field.get(client);original.eventLoop().submit(()->{}).get(3,TimeUnit.SECONDS);ordinary=evidence.attempts();
            var held=new java.util.concurrent.atomic.AtomicReference<ChannelPromise>();var queued=new CountDownLatch(1);var release=new CountDownLatch(1);
            if(mode==Mode.HELD_WRITE||mode==Mode.WRITE_FAILURE_THEN_CLOSE)original.eventLoop().submit(()->original.pipeline().addLast(new ChannelOutboundHandlerAdapter(){@Override public void write(ChannelHandlerContext ctx,Object message,ChannelPromise promise){if(message instanceof TextWebSocketFrame text&&text.text().equals("{")){held.set(promise);ctx.write(message,ctx.newPromise());}else ctx.write(message,promise);}})).get(3,TimeUnit.SECONDS);
            if(mode==Mode.EARLY_CLOSE){original.eventLoop().execute(()->{queued.countDown();try{if(!release.await(3,TimeUnit.SECONDS))throw new AssertionError("TEST_ONLY barrier timeout");for(var entry:original.pipeline()){var handler=entry.getValue();if(handler.getClass().isAnonymousClass()&&handler instanceof ChannelInboundHandlerAdapter inbound&&!(handler instanceof SimpleChannelInboundHandler<?>)){inbound.channelRead(original.pipeline().context(handler),new CloseWebSocketFrame(1002,"TEST_ONLY_UNRELATED"));break;}}}catch(Exception error){throw new AssertionError(error);}});assertThat(queued.await(3,TimeUnit.SECONDS)).isTrue();}
            long intended=System.nanoTime();
            var probe=client.probe(oversized?VirtualClient.ProbeKind.OVERSIZED:VirtualClient.ProbeKind.MALFORMED,intended);
            release.countDown();
            if(mode==Mode.WRITE_FAILURE_THEN_CLOSE){var ctx=delayedHandler.get(3,TimeUnit.SECONDS);original.eventLoop().submit(()->held.get().setFailure(new java.io.IOException("TEST_ONLY original write failed"))).get(3,TimeUnit.SECONDS);ctx.executor().submit(()->ctx.fireChannelRead(delayedInput.getAndSet(null))).get(3,TimeUnit.SECONDS);}
            var observed=probe.observed().toCompletableFuture().get(3,TimeUnit.SECONDS);
            if(mode==Mode.HELD_WRITE){original.closeFuture().sync();assertThat(probe.physicalCompletion().toCompletableFuture()).isNotDone();assertThat(credits.count()).isEqualTo(1);assertThat(credits.bytes()).isEqualTo(1);original.eventLoop().submit(()->held.get().setSuccess()).get(3,TimeUnit.SECONDS);}
            probe.physicalCompletion().toCompletableFuture().get(3,TimeUnit.SECONDS);
            switch(mode){
                case NORMAL,HELD_WRITE,WRITE_FAILURE_THEN_CLOSE -> {assertThat(observed.closeCode()).as(observed.toString()).isEqualTo(oversized?1009:1002);assertThat(observed.outcome()).isEqualTo(VirtualClient.ProbeOutcome.PROTOCOL_REJECTED);}
                case ABRUPT -> {assertThat(observed.closeCode()).isEqualTo(-1);assertThat(observed.outcome()).isEqualTo(VirtualClient.ProbeOutcome.UNCLASSIFIED_CLOSE);}
                case WRONG_CLOSE -> {assertThat(observed.closeCode()).isEqualTo(1000);assertThat(observed.outcome()).isEqualTo(VirtualClient.ProbeOutcome.UNCLASSIFIED_CLOSE);}
                case SILENT -> {assertThat(observed.closeCode()).isEqualTo(-1);assertThat(observed.outcome()).isEqualTo(VirtualClient.ProbeOutcome.DEADLINE_UNKNOWN);assertThat(observed.finishedNanos()-intended).isBetween(TimeUnit.MILLISECONDS.toNanos(1900),TimeUnit.MILLISECONDS.toNanos(2600));}
                case EARLY_CLOSE -> assertThat(observed.outcome()).isEqualTo(VirtualClient.ProbeOutcome.ADMISSION_REJECTED);
                case CREDIT_LIMIT -> {assertThat(observed.outcome()).isEqualTo(VirtualClient.ProbeOutcome.CREDIT_REJECTED);assertThat(original.isActive()).isTrue();assertThat(client.authenticated()).isTrue();}
            }assertThat(observed.intendedNanos()).isEqualTo(intended);assertThat(observed.generation()).isEqualTo(client.generation());assertThat(evidence.attempts()).isEqualTo(ordinary);assertThat(credits.count()).isZero();assertThat(credits.bytes()).isZero();assertThat(budget.count()).isZero();
        }finally{var retained=delayedInput.getAndSet(null);if(retained!=null)retained.release();if(client!=null)client.close().toCompletableFuture().get(3,TimeUnit.SECONDS);if(server!=null)server.close().sync();loops.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();children.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();boss.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();}
    }
}
