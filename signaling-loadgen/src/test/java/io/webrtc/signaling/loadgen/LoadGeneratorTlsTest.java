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
import com.fasterxml.jackson.databind.*;
import java.io.File;
import java.net.*;
import java.security.*;
import java.security.interfaces.RSAPublicKey;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

/** Real test-only TLS and signed issuer identities. This fixture is never release evidence. */
class LoadGeneratorTlsTest {
    static final ObjectMapper JSON=new ObjectMapper();
    static File cert(String name)throws Exception{return new File(Objects.requireNonNull(LoadGeneratorTlsTest.class.getResource("/test-only-pki/"+name)).toURI());}
    static String token(KeyPair keys)throws Exception {
        var encode=Base64.getUrlEncoder().withoutPadding();long now=Instant.now().getEpochSecond();String header=encode.encodeToString("{\"alg\":\"RS256\",\"kid\":\"test\"}".getBytes(StandardCharsets.UTF_8));String body=encode.encodeToString(JSON.writeValueAsBytes(Map.of("iss","TEST_ONLY_ISSUER","aud","TEST_ONLY_AUDIENCE","userId","test-user","jti","test-jti","iat",now,"exp",now+600)));var signature=Signature.getInstance("SHA256withRSA");signature.initSign(keys.getPrivate());signature.update((header+"."+body).getBytes(StandardCharsets.US_ASCII));return header+"."+body+"."+encode.encodeToString(signature.sign());
    }
    @Test void genuineWssValidatesRs256AndMeasuresIntendedArrivalThenRejectsWrongHostname()throws Exception {
        genuineWss(false);
    }
    @Test void drainWaitsForOriginalOrdinaryWriteEvenAfterReplyAndSocketClosure()throws Exception {
        genuineWss(true);
    }
    void genuineWss(boolean holdWrite)throws Exception {
        var keygen=KeyPairGenerator.getInstance("RSA");keygen.initialize(2048);var keys=keygen.generateKeyPair();String jwt=token(keys);
        var verifier=new Rs256TokenVerifier(new IdentitySecurityContract("TEST_ONLY_ISSUER","TEST_ONLY_AUDIENCE",Duration.ofHours(1),Duration.ZERO,Duration.ofSeconds(4),Duration.ofSeconds(5),true,"TEST_ONLY_SOURCE"),new TrustedRsaKeys(Map.of("test",(RSAPublicKey)keys.getPublic()),null,Duration.ofSeconds(1)),8192);
        var serverTls=SslContextBuilder.forServer(cert("server.crt"),cert("server.key")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();var clientTls=SslContextBuilder.forClient().trustManager(cert("ca.crt")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
        var boss=new NioEventLoopGroup(1);var children=new NioEventLoopGroup(1);var sources=new NioEventLoopGroup(1);var calls=new AtomicInteger();var pings=new AtomicInteger();var evidence=new EvidenceWriter();var credits=new VirtualClient.Credits(4,327680);var clients=new ArrayList<VirtualClient>();Channel server=null;
        var held=new AtomicReference<ChannelPromise>();
        try {
            server=new ServerBootstrap().group(boss,children).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<Channel>(){protected void initChannel(Channel c){c.pipeline().addLast(serverTls.newHandler(c.alloc()),new HttpServerCodec(),new HttpObjectAggregator(81920),new ChannelInboundHandlerAdapter(){@Override public void channelRead(ChannelHandlerContext ctx,Object frame){if(frame instanceof PingWebSocketFrame)pings.incrementAndGet();ctx.fireChannelRead(frame);}},new WebSocketServerProtocolHandler(WebSocketServerProtocolConfig.newBuilder().websocketPath("/ws").subprotocols("webrtc-signaling.v1").dropPongFrames(false).maxFramePayloadLength(81920).build()),new SimpleChannelInboundHandler<TextWebSocketFrame>(){@Override public void userEventTriggered(ChannelHandlerContext ctx,Object event){if(event instanceof WebSocketServerProtocolHandler.HandshakeComplete handshake){if(!"webrtc-signaling.v1".equals(handshake.selectedSubprotocol())){ctx.close();return;}}ctx.fireUserEventTriggered(event);}protected void channelRead0(ChannelHandlerContext ctx,TextWebSocketFrame frame)throws Exception {var body=JSON.readTree(frame.text());if(body.path("type").asText().equals("AUTH")){assertThat(verifier.validate(body.path("payload").path("token").asText(),Instant.now()).userId().value()).isEqualTo("test-user");ctx.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"AUTH_OK\",\"connectionGeneration\":\"1\"}"));}else{calls.incrementAndGet();ctx.writeAndFlush(new TextWebSocketFrame(JSON.createObjectNode().put("v",1).put("type","ACK_COMMITTED").put("requestId",body.path("requestId").asText()).put("ackCommitted",true).toString()));}}});}}).bind("127.0.0.1",0).sync().channel();int port=((InetSocketAddress)server.localAddress()).getPort();
            var client=new VirtualClient(0,"test-user","c001",URI.create("wss://localhost:"+port+"/ws"),new InetSocketAddress("127.0.0.1",0),clientTls,sources,credits,evidence,()->jwt,(c,e)->{});clients.add(client);
            assertThat(client.connect(System.nanoTime()).toCompletableFuture().get(6,TimeUnit.SECONDS).path("type").asText()).isEqualTo("AUTH_OK");assertThat(client.authenticated()).isTrue();
            var channelField=VirtualClient.class.getDeclaredField("channel");channelField.setAccessible(true);var original=(Channel)channelField.get(client);
            original.eventLoop().submit(()->{}).get(3,TimeUnit.SECONDS);
            if(holdWrite)original.eventLoop().submit(()->original.pipeline().addLast(new ChannelOutboundHandlerAdapter(){@Override public void write(ChannelHandlerContext ctx,Object message,ChannelPromise promise){if(message instanceof TextWebSocketFrame text&&text.text().contains("INVITE")){held.set(promise);ctx.write(message,ctx.newPromise());}else ctx.write(message,promise);}})).get(3,TimeUnit.SECONDS);
            var request=JSON.createObjectNode().put("v",1).put("type","INVITE").put("requestId",UUID.randomUUID().toString());request.putObject("payload").put("targetUserId","test-callee");client.request(request,System.nanoTime()-100_000_000L,EvidenceWriter.Operation.INVITE).toCompletableFuture().get(3,TimeUnit.SECONDS);assertThat(calls).hasValue(1);assertThat(evidence.percentileMillis(99.9)).isGreaterThanOrEqualTo(100);
            if(holdWrite){var drained=client.drain();original.closeFuture().sync();original.eventLoop().submit(()->{}).get(3,TimeUnit.SECONDS);assertThat(drained.toCompletableFuture()).isNotDone();assertThat(credits.count()).isEqualTo(1);original.eventLoop().submit(()->held.get().setSuccess()).get(3,TimeUnit.SECONDS);drained.toCompletableFuture().get(3,TimeUnit.SECONDS);assertThat(credits.count()).isZero();assertThat(credits.bytes()).isZero();return;}
            client.heartbeat();for(int n=0;n<100&&pings.get()==0;n++)Thread.sleep(10);assertThat(pings).hasValue(1);
            var wrong=new VirtualClient(1,"test-user","c001",URI.create("wss://127.0.0.1:"+port+"/ws"),new InetSocketAddress("127.0.0.1",0),clientTls,sources,credits,evidence,()->jwt,(c,e)->{});clients.add(wrong);assertThatThrownBy(()->wrong.connect(System.nanoTime()).toCompletableFuture().get(6,TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);assertThat(wrong.authenticated()).isFalse();
        }finally{if(held.get()!=null)held.get().trySuccess();for(var client:clients)client.close().toCompletableFuture().get(3,TimeUnit.SECONDS);if(server!=null)server.close().sync();sources.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();children.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();boss.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();}
        assertThat(evidence.attempts()-evidence.successes()).isGreaterThanOrEqualTo(1);
        assertThat(credits.count()).isZero();assertThat(credits.bytes()).isZero();
    }
    @Test void delayedOldSocketCallbacksCannotCompleteOrFailRequestsOnNewSocket()throws Exception {
        var serverTls=SslContextBuilder.forServer(cert("server.crt"),cert("server.key")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
        var clientTls=SslContextBuilder.forClient().trustManager(cert("ca.crt")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
        var boss=new NioEventLoopGroup(1);var children=new NioEventLoopGroup(1);var sources=new NioEventLoopGroup(1);
        var requestSeen=new CountDownLatch(1);var currentPeer=new AtomicReference<Channel>();var closedEvents=new AtomicInteger();
        var credits=new VirtualClient.Credits(4,327680);VirtualClient client=null;Channel server=null;
        try {
            server=new ServerBootstrap().group(boss,children).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<Channel>(){protected void initChannel(Channel c){c.pipeline().addLast(serverTls.newHandler(c.alloc()),new HttpServerCodec(),new HttpObjectAggregator(81920),new WebSocketServerProtocolHandler(WebSocketServerProtocolConfig.newBuilder().websocketPath("/ws").subprotocols("webrtc-signaling.v1").build()),new SimpleChannelInboundHandler<TextWebSocketFrame>(){@Override public void userEventTriggered(ChannelHandlerContext ctx,Object event){if(event instanceof WebSocketServerProtocolHandler.HandshakeComplete handshake){if(!"webrtc-signaling.v1".equals(handshake.selectedSubprotocol())){ctx.close();return;}}ctx.fireUserEventTriggered(event);}protected void channelRead0(ChannelHandlerContext ctx,TextWebSocketFrame frame)throws Exception {var body=JSON.readTree(frame.text());if(body.path("type").asText().equals("AUTH")){currentPeer.set(ctx.channel());ctx.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"AUTH_OK\",\"connectionGeneration\":\"1\"}"));}else requestSeen.countDown();}});}}).bind("127.0.0.1",0).sync().channel();
            int port=((InetSocketAddress)server.localAddress()).getPort();
            client=new VirtualClient(0,"test-user","c001",URI.create("wss://localhost:"+port+"/ws"),new InetSocketAddress("127.0.0.1",0),clientTls,sources,credits,new EvidenceWriter(),()->"TEST_ONLY",(c,event)->{if(event.path("type").asText().equals("SOCKET_CLOSED"))closedEvents.incrementAndGet();});
            client.connect(System.nanoTime()).toCompletableFuture().get(6,TimeUnit.SECONDS);
            var channelField=VirtualClient.class.getDeclaredField("channel");channelField.setAccessible(true);
            var old=(Channel)channelField.get(client);var context=old.pipeline().lastContext();var handler=(ChannelInboundHandler)context.handler();
            client.close().toCompletableFuture().get(3,TimeUnit.SECONDS);old.eventLoop().submit(()->{}).get(3,TimeUnit.SECONDS);
            client.connect(System.nanoTime()).toCompletableFuture().get(6,TimeUnit.SECONDS);
            String id=UUID.randomUUID().toString();var request=JSON.createObjectNode().put("v",1).put("type","SYNC_CALL").put("requestId",id);request.putObject("payload");var reply=client.request(request,System.nanoTime(),EvidenceWriter.Operation.SYNC).toCompletableFuture();
            assertThat(requestSeen.await(3,TimeUnit.SECONDS)).isTrue();int eventsBefore=closedEvents.get();
            var delayed=new TextWebSocketFrame(JSON.createObjectNode().put("type","ACK_COMMITTED").put("requestId",id).toString());
            old.eventLoop().submit(()->{try{handler.channelRead(context,delayed);handler.channelInactive(context);}catch(Exception e){throw new RuntimeException(e);}}).get(3,TimeUnit.SECONDS);
            assertThat(reply).isNotDone();assertThat(client.authenticated()).isTrue();assertThat(closedEvents.get()).isEqualTo(eventsBefore);assertThat(credits.count()).isEqualTo(1);
            currentPeer.get().writeAndFlush(new TextWebSocketFrame(JSON.createObjectNode().put("type","SNAPSHOT").put("requestId",id).toString())).sync();
            assertThat(reply.get(3,TimeUnit.SECONDS).path("type").asText()).isEqualTo("SNAPSHOT");
        }finally{if(client!=null)client.close().toCompletableFuture().get(3,TimeUnit.SECONDS);if(server!=null)server.close().sync();sources.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();children.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();boss.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();}
        assertThat(credits.count()).isZero();assertThat(credits.bytes()).isZero();
    }

    @Test void actualTlsUpgradeRetryAfterSurvivesHandshakeFailure()throws Exception {
        var serverTls=SslContextBuilder.forServer(cert("server.crt"),cert("server.key")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();var clientTls=SslContextBuilder.forClient().trustManager(cert("ca.crt")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
        var boss=new NioEventLoopGroup(1);var children=new NioEventLoopGroup(1);var sources=new NioEventLoopGroup(1);Channel server=null;VirtualClient client=null;
        try{
            server=new ServerBootstrap().group(boss,children).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<Channel>(){protected void initChannel(Channel c){c.pipeline().addLast(serverTls.newHandler(c.alloc()),new HttpServerCodec(),new HttpObjectAggregator(81920),new SimpleChannelInboundHandler<FullHttpRequest>(){protected void channelRead0(ChannelHandlerContext ctx,FullHttpRequest request){var reply=new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,HttpResponseStatus.SERVICE_UNAVAILABLE);reply.headers().set(HttpHeaderNames.CONTENT_LENGTH,0).set(HttpHeaderNames.RETRY_AFTER,"12");ctx.writeAndFlush(reply).addListener(ChannelFutureListener.CLOSE);}});}}).bind("127.0.0.1",0).sync().channel();
            int port=((InetSocketAddress)server.localAddress()).getPort();client=new VirtualClient(0,"TEST_ONLY","c001",URI.create("wss://localhost:"+port+"/ws"),new InetSocketAddress("127.0.0.1",0),clientTls,sources,new VirtualClient.Credits(4,327680),new EvidenceWriter(),()->{throw new AssertionError("AUTH must not start on rejected upgrade");},(c,e)->{});
            var original=client;assertThatThrownBy(()->original.connect(System.nanoTime()).toCompletableFuture().get(3,TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);assertThat(client.retryAfterNanos()).isBetween(Duration.ofSeconds(10).toNanos(),Duration.ofSeconds(12).toNanos());client.close().toCompletableFuture().get(2,TimeUnit.SECONDS);
            var runner=new ScenarioRunner();var stateType=Class.forName(ScenarioRunner.class.getName()+"$State");var constructor=stateType.getDeclaredConstructor(VirtualClient.class);constructor.setAccessible(true);var state=constructor.newInstance(client);var schedule=ScenarioRunner.class.getDeclaredMethod("scheduleReconnect",stateType,long.class);schedule.setAccessible(true);schedule.invoke(runner,state,client.generation());var deadline=stateType.getDeclaredField("reconnectAt");deadline.setAccessible(true);long originalDeadline=deadline.getLong(state);assertThat(originalDeadline-System.nanoTime()).isBetween(Duration.ofSeconds(10).toNanos(),Duration.ofSeconds(12).toNanos());schedule.invoke(runner,state,client.generation());assertThat(deadline.getLong(state)).isEqualTo(originalDeadline);var attempts=stateType.getDeclaredField("reconnectAttempt");attempts.setAccessible(true);assertThat(attempts.getInt(state)).isEqualTo(1);

        }finally{if(client!=null)client.close().toCompletableFuture().get(3,TimeUnit.SECONDS);if(server!=null)server.close().sync();sources.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();children.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();boss.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();}
    }

}
