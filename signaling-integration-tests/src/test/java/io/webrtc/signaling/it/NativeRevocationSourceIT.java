package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.auth.RevocationState;
import io.webrtc.signaling.storage.worker.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.*;
import io.netty.buffer.Unpooled;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

/** Actual mTLS source and PostgreSQL, with explicit TEST_ONLY source identity and PKI. */
class NativeRevocationSourceIT {
    @Test void originalSignedSourcePagesCommitRevocationAndKeepPartialCatchUpUnhealthy()throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var route=SessionAuthReadIT.route(f,f.sender("native-source-revoked"));var principal=SessionAuthReadIT.principal(route);
            var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            var verifier=new RevocationSourceVerifier("c001",route.key().issuer(),Map.of("TEST_ONLY_SOURCE",keys.getPublic()));
            var reconciler=new RevocationReconciler(f.runtime.sql,"c001",1,Duration.ofSeconds(1),verifier);
            var mode=new AtomicInteger();var offsets=new ConcurrentLinkedQueue<Long>();var heldResponse=new CompletableFuture<Runnable>();UUID pod=UUID.randomUUID(),boot=UUID.randomUUID();
            var tls=SslContextBuilder.forServer(NativeClockSourceIT.cert("server.crt"),NativeClockSourceIT.cert("server.key")).trustManager(NativeClockSourceIT.cert("ca.crt")).clientAuth(ClientAuth.REQUIRE).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
            var boss=new NioEventLoopGroup(1);var children=new NioEventLoopGroup(1);Channel server=null;
            try{
                server=new ServerBootstrap().group(boss,children).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<Channel>(){protected void initChannel(Channel channel){channel.pipeline().addLast(tls.newHandler(channel.alloc()),new HttpServerCodec(),new HttpObjectAggregator(8192),new SimpleChannelInboundHandler<FullHttpRequest>(){protected void channelRead0(ChannelHandlerContext ctx,FullHttpRequest request)throws Exception {
                    assertThat(request.uri()).isEqualTo("/v1/revocations");assertThat(request.method()).isEqualTo(HttpMethod.GET);
                    assertThat(request.headers().get("X-Signaling-Pod-Uid")).isEqualTo(pod.toString());assertThat(request.headers().get("X-Signaling-Process-Boot")).isEqualTo(boot.toString());
                    long from=Long.parseLong(request.headers().get("X-Signaling-Source-Offset"));offsets.add(from);Instant now=Instant.now();
                    long high=mode.get()==2?1:mode.get()==3?2:mode.get()==4?from+1:from;
                    var events=mode.get()==2?List.of(new RevocationState.Event(route.key().issuer(),route.user(),route.key().jti(),principal.securityEpoch(),1,now)):List.<RevocationState.Event>of();
                    var page=new RevocationReconciler.Batch(mode.get()==4?from+1:from,high,events,now,"",List.of(),mode.get()==2?2:high);
                    var signature=Signature.getInstance("Ed25519");signature.initSign(keys.getPrivate());signature.update(RevocationSourceVerifier.signingBytes("c001",route.key().issuer(),page));
                    String proof="TEST_ONLY_SOURCE."+(mode.get()==1?"a".repeat(86):Base64.getUrlEncoder().withoutPadding().encodeToString(signature.sign()));
                    page=new RevocationReconciler.Batch(page.fromOffset(),high,events,now,proof,List.of(),page.currentSourceHighWater());
                    String body=NativeClockSourceIT.JSON.writeValueAsString(page);
                    if(mode.get()==5)body=body.substring(0,body.length()-1)+",\"highWater\":0}";
                    if(mode.get()==6)body=body.substring(0,body.length()-1)+",\"healthy\":true}";
                    if(mode.get()==7){var value=NativeClockSourceIT.JSON.readTree(body);((com.fasterxml.jackson.databind.node.ObjectNode)value).remove("fromOffset");body=value.toString();}
                    var response=new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,HttpResponseStatus.OK,Unpooled.copiedBuffer(body,StandardCharsets.UTF_8));response.headers().set(HttpHeaderNames.CONTENT_TYPE,"application/json").setInt(HttpHeaderNames.CONTENT_LENGTH,response.content().readableBytes());
                    Runnable send=()->ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                    if(mode.get()==8)heldResponse.complete(send);else send.run();
                }});}}).bind("127.0.0.1",0).sync().channel();int port=((InetSocketAddress)server.localAddress()).getPort();
                try(var source=new NativeRevocationSource(URI.create("https://localhost:"+port+"/v1/revocations"),NativeClockSourceIT.clientTls(true),verifier,reconciler,pod,boot)){
                    assertThat(source.usable()).isFalse();settled(source.poll(Duration.ofSeconds(2)));assertThat(source.usable()).isTrue();
                    try(var c=f.connection()){assertThat(reconciler.allowed(c,principal)).isTrue();}
                    java.util.concurrent.locks.LockSupport.parkNanos(Duration.ofMillis(1100).toNanos());
                    assertThat(source.usable()).isFalse();
                    mode.set(1);var invalid=source.poll(Duration.ofSeconds(2));assertThat(invalid.logical().toCompletableFuture()).isCompletedExceptionally();invalid.physicalCompletion().toCompletableFuture().get(2,TimeUnit.SECONDS);assertThat(source.usable()).isFalse();
                    for(int malformed:new int[]{4,5,6,7}){
                        mode.set(malformed);var rejected=source.poll(Duration.ofSeconds(2));assertThat(rejected.logical().toCompletableFuture()).isCompletedExceptionally();rejected.physicalCompletion().toCompletableFuture().get(2,TimeUnit.SECONDS);assertThat(source.usable()).isFalse();
                    }
                    assertThat(CoordinatorGrantIT.done(reconciler.progress()).offset()).isZero();
                    mode.set(2);settled(source.poll(Duration.ofSeconds(2)));assertThat(source.usable()).isFalse();assertThat(f.sessions.lookupLiveRoutes(route.user(),1).toCompletableFuture().join()).isEmpty();
                    assertThat(CoordinatorGrantIT.done(reconciler.progress()).checkedAt()).isEqualTo(Instant.EPOCH);
                    mode.set(3);settled(source.poll(Duration.ofSeconds(2)));assertThat(source.usable()).isTrue();assertThat(CoordinatorGrantIT.done(reconciler.progress()).offset()).isEqualTo(2);
                    try(var c=f.connection()){assertThat(reconciler.allowed(c,principal)).isFalse();}
                    mode.set(0);var eventsSeen=new CountDownLatch(1);
                    try(var scheduler=new NativeWorkerScheduler(List.of(source.job(Duration.ofMillis(100))),event->{if(event.status()==NativeWorkerScheduler.Status.COMPLETED)eventsSeen.countDown();})){
                        scheduler.start();assertThat(eventsSeen.await(3,TimeUnit.SECONDS)).isTrue();scheduler.drain().toCompletableFuture().get(3,TimeUnit.SECONDS);
                    }
                    source.drain().toCompletableFuture().get(2,TimeUnit.SECONDS);assertThat(source.usable()).isFalse();assertThat(source.poll(Duration.ofSeconds(2)).logical().toCompletableFuture()).isCompletedExceptionally();
                }
                assertThat(offsets).containsSubsequence(0L,0L,0L,1L,2L);
                try(var wrong=new NativeRevocationSource(URI.create("https://127.0.0.1:"+port+"/v1/revocations"),NativeClockSourceIT.clientTls(true),verifier,reconciler,pod,boot)){
                    var operation=wrong.poll(Duration.ofSeconds(2));assertThat(operation.logical().toCompletableFuture()).isCompletedExceptionally();operation.physicalCompletion().toCompletableFuture().get(2,TimeUnit.SECONDS);assertThat(wrong.usable()).isFalse();
                }
                mode.set(8);
                try(var waiting=new NativeRevocationSource(URI.create("https://localhost:"+port+"/v1/revocations"),NativeClockSourceIT.clientTls(true),verifier,reconciler,pod,boot);var tasks=Executors.newVirtualThreadPerTaskExecutor()){
                    var poll=tasks.submit(()->waiting.poll(Duration.ofSeconds(2)));var release=heldResponse.get(1,TimeUnit.SECONDS);
                    var draining=waiting.drain().toCompletableFuture();assertThat(draining).isNotDone();assertThat(poll).isNotDone();assertThat(waiting.usable()).isFalse();
                    release.run();settled(poll.get(2,TimeUnit.SECONDS));draining.get(2,TimeUnit.SECONDS);assertThat(waiting.usable()).isFalse();
                }
            }finally{if(server!=null)server.close().sync();children.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();boss.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();}
        }
    }
    static void settled(io.webrtc.signaling.rpc.RpcOperation<?> operation)throws Exception {operation.logical().toCompletableFuture().get(2,TimeUnit.SECONDS);operation.physicalCompletion().toCompletableFuture().get(2,TimeUnit.SECONDS);}
}
