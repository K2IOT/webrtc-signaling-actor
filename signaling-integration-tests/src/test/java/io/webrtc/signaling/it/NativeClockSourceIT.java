package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.*;
import io.netty.buffer.Unpooled;
import com.fasterxml.jackson.databind.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.security.cert.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.net.ssl.*;
import org.junit.jupiter.api.Test;

/** Actual mutually authenticated source I/O; TEST_ONLY enrolled source, never production attestation. */
class NativeClockSourceIT {
    static final ObjectMapper JSON=new ObjectMapper().findAndRegisterModules().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    static File cert(String name){return new File(Objects.requireNonNull(NativeClockSourceIT.class.getResource("/test-only-pki/session/"+name)).getFile());}
    static SSLContext clientTls(boolean identity)throws Exception {
        var trust=KeyStore.getInstance(KeyStore.getDefaultType());trust.load(null,null);try(var in=new FileInputStream(cert("ca.crt"))){trust.setCertificateEntry("TEST_ONLY_CA",CertificateFactory.getInstance("X.509").generateCertificate(in));}
        var tm=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());tm.init(trust);
        KeyManager[] keys=null;if(identity){var store=KeyStore.getInstance(KeyStore.getDefaultType());store.load(null,null);String pem=Files.readString(cert("gateway.key").toPath()).replace("-----BEGIN PRIVATE KEY-----","").replace("-----END PRIVATE KEY-----","").replaceAll("\\s","");var privateKey=KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(pem)));java.security.cert.Certificate leaf;try(var in=new FileInputStream(cert("gateway.crt"))){leaf=CertificateFactory.getInstance("X.509").generateCertificate(in);}store.setKeyEntry("TEST_ONLY_CLIENT",privateKey,new char[0],new java.security.cert.Certificate[]{leaf});var km=KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());km.init(store,new char[0]);keys=km.getKeyManagers();}
        var tls=SSLContext.getInstance("TLSv1.3");tls.init(keys,tm.getTrustManagers(),null);return tls;
    }
    @Test void actualMtlsClockPollFeedsTheBoundedWorkerAndRejectsTamperBadHostnameAndMissingClientIdentity()throws Exception {
        var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();UUID pod=UUID.randomUUID(),boot=UUID.randomUUID();var sequence=new AtomicLong();var mode=new AtomicInteger();var requests=new AtomicInteger();var heldResponse=new CompletableFuture<Runnable>();
        var monitor=new ClockSafetyMonitor("c001",1,pod,boot,Map.of("TEST_ONLY_SOURCE",keys.getPublic()),Clock.systemUTC(),System::nanoTime);
        var serverTls=SslContextBuilder.forServer(cert("server.crt"),cert("server.key")).trustManager(cert("ca.crt")).clientAuth(ClientAuth.REQUIRE).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
        var boss=new NioEventLoopGroup(1);var children=new NioEventLoopGroup(1);Channel server=null;
        try {
            server=new ServerBootstrap().group(boss,children).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<Channel>(){protected void initChannel(Channel c){c.pipeline().addLast(serverTls.newHandler(c.alloc()),new HttpServerCodec(),new HttpObjectAggregator(8192),new SimpleChannelInboundHandler<FullHttpRequest>(){protected void channelRead0(ChannelHandlerContext ctx,FullHttpRequest request)throws Exception {
                requests.incrementAndGet();assertThat(request.uri()).isEqualTo("/v1/clock-bound");assertThat(request.method()).isEqualTo(HttpMethod.GET);
                assertThat(request.headers().get("X-Signaling-Pod-Uid")).isEqualTo(pod.toString());assertThat(request.headers().get("X-Signaling-Process-Boot")).isEqualTo(boot.toString());
                Instant now=Instant.now();var report=new ClockSafetyMonitor.Report("TEST_ONLY_SOURCE","c001",1,pod,boot,sequence.incrementAndGet(),now,now.plusSeconds(5),250000,1000,true);
                var signature=Signature.getInstance("Ed25519");signature.initSign(keys.getPrivate());signature.update(ClockSafetyMonitor.signingBytes(report));String signed=Base64.getUrlEncoder().withoutPadding().encodeToString(signature.sign());
                var payload=JSON.createObjectNode();payload.set("report",JSON.valueToTree(report));payload.put("signature",mode.get()==1?"a".repeat(86):signed);String body=mode.get()==2?"x".repeat(32769):payload.toString();if(mode.get()==3)body=body.substring(0,body.length()-1)+",\"signature\":\""+signed+"\"}";
                var response=new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,HttpResponseStatus.OK,Unpooled.copiedBuffer(body,StandardCharsets.UTF_8));response.headers().set(HttpHeaderNames.CONTENT_TYPE,"application/json").setInt(HttpHeaderNames.CONTENT_LENGTH,response.content().readableBytes());if(mode.get()==4)heldResponse.complete(()->ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE));else ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            }});}}).bind("127.0.0.1",0).sync().channel();int port=((InetSocketAddress)server.localAddress()).getPort();
            try(var source=new NativeClockSource(URI.create("https://localhost:"+port+"/v1/clock-bound"),clientTls(true),monitor,pod,boot)){
                assertThat(source.poll(Duration.ofSeconds(1))).isTrue();assertThat(monitor.valid()).isTrue();
                for(int invalid:new int[]{1,2,3}){mode.set(invalid);assertThat(source.poll(Duration.ofSeconds(1))).isFalse();assertThat(monitor.valid()).isFalse();}
                mode.set(0);var observed=new CountDownLatch(1);try(var workers=new NativeWorkerScheduler(List.of(source.job(Duration.ofMillis(100))),event->{if(event.status()==NativeWorkerScheduler.Status.COMPLETED)observed.countDown();})){workers.start();assertThat(observed.await(3,TimeUnit.SECONDS)).isTrue();assertThat(monitor.valid()).isTrue();workers.drain().toCompletableFuture().get(3,TimeUnit.SECONDS);}
                source.drain().toCompletableFuture().get(1,TimeUnit.SECONDS);assertThat(source.poll(Duration.ofSeconds(1))).isFalse();assertThat(monitor.valid()).isFalse();
            }
            mode.set(4);
            try(var waiting=new NativeClockSource(URI.create("https://localhost:"+port+"/v1/clock-bound"),clientTls(true),monitor,pod,boot);var tasks=Executors.newVirtualThreadPerTaskExecutor()){
                var poll=tasks.submit(()->waiting.poll(Duration.ofSeconds(1)));var release=heldResponse.get(1,TimeUnit.SECONDS);
                var drained=waiting.drain().toCompletableFuture();assertThat(drained).isNotDone();assertThat(poll).isNotDone();assertThat(monitor.valid()).isFalse();
                release.run();assertThat(poll.get(1,TimeUnit.SECONDS)).isFalse();drained.get(1,TimeUnit.SECONDS);assertThat(monitor.valid()).isFalse();
            }finally{mode.set(0);}
            try(var wrong=new NativeClockSource(URI.create("https://127.0.0.1:"+port+"/v1/clock-bound"),clientTls(true),monitor,pod,boot)){assertThat(wrong.poll(Duration.ofSeconds(1))).isFalse();}
            try(var anonymous=new NativeClockSource(URI.create("https://localhost:"+port+"/v1/clock-bound"),clientTls(false),monitor,pod,boot)){assertThat(anonymous.poll(Duration.ofSeconds(1))).isFalse();}
            assertThat(requests.get()).isGreaterThanOrEqualTo(5);
        }finally{if(server!=null)server.close().sync();children.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();boss.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();}
    }
}
