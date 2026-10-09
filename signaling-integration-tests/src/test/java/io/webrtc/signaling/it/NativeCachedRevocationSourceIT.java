package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.*;
import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.worker.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class NativeCachedRevocationSourceIT {
  @Test
  void nativeMtlsFeedClosesOnMalformedInputAndDrainsItsOriginalSocket() throws Exception {
    var identity =
        new IdentitySecurityContract(
            "TEST_ONLY_ISSUER",
            "TEST_ONLY_AUDIENCE",
            Duration.ofMinutes(10),
            Duration.ofSeconds(30),
            Duration.ofMillis(500),
            Duration.ofSeconds(5),
            true,
            "TEST_ONLY_SOURCE");
    var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    var verifier =
        new RevocationSourceVerifier("c001", identity.issuer(), Map.of("source", keys.getPublic()));
    var cache =
        new NativeCachedSecurity(identity, verifier, 16, Clock.systemUTC(), System::nanoTime);
    UUID pod = UUID.randomUUID(), boot = UUID.randomUUID();
    var offsets = new ConcurrentLinkedQueue<Long>();
    var mode = new AtomicInteger();
    var held = new CompletableFuture<Runnable>();
    var tls =
        SslContextBuilder.forServer(
                NativeClockSourceIT.cert("server.crt"), NativeClockSourceIT.cert("server.key"))
            .trustManager(NativeClockSourceIT.cert("ca.crt"))
            .clientAuth(ClientAuth.REQUIRE)
            .sslProvider(SslProvider.JDK)
            .protocols("TLSv1.3")
            .build();
    var boss = new NioEventLoopGroup(1);
    var children = new NioEventLoopGroup(1);
    Channel server = null;
    try {
      server =
          new ServerBootstrap()
              .group(boss, children)
              .channel(NioServerSocketChannel.class)
              .childHandler(
                  new ChannelInitializer<Channel>() {
                    protected void initChannel(Channel channel) {
                      channel
                          .pipeline()
                          .addLast(
                              tls.newHandler(channel.alloc()),
                              new HttpServerCodec(),
                              new HttpObjectAggregator(8192),
                              new SimpleChannelInboundHandler<FullHttpRequest>() {
                                protected void channelRead0(
                                    ChannelHandlerContext ctx, FullHttpRequest request)
                                    throws Exception {
                                  assertThat(request.headers().get("X-Signaling-Pod-Uid"))
                                      .isEqualTo(pod.toString());
                                  assertThat(request.headers().get("X-Signaling-Process-Boot"))
                                      .isEqualTo(boot.toString());
                                  long from =
                                      Long.parseLong(
                                          request.headers().get("X-Signaling-Source-Offset"));
                                  offsets.add(from);
                                  Instant checked = Instant.now();
                                  var events =
                                      from == 0
                                          ? List.of(
                                              new RevocationState.Event(
                                                  identity.issuer(),
                                                  new UserId("revoked"),
                                                  null,
                                                  1,
                                                  1,
                                                  checked))
                                          : List.<RevocationState.Event>of();
                                  var unsigned =
                                      new RevocationReconciler.Batch(
                                          from, 1, events, checked, "", List.of(), 1);
                                  var signer = Signature.getInstance("Ed25519");
                                  signer.initSign(keys.getPrivate());
                                  signer.update(
                                      RevocationSourceVerifier.signingBytes(
                                          "c001", identity.issuer(), unsigned));
                                  var page =
                                      new RevocationReconciler.Batch(
                                          from,
                                          1,
                                          events,
                                          checked,
                                          "source."
                                              + Base64.getUrlEncoder()
                                                  .withoutPadding()
                                                  .encodeToString(signer.sign()),
                                          List.of(),
                                          1);
                                  String body = NativeClockSourceIT.JSON.writeValueAsString(page);
                                  if (mode.get() == 1)
                                    body =
                                        body.substring(0, body.length() - 1) + ",\"healthy\":true}";
                                  var response =
                                      new DefaultFullHttpResponse(
                                          HttpVersion.HTTP_1_1,
                                          HttpResponseStatus.OK,
                                          Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
                                  response
                                      .headers()
                                      .set(HttpHeaderNames.CONTENT_TYPE, "application/json")
                                      .setInt(
                                          HttpHeaderNames.CONTENT_LENGTH,
                                          response.content().readableBytes());
                                  Runnable send =
                                      () ->
                                          ctx.writeAndFlush(response)
                                              .addListener(ChannelFutureListener.CLOSE);
                                  if (mode.get() == 2) held.complete(send);
                                  else send.run();
                                }
                              });
                    }
                  })
              .bind("127.0.0.1", 0)
              .sync()
              .channel();
      int port = ((InetSocketAddress) server.localAddress()).getPort();
      try (var source =
          new NativeCachedRevocationSource(
              URI.create("https://localhost:" + port + "/v1/revocations"),
              NativeClockSourceIT.clientTls(true),
              cache,
              pod,
              boot)) {
        assertThat(source.usable()).isFalse();
        var first = source.poll(Duration.ofSeconds(2));
        first.physicalCompletion().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertThat(first.logical().toCompletableFuture().join()).isEqualTo(1);
        assertThat(source.usable()).isTrue();
        var principal =
            new AuthPrincipal(
                new UserId("revoked"),
                new SessionKey(identity.issuer(), "jti"),
                Instant.now().plusSeconds(60),
                Instant.now(),
                "rsa",
                1);
        assertThat(cache.check(principal, Instant.now())).isEqualTo(AuthorizationStatus.REVOKED);
        mode.set(1);
        var malformed = source.poll(Duration.ofSeconds(2));
        malformed.physicalCompletion().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertThat(malformed.logical().toCompletableFuture()).isCompletedExceptionally();
        assertThat(source.usable()).isFalse();
        mode.set(0);
        source
            .poll(Duration.ofSeconds(2))
            .physicalCompletion()
            .toCompletableFuture()
            .get(2, TimeUnit.SECONDS);
        assertThat(source.usable()).isTrue();
        mode.set(2);
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
          var running = tasks.submit(() -> source.poll(Duration.ofSeconds(2)));
          var release = held.get(1, TimeUnit.SECONDS);
          var drain = source.drain().toCompletableFuture();
          assertThat(drain).isNotDone();
          assertThat(source.usable()).isFalse();
          release.run();
          var original = running.get(2, TimeUnit.SECONDS);
          original.physicalCompletion().toCompletableFuture().get(2, TimeUnit.SECONDS);
          drain.get(2, TimeUnit.SECONDS);
          assertThat(source.poll(Duration.ofSeconds(1)).logical().toCompletableFuture())
              .isCompletedExceptionally();
        }
        assertThat(offsets).containsSubsequence(0L, 1L, 1L, 1L);
      }
      var wrongCache =
          new NativeCachedSecurity(identity, verifier, 16, Clock.systemUTC(), System::nanoTime);
      try (var wrong =
          new NativeCachedRevocationSource(
              URI.create("https://127.0.0.1:" + port + "/v1/revocations"),
              NativeClockSourceIT.clientTls(true),
              wrongCache,
              pod,
              boot)) {
        var rejected = wrong.poll(Duration.ofSeconds(1));
        rejected.physicalCompletion().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertThat(rejected.logical().toCompletableFuture()).isCompletedExceptionally();
        assertThat(wrong.usable()).isFalse();
      }
    } finally {
      if (server != null) server.close().sync();
      boss.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
      children.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
    }
  }
}
