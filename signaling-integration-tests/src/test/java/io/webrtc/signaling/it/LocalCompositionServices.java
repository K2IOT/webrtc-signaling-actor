package io.webrtc.signaling.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.storage.worker.RevocationReconciler;
import io.webrtc.signaling.storage.worker.RevocationSourceVerifier;

import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.Signature;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The TEST_ONLY services outside the application under test: one mutually authenticated HTTPS
 * source (TLS 1.3) signing clock reports and empty revocation pages with Ed25519, and a fake
 * Kubernetes API (HTTPS, bearer token) listing the actor pods. Neither is part of any image.
 *
 * <p>The clock report claims the enrolled maxima (250 ms pair uncertainty, 1000 ppm) because this
 * fixture measures nothing; it is not clock-quality evidence. Workload authentication is the mTLS
 * client certificate chain; the pod and boot headers are echoed into the signed report, which the
 * monitor then pins to its own pod and process boot.
 */
final class LocalCompositionServices implements AutoCloseable {
    static final String SOURCE_KEY_ID = "local-source";
    static final ObjectMapper JSON =
            new ObjectMapper()
                    .findAndRegisterModules()
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final KeyPair signing;
    private final String cell;
    private final String issuer;
    private final AtomicLong sequence = new AtomicLong();
    private final NioEventLoopGroup boss = new NioEventLoopGroup(1);
    private final NioEventLoopGroup workers = new NioEventLoopGroup(2);
    private final Channel source;
    private final HttpsServer kube;
    private final AtomicReference<String> pods = new AtomicReference<>("{\"items\":[]}");
    final Set<UUID> clockPods = ConcurrentHashMap.newKeySet();
    final Set<UUID> revocationPods = ConcurrentHashMap.newKeySet();
    /** Monotonic time of the latest successfully answered poll, per pod. */
    final Map<UUID, Long> lastClockNanos = new ConcurrentHashMap<>();
    final Map<UUID, Long> lastRevocationNanos = new ConcurrentHashMap<>();
    final AtomicLong kubeRequests = new AtomicLong();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final String kubeToken;
    final String namespace;

    LocalCompositionServices(
            LocalCompositionPki.Leaf sourceLeaf,
            LocalCompositionPki.Leaf kubeLeaf,
            LocalCompositionPki.Ca ca,
            KeyPair signing,
            String cell,
            String issuer,
            InetSocketAddress sourceAddress,
            InetSocketAddress kubeAddress,
            String namespace,
            String kubeToken)
            throws Exception {
        this.signing = signing;
        this.cell = cell;
        this.issuer = issuer;
        this.namespace = namespace;
        this.kubeToken = kubeToken;
        var tls =
                SslContextBuilder.forServer(sourceLeaf.certificate().toFile(), sourceLeaf.key().toFile())
                        .trustManager(ca.certificate().toFile())
                        .clientAuth(ClientAuth.REQUIRE)
                        .sslProvider(SslProvider.JDK)
                        .protocols("TLSv1.3")
                        .build();
        source =
                new ServerBootstrap()
                        .group(boss, workers)
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
                                                                    ChannelHandlerContext ctx, FullHttpRequest request) {
                                                                respond(ctx, request);
                                                            }
                                                        });
                                    }
                                })
                        .bind(sourceAddress)
                        .sync()
                        .channel();
        kube = HttpsServer.create(kubeAddress, 16);
        kube.setHttpsConfigurator(new HttpsConfigurator(LocalCompositionPki.context(kubeLeaf, ca)));
        kube.createContext("/api/v1/namespaces/" + namespace + "/pods", this::pods);
        kube.start();
    }

    int sourcePort() {
        return ((InetSocketAddress) source.localAddress()).getPort();
    }

    int kubePort() {
        return kube.getAddress().getPort();
    }

    /** Replaces the pod list: one Running pod per address, management on its fixed port 8558. */
    void pods(List<String> addresses) {
        var items =
                addresses.stream()
                        .map(
                                ip ->
                                        "{\"metadata\":{},\"status\":{\"podIP\":\""
                                                + ip
                                                + "\",\"phase\":\"Running\",\"containerStatuses\":[{\"name\":\"actor\",\"state\":{\"running\":{}},\"ready\":false}]},"
                                                + "\"spec\":{\"containers\":[{\"name\":\"actor\",\"ports\":[{\"name\":\"management\",\"containerPort\":8558}]}]}}")
                        .toList();
        pods.set("{\"items\":[" + String.join(",", items) + "]}");
    }

    private void pods(com.sun.net.httpserver.HttpExchange exchange) throws java.io.IOException {
        try {
            String query =
                    URLDecoder.decode(
                            String.valueOf(exchange.getRequestURI().getRawQuery()), StandardCharsets.UTF_8);
            boolean authorized =
                    ("Bearer " + kubeToken).equals(exchange.getRequestHeaders().getFirst("Authorization"));
            boolean selected = query.contains("labelSelector=app=webrtc-signaling,plane=actor,cell=" + cell);
            if (!authorized || !selected) {
                failure.compareAndSet(null, new AssertionError("TEST_ONLY kube request rejected: " + query));
                exchange.sendResponseHeaders(authorized ? 400 : 401, -1);
                return;
            }
            kubeRequests.incrementAndGet();
            byte[] body = pods.get().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
        } finally {
            exchange.close();
        }
    }

    private void respond(ChannelHandlerContext ctx, FullHttpRequest request) {
        try {
            if (request.method() != HttpMethod.GET) throw new IllegalArgumentException("method");
            var pod = UUID.fromString(request.headers().get("X-Signaling-Pod-Uid"));
            var boot = UUID.fromString(request.headers().get("X-Signaling-Process-Boot"));
            String body =
                    switch (request.uri()) {
                        case "/v1/clock-bound" -> clock(pod, boot);
                        case "/v1/revocations" ->
                                revocations(pod, Long.parseLong(request.headers().get("X-Signaling-Source-Offset")));
                        default -> throw new IllegalArgumentException("path");
                    };
            var response =
                    new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1,
                            HttpResponseStatus.OK,
                            Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
            response
                    .headers()
                    .set(HttpHeaderNames.CONTENT_TYPE, "application/json")
                    .setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        } catch (Exception rejected) {
            failure.compareAndSet(null, rejected);
            var response =
                    new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }

    private String clock(UUID pod, UUID boot) throws Exception {
        clockPods.add(pod);
        lastClockNanos.put(pod, System.nanoTime());
        var now = Instant.now();
        var report =
                new ClockSafetyMonitor.Report(
                        SOURCE_KEY_ID,
                        cell,
                        1,
                        pod,
                        boot,
                        sequence.incrementAndGet(),
                        now,
                        now.plusSeconds(5),
                        250000,
                        1000,
                        true);
        var signature = Signature.getInstance("Ed25519");
        signature.initSign(signing.getPrivate());
        signature.update(ClockSafetyMonitor.signingBytes(report));
        var payload = JSON.createObjectNode();
        payload.set("report", JSON.valueToTree(report));
        payload.put("signature", Base64.getUrlEncoder().withoutPadding().encodeToString(signature.sign()));
        return payload.toString();
    }

    /** An empty, caught-up page at the requested offset: no user or key is revoked. */
    private String revocations(UUID pod, long from) throws Exception {
        revocationPods.add(pod);
        lastRevocationNanos.put(pod, System.nanoTime());
        var now = Instant.now();
        var unsigned = new RevocationReconciler.Batch(from, from, List.of(), now, "", List.of(), from);
        var signature = Signature.getInstance("Ed25519");
        signature.initSign(signing.getPrivate());
        signature.update(RevocationSourceVerifier.signingBytes(cell, issuer, unsigned));
        var page =
                new RevocationReconciler.Batch(
                        from,
                        from,
                        List.of(),
                        now,
                        SOURCE_KEY_ID + "." + Base64.getUrlEncoder().withoutPadding().encodeToString(signature.sign()),
                        List.of(),
                        from);
        return JSON.writeValueAsString(page);
    }

    @Override
    public void close() throws Exception {
        try {
            kube.stop(0);
        } finally {
            source.close().sync();
            boss.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
            workers.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
        }
    }
}
