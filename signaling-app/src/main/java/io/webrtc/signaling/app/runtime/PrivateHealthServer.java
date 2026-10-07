package io.webrtc.signaling.app.runtime;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/** Private probes read cached facts only. A scrape retains its credit through the socket write. */
public final class PrivateHealthServer implements AutoCloseable {
    private static final int MAX_METRICS_BYTES = 8 * 1024 * 1024;
    private final InetSocketAddress address;
    private final BooleanSupplier live, ready;
    private final Supplier<String> metrics;
    private final EventLoopGroup accept = new NioEventLoopGroup(1);
    private final EventLoopGroup io = new NioEventLoopGroup(1);
    private final ThreadPoolExecutor scrape = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(1), r -> { var t = new Thread(r, "private-metrics"); t.setDaemon(true); return t; },
        new ThreadPoolExecutor.AbortPolicy());
    private final Semaphore scrapeCredit = new Semaphore(1);
    private final CompletableFuture<Void> stopped = new CompletableFuture<>();
    private Channel listener;
    private boolean starting, stopping;

    public PrivateHealthServer(InetSocketAddress address, BooleanSupplier live, BooleanSupplier ready,
                               Supplier<String> metrics) {
        this.address = Objects.requireNonNull(address);
        if (address.isUnresolved() || address.getAddress().isAnyLocalAddress())
            throw new IllegalArgumentException("A resolved private pod address is required");
        this.live = Objects.requireNonNull(live); this.ready = Objects.requireNonNull(ready);
        this.metrics = Objects.requireNonNull(metrics);
    }

    public synchronized CompletionStage<Void> start() {
        if (starting || stopping) return CompletableFuture.failedFuture(new IllegalStateException("Health lifecycle"));
        starting = true;
        var started = new CompletableFuture<Void>();
        new ServerBootstrap().group(accept, io).channel(NioServerSocketChannel.class)
            .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(16384, 32768))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override protected void initChannel(SocketChannel channel) {
                    channel.pipeline().addLast(new HttpServerCodec(512, 2048, 1024),
                        new HttpObjectAggregator(1024), new Handler());
                }
            }).bind(address).addListener((ChannelFuture future) -> {
                synchronized (PrivateHealthServer.this) {
                    if (future.isSuccess()) {
                        listener = future.channel();
                        if (stopping) listener.close();
                        started.complete(null);
                    } else started.completeExceptionally(future.cause());
                }
            });
        return started;
    }

    public synchronized int port() {
        if (listener == null) throw new IllegalStateException("Not bound");
        return ((InetSocketAddress) listener.localAddress()).getPort();
    }

    private final class Handler extends SimpleChannelInboundHandler<FullHttpRequest> {
        @Override protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            if (!request.decoderResult().isSuccess() || request.content().isReadable()) {
                reply(ctx, HttpResponseStatus.BAD_REQUEST, "", false); return;
            }
            if (!request.method().equals(HttpMethod.GET)) { reply(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED, "", false); return; }
            try {
                switch (request.uri()) {
                    case "/live" -> reply(ctx, live.getAsBoolean() ? HttpResponseStatus.OK : HttpResponseStatus.SERVICE_UNAVAILABLE, "", false);
                    case "/ready" -> reply(ctx, live.getAsBoolean() && ready.getAsBoolean() ? HttpResponseStatus.OK : HttpResponseStatus.SERVICE_UNAVAILABLE, "", false);
                    case "/metrics" -> metrics(ctx);
                    default -> reply(ctx, HttpResponseStatus.NOT_FOUND, "", false);
                }
            } catch (RuntimeException unavailable) { reply(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, "", false); }
        }
        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { ctx.close(); }
    }

    private void metrics(ChannelHandlerContext ctx) {
        if (!scrapeCredit.tryAcquire()) { reply(ctx, HttpResponseStatus.TOO_MANY_REQUESTS, "", false); return; }
        try {
            scrape.execute(() -> {
                byte[] output;
                HttpResponseStatus status;
                try {
                    String value = Objects.requireNonNull(metrics.get());
                    if (value.length() > MAX_METRICS_BYTES) throw new IllegalStateException("Scrape too large");
                    output = value.getBytes(StandardCharsets.UTF_8);
                    if (output.length > MAX_METRICS_BYTES) throw new IllegalStateException("Scrape too large");
                    status = HttpResponseStatus.OK;
                } catch (RuntimeException unavailable) { output = new byte[0]; status = HttpResponseStatus.SERVICE_UNAVAILABLE; }
                byte[] body = output; HttpResponseStatus result = status;
                try { ctx.executor().execute(() -> replyBytes(ctx, result, body, true)); }
                catch (RejectedExecutionException closed) { scrapeCredit.release(); }
            });
        } catch (RejectedExecutionException closed) { scrapeCredit.release(); reply(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, "", false); }
    }

    private void reply(ChannelHandlerContext ctx, HttpResponseStatus status, String body, boolean metricsCredit) {
        replyBytes(ctx, status, body.getBytes(StandardCharsets.UTF_8), metricsCredit);
    }
    private void replyBytes(ChannelHandlerContext ctx, HttpResponseStatus status, byte[] body, boolean metricsCredit) {
        var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(body));
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length)
            .set(HttpHeaderNames.CONTENT_TYPE, metricsCredit ? "text/plain; version=0.0.4; charset=utf-8" : "text/plain; charset=utf-8")
            .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        try {
            ctx.writeAndFlush(response).addListener(f -> { if (metricsCredit) scrapeCredit.release(); ctx.close(); });
        } catch (RuntimeException closed) {
            response.release(); if (metricsCredit) scrapeCredit.release(); ctx.close();
        }
    }

    public synchronized CompletionStage<Void> stop() {
        if (stopping) return stopped;
        stopping = true; scrape.shutdown();
        if (listener != null) listener.close();
        var accepted = accept.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        var workers = io.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        var a = new CompletableFuture<Void>(); var b = new CompletableFuture<Void>();
        accepted.addListener(f -> { if (f.isSuccess()) a.complete(null); else a.completeExceptionally(f.cause()); });
        workers.addListener(f -> { if (f.isSuccess()) b.complete(null); else b.completeExceptionally(f.cause()); });
        CompletableFuture.allOf(a, b).whenComplete((v, e) -> {
            if(e!=null){stopped.completeExceptionally(e);return;}
            Thread.startVirtualThread(()->{
                try{
                    if(!scrape.awaitTermination(2,TimeUnit.SECONDS))throw new TimeoutException("Native metrics cleanup unproven");
                    stopped.complete(null);
                }catch(InterruptedException interrupted){Thread.currentThread().interrupt();stopped.completeExceptionally(interrupted);}
                catch(Exception unknown){stopped.completeExceptionally(unknown);}
            });
        });
        return stopped;
    }
    @Override public void close() throws Exception { stop().toCompletableFuture().get(5, TimeUnit.SECONDS); }
}
