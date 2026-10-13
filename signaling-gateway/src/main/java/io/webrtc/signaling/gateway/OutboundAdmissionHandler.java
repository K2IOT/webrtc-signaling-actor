package io.webrtc.signaling.gateway;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import io.webrtc.signaling.rpc.DeliveryCreditController;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Charges owned WebSocket frames through actual Netty write completion. HTTP upgrade has its
 * separate edge budget.
 */
public final class OutboundAdmissionHandler extends ChannelDuplexHandler {
    private final DeliveryCreditController aggregate;
    private final OutboundQueue queue;
    private io.netty.util.concurrent.ScheduledFuture<?> pressure;
    private boolean resync;

    public OutboundAdmissionHandler(DeliveryCreditController aggregate) {
        this.aggregate = java.util.Objects.requireNonNull(aggregate);
        queue = new OutboundQueue(63, 262016, Duration.ofSeconds(10), aggregate, System::nanoTime);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object message, ChannelPromise logical) {
        if (!(message instanceof WebSocketFrame frame)) {
            ctx.write(message, logical);
            return;
        }
        int bytes = frame.content().readableBytes() + 14;
        var transferred = new AtomicBoolean();
        queue.writable(ctx.channel().isWritable());
        var result =
                queue.offer(
                        message instanceof RelayFrame ? OutboundQueue.Kind.RELAY : OutboundQueue.Kind.CONTROL,
                        bytes,
                        () -> {
                            transferred.set(true);
                            var physical = new CompletableFuture<Void>();
                            var promise = ctx.newPromise();
                            promise.addListener(
                                    done -> {
                                        if (done.isSuccess()) physical.complete(null);
                                        else physical.completeExceptionally(new OutboundQueue.ResyncRequired());
                                        if (message instanceof RelayFrame relay) relay.physical.complete(null);
                                        if (message instanceof ControlFrame control) control.physical.complete(null);
                                    });
                            ctx.writeAndFlush(message, promise);
                            return physical;
                        });
        result.whenComplete(
                (done, error) -> {
                    if (!transferred.get()) {
                        ReferenceCountUtil.release(message);
                        if (message instanceof RelayFrame relay) relay.physical.complete(null);
                        if (message instanceof ControlFrame control) control.physical.complete(null);
                    }
                    if (error == null) logical.trySuccess();
                    else logical.tryFailure(new OutboundQueue.ResyncRequired());
                    if (queue.retainedCount() == 0 && pressure != null) {
                        pressure.cancel(false);
                        pressure = null;
                    }
                    if (error != null) closeSlowConsumer(ctx);
                });
        if (queue.retainedCount() > 0 && pressure == null)
            pressure =
                    ctx.executor()
                            .schedule(
                                    () -> {
                                        pressure = null;
                                        queue.close();
                                        closeSlowConsumer(ctx);
                                    },
                                    10,
                                    TimeUnit.SECONDS);
    }

    private void closeSlowConsumer(ChannelHandlerContext ctx) {
        if (resync) return;
        resync = true;
        queue.close();
        try {
            var credit = aggregate.acquire(true, 128);
            var promise = ctx.newPromise();
            promise.addListener(done -> credit.close());
            GatewayRejection.closePressure(ctx, promise);
        } catch (DeliveryCreditController.Overloaded full) {
            GatewayRejection.forceTransportClose(ctx.channel());
        }
        ctx.fireUserEventTriggered("RESYNC_REQUIRED");
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        queue.writable(ctx.channel().isWritable());
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (pressure != null) pressure.cancel(false);
        pressure = null;
        queue.close();
        ctx.fireChannelInactive();
    }

    public int retainedBytes() {
        return queue.retainedBytes();
    }

    public static final class RelayFrame extends TextWebSocketFrame {
        private final CompletableFuture<Void> physical = new CompletableFuture<>();

        public RelayFrame(String payload) {
            super(payload);
        }

        /**
         * Cleanup, independent of successful delivery or the caller's logical timeout.
         */
        public CompletionStage<Void> physicalCompletion() {
            return physical.minimalCompletionStage();
        }
    }

    /**
     * Protected CONTROL frame with original socket-write cleanup independent of queue timeout.
     */
    public static final class ControlFrame extends TextWebSocketFrame {
        private final CompletableFuture<Void> physical = new CompletableFuture<>();

        public ControlFrame(String payload) {
            super(payload);
        }

        public CompletionStage<Void> physicalCompletion() {
            return physical.minimalCompletionStage();
        }
    }
}
