package io.webrtc.signaling.gateway;

import io.netty.channel.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.ReferenceCountUtil;
import java.util.concurrent.TimeUnit;

/**
 * Charge partial messages before aggregation; one timeout only for an actively fragmented message.
 */
public final class FragmentAdmissionHandler extends ChannelInboundHandlerAdapter {
  private final GatewayIngressBudget budget;
  private GatewayIngressBudget.Ticket retained;
  private int bytes, fragments;
  private io.netty.util.concurrent.ScheduledFuture<?> timeout;

  public FragmentAdmissionHandler(GatewayIngressBudget budget) {
    this.budget = java.util.Objects.requireNonNull(budget);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object message) {
    boolean end = false, forwarded = false;
    try {
      if (message instanceof TextWebSocketFrame text && !text.isFinalFragment()) {
        if (retained != null) throw new IllegalArgumentException("Nested fragment");
        bytes = text.content().readableBytes();
        fragments = 1;
        retained = budget.acquire(Math.max(1, bytes));
        timeout =
            ctx.executor()
                .schedule(
                    () -> {
                      ctx.close();
                    },
                    10,
                    TimeUnit.SECONDS);
      } else if (message instanceof ContinuationWebSocketFrame continuation) {
        if (retained == null || ++fragments > 64)
          throw new IllegalArgumentException("Invalid fragments");
        int more = continuation.content().readableBytes();
        if (bytes + more > 81920)
          throw new IllegalArgumentException("Fragment aggregate too large");
        if (more > 0) retained.grow(more);
        bytes += more;
        end = continuation.isFinalFragment();
      }
      forwarded = true;
      ctx.fireChannelRead(message);
      if (end) release();
    } catch (RuntimeException invalid) {
      if (!forwarded) ReferenceCountUtil.release(message);
      ctx.close();
    }
  }

  private void release() {
    if (timeout != null) timeout.cancel(false);
    timeout = null;
    if (retained != null) retained.close();
    retained = null;
    bytes = fragments = 0;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    try {
      ctx.fireChannelInactive();
    } finally {
      release();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    ctx.close();
  }
}
