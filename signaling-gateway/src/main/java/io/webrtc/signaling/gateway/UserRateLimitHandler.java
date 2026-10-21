package io.webrtc.signaling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.webrtc.signaling.protocol.ErrorCode;
import java.time.Clock;
import java.util.Objects;

/**
 * Applies the shared per-user bucket to authenticated, non-critical admitted frames. A denied frame
 * releases its ingress credits and returns a typed {@code RATE_LIMITED} error; the socket stays
 * open.
 */
final class UserRateLimitHandler extends ChannelInboundHandlerAdapter {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final ConnectionRegistry registry;
  private final UserRateLimiter limiter;
  private final Clock clock;

  UserRateLimitHandler(ConnectionRegistry registry, UserRateLimiter limiter, Clock clock) {
    this.registry = Objects.requireNonNull(registry);
    this.limiter = Objects.requireNonNull(limiter);
    this.clock = Objects.requireNonNull(clock);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object message) {
    if (!(message instanceof FrameAdmissionHandler.Admitted admitted)
        || GatewayInputClass.critical(admitted.envelope().type())) {
      ctx.fireChannelRead(message);
      return;
    }
    var id = ctx.channel().attr(ConnectionRegistry.CONNECTION).get();
    var binding = id == null ? null : registry.binding(id);
    if (binding == null) {
      ctx.fireChannelRead(message);
      return;
    }
    var principal = binding.principal();
    long retryAfterMs =
        limiter.tryAcquire(principal.key().issuer() + '\u0000' + principal.userId().value());
    if (retryAfterMs == 0) {
      ctx.fireChannelRead(message);
      return;
    }
    try {
      ctx.writeAndFlush(new TextWebSocketFrame(rejection(admitted, retryAfterMs)));
    } finally {
      admitted.release().run();
    }
  }

  private String rejection(FrameAdmissionHandler.Admitted admitted, long retryAfterMs) {
    var root = JSON.createObjectNode().put("v", 1).put("type", "ERROR");
    var requestId = admitted.envelope().requestId();
    if (requestId != null) root.put("requestId", requestId.value().toString());
    root.put("serverTime", clock.instant().toString()).put("ackCommitted", false);
    root.putObject("error")
        .put("code", ErrorCode.RATE_LIMITED.name())
        .put("retryAfterMs", retryAfterMs);
    return root.toString();
  }
}
