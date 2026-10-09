package io.webrtc.signaling.gateway;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException;

/**
 * Owns the bounded response for a native decoder error. Install immediately before
 * WebSocketProtocolHandler, after its UTF-8 validator, with closeOnProtocolViolation=false. The
 * native decoder still rejects before allocating an oversized payload. Immediate transport close
 * while unread TLS body bytes arrive can lose the response; allow the peer to consume it within the
 * original one-second cleanup window.
 */
public final class DecoderRejectionHandler extends ChannelInboundHandlerAdapter {
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (!(cause instanceof CorruptedWebSocketFrameException nativeError)) {
      ctx.fireExceptionCaught(cause);
      return;
    }
    var status = nativeError.closeStatus();
    var reason =
        status != null && status.code() == 1009
            ? GatewayRejection.Reason.PROTOCOL_FRAME_TOO_LARGE
            : status != null && status.code() == 1007
                ? GatewayRejection.Reason.PROTOCOL_INVALID_UTF8
                : GatewayRejection.Reason.PROTOCOL_REJECTED;
    GatewayRejection.closeDecoder(ctx.channel(), reason);
  }
}
