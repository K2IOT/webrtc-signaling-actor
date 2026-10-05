package io.webrtc.signaling.gateway;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException;
import java.util.concurrent.TimeUnit;

/**
 * The native decoder writes its close frame before throwing a protocol violation.
 * Install after that decoder and before WebSocketProtocolHandler, whose generic
 * exception path would close TLS before the original close-frame write settles.
 * Requires the decoder's closeOnProtocolViolation=true (the pinned native default).
 */
public final class DecoderRejectionHandler extends ChannelInboundHandlerAdapter {
    private boolean rejecting;
    @Override public void exceptionCaught(ChannelHandlerContext ctx,Throwable cause){
        if(!(cause instanceof CorruptedWebSocketFrameException)){ctx.fireExceptionCaught(cause);return;}
        if(rejecting)return;
        rejecting=true;
        // The decoder's original write owns normal closure. An unknown write must
        // still have bounded socket cleanup; this timer never reports write success.
        var deadline=ctx.executor().schedule(()->{ctx.close();},1,TimeUnit.SECONDS);
        ctx.channel().closeFuture().addListener(done->deadline.cancel(false));
    }
}
