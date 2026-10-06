package io.webrtc.signaling.gateway;
import io.webrtc.signaling.auth.AuthorizationStatus;
import io.netty.channel.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.buffer.Unpooled;
import java.time.*;
import java.util.*;
/** Invoked by the shared process wheel, not a scheduled task per authenticated socket. */
public final class HeartbeatHandler extends ChannelInboundHandlerAdapter {
    private final ConnectionRegistry registry;private final GatewayServices services;private final Clock clock;private ChannelHandlerContext context;private Instant nextPing,pongDeadline,expiringFor;private long sequence;private boolean pending;
    public HeartbeatHandler(ConnectionRegistry registry,GatewayServices services,Clock clock){this.registry=Objects.requireNonNull(registry);this.services=Objects.requireNonNull(services);this.clock=Objects.requireNonNull(clock);nextPing=clock.instant().plusSeconds(30);}
    void initialPing(Instant due){nextPing=due;}
    @Override public void handlerAdded(ChannelHandlerContext ctx){context=ctx;}
    @Override public void channelRead(ChannelHandlerContext ctx,Object message){if(message instanceof PongWebSocketFrame pong){try{if(pending&&pong.content().readableBytes()==8&&pong.content().getLong(pong.content().readerIndex())==sequence)pending=false;}finally{pong.release();}return;}if(message instanceof PingWebSocketFrame ping){ctx.writeAndFlush(new PongWebSocketFrame(ping.content().retain()));ping.release();return;}ctx.fireChannelRead(message);}
    public void tick(Instant now){if(context==null||!context.channel().isActive())return;var id=context.channel().attr(ConnectionRegistry.CONNECTION).get();var binding=registry.binding(id);if(binding==null)return;
        if(GatewayRejection.rejecting(context.channel()))return;
        if(!services.currentBoot()){context.close();return;}
        if(!registry.current(id,binding.route())){GatewayRejection.close(context.channel(),GatewayRejection.Reason.STALE_CONNECTION);return;}
        var security=services.cachedSecurity(binding.principal(),now);if(security!=AuthorizationStatus.ALLOWED){if(security==null)context.close();else GatewayRejection.close(context.channel(),GatewayRejection.security(security));return;}
        if(pending&&!now.isBefore(pongDeadline)){context.close();return;}
        if(!binding.principal().expiresAt().isAfter(now.plusSeconds(60))&&!binding.principal().expiresAt().equals(expiringFor)){expiringFor=binding.principal().expiresAt();context.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"AUTH_EXPIRING\"}"));}
        if(!now.isBefore(nextPing)){sequence++;pending=true;pongDeadline=now.plusSeconds(10);nextPing=now.plusSeconds(30);context.writeAndFlush(new PingWebSocketFrame(Unpooled.buffer(8).writeLong(sequence)));}
    }
}
