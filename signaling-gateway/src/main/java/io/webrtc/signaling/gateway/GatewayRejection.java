package io.webrtc.signaling.gateway;

import io.netty.channel.*;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.util.AttributeKey;
import io.webrtc.signaling.auth.AuthorizationStatus;
import java.util.concurrent.TimeUnit;

/** Fixed native reasons only; never carries a token, subject, exception message or source URL. */
final class GatewayRejection {
    enum Reason { AUTH_REVOKED, AUTH_FRESHNESS_UNKNOWN, AUTH_TOKEN_EXPIRED, AUTHORIZATION_REJECTED, AUTH_REQUIRED, STALE_CONNECTION, PROTOCOL_REJECTED, PROTOCOL_FRAME_TOO_LARGE, PROTOCOL_INVALID_UTF8 }
    private static final AttributeKey<Boolean> REJECTING=AttributeKey.valueOf("signaling.rejecting");
    static boolean rejecting(Channel channel){return Boolean.TRUE.equals(channel.attr(REJECTING).get());}
    static Reason security(AuthorizationStatus status){return switch(status){
        case REVOKED -> Reason.AUTH_REVOKED;
        case FRESHNESS_UNKNOWN -> Reason.AUTH_FRESHNESS_UNKNOWN;
        case TOKEN_EXPIRED -> Reason.AUTH_TOKEN_EXPIRED;
        case ALLOWED -> throw new IllegalArgumentException("Allowed status is not a rejection");
    };}
    static void close(Channel channel,Reason reason){close(channel,reason,false);}
    static void closeDecoder(Channel channel,Reason reason){close(channel,reason,true);}
    private static void close(Channel channel,Reason reason,boolean awaitPeer){
        if(!channel.isActive()||Boolean.TRUE.equals(channel.attr(REJECTING).getAndSet(Boolean.TRUE)))return;
        var deadline=channel.eventLoop().schedule(()->forceTransportClose(channel),1,TimeUnit.SECONDS);
        channel.closeFuture().addListener(done->deadline.cancel(false));
        int code=switch(reason){case PROTOCOL_REJECTED->1002;case PROTOCOL_FRAME_TOO_LARGE->1009;case PROTOCOL_INVALID_UTF8->1007;default->1008;};
        var original=channel.writeAndFlush(new CloseWebSocketFrame(code,reason.name()));
        if(awaitPeer)original.addListener(done->{if(!done.isSuccess())forceTransportClose(channel);});
        else original.addListener(ChannelFutureListener.CLOSE);
    }
    /** Deadline cleanup of this exact owned socket must not await a second unknown TLS/WS close. */
    static void forceTransportClose(Channel channel){
        var first=channel.pipeline().firstContext();
        if(first==null)channel.close();else first.close();
    }
    private GatewayRejection(){}
}
