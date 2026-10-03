package io.webrtc.signaling.gateway;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.rpc.DeliveryCreditController;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.ReferenceCountUtil;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
class OutboundAdmissionHandlerTest {
    static final class BlockedTransport extends ChannelDuplexHandler {
        final List<Object> messages=new ArrayList<>();final List<ChannelPromise> promises=new ArrayList<>();
        @Override public void write(ChannelHandlerContext ctx,Object message,ChannelPromise promise){messages.add(message);promises.add(promise);}
        @Override public void close(ChannelHandlerContext ctx,ChannelPromise promise){for(var message:messages)ReferenceCountUtil.release(message);for(var pending:promises)pending.tryFailure(new java.nio.channels.ClosedChannelException());messages.clear();promises.clear();ctx.close(promise);}
    }
    @Test void physicalWritesRemainChargedAndTenSecondsOfPressureClosesOnlySlowChannel(){
        var aggregate=new DeliveryCreditController(64,1048576,8,65536);var slowTransport=new BlockedTransport();var channel=new EmbeddedChannel(slowTransport,new OutboundAdmissionHandler(aggregate));var good=new EmbeddedChannel(new OutboundAdmissionHandler(aggregate));
        try{var frame=new TextWebSocketFrame("x".repeat(8192));var pending=channel.writeAndFlush(frame);assertThat(pending.isDone()).isFalse();assertThat(aggregate.retainedBytes()).isGreaterThanOrEqualTo(8192);assertThat(good.writeAndFlush(new TextWebSocketFrame("ok")).isSuccess()).isTrue();((TextWebSocketFrame)good.readOutbound()).release();channel.advanceTimeBy(11,TimeUnit.SECONDS);channel.runScheduledPendingTasks();assertThat(channel.isActive()).isFalse();assertThat(pending.isSuccess()).isFalse();assertThat(frame.refCnt()).isZero();assertThat(aggregate.retainedBytes()).isZero();assertThat(good.isActive()).isTrue();}finally{channel.finishAndReleaseAll();good.finishAndReleaseAll();}
    }
}
