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
    @Test void pressureRejectionPreservesOriginalCloseWriteAndDeniesNewIngressDuringCleanup(){
        var aggregate=new DeliveryCreditController(64,1048576,8,65536);var transport=new BlockedTransport();
        var channel=new EmbeddedChannel(transport,new OutboundAdmissionHandler(aggregate));
        try{
            var original=channel.writeAndFlush(new TextWebSocketFrame("TEST_ONLY_original"));
            channel.advanceTimeBy(10,TimeUnit.SECONDS);channel.runScheduledPendingTasks();
            var closes=transport.messages.stream().filter(CloseWebSocketFrame.class::isInstance).map(CloseWebSocketFrame.class::cast).toList();
            assertThat(closes).hasSize(1);assertThat(closes.getFirst().statusCode()).isEqualTo(1013);assertThat(closes.getFirst().reasonText()).isEqualTo("RESYNC_REQUIRED");
            assertThat(channel.isActive()).isTrue();assertThat(GatewayRejection.rejecting(channel)).isTrue();
            assertThat(original.isSuccess()).isFalse();assertThat(aggregate.retainedBytes()).isPositive();
        }finally{transport.abort();channel.finishAndReleaseAll();}
    }
    static final class BlockedTransport extends ChannelDuplexHandler {
        final List<Object> messages=new ArrayList<>();final List<ChannelPromise> promises=new ArrayList<>();
        @Override public void write(ChannelHandlerContext ctx,Object message,ChannelPromise promise){messages.add(message);promises.add(promise);}
        void abort(){for(var message:messages)ReferenceCountUtil.release(message);for(var pending:promises)pending.tryFailure(new java.nio.channels.ClosedChannelException());messages.clear();promises.clear();}
        @Override public void close(ChannelHandlerContext ctx,ChannelPromise promise){abort();ctx.close(promise);}
    }
    @Test void physicalWritesRemainChargedAndTenSecondsOfPressureClosesOnlySlowChannel(){
        var aggregate=new DeliveryCreditController(64,1048576,8,65536);var slowTransport=new BlockedTransport();var channel=new EmbeddedChannel(slowTransport,new OutboundAdmissionHandler(aggregate));var good=new EmbeddedChannel(new OutboundAdmissionHandler(aggregate));
        try{var frame=new TextWebSocketFrame("x".repeat(8192));var pending=channel.writeAndFlush(frame);assertThat(pending.isDone()).isFalse();assertThat(aggregate.retainedBytes()).isGreaterThanOrEqualTo(8192);assertThat(good.writeAndFlush(new TextWebSocketFrame("ok")).isSuccess()).isTrue();((TextWebSocketFrame)good.readOutbound()).release();channel.advanceTimeBy(11,TimeUnit.SECONDS);channel.runScheduledPendingTasks();assertThat(channel.isActive()).isTrue();channel.advanceTimeBy(1100,TimeUnit.MILLISECONDS);channel.runScheduledPendingTasks();assertThat(channel.isActive()).isFalse();assertThat(aggregate.retainedBytes()).isPositive();slowTransport.abort();assertThat(pending.isSuccess()).isFalse();assertThat(frame.refCnt()).isZero();assertThat(aggregate.retainedBytes()).isZero();assertThat(good.isActive()).isTrue();}finally{slowTransport.abort();channel.finishAndReleaseAll();good.finishAndReleaseAll();}
    }
}
