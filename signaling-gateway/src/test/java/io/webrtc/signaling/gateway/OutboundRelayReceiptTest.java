package io.webrtc.signaling.gateway;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.rpc.DeliveryCreditController;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
/** TEST_ONLY Netty fault transport keeps a started write through an unknown close. */
class OutboundRelayReceiptTest {
    static final class UnknownCloseTransport extends ChannelDuplexHandler {
        final List<Object> messages=new ArrayList<>();final List<ChannelPromise> promises=new ArrayList<>();
        @Override public void write(ChannelHandlerContext ctx,Object message,ChannelPromise promise){messages.add(message);promises.add(promise);}
        @Override public void close(ChannelHandlerContext ctx,ChannelPromise promise){ctx.close(promise);}
        void settled(){for(var message:messages)ReferenceCountUtil.release(message);for(var promise:promises)promise.tryFailure(new java.nio.channels.ClosedChannelException());messages.clear();promises.clear();}
    }
    @Test void volatileWriteReceiptWaitsForOriginalTransportEvenAfterLogicalPressureTimeout(){
        var aggregate=new DeliveryCreditController(64,1048576,8,65536);var transport=new UnknownCloseTransport();var channel=new EmbeddedChannel(transport,new OutboundAdmissionHandler(aggregate));
        var frame=new OutboundAdmissionHandler.RelayFrame("x".repeat(8192));
        try{
            var logical=channel.writeAndFlush(frame);assertThat(logical.isDone()).isFalse();assertThat(frame.physicalCompletion().toCompletableFuture()).isNotDone();
            channel.advanceTimeBy(11,TimeUnit.SECONDS);channel.runScheduledPendingTasks();
            assertThat(channel.isActive()).isTrue();assertThat(GatewayRejection.rejecting(channel)).isTrue();
            channel.advanceTimeBy(1100,TimeUnit.MILLISECONDS);channel.runScheduledPendingTasks();
            assertThat(logical.isSuccess()).isFalse();assertThat(channel.isActive()).isFalse();assertThat(frame.physicalCompletion().toCompletableFuture()).isNotDone();assertThat(aggregate.retainedBytes()).isGreaterThanOrEqualTo(8192);
            transport.settled();assertThat(frame.physicalCompletion().toCompletableFuture()).isDone();assertThat(aggregate.retainedBytes()).isZero();assertThat(frame.refCnt()).isZero();
        }finally{transport.settled();channel.finishAndReleaseAll();}
    }
    @Test void knownRejectedUnstartedFrameProducesCleanupOnlyAfterReleasingItsBuffer(){
        var aggregate=new DeliveryCreditController(4,1024,1,128);var channel=new EmbeddedChannel(new OutboundAdmissionHandler(aggregate));var frame=new OutboundAdmissionHandler.RelayFrame("x".repeat(8192));
        try{assertThat(channel.writeAndFlush(frame).isSuccess()).isFalse();assertThat(frame.refCnt()).isZero();assertThat(frame.physicalCompletion().toCompletableFuture()).isDone();assertThat(aggregate.retainedBytes()).isZero();}finally{channel.finishAndReleaseAll();}
    }
    @Test void actualSuccessfulNettyWriteHasASeparatePhysicalReceipt(){
        var aggregate=new DeliveryCreditController(64,1048576,8,65536);var channel=new EmbeddedChannel(new OutboundAdmissionHandler(aggregate));var frame=new OutboundAdmissionHandler.RelayFrame("TEST_ONLY_FRAME");
        try{assertThat(channel.writeAndFlush(frame).isSuccess()).isTrue();assertThat(frame.physicalCompletion().toCompletableFuture()).isDone();assertThat(aggregate.retainedBytes()).isZero();ReferenceCountUtil.release(channel.readOutbound());}finally{channel.finishAndReleaseAll();}
    }
}
