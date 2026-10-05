package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DecoderRejectionTest {
    @Test void nativeDecoderCloseWriteMustFinishBeforeProtocolHandlerClosesTransport(){run(true);}
    @Test void unknownDecoderCloseWriteHasBoundedSocketCleanup(){run(false);}
    @Test void unrelatedExceptionsAreForwarded(){
        var failure=new AtomicReference<Throwable>();var original=new IllegalStateException("TEST_ONLY");
        var channel=new EmbeddedChannel(new DecoderRejectionHandler(),new ChannelInboundHandlerAdapter(){@Override public void exceptionCaught(ChannelHandlerContext ctx,Throwable cause){failure.set(cause);ctx.close();}});
        try{channel.pipeline().fireExceptionCaught(original);assertThat(failure.get()).isSameAs(original);assertThat(channel.isActive()).isFalse();}finally{channel.finishAndReleaseAll();}
    }
    void run(boolean completeWrite){
        var held=new AtomicReference<ChannelPromise>();var frame=new AtomicReference<CloseWebSocketFrame>();var context=new AtomicReference<ChannelHandlerContext>();
        var output=new ChannelOutboundHandlerAdapter(){@Override public void write(ChannelHandlerContext ctx,Object message,ChannelPromise promise){if(message instanceof CloseWebSocketFrame close&&held.compareAndSet(null,promise)){context.set(ctx);frame.set(close);}else ctx.write(message,promise);}};
        var server=new EmbeddedChannel(output,new WebSocket08FrameDecoder(true,false,81920),new DecoderRejectionHandler(),new WebSocketServerProtocolHandler("/ws"));
        var client=new EmbeddedChannel(new WebSocket08FrameEncoder(true));
        try{
            client.writeOutbound(new TextWebSocketFrame("x".repeat(81921)));ByteBuf bytes=client.readOutbound();
            try{server.writeInbound(bytes);}catch(CorruptedWebSocketFrameException nativeRejection){assertThat(nativeRejection.closeStatus().code()).isEqualTo(1009);}
            server.runPendingTasks();assertThat(frame.get()).isNotNull();assertThat(frame.get().statusCode()).isEqualTo(1009);
            assertThat(held.get().isDone()).isFalse();assertThat(server.isActive()).isTrue();
            if(!completeWrite){server.advanceTimeBy(1,java.util.concurrent.TimeUnit.SECONDS);server.runScheduledPendingTasks();assertThat(server.isActive()).isFalse();assertThat(held.get().isDone()).isFalse();return;}
            context.get().writeAndFlush(frame.getAndSet(null),held.get());server.runPendingTasks();
            assertThat(held.get().isSuccess()).isTrue();assertThat(server.isActive()).isFalse();
            var original=(CloseWebSocketFrame)server.readOutbound();try{assertThat(original.statusCode()).isEqualTo(1009);}finally{original.release();}
        }finally{var retained=frame.getAndSet(null);if(retained!=null)retained.release();if(held.get()!=null&&!held.get().isDone())held.get().tryFailure(new IllegalStateException("TEST_ONLY cleanup"));server.finishAndReleaseAll();client.finishAndReleaseAll();}
    }
}
