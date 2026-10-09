package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.*;

import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.*;
import io.webrtc.signaling.protocol.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class FrameRejectionTest {
  @Test
  void malformedJsonReturnsBoundedProtocolCloseWithoutReflectingPayload() {
    var credit = new GatewayIngressBudget(8, 81920);
    var channel =
        new EmbeddedChannel(
            new FrameAdmissionHandler(
                new ProtocolValidator(ProtocolLimits.v1()), Runnable::run, credit));
    try {
      channel.writeInbound(new TextWebSocketFrame("{TEST_ONLY_PRIVATE_TOKEN"));
      channel.runPendingTasks();
      Object original = channel.readOutbound();
      assertThat(original).isInstanceOf(CloseWebSocketFrame.class);
      var close = (CloseWebSocketFrame) original;
      try {
        assertThat(close.statusCode()).isEqualTo(1002);
        assertThat(close.reasonText()).isEqualTo("PROTOCOL_REJECTED");
      } finally {
        close.release();
      }
      assertThat(channel.isActive()).isFalse();
      assertThat(credit.count()).isZero();
      assertThat(credit.bytes()).isZero();
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void pendingProtocolRejectionCannotAdmitFollowingValidFrame() {
    var frame = new AtomicReference<CloseWebSocketFrame>();
    var promise = new AtomicReference<ChannelPromise>();
    var admitted = new java.util.concurrent.atomic.AtomicInteger();
    var credit = new GatewayIngressBudget(8, 81920);
    var channel =
        new EmbeddedChannel(
            new ChannelOutboundHandlerAdapter() {
              @Override
              public void write(
                  ChannelHandlerContext ctx, Object message, ChannelPromise original) {
                if (message instanceof CloseWebSocketFrame close) {
                  frame.set(close);
                  promise.set(original);
                } else ctx.write(message, original);
              }
            },
            new FrameAdmissionHandler(
                new ProtocolValidator(ProtocolLimits.v1()), Runnable::run, credit),
            new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(ChannelHandlerContext ctx, Object message) {
                var input = (FrameAdmissionHandler.Admitted) message;
                admitted.incrementAndGet();
                input.release().run();
              }
            });
    try {
      channel.writeInbound(new TextWebSocketFrame("{"));
      channel.runPendingTasks();
      assertThat(frame.get()).isNotNull();
      assertThat(channel.isActive()).isTrue();
      channel.writeInbound(
          new TextWebSocketFrame(
              "{\"v\":1,\"type\":\"AUTH\",\"payload\":{\"token\":\"TEST_ONLY\"}}"));
      channel.runPendingTasks();
      assertThat(admitted).hasValue(0);
      assertThat(credit.count()).isZero();
      channel.advanceTimeBy(1, java.util.concurrent.TimeUnit.SECONDS);
      channel.runScheduledPendingTasks();
      assertThat(channel.isActive()).isFalse();
      assertThat(promise.get().isDone()).isFalse();
    } finally {
      if (frame.get() != null) frame.get().release();
      if (promise.get() != null && !promise.get().isDone())
        promise.get().tryFailure(new IllegalStateException("TEST_ONLY cleanup"));
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void expiredCpuQueueCannotClaimProtocolRejection() throws Exception {
    var retained = new AtomicReference<Runnable>();
    var credit = new GatewayIngressBudget(8, 81920);
    var channel =
        new EmbeddedChannel(
            new FrameAdmissionHandler(
                new ProtocolValidator(ProtocolLimits.v1()), retained::set, credit));
    try {
      channel.writeInbound(new TextWebSocketFrame("{"));
      assertThat(credit.count()).isEqualTo(1);
      Thread.sleep(270);
      retained.get().run();
      channel.runPendingTasks();
      assertThat((Object) channel.readOutbound()).isNull();
      assertThat(channel.isActive()).isFalse();
      assertThat(credit.count()).isZero();
    } finally {
      channel.finishAndReleaseAll();
    }
  }
}
