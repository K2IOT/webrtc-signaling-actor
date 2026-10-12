package io.webrtc.signaling.gateway;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.ReferenceCountUtil;
import io.webrtc.signaling.protocol.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Bounded owned copies; large JSON is parsed on the supplied bounded CPU executor. */
public final class FrameAdmissionHandler extends ChannelInboundHandlerAdapter {
  public record Admitted(
      SignalEnvelope envelope, int bytes, long submittedNanos, Runnable release) {}

  private record Input(
      byte[] bytes,
      long submitted,
      GatewayIngressBudget.Ticket credit,
      boolean critical,
      AtomicBoolean released) {}

  private static final GatewayIngressBudget SHARED =
      new GatewayIngressBudget(4096, 33554432, 1024, 8388608);
  private final GatewayIngressBudget budget;
  private final ProtocolValidator protocol;
  private final Executor cpu;
  private final ArrayDeque<Input> waiting = new ArrayDeque<>();
  private final ArrayDeque<Input> criticalWaiting = new ArrayDeque<>();
  private int retainedBytes, inFlight, normalBytes, criticalBytes, normalInFlight, criticalInFlight;
  private boolean running;
  private boolean stopped;
  private double rateTokens = 40, totalTokens = 60;
  private long rateAt = System.nanoTime();

  public FrameAdmissionHandler(ProtocolValidator protocol, Executor cpu) {
    this(protocol, cpu, SHARED);
  }

  public FrameAdmissionHandler(
      ProtocolValidator protocol, Executor cpu, GatewayIngressBudget budget) {
    this.budget = Objects.requireNonNull(budget);
    this.protocol = Objects.requireNonNull(protocol);
    this.cpu = Objects.requireNonNull(cpu);
  }

  @Override
  public synchronized void channelRead(ChannelHandlerContext ctx, Object message) {
    if (stopped || GatewayRejection.rejecting(ctx.channel())) {
      ReferenceCountUtil.release(message);
      return;
    }
    if (!(message instanceof TextWebSocketFrame text)) {
      if (message instanceof BinaryWebSocketFrame) {
        ReferenceCountUtil.release(message);
        ctx.close();
      } else ctx.fireChannelRead(message);
      return;
    }
    try {
      long now = System.nanoTime();
      rateTokens = Math.min(40, rateTokens + (now - rateAt) * 20.0 / TimeUnit.SECONDS.toNanos(1));
      totalTokens = Math.min(60, totalTokens + (now - rateAt) * 30.0 / TimeUnit.SECONDS.toNanos(1));
      rateAt = now;
      int bytes = text.content().readableBytes();
      boolean critical =
          Boolean.TRUE.equals(ctx.channel().attr(ConnectionRegistry.AUTHENTICATED).get())
              && GatewayInputClass.critical(text.content());
      if (totalTokens < 1 || (!critical && rateTokens < 1)) {
        ctx.close();
        return;
      }
      totalTokens--;
      if (!critical) rateTokens--;
      if (bytes < 1
          || bytes > 81920
          || inFlight >= 64
          || retainedBytes + bytes > 262144
          || (critical
              ? criticalInFlight >= 8 || criticalBytes + bytes > 131072
              : normalInFlight >= 56 || normalBytes + bytes > 131072)) {
        ctx.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"OVERLOADED\"}"));
        return;
      }
      GatewayIngressBudget.Ticket credit;
      try {
        credit = budget.acquire(bytes, critical);
      } catch (RejectedExecutionException full) {
        ctx.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"OVERLOADED\"}"));
        return;
      }
      byte[] owned;
      try {
        owned = ByteBufUtil.getBytes(text.content());
      } catch (RuntimeException allocation) {
        credit.close();
        throw allocation;
      }
      (critical ? criticalWaiting : waiting)
          .add(new Input(owned, now, credit, critical, new AtomicBoolean()));
      if (critical) {
        criticalInFlight++;
        criticalBytes += bytes;
      } else {
        normalInFlight++;
        normalBytes += bytes;
      }
      retainedBytes += bytes;
      inFlight++;
      start(ctx);
    } finally {
      text.release();
    }
  }

  private synchronized void start(ChannelHandlerContext ctx) {
    if (stopped
        || running
        || (waiting.isEmpty() && criticalWaiting.isEmpty())
        || GatewayRejection.rejecting(ctx.channel())) return;
    Input input = criticalWaiting.isEmpty() ? waiting.remove() : criticalWaiting.remove();
    running = true;
    try {
      Runnable parse =
          () -> {
            SignalEnvelope parsed = null;
            Throwable failure = null;
            try {
              if (System.nanoTime() - input.submitted() > TimeUnit.MILLISECONDS.toNanos(250))
                throw new RejectedExecutionException("INGRESS_QUEUE_EXPIRED");
              parsed = protocol.decodePublic(input.bytes());
              if (input.critical() && !GatewayInputClass.critical(parsed.type()))
                throw new ProtocolException(ErrorCode.INVALID_FRAME);
            } catch (Throwable invalid) {
              failure = invalid;
            }
            var value = parsed;
            var error = failure;
            Runnable release = () -> complete(ctx, input);
            try {
              ctx.executor()
                  .execute(
                      () -> {
                        parsed(ctx, input, value, error, release);
                      });
            } catch (RejectedExecutionException closed) {
              rejected(ctx, input);
            }
          };
      if (cpu instanceof GatewayCpuExecutor priority) priority.execute(input.critical(), parse);
      else cpu.execute(parse);
    } catch (RejectedExecutionException overloaded) {
      release(input);
      running = false;
      ctx.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"OVERLOADED\"}"));
      start(ctx);
    }
  }

  private synchronized void parsed(
      ChannelHandlerContext ctx,
      Input input,
      SignalEnvelope value,
      Throwable error,
      Runnable release) {
    running = false;
    if (stopped || !ctx.channel().isActive()) {
      release.run();
      return;
    }
    if (error != null) {
      try {
        if (error instanceof ProtocolException)
          GatewayRejection.close(ctx.channel(), GatewayRejection.Reason.PROTOCOL_REJECTED);
        else ctx.close();
      } finally {
        release.run();
      }
      return;
    }
    GatewayCpuExecutor.handoff(
        input.critical(),
        () ->
            ctx.fireChannelRead(
                new Admitted(value, input.bytes().length, input.submitted(), release)));
    start(ctx);
  }

  @Override
  public synchronized void channelInactive(ChannelHandlerContext ctx) {
    stopWaiting();
    ctx.fireChannelInactive();
  }

  private synchronized void release(Input input) {
    if (!input.released().compareAndSet(false, true)) return;
    retainedBytes -= input.bytes().length;
    inFlight--;
    if (input.critical()) {
      criticalInFlight--;
      criticalBytes -= input.bytes().length;
    } else {
      normalInFlight--;
      normalBytes -= input.bytes().length;
    }
    input.credit().close();
  }

  private void complete(ChannelHandlerContext ctx, Input input) {
    release(input);
    if (ctx.executor().inEventLoop()) start(ctx);
    else {
      try {
        ctx.executor().execute(() -> start(ctx));
      } catch (RejectedExecutionException closed) {
        synchronized (this) {
          stopWaiting();
        }
      }
    }
  }

  private synchronized void rejected(ChannelHandlerContext ctx, Input input) {
    running = false;
    release(input);
    stopWaiting();
    ctx.channel().close();
  }

  private void stopWaiting() {
    stopped = true;
    while (!waiting.isEmpty()) release(waiting.remove());
    while (!criticalWaiting.isEmpty()) release(criticalWaiting.remove());
  }

  public synchronized int retainedBytes() {
    return retainedBytes;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    ctx.close();
  }
}
