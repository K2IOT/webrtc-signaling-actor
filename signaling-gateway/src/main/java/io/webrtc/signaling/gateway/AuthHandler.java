package io.webrtc.signaling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.ReferenceCountUtil;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.SessionRepository;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

/** AUTH_OK is emitted only after committed native registration and a still-current gateway boot. */
public final class AuthHandler extends ChannelInboundHandlerAdapter {
  private final ConnectionRegistry registry;
  private final GatewayServices services;
  private final Clock clock;
  private final ProtocolValidator protocol = new ProtocolValidator(ProtocolLimits.v1());
  private static final Executor DEFAULT_CPU =
      new ThreadPoolExecutor(
          2,
          2,
          0,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(1024),
          Thread.ofPlatform().daemon().name("gateway-binding-", 0).factory(),
          new ThreadPoolExecutor.AbortPolicy());
  private final Executor cpu;
  private UUID connection;
  private SessionRepository.Route route;
  private String verifiedToken;
  private boolean verifying;
  private io.netty.util.concurrent.ScheduledFuture<?> authTimeout;

  public AuthHandler(ConnectionRegistry registry, GatewayServices services, Clock clock) {
    this(registry, services, clock, DEFAULT_CPU);
  }

  public AuthHandler(
      ConnectionRegistry registry, GatewayServices services, Clock clock, Executor cpu) {
    this.cpu = Objects.requireNonNull(cpu);
    this.registry = Objects.requireNonNull(registry);
    this.services = Objects.requireNonNull(services);
    this.clock = Objects.requireNonNull(clock);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    try {
      connection = ctx.channel().attr(ConnectionRegistry.CONNECTION).get();
      if (connection == null) connection = registry.attach(ctx.channel());
      authTimeout =
          ctx.executor()
              .schedule(
                  () -> {
                    if (route == null) ctx.close();
                  },
                  5,
                  TimeUnit.SECONDS);
      ctx.fireChannelActive();
    } catch (RejectedExecutionException full) {
      ctx.close();
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object message) {
    if (GatewayRejection.rejecting(ctx.channel())) {
      if (message instanceof FrameAdmissionHandler.Admitted admitted) admitted.release().run();
      else ReferenceCountUtil.release(message);
      return;
    }
    if (!(message instanceof FrameAdmissionHandler.Admitted frame)) {
      if (message instanceof CloseWebSocketFrame) {
        ReferenceCountUtil.release(message);
        ctx.close();
        return;
      }
      if (route == null) {
        ReferenceCountUtil.release(message);
        ctx.close();
        return;
      }
      ctx.fireChannelRead(message);
      return;
    }
    var envelope = frame.envelope();
    if (!services.currentBoot()) {
      frame.release().run();
      ctx.close();
      return;
    }
    if (route != null && !registry.current(connection, route)) {
      frame.release().run();
      GatewayRejection.close(ctx.channel(), GatewayRejection.Reason.STALE_CONNECTION);
      return;
    }
    if (envelope.type() == SignalEnvelope.Type.AUTH
        || envelope.type() == SignalEnvelope.Type.AUTH_REFRESH) {
      boolean refresh = envelope.type() == SignalEnvelope.Type.AUTH_REFRESH;
      if (verifying || refresh != (route != null)) {
        frame.release().run();
        ctx.close();
        return;
      }
      verify(ctx, frame, refresh);
      return;
    }
    var binding = registry.binding(connection);
    if (route == null || binding == null) {
      frame.release().run();
      GatewayRejection.close(ctx.channel(), GatewayRejection.Reason.AUTH_REQUIRED);
      return;
    }
    var security = services.cachedSecurity(binding.principal(), clock.instant());
    if (security != AuthorizationStatus.ALLOWED) {
      frame.release().run();
      if (security == null) ctx.close();
      else GatewayRejection.close(ctx.channel(), GatewayRejection.security(security));
      return;
    }
    try {
      var sender =
          new AuthenticatedSession(
              route.user(),
              route.key(),
              route.incarnation(),
              route.connectionGeneration(),
              connection);
      var committedRoute = route;
      var originalToken = verifiedToken;
      cpu.execute(
          () -> {
            try {
              var command = protocol.bind(envelope, sender);
              var result =
                  services.command(command, committedRoute, originalToken, remaining(frame));
              respond(ctx, frame, result, false);
            } catch (RuntimeException invalid) {
              ctx.executor()
                  .execute(
                      () -> {
                        frame.release().run();
                        ctx.close();
                      });
            }
          });
    } catch (RuntimeException invalid) {
      frame.release().run();
      ctx.close();
    }
  }

  private Duration remaining(FrameAdmissionHandler.Admitted frame) {
    long left = TimeUnit.SECONDS.toNanos(2) - (System.nanoTime() - frame.submittedNanos());
    if (left <= 0) throw new RejectedExecutionException("INGRESS_EXPIRED");
    return Duration.ofNanos(left);
  }

  private void verify(
      ChannelHandlerContext ctx, FrameAdmissionHandler.Admitted frame, boolean refresh) {
    verifying = true;
    SessionRepository.Route expected = route;
    var failed = new java.util.concurrent.atomic.AtomicBoolean();
    var nativeClosed = new java.util.concurrent.atomic.AtomicBoolean();
    java.util.function.Consumer<SessionRepository.Route> closeOnce =
        nativeRoute -> {
          if (nativeClosed.compareAndSet(false, true)) services.close(nativeRoute);
        };
    try {
      String token =
          new ObjectMapper().readTree(frame.envelope().payloadJson()).path("token").asText();
      var checked =
          services
              .verify(token, clock.instant())
              .thenCompose(
                  principal -> {
                    if (!services.currentBoot())
                      return CompletableFuture.failedFuture(
                          new IllegalStateException("Gateway boot unavailable"));
                    var security = services.cachedSecurity(principal, clock.instant());
                    if (security != AuthorizationStatus.ALLOWED)
                      return CompletableFuture.failedFuture(
                          security == null
                              ? new IllegalStateException("Security source unavailable")
                              : new NativeSecurityRejection(security));
                    if (refresh
                        && (!principal.userId().equals(expected.user())
                            || !principal.key().equals(expected.key())))
                      return CompletableFuture.failedFuture(new AuthException());
                    var original =
                        refresh
                            ? services.refresh(expected, principal, token, remaining(frame))
                            : services.register(principal, token, connection, remaining(frame));
                    // Keep the original completion independent of the logical deadline. A late
                    // COMMIT still needs cleanup.
                    original.whenComplete(
                        (nativeRoute, error) -> {
                          if (nativeRoute != null
                              && (failed.get()
                                  || !ctx.channel().isActive()
                                  || System.nanoTime() - frame.submittedNanos()
                                      >= TimeUnit.SECONDS.toNanos(2)))
                            closeOnce.accept(nativeRoute);
                        });
                    return original.thenApply(
                        committed -> new AbstractMap.SimpleImmutableEntry<>(principal, committed));
                  });
      checked
          .toCompletableFuture()
          .orTimeout(remaining(frame).toNanos(), TimeUnit.NANOSECONDS)
          .whenComplete(
              (value, error) -> {
                if (error != null) failed.set(true);
                ctx.executor()
                    .execute(
                        () -> {
                          verifying = false;
                          try {
                            if (error != null) {
                              failed.set(true);
                              if (value != null) closeOnce.accept(value.getValue());
                              rejectKnownOrClose(ctx, error);
                              return;
                            }
                            if (!ctx.channel().isActive() || !services.currentBoot()) {
                              failed.set(true);
                              closeOnce.accept(value.getValue());
                              ctx.close();
                              return;
                            }
                            var security = services.cachedSecurity(value.getKey(), clock.instant());
                            if (security != AuthorizationStatus.ALLOWED) {
                              failed.set(true);
                              closeOnce.accept(value.getValue());
                              if (security == null) ctx.close();
                              else
                                GatewayRejection.close(
                                    ctx.channel(), GatewayRejection.security(security));
                              return;
                            }
                            if (!registry.bind(connection, value.getValue(), value.getKey())) {
                              failed.set(true);
                              closeOnce.accept(value.getValue());
                              GatewayRejection.close(
                                  ctx.channel(), GatewayRejection.Reason.AUTHORIZATION_REJECTED);
                              return;
                            }
                            route = value.getValue();
                            verifiedToken = token;
                            authTimeout.cancel(false);
                            ctx.fireUserEventTriggered(
                                new GatewayServer.Authenticated(registry.binding(connection)));
                            ctx.writeAndFlush(
                                new TextWebSocketFrame(
                                    "{\"v\":1,\"type\":\"AUTH_OK\",\"sessionIncarnation\":\""
                                        + route.incarnation().value()
                                        + "\",\"connectionGeneration\":\""
                                        + route.connectionGeneration()
                                        + "\"}"));
                          } finally {
                            frame.release().run();
                          }
                        });
              });
    } catch (Exception invalid) {
      failed.set(true);
      verifying = false;
      frame.release().run();
      ctx.close();
    }
  }

  private static final class NativeSecurityRejection extends RuntimeException {
    final AuthorizationStatus status;

    NativeSecurityRejection(AuthorizationStatus status) {
      super("Native authorization rejected");
      this.status = status;
    }
  }

  private static void rejectKnownOrClose(ChannelHandlerContext ctx, Throwable error) {
    for (int depth = 0; depth < 8; depth++) {
      if (error instanceof NativeSecurityRejection known) {
        GatewayRejection.close(ctx.channel(), GatewayRejection.security(known.status));
        return;
      }
      if (error instanceof AuthException) {
        GatewayRejection.close(ctx.channel(), GatewayRejection.Reason.AUTHORIZATION_REJECTED);
        return;
      }
      if ((error instanceof CompletionException || error instanceof ExecutionException)
          && error.getCause() != null
          && error.getCause() != error) error = error.getCause();
      else break;
    }
    ctx.close();
  }

  private void respond(
      ChannelHandlerContext ctx,
      FrameAdmissionHandler.Admitted frame,
      CompletionStage<String> response,
      boolean auth) {
    response
        .toCompletableFuture()
        .orTimeout(remaining(frame).toNanos(), TimeUnit.NANOSECONDS)
        .whenComplete(
            (value, error) ->
                ctx.executor()
                    .execute(
                        () -> {
                          try {
                            if (ctx.channel().isActive()
                                && services.currentBoot()
                                && registry.current(connection, route)) {
                              String output =
                                  error == null ? value : "{\"v\":1,\"type\":\"OUTCOME_UNKNOWN\"}";
                              if (output == null
                                  || output.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                                      > 98304) {
                                ctx.close();
                                return;
                              }
                              ctx.writeAndFlush(new TextWebSocketFrame(output));
                            }
                          } finally {
                            frame.release().run();
                          }
                        }));
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    if (authTimeout != null) authTimeout.cancel(false);
    if (connection != null) registry.remove(connection);
    if (route != null) services.close(route);
    verifiedToken = null;
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {
    ctx.close();
  }
}
