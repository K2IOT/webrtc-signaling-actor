package io.webrtc.signaling.gateway;

import io.netty.util.AttributeKey;
import io.webrtc.signaling.auth.AuthorizationStatus;
import java.time.*;
import java.util.Objects;
import java.util.concurrent.*;

/** One process task reads cached safety only; healthy sockets add no event-loop tasks. */
public final class GatewaySecuritySweep implements AutoCloseable {
  public record Settings(Duration interval) {
    public Settings {
      if (interval == null
          || interval.compareTo(Duration.ofMillis(25)) < 0
          || interval.compareTo(Duration.ofMillis(250)) > 0)
        throw new IllegalArgumentException("Gateway cached safety sweep outside bound");
    }

    public static Settings candidate() {
      return new Settings(Duration.ofMillis(100));
    }
  }

  private static final AttributeKey<Boolean> QUEUED =
      AttributeKey.valueOf("signaling.security-close-queued");
  private final ConnectionRegistry registry;
  private final GatewayServices services;
  private final Clock clock;
  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().daemon().name("gateway-cached-safety").factory());

  public GatewaySecuritySweep(
      ConnectionRegistry registry, GatewayServices services, Clock clock, Settings settings) {
    this.registry = Objects.requireNonNull(registry);
    this.services = Objects.requireNonNull(services);
    this.clock = Objects.requireNonNull(clock);
    long interval = Objects.requireNonNull(settings).interval().toNanos();
    scheduler.scheduleWithFixedDelay(this::scan, interval, interval, TimeUnit.NANOSECONDS);
  }

  private GatewayRejection.Reason rejection(ConnectionRegistry.Binding binding) {
    try {
      if (!services.currentBoot()) return GatewayRejection.Reason.AUTH_FRESHNESS_UNKNOWN;
      if (!registry.current(binding.route().connectionId(), binding.route()))
        return GatewayRejection.Reason.STALE_CONNECTION;
      var now = clock.instant();
      if (!binding.principal().expiresAt().isAfter(now))
        return GatewayRejection.Reason.AUTH_TOKEN_EXPIRED;
      var status = services.cachedSecurity(binding.principal(), now);
      return status == AuthorizationStatus.ALLOWED
          ? null
          : status == null
              ? GatewayRejection.Reason.AUTH_FRESHNESS_UNKNOWN
              : GatewayRejection.security(status);
    } catch (RuntimeException unknown) {
      return GatewayRejection.Reason.AUTH_FRESHNESS_UNKNOWN;
    }
  }

  private void scan() {
    for (var binding : registry.snapshot()) {
      var channel = binding.channel();
      if (!channel.isActive()
          || GatewayRejection.rejecting(channel)
          || rejection(binding) == null
          || Boolean.TRUE.equals(channel.attr(QUEUED).getAndSet(Boolean.TRUE))) continue;
      try {
        channel
            .eventLoop()
            .execute(
                () -> {
                  try {
                    // AUTH_REFRESH or a replaced binding cannot inherit a closure from the old
                    // snapshot.
                    if (registry.binding(binding.route().connectionId()) != binding) return;
                    var reason = rejection(binding);
                    if (reason != null) GatewayRejection.close(channel, reason);
                  } finally {
                    channel.attr(QUEUED).set(Boolean.FALSE);
                  }
                });
      } catch (RejectedExecutionException stopped) {
        channel.attr(QUEUED).set(Boolean.FALSE);
        channel.close();
      }
    }
  }

  @Override
  public void close() {
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(2, TimeUnit.SECONDS))
        throw new IllegalStateException("Gateway safety sweep cleanup unproven");
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Gateway safety sweep cleanup interrupted", interrupted);
    }
  }
}
