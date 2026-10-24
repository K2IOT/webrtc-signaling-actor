package io.webrtc.signaling.gateway;

import io.netty.util.AttributeKey;
import io.webrtc.signaling.auth.AuthorizationStatus;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * One process task reads cached safety only; healthy sockets add no event-loop tasks.
 */
public final class GatewaySecuritySweep implements AutoCloseable {
    private static final AttributeKey<Boolean> QUEUED =
            AttributeKey.valueOf("signaling.security-close-queued");
    private static final int SLICE = 2048;
    private final long periodNanos;
    private final ConnectionRegistry registry;
    private final GatewayServices services;
    private final Clock clock;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                    Thread.ofPlatform().daemon().name("gateway-cached-safety").factory());
    private int cursor;
    private long plannedNanos;

    public GatewaySecuritySweep(
            ConnectionRegistry registry, GatewayServices services, Clock clock, Settings settings) {
        this.registry = Objects.requireNonNull(registry);
        this.services = Objects.requireNonNull(services);
        this.clock = Objects.requireNonNull(clock);
        long interval = Objects.requireNonNull(settings).interval().toNanos();
        // Cover every fixed slot once per configured interval, including empty/removed slots.
        // The fixed cursor cannot be held back by refresh/churn at the front of a map.
        long slices = (registry.scanCapacity() + SLICE - 1L) / SLICE;
        periodNanos = Math.max(1, interval / slices);
        plannedNanos = System.nanoTime();
        // Progress records the last planned tick, so a sweep that keeps time already reads one period
        // old just before its next tick. The window allows one interval of lateness beyond that.
        registry.beginSafetySweep(interval + periodNanos, plannedNanos);
        scheduler.scheduleAtFixedRate(this::scan, periodNanos, periodNanos, TimeUnit.NANOSECONDS);
    }

    private GatewayRejection.Reason rejection(ConnectionRegistry.Binding binding) {
        try {
            if (!services.currentBoot() || !registry.safetyFresh())
                return GatewayRejection.Reason.AUTH_FRESHNESS_UNKNOWN;
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
        plannedNanos += periodNanos;
        registry.safetyProgress(plannedNanos);
        int end = Math.min(registry.scanCapacity(), cursor + SLICE);
        while (cursor < end) {
            var binding = registry.scanBinding(cursor++);
            if (binding == null) continue;
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
        if (cursor == registry.scanCapacity()) cursor = 0;
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
}
