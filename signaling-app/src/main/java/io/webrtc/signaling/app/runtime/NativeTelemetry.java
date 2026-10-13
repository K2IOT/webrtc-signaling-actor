package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.observability.OperationObservation;
import io.webrtc.signaling.observability.SignalingMetrics;
import io.webrtc.signaling.observability.SignalingMetrics.*;
import io.webrtc.signaling.observability.TraceContext;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Process-owned observer. Native receipts retain correlation after logical expiration.
 */
public final class NativeTelemetry implements OperationObservation {
    private final SignalingMetrics metrics;
    private final Map<Operation, AtomicInteger> pending = new EnumMap<>(Operation.class);
    private final Map<Lag, java.util.function.LongSupplier> lagSources =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final ArrayDeque<RenewalSample> renewals = new ArrayDeque<>();
    private final java.util.Set<io.webrtc.signaling.storage.DbPools> pools =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    public NativeTelemetry(SignalingMetrics metrics) {
        this.metrics = Objects.requireNonNull(metrics);
        for (var operation : Operation.values()) {
            var count = new AtomicInteger();
            pending.put(operation, count);
            metrics.pendingPhysical(operation, count::get);
        }
    }

    private static Operation workerOperation(String name) {
        return switch (name) {
            case "actor_membership" -> Operation.MEMBERSHIP;
            case "clock_source", "clock_attestation" -> Operation.CLOCK_SOURCE;
            case "cell_primary", "cell_health", "cell_primary_health" -> Operation.CELL_HEALTH_SOURCE;
            case "revocation_source", "cached_revocation" -> Operation.REVOCATION;
            case "participant_renewal" -> Operation.RENEW;
            case "recovery_sweep", "call_recovery" -> Operation.WAKE;
            case "outbox_replay", "control_outbox" -> Operation.OUTBOX;
            case "retention" -> Operation.RETENTION;
            case "privacy_deletion" -> Operation.PRIVACY;
            default -> Operation.QUERY;
        };
    }

    @Override
    public Receipt start(Operation operation) {
        return observation(operation, true);
    }

    public Receipt startLogical(Operation operation) {
        return observation(operation, false);
    }

    private Receipt observation(Operation operation, boolean tracksPhysical) {
        Objects.requireNonNull(operation);
        long started = System.nanoTime();
        var parent = MDC.get("traceId");
        var trace =
                parent != null && parent.matches("[a-f0-9]{32}") && !parent.equals("0".repeat(32))
                        ? new TraceContext(parent, TraceContext.root(false).spanId(), false)
                        : TraceContext.root(false);
        if (tracksPhysical) pending.get(operation).incrementAndGet();
        return new Receipt() {
            private final AtomicBoolean logical = new AtomicBoolean(), physical = new AtomicBoolean();

            public String traceId() {
                return trace.traceId();
            }

            public AutoCloseable activate() {
                var previous = MDC.getCopyOfContextMap();
                MDC.put("traceId", trace.traceId());
                MDC.put("spanId", trace.spanId());
                MDC.put("operation", operation.name());
                return () -> {
                    if (previous == null) MDC.clear();
                    else MDC.setContextMap(previous);
                };
            }

            public void logical(Outcome outcome) {
                if (!logical.compareAndSet(false, true)) return;
                metrics.operation(operation, outcome, elapsed());
                try (var ignored = activate()) {
                    MDC.put("outcome", outcome.name());
                    MDC.put("durationNanos", Long.toString(elapsed().toNanos()));
                    LoggerFactory.getLogger("io.webrtc.signaling.telemetry.operation")
                            .info("native-operation");
                } catch (Exception ignored) {
                    /* Observation cannot replace authority. */
                }
            }

            public void physical(boolean finished) {
                if (!tracksPhysical || !physical.compareAndSet(false, true)) return;
                pending.get(operation).decrementAndGet();
                metrics.physical(operation, finished, elapsed());
            }

            public void queue(long nanos) {
                timing("QUEUE", nanos);
                metrics.lag(Lag.QUEUE_AGE, Duration.ofNanos(Math.max(0, nanos)));
            }

            public void poolWait(long nanos) {
                timing("POOL_WAIT", nanos);
                metrics.lag(Lag.DB_POOL_WAIT, Duration.ofNanos(Math.max(0, nanos)));
            }

            public void commit(long nanos) {
                timing("COMMIT", nanos);
            }

            public void relay(RelayOutcome outcome) {
                metrics.relay(outcome, elapsed());
            }

            private void timing(String kind, long nanos) {
                metrics.timing(operation, kind, Duration.ofNanos(Math.max(0, nanos)));
            }

            private Duration elapsed() {
                return Duration.ofNanos(Math.max(0, System.nanoTime() - started));
            }
        };
    }

    @Override
    public void recoveryHydration(HydrationOutcome outcome, Duration elapsed) {
        metrics.recoveryHydration(outcome, elapsed);
    }

    public Receipt workerStarted(String name) {
        return start(workerOperation(name));
    }

    public void lagSource(Lag kind, java.util.function.LongSupplier source) {
        if (lagSources.putIfAbsent(Objects.requireNonNull(kind), Objects.requireNonNull(source))
                != null) throw new IllegalStateException("Native lag owner already installed: " + kind);
    }

    @Override
    public synchronized void renewalObservation(
            java.time.Instant checkedAt, java.time.Instant leaseUntil, Duration phaseTtl) {
        long remaining = Duration.between(checkedAt, leaseUntil).toNanos();
        long sourceAge = Duration.between(checkedAt, java.time.Instant.now()).toNanos();
        if (phaseTtl == null
                || phaseTtl.isNegative()
                || phaseTtl.isZero()
                || phaseTtl.compareTo(Duration.ofSeconds(60)) > 0
                || remaining <= 0
                || remaining > phaseTtl.toNanos()
                || sourceAge < 0
                || sourceAge > Duration.ofSeconds(5).toNanos()) return;
        metrics.participantLease(
                Duration.ofNanos(sourceAge), Duration.ofNanos(Math.max(0, remaining - sourceAge)));
        while (renewals.size() >= 64) renewals.removeFirst();
        renewals.addLast(
                new RenewalSample(phaseTtl.toNanos() - remaining + sourceAge, System.nanoTime()));
    }

    private synchronized void sampleRenewals() {
        long now = System.nanoTime(), maximum = -1;
        while (!renewals.isEmpty()
                && now - renewals.getFirst().observed() > Duration.ofSeconds(5).toNanos())
            renewals.removeFirst();
        for (var sample : renewals)
            maximum = Math.max(maximum, sample.age() + Math.max(0, now - sample.observed()));
        if (maximum < 0) metrics.lagUnknown(Lag.USER_RENEWAL);
        else metrics.lag(Lag.USER_RENEWAL, Duration.ofNanos(maximum));
    }

    public void pools(io.webrtc.signaling.storage.DbPools owner) {
        pools.add(owner);
    }

    public String scrape() {
        lagSources.forEach(
                (kind, source) -> {
                    try {
                        long nanos = source.getAsLong();
                        if (nanos < 0) metrics.lagUnknown(kind);
                        else metrics.lag(kind, Duration.ofNanos(nanos));
                    } catch (RuntimeException unavailable) {
                        metrics.lagUnknown(kind);
                    }
                });
        sampleRenewals();
        for (var kind : Pool.values()) {
            int active = 0, pending = 0;
            boolean observed = !pools.isEmpty();
            for (var owner : pools) {
                var value = owner.observation(kind == Pool.SAFETY);
                if (value == null) {
                    observed = false;
                    break;
                }
                active += value.active();
                pending += value.pending();
            }
            if (observed) metrics.pool(kind, active, pending);
            else metrics.poolUnknown(kind);
        }
        return metrics.scrape();
    }

    public SignalingMetrics metrics() {
        return metrics;
    }

    private record RenewalSample(long age, long observed) {
    }
}
