package io.webrtc.signaling.observability;

import io.micrometer.core.instrument.*;
import io.micrometer.prometheusmetrics.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * All dimensions are enums or one immutable deployment identity; no per-user/call/request labels.
 */
public final class SignalingMetrics implements AutoCloseable {
  public enum Plane {
    GATEWAY,
    ACTOR,
    CONTROL,
    LOADGEN
  }

  public enum Operation {
    AUTH,
    AUTH_REFRESH,
    REGISTER_SESSION,
    SESSION_CLOSE,
    INVITE,
    ACCEPT,
    REJECT,
    DECLINE_ALL,
    CANCEL,
    HANGUP,
    SYNC_CALL,
    GET_COMMAND_RESULT,
    RESUME,
    NEGOTIATE_REQUEST,
    MEDIA_CONNECTED,
    MEDIA_DISCONNECTED,
    ICE_RESTARTING,
    MEDIA_FAILED,
    MEDIA_RECOVERED,
    EVENT_RECEIVED,
    RESERVE,
    CLAIM,
    CONFIRM,
    RENEW,
    RELEASE,
    QUERY,
    WAKE,
    ROOT_PULSE,
    OUTBOX,
    RETENTION,
    REVOCATION,
    PRIVACY
  }

  public enum Outcome {
    COMMITTED,
    READ,
    USER_BUSY,
    CALL_ENDED,
    TOKEN_EXPIRED,
    FORBIDDEN,
    STALE_VERSION,
    RETRYABLE_CONFLICT,
    BACKPRESSURE,
    OUTCOME_UNKNOWN,
    UNAVAILABLE,
    DEADLINE_EXCEEDED,
    INTERNAL_ERROR;

    public boolean serviceFailure() {
      return ordinal() >= RETRYABLE_CONFLICT.ordinal();
    }
  }

  public enum RecoverySli {
    WORKFLOW_CONVERGENCE,
    ACTIVE_CALL_PRESERVATION,
    SIGNALING_REATTACHMENT,
    MEDIA_CONTINUITY
  }

  public enum RelayOutcome {
    WRITE_COMPLETED,
    RESYNC_REQUIRED,
    UNAUTHORIZED,
    BACKPRESSURE,
    OUTCOME_UNKNOWN
  }

  public enum Lag {
    QUEUE_AGE,
    DB_POOL_WAIT,
    WAL_COMMIT,
    ROOT_RENEWAL,
    USER_RENEWAL,
    REVOCATION,
    RECOVERY_SWEEP,
    OLDEST_UNRECONCILED,
    WORKFLOW_CONVERGENCE
  }

  public enum Pool {
    SAFETY,
    CONTROL
  }

  private record Measurement(io.micrometer.core.instrument.Timer timer, Counter count) {}

  private record Recovery(Counter eligible, Counter successful) {}

  private final PrometheusMeterRegistry registry =
      new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
  private final Map<Operation, Map<Outcome, Measurement>> measurements =
      new EnumMap<>(Operation.class);
  private final Map<RecoverySli, Recovery> recovery = new EnumMap<>(RecoverySli.class);
  private final Map<Lag, AtomicLong> lags = new EnumMap<>(Lag.class);
  private final Map<Pool, AtomicInteger[]> pools = new EnumMap<>(Pool.class);
  private final Map<RelayOutcome, io.micrometer.core.instrument.Timer> relay =
      new EnumMap<>(RelayOutcome.class);
  private final AtomicLong hardBound = new AtomicLong(-1);
  private final LongAdder business = new LongAdder(), failures = new LongAdder();

  public SignalingMetrics(String cell, Plane plane, String configurationFingerprint) {
    if (cell == null
        || !cell.matches("c[0-9]{3}")
        || plane == null
        || configurationFingerprint == null
        || !configurationFingerprint.matches("[a-f0-9]{64}"))
      throw new IllegalArgumentException("Invalid immutable metric identity");
    registry.config().commonTags("cell", cell, "plane", plane.name());
    Gauge.builder("signaling.build.info", () -> 1)
        .tag("configuration_fingerprint", configurationFingerprint)
        .register(registry);
    for (var operation : Operation.values()) {
      var outcomes = new EnumMap<Outcome, Measurement>(Outcome.class);
      for (var outcome : Outcome.values()) {
        String classification = outcome.serviceFailure() ? "SERVICE_FAILURE" : "BUSINESS";
        var timer =
            io.micrometer.core.instrument.Timer.builder("signaling.operation.latency")
                .tags(
                    "operation",
                    operation.name(),
                    "result_class",
                    classification,
                    "outcome",
                    outcome.name())
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofNanos(1000))
                .maximumExpectedValue(Duration.ofSeconds(2))
                .serviceLevelObjectives(
                    Duration.ofMillis(1),
                    Duration.ofMillis(20),
                    Duration.ofMillis(100),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(2))
                .register(registry);
        var count =
            Counter.builder("signaling.operation")
                .tags(
                    "operation",
                    operation.name(),
                    "result_class",
                    classification,
                    "outcome",
                    outcome.name())
                .register(registry);
        outcomes.put(outcome, new Measurement(timer, count));
      }
      measurements.put(operation, outcomes);
    }
    for (var outcome : RelayOutcome.values())
      relay.put(
          outcome,
          io.micrometer.core.instrument.Timer.builder("signaling.relay.latency")
              .tag("outcome", outcome.name())
              .publishPercentileHistogram()
              .minimumExpectedValue(Duration.ofNanos(1000))
              .maximumExpectedValue(Duration.ofSeconds(2))
              .serviceLevelObjectives(
                  Duration.ofMillis(20), Duration.ofMillis(50), Duration.ofMillis(150))
              .register(registry));
    Gauge.builder(
            "signaling.security.hard.bound.seconds",
            hardBound,
            n -> n.longValue() < 0 ? Double.NaN : n.doubleValue() / 1e9)
        .register(registry);
    for (var sli : RecoverySli.values()) {
      var eligible =
          Counter.builder("signaling.recovery.eligible").tag("sli", sli.name()).register(registry);
      var success =
          Counter.builder("signaling.recovery.successful")
              .tag("sli", sli.name())
              .register(registry);
      recovery.put(sli, new Recovery(eligible, success));
      Gauge.builder("signaling.recovery.ratio", () -> ratio(sli))
          .tag("sli", sli.name())
          .register(registry);
    }
    for (var kind : Lag.values()) {
      var value = new AtomicLong();
      lags.put(kind, value);
      Gauge.builder("signaling.progress.lag.seconds", value, n -> n.doubleValue() / 1e9)
          .tag("kind", kind.name())
          .register(registry);
    }
    for (var pool : Pool.values()) {
      var values = new AtomicInteger[] {new AtomicInteger(), new AtomicInteger()};
      pools.put(pool, values);
      Gauge.builder("signaling.db.pool.active", values[0], AtomicInteger::doubleValue)
          .tag("pool", pool.name())
          .register(registry);
      Gauge.builder("signaling.db.pool.pending", values[1], AtomicInteger::doubleValue)
          .tag("pool", pool.name())
          .register(registry);
    }
  }

  public void operation(Operation operation, Outcome outcome, Duration elapsed) {
    Objects.requireNonNull(elapsed);
    if (elapsed.isNegative()) throw new IllegalArgumentException("Negative duration");
    var measure =
        measurements.get(Objects.requireNonNull(operation)).get(Objects.requireNonNull(outcome));
    measure.count().increment();
    measure.timer().record(elapsed);
    if (outcome.serviceFailure()) failures.increment();
    else business.increment();
  }

  public void relay(RelayOutcome outcome, Duration elapsed) {
    if (elapsed == null || elapsed.isNegative())
      throw new IllegalArgumentException("Invalid relay duration");
    relay.get(Objects.requireNonNull(outcome)).record(elapsed);
  }

  public void securityHardBound(Duration bound) {
    if (bound == null
        || bound.isNegative()
        || bound.isZero()
        || bound.compareTo(Duration.ofSeconds(5)) > 0)
      throw new IllegalArgumentException("Invalid configured security bound");
    hardBound.set(bound.toNanos());
  }

  public void recovery(RecoverySli sli, boolean eligible, boolean successful) {
    var measure = recovery.get(Objects.requireNonNull(sli));
    if (!eligible) return;
    measure.eligible().increment();
    if (successful) measure.successful().increment();
  }

  public double ratio(RecoverySli sli) {
    var measure = recovery.get(Objects.requireNonNull(sli));
    return measure.eligible().count() == 0
        ? Double.NaN
        : measure.successful().count() / measure.eligible().count();
  }

  public void lag(Lag kind, Duration value) {
    if (value == null || value.isNegative()) throw new IllegalArgumentException("Invalid lag");
    lags.get(Objects.requireNonNull(kind)).set(value.toNanos());
  }

  public void pool(Pool pool, int active, int pending) {
    if (active < 0 || pending < 0 || active > 65536 || pending > 65536)
      throw new IllegalArgumentException("Invalid pool count");
    var values = pools.get(Objects.requireNonNull(pool));
    values[0].set(active);
    values[1].set(pending);
  }

  public long businessOutcomes() {
    return business.sum();
  }

  public long serviceFailures() {
    return failures.sum();
  }

  public String scrape() {
    return registry.scrape();
  }

  public MeterRegistry registry() {
    return registry;
  }

  @Override
  public void close() {
    registry.close();
  }
}
