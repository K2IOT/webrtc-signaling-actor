package io.webrtc.signaling.localplatform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Builds the signed clock report the actor-side {@code NativeClockSource} polls.
 *
 * <p>The report is bound to the enrolled caller: cell from the verified certificate identity, pod
 * and boot from the {@link BootRegistry.Registration}, storage epoch from configuration. The
 * figures come from {@link HostClockObserver#snapshot()}; an unusable snapshot is signed as {@code
 * continuous=false}, never rewritten into a safe-looking report.
 *
 * <p>Reported figures are this process's own measurement plus one fixed, stated allowance, not a
 * claim about the host's clock discipline. The consumer ({@code ClockSafetyMonitor.valid()}) uses
 * {@code pairUncertaintyMicros} to bound its own wall-versus-elapsed pair as well, because the
 * source's figure is the only bound it gets. The actor, gateway and control read their wall and
 * monotonic clocks at separate instants, across JVM, GC and scheduler jitter and an HTTP hop; this
 * source cannot measure that, so it states it: {@code pairUncertaintyMicros = min(MAX_INT, measured
 * + }{@value #CONSUMER_OBSERVATION_ALLOWANCE_MICROS}{@code )}. {@code relativeRateErrorPpm} is
 * reported as measured. {@code continuous} is true only when the measurement is continuous and the
 * reported figures stay within the consumer's limits ({@value #MAX_PAIR_UNCERTAINTY_MICROS}
 * microseconds, {@value #MAX_RATE_ERROR_PPM} ppm), so a measured uncertainty above 150000
 * microseconds is reported unsafe. Unsafe snapshots keep figures of at least 1_000_000. Validity is
 * exactly {@value #VALIDITY_SECONDS} seconds from the (microsecond-truncated) observation time,
 * within the five second ceiling the consumer enforces.
 *
 * <p>The sequence of each pod is {@code max(last + 1, epochMicros(now))}: strictly increasing
 * within an instance, and, because of the wall-clock floor, above earlier sequences after a restart
 * as long as the wall clock has advanced by more than the counter ran ahead of it. The pod table is
 * bounded at {@link BootRegistry#MAX_PODS}; a new pod beyond the bound is refused rather than an
 * old counter evicted.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
public final class ClockReportService {

  static final long VALIDITY_SECONDS = 2;

  /**
   * LOCAL_TEST_ONLY allowance, in microseconds, for the consumer's own observation of its wall and
   * monotonic clocks (separate reads across JVM, GC and scheduler jitter and an HTTP hop), which
   * this source cannot measure and therefore states. Added to the measured pair uncertainty.
   */
  static final int CONSUMER_OBSERVATION_ALLOWANCE_MICROS = 100_000;

  /** The consumer's limits; a report above either is never continuous. */
  static final int MAX_PAIR_UNCERTAINTY_MICROS = 250_000;

  static final int MAX_RATE_ERROR_PPM = 1_000;

  private static final ObjectMapper JSON =
      new ObjectMapper()
          .findAndRegisterModules()
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

  private final SourceSigner signer;
  private final HostClockObserver observer;
  private final Map<String, Long> storageEpochs;
  private final Clock wallClock;
  private final Object lock = new Object();
  private final Map<UUID, Long> lastSequence = new HashMap<>();

  public ClockReportService(
      SourceSigner signer,
      HostClockObserver observer,
      Map<String, Long> storageEpochs,
      Clock wallClock) {
    this.signer = Objects.requireNonNull(signer, "signer");
    this.observer = Objects.requireNonNull(observer, "observer");
    this.storageEpochs = Map.copyOf(Objects.requireNonNull(storageEpochs, "storageEpochs"));
    this.wallClock = Objects.requireNonNull(wallClock, "wallClock");
  }

  /**
   * Returns {@code {"report": {...}, "signature": "<86 chars>"}} for the registered caller.
   *
   * @throws IllegalArgumentException if the caller's cell has no configured storage epoch
   * @throws IllegalStateException if a new pod would exceed the pod bound (over HTTP the same
   *     condition surfaces as 403 from the {@link BootRegistry} bound; this one is a second line of
   *     defence, reachable only when the service is called directly)
   */
  public String clockBody(BootRegistry.Registration registration) {
    Objects.requireNonNull(registration, "registration");
    String cell = registration.peer().cell();
    Long storageEpoch = cell == null ? null : storageEpochs.get(cell);
    if (storageEpoch == null) {
      throw new IllegalArgumentException();
    }
    HostClockObserver.Measurement measurement = observer.snapshot();
    Instant observedAt = measurement.observedAt().truncatedTo(ChronoUnit.MICROS);
    int pairUncertainty = reportedPairUncertainty(measurement.pairUncertaintyMicros());
    int rateError = measurement.relativeRateErrorPpm();
    boolean continuous =
        measurement.continuous()
            && pairUncertainty <= MAX_PAIR_UNCERTAINTY_MICROS
            && rateError <= MAX_RATE_ERROR_PPM;
    ClockSafetyMonitor.Report report =
        new ClockSafetyMonitor.Report(
            signer.keyId(),
            cell,
            storageEpoch,
            registration.pod(),
            registration.boot(),
            nextSequence(registration.pod()),
            observedAt,
            observedAt.plusSeconds(VALIDITY_SECONDS),
            pairUncertainty,
            rateError,
            continuous);
    byte[] signature = signer.sign(ClockSafetyMonitor.signingBytes(report));
    try {
      ObjectNode payload = JSON.createObjectNode();
      payload.set("report", JSON.valueToTree(report));
      payload.put("signature", Base64.getUrlEncoder().withoutPadding().encodeToString(signature));
      return JSON.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("clock report could not be serialised", null);
    }
  }

  /** The measured pair uncertainty plus the stated allowance, clamped to {@code int}. */
  static int reportedPairUncertainty(int measuredMicros) {
    return (int)
        Math.min(Integer.MAX_VALUE, (long) measuredMicros + CONSUMER_OBSERVATION_ALLOWANCE_MICROS);
  }

  private long nextSequence(UUID pod) {
    Instant now = wallClock.instant();
    long floor =
        Math.addExact(Math.multiplyExact(now.getEpochSecond(), 1_000_000L), now.getNano() / 1_000L);
    synchronized (lock) {
      Long last = lastSequence.get(pod);
      if (last == null && lastSequence.size() >= BootRegistry.MAX_PODS) {
        throw new IllegalStateException("too many pods");
      }
      long next = last == null ? floor : Math.max(Math.addExact(last, 1), floor);
      lastSequence.put(pod, next);
      return next;
    }
  }
}
