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
 * and boot from the {@link BootRegistry.Registration}, storage epoch from configuration. The three
 * figures and {@code continuous} come from {@link HostClockObserver#snapshot()}; an unusable
 * snapshot is signed as {@code continuous=false}, never rewritten into a safe-looking report.
 * Validity is exactly {@value #VALIDITY_SECONDS} seconds from the (microsecond-truncated)
 * observation time, within the five second ceiling the consumer enforces.
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
   * @throws IllegalStateException if a new pod would exceed the pod bound
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
            measurement.pairUncertaintyMicros(),
            measurement.relativeRateErrorPpm(),
            measurement.continuous());
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
