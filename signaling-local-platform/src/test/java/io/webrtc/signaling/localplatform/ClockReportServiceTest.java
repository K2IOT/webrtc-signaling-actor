package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.Signature;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ClockReportServiceTest {

  private static final long MS = 1_000_000L;
  private static final long MONO_ORIGIN = 5_000_000_000_000L;

  /** 2026-01-01T00:00:00Z plus 1234 ns so truncation to microseconds is observable. */
  private static final Instant WALL_ORIGIN = Instant.parse("2026-01-01T00:00:00Z").plusNanos(1234);

  /**
   * Same parsing configuration as the actor-side {@code NativeClockSource}: strict duplicates,
   * bounded strings, no scalar coercion, null primitives and trailing tokens rejected.
   */
  private static final ObjectMapper CONSUMER =
      new ObjectMapper(
              JsonFactory.builder()
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNestingDepth(8)
                          .maxStringLength(4096)
                          .maxNumberLength(20)
                          .build())
                  .build())
          .findAndRegisterModules()
          .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
          .enable(
              DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
              DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  @TempDir Path dir;

  private final AtomicLong mono = new AtomicLong(MONO_ORIGIN);
  private final KeyPair pair = TestKeys.generate("Ed25519");
  private final Map<String, Long> epochs = Map.of("cell-a", 7L, "cell-b", 9L);
  private SourceSigner signer;
  private HostClockObserver observer;
  private Clock wallClock;
  private final UUID pod = UUID.randomUUID();
  private final UUID boot = UUID.randomUUID();
  private final PeerIdentity actorA =
      new PeerIdentity(
          "spiffe://signaling/local-test-only/cell/cell-a/actor/actor-1",
          "cell-a",
          "actor",
          "actor-1");

  @BeforeEach
  void setUp() throws Exception {
    signer = SourceSigner.load(TestKeys.writePkcs8Pem(dir, "key.pem", pair), "local-source");
    wallClock =
        new Clock() {
          @Override
          public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
          }

          @Override
          public Clock withZone(java.time.ZoneId zone) {
            return this;
          }

          @Override
          public Instant instant() {
            return wall();
          }
        };
    observer = new HostClockObserver(mono::get, this::wall, Duration.ofMillis(100));
  }

  private Instant wall() {
    return WALL_ORIGIN.plusNanos(mono.get() - MONO_ORIGIN);
  }

  /** Samples every 100 ms of fake time; 31 samples cover 3 s, so the window qualifies. */
  private void warmUp() {
    for (int i = 0; i < 31; i++) {
      observer.sample();
      mono.addAndGet(100 * MS);
    }
    // The newest sample must be younger than 500 ms for the snapshot to stay usable.
    mono.addAndGet(-100 * MS + 10 * MS);
  }

  private ClockReportService service() {
    return new ClockReportService(signer, observer, epochs, wallClock);
  }

  private BootRegistry.Registration registration() {
    return new BootRegistry.Registration(actorA, pod, boot);
  }

  private record Body(ClockSafetyMonitor.Report report, String signature) {}

  private static Body parse(String body) throws Exception {
    JsonNode value = CONSUMER.readTree(body);
    assertThat(value.isObject()).isTrue();
    assertThat(value.size()).isEqualTo(2);
    assertThat(value.path("signature").isTextual()).isTrue();
    ClockSafetyMonitor.Report report =
        CONSUMER.treeToValue(value.get("report"), ClockSafetyMonitor.Report.class);
    return new Body(report, value.get("signature").asText());
  }

  private static boolean verifies(PublicKey key, ClockSafetyMonitor.Report report, String signed)
      throws Exception {
    Signature verifier = Signature.getInstance("Ed25519");
    verifier.initVerify(key);
    verifier.update(ClockSafetyMonitor.signingBytes(report));
    return verifier.verify(Base64.getUrlDecoder().decode(signed));
  }

  @Test
  void reportVerifiesWithEd25519AndSigningBytes() throws Exception {
    warmUp();

    Body body = parse(service().clockBody(registration()));

    assertThat(body.signature()).hasSize(86).matches("[A-Za-z0-9_-]{86}");
    assertThat(verifies(pair.getPublic(), body.report(), body.signature())).isTrue();
    assertThat(verifies(signer.publicKey(), body.report(), body.signature())).isTrue();
    assertThat(body.report().keyId()).isEqualTo("local-source");
    assertThat(body.report().podUid()).isEqualTo(pod);
    assertThat(body.report().processBoot()).isEqualTo(boot);
    // Tampering with any signed field breaks the signature.
    ClockSafetyMonitor.Report tampered =
        new ClockSafetyMonitor.Report(
            body.report().keyId(),
            body.report().cell(),
            body.report().storageEpoch(),
            body.report().podUid(),
            body.report().processBoot(),
            body.report().sequence(),
            body.report().observedAt(),
            body.report().validUntil(),
            body.report().pairUncertaintyMicros(),
            body.report().relativeRateErrorPpm(),
            !body.report().continuous());
    assertThat(verifies(pair.getPublic(), tampered, body.signature())).isFalse();
  }

  @Test
  void reportCarriesCallerCellAndConfiguredStorageEpoch() throws Exception {
    warmUp();
    ClockReportService service = service();
    PeerIdentity actorB =
        new PeerIdentity(
            "spiffe://signaling/local-test-only/cell/cell-b/actor", "cell-b", "actor", null);

    Body a = parse(service.clockBody(registration()));
    Body b =
        parse(service.clockBody(new BootRegistry.Registration(actorB, UUID.randomUUID(), boot)));

    assertThat(a.report().cell()).isEqualTo("cell-a");
    assertThat(a.report().storageEpoch()).isEqualTo(7L);
    assertThat(b.report().cell()).isEqualTo("cell-b");
    assertThat(b.report().storageEpoch()).isEqualTo(9L);
  }

  @Test
  void cellWithoutConfiguredStorageEpochIsRefused() {
    warmUp();
    PeerIdentity stranger =
        new PeerIdentity(
            "spiffe://signaling/local-test-only/cell/cell-z/actor", "cell-z", "actor", null);

    assertThatThrownBy(
            () -> service().clockBody(new BootRegistry.Registration(stranger, pod, boot)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void observedAtIsTruncatedToMicrosecondsAndValidityIsExactlyTwoSeconds() throws Exception {
    warmUp();
    Instant measured = observer.snapshot().observedAt();
    assertThat(measured.getNano() % 1000).isNotZero();

    ClockSafetyMonitor.Report report = parse(service().clockBody(registration())).report();

    assertThat(report.observedAt())
        .isEqualTo(measured.truncatedTo(java.time.temporal.ChronoUnit.MICROS));
    assertThat(report.observedAt().getNano() % 1000).isZero();
    assertThat(report.validUntil()).isEqualTo(report.observedAt().plusSeconds(2));
  }

  @Test
  void validityNeverExceedsFiveSeconds() throws Exception {
    ClockReportService service = service();
    List<String> bodies = new ArrayList<>();
    bodies.add(service.clockBody(registration()));
    warmUp();
    bodies.add(service.clockBody(registration()));
    for (String raw : bodies) {
      ClockSafetyMonitor.Report report = parse(raw).report();
      assertThat(Duration.between(report.observedAt(), report.validUntil()))
          .isPositive()
          .isLessThanOrEqualTo(Duration.ofSeconds(5));
    }
  }

  @Test
  void sequenceStrictlyIncreasesAcrossCalls() throws Exception {
    warmUp();
    ClockReportService service = service();

    long previous = 0;
    for (int i = 0; i < 200; i++) {
      long sequence = parse(service.clockBody(registration())).report().sequence();
      assertThat(sequence).isGreaterThan(previous);
      previous = sequence;
    }
    // The wall clock did not move between calls, so the per-pod counter alone ordered them.
    assertThat(previous).isGreaterThanOrEqualTo(wall().getEpochSecond() * 1_000_000L);
  }

  @Test
  void sequenceStrictlyIncreasesAcrossThreads() throws Exception {
    warmUp();
    ClockReportService service = service();
    int threads = 8;
    int perThread = 200;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Callable<List<Long>>> tasks = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        tasks.add(
            () -> {
              List<Long> seen = new ArrayList<>();
              for (int i = 0; i < perThread; i++) {
                seen.add(parse(service.clockBody(registration())).report().sequence());
              }
              return seen;
            });
      }
      List<Long> all = new ArrayList<>();
      for (Future<List<Long>> future : pool.invokeAll(tasks)) {
        List<Long> seen = future.get();
        for (int i = 1; i < seen.size(); i++) {
          assertThat(seen.get(i)).isGreaterThan(seen.get(i - 1));
        }
        all.addAll(seen);
      }
      assertThat(all).hasSize(threads * perThread).doesNotHaveDuplicates();
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void sequencesArePerPod() throws Exception {
    warmUp();
    ClockReportService service = service();
    UUID other = UUID.randomUUID();

    long first = parse(service.clockBody(registration())).report().sequence();
    long otherFirst =
        parse(service.clockBody(new BootRegistry.Registration(actorA, other, boot)))
            .report()
            .sequence();
    long second = parse(service.clockBody(registration())).report().sequence();

    assertThat(second).isGreaterThan(first);
    // A different pod starts from the wall clock, not from another pod's counter.
    assertThat(otherFirst).isLessThan(second);
  }

  @Test
  void sequenceStaysAboveEarlierOnesAfterServiceRestart() throws Exception {
    warmUp();
    ClockReportService before = service();
    long last = 0;
    for (int i = 0; i < 5; i++) {
      last = parse(before.clockBody(registration())).report().sequence();
    }
    // A restart loses the in-memory counter; the wall clock has moved on in the meantime, so the
    // wall-derived floor keeps the new sequences above the old ones.
    mono.addAndGet(MS);

    ClockReportService restarted = service();
    long after = parse(restarted.clockBody(registration())).report().sequence();

    assertThat(after).isGreaterThan(last);
    long afterAgain = parse(restarted.clockBody(registration())).report().sequence();
    assertThat(afterAgain).isGreaterThan(after);
  }

  @Test
  void sequenceFollowsTheWallClockWhenItMovesForward() throws Exception {
    warmUp();
    ClockReportService service = service();
    long first = parse(service.clockBody(registration())).report().sequence();

    mono.addAndGet(50 * MS);
    long second = parse(service.clockBody(registration())).report().sequence();

    assertThat(second).isGreaterThanOrEqualTo(first + 50_000L);
  }

  @Test
  void unsafeMeasurementIsSignedAsNotContinuous() throws Exception {
    // Cold observer: never sampled.
    Body body = parse(service().clockBody(registration()));

    assertThat(body.report().continuous()).isFalse();
    assertThat(body.report().pairUncertaintyMicros()).isEqualTo(1_000_000);
    assertThat(body.report().relativeRateErrorPpm()).isEqualTo(1_000_000);
    assertThat(verifies(pair.getPublic(), body.report(), body.signature())).isTrue();
  }

  @Test
  void staleObserverIsSignedAsNotContinuous() throws Exception {
    warmUp();
    mono.addAndGet(2_000 * MS);

    Body body = parse(service().clockBody(registration()));

    assertThat(body.report().continuous()).isFalse();
    assertThat(verifies(pair.getPublic(), body.report(), body.signature())).isTrue();
  }

  @Test
  void figuresComeFromTheMeasurementNotFromConstants() throws Exception {
    warmUp();
    HostClockObserver.Measurement measured = observer.snapshot();

    ClockSafetyMonitor.Report report = parse(service().clockBody(registration())).report();

    assertThat(report.continuous()).isEqualTo(measured.continuous()).isTrue();
    assertThat(report.pairUncertaintyMicros()).isEqualTo(measured.pairUncertaintyMicros());
    assertThat(report.relativeRateErrorPpm()).isEqualTo(measured.relativeRateErrorPpm());
    assertThat(report.pairUncertaintyMicros()).isLessThan(250_000);
  }

  @Test
  void safeMeasurementIsAcceptedByClockSafetyMonitor() throws Exception {
    warmUp();
    String raw = service().clockBody(registration());
    Body body = parse(raw);
    ClockSafetyMonitor monitor =
        new ClockSafetyMonitor(
            "cell-a",
            7L,
            pod,
            boot,
            Map.of(signer.keyId(), signer.publicKey()),
            wallClock,
            mono::get);

    boolean accepted = monitor.observe(body.report(), body.signature(), mono.get());

    assertThat(accepted).isTrue();
    assertThat(monitor.valid()).isTrue();
  }

  @Test
  void unsafeMeasurementIsNotAcceptedAsValidByClockSafetyMonitor() throws Exception {
    Body body = parse(service().clockBody(registration()));
    ClockSafetyMonitor monitor =
        new ClockSafetyMonitor(
            "cell-a",
            7L,
            pod,
            boot,
            Map.of(signer.keyId(), signer.publicKey()),
            wallClock,
            mono::get);

    assertThat(monitor.observe(body.report(), body.signature(), mono.get())).isFalse();
    assertThat(monitor.valid()).isFalse();
  }

  @Test
  void monitorBoundToAnotherPodOrBootRejectsTheReport() throws Exception {
    warmUp();
    Body body = parse(service().clockBody(registration()));
    for (UUID[] ids : new UUID[][] {{UUID.randomUUID(), boot}, {pod, UUID.randomUUID()}}) {
      ClockSafetyMonitor monitor =
          new ClockSafetyMonitor(
              "cell-a",
              7L,
              ids[0],
              ids[1],
              Map.of(signer.keyId(), signer.publicKey()),
              wallClock,
              mono::get);
      assertThat(monitor.observe(body.report(), body.signature(), mono.get())).isFalse();
    }
  }

  @Test
  void restartedServiceReportIsStillAcceptedByAMonitorThatSawTheEarlierOne() throws Exception {
    warmUp();
    ClockSafetyMonitor monitor =
        new ClockSafetyMonitor(
            "cell-a",
            7L,
            pod,
            boot,
            Map.of(signer.keyId(), signer.publicKey()),
            wallClock,
            mono::get);
    Body first = parse(service().clockBody(registration()));
    assertThat(monitor.observe(first.report(), first.signature(), mono.get())).isTrue();

    mono.addAndGet(MS);
    Body second = parse(service().clockBody(registration()));

    assertThat(second.report().sequence()).isGreaterThan(first.report().sequence());
    assertThat(monitor.observe(second.report(), second.signature(), mono.get())).isTrue();
  }

  @Test
  void bodyIsUnder32768Bytes() throws Exception {
    warmUp();

    String body = service().clockBody(registration());

    assertThat(body.getBytes(StandardCharsets.UTF_8).length).isLessThan(32768);
  }

  @Test
  void bodyUsesIsoInstantsAndTheExactTopLevelShape() throws Exception {
    warmUp();

    JsonNode node = new ObjectMapper().readTree(service().clockBody(registration()));

    assertThat(node.fieldNames()).toIterable().containsExactlyInAnyOrder("report", "signature");
    assertThat(node.at("/report/observedAt").isTextual()).isTrue();
    assertThat(node.at("/report/validUntil").isTextual()).isTrue();
    assertThat(node.at("/report/observedAt").asText()).endsWith("Z");
    assertThat(node.at("/report/sequence").isIntegralNumber()).isTrue();
  }

  @Test
  void podTableIsBounded() {
    warmUp();
    ClockReportService service = service();
    for (int i = 0; i < BootRegistry.MAX_PODS; i++) {
      service.clockBody(new BootRegistry.Registration(actorA, new UUID(0, i), boot));
    }

    assertThatThrownBy(
            () -> service.clockBody(new BootRegistry.Registration(actorA, new UUID(1, 0), boot)))
        .isInstanceOf(IllegalStateException.class);
    // A pod that is already known keeps working when the table is full.
    assertThat(service.clockBody(new BootRegistry.Registration(actorA, new UUID(0, 5), boot)))
        .isNotBlank();
  }

  @Test
  void constructorRejectsNulls() {
    assertThatThrownBy(() -> new ClockReportService(null, observer, epochs, wallClock))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new ClockReportService(signer, null, epochs, wallClock))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new ClockReportService(signer, observer, null, wallClock))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new ClockReportService(signer, observer, epochs, null))
        .isInstanceOf(NullPointerException.class);
  }
}
