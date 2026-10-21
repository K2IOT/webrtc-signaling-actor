package io.webrtc.signaling.localplatform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class HostClockObserverTest {

  private static final long MS = 1_000_000L;
  private static final int UNSAFE = 1_000_000;

  private final FakeClocks clocks = new FakeClocks();
  private final HostClockObserver observer =
      new HostClockObserver(clocks::mono, clocks::wall, Duration.ofMillis(100));

  /** Takes {@code count} samples, advancing the fake clocks by 100 ms after each one. */
  private void ticks(int count) {
    for (int i = 0; i < count; i++) {
      observer.sample();
      clocks.advance(100 * MS);
    }
  }

  @Test
  void neverSampledIsNotContinuousAndUnsafe() {
    HostClockObserver.Measurement m = observer.snapshot();

    assertThat(m.continuous()).isFalse();
    assertThat(m.pairUncertaintyMicros()).isEqualTo(UNSAFE);
    assertThat(m.relativeRateErrorPpm()).isEqualTo(UNSAFE);
  }

  @Test
  void coldStartIsNotContinuous() {
    ticks(10);
    assertThat(observer.snapshot().continuous()).isFalse();

    // 19 samples span 1.8 s, still below the 2 s requirement.
    ticks(9);
    HostClockObserver.Measurement m = observer.snapshot();
    assertThat(m.continuous()).isFalse();
    assertThat(m.pairUncertaintyMicros()).isEqualTo(UNSAFE);
    assertThat(m.relativeRateErrorPpm()).isEqualTo(UNSAFE);
  }

  @Test
  void steadyClockIsContinuousWithinLimits() {
    ticks(31);

    HostClockObserver.Measurement m = observer.snapshot();
    assertThat(m.continuous()).isTrue();
    assertThat(m.pairUncertaintyMicros()).isLessThanOrEqualTo(250_000);
    assertThat(m.relativeRateErrorPpm()).isLessThanOrEqualTo(1000);
    assertThat(m.observedAt()).isEqualTo(clocks.wallAt(clocks.mono() - 100 * MS));
  }

  @Test
  void wallStepBackwardMakesReportUnsafe() {
    assertStepIsUnsafeForTheWholeWindowThenRecovers(-200 * MS);
  }

  @Test
  void wallStepForwardIsUnsafe() {
    assertStepIsUnsafeForTheWholeWindowThenRecovers(200 * MS);
  }

  private void assertStepIsUnsafeForTheWholeWindowThenRecovers(long stepNanos) {
    ticks(31);
    assertThat(observer.snapshot().continuous()).isTrue();

    clocks.stepWall(stepNanos);

    // The sample that saw the step plus the following ones up to step + 4.9 s keep the
    // pre-step sample inside the 5 s window.
    for (int i = 0; i < 50; i++) {
      ticks(1);
      HostClockObserver.Measurement m = observer.snapshot();
      assertThat(m.continuous()).as("sample %d after the step", i).isFalse();
      assertThat(m.relativeRateErrorPpm()).as("sample %d after the step", i).isGreaterThan(1000);
    }

    // At step + 5.0 s the pre-step sample leaves the window and the report recovers.
    ticks(1);
    HostClockObserver.Measurement recovered = observer.snapshot();
    assertThat(recovered.continuous()).isTrue();
    assertThat(recovered.relativeRateErrorPpm()).isLessThanOrEqualTo(1000);
  }

  @Test
  void stepBelowJumpThresholdIsStillReportedAsRateError() {
    ticks(31);
    clocks.stepWall(40 * MS);
    ticks(1);

    HostClockObserver.Measurement m = observer.snapshot();
    assertThat(m.relativeRateErrorPpm()).isGreaterThan(1000);
  }

  @Test
  void slewAbove1000PpmIsReportedAsRateError() {
    clocks.setWallSlewPpm(2000);
    ticks(31);

    HostClockObserver.Measurement m = observer.snapshot();
    assertThat(m.relativeRateErrorPpm()).isGreaterThan(1000).isLessThanOrEqualTo(2001);
  }

  @Test
  void slewAt500PpmStaysWithinLimit() {
    clocks.setWallSlewPpm(500);
    ticks(31);

    HostClockObserver.Measurement m = observer.snapshot();
    assertThat(m.continuous()).isTrue();
    assertThat(m.relativeRateErrorPpm()).isBetween(1, 1000);
  }

  @Test
  void stallRaisesPairUncertainty() {
    ticks(31);
    clocks.stallNextWallRead(600 * MS);
    ticks(1);

    assertThat(observer.snapshot().pairUncertaintyMicros()).isEqualTo(300_001);

    // The stalled sample is centred 0.3 s into the stall; 4.6 s later it is still in the window.
    ticks(40);
    assertThat(observer.snapshot().pairUncertaintyMicros()).isGreaterThan(250_000);

    // Once the stalled sample leaves the 5 s window the figure drops back.
    ticks(20);
    assertThat(observer.snapshot().pairUncertaintyMicros()).isLessThanOrEqualTo(250_000);
  }

  @Test
  void staleSnapshotIsNotContinuous() {
    ticks(31);
    assertThat(observer.snapshot().continuous()).isTrue();

    // The newest sample is exactly 500 ms old: still acceptable.
    clocks.advance(400 * MS);
    assertThat(observer.snapshot().continuous()).isTrue();

    // 600 ms without a sample: stale, and no safe figures are claimed either.
    clocks.advance(100 * MS);
    HostClockObserver.Measurement m = observer.snapshot();
    assertThat(m.continuous()).isFalse();
    assertThat(m.pairUncertaintyMicros()).isEqualTo(UNSAFE);
    assertThat(m.relativeRateErrorPpm()).isEqualTo(UNSAFE);
  }

  @Test
  void monotonicRegressionDiscardsTheWindow() {
    ticks(31);
    clocks.advance(-5_000 * MS);
    observer.sample();

    HostClockObserver.Measurement m = observer.snapshot();
    assertThat(m.continuous()).isFalse();
    assertThat(m.pairUncertaintyMicros()).isEqualTo(UNSAFE);
  }

  @Test
  void unorderedReadsInOneSampleDiscardTheWindow() {
    ticks(31);
    clocks.rewindOnWallRead(10 * MS);
    observer.sample();

    assertThat(observer.snapshot().continuous()).isFalse();
  }

  @Test
  void constructorRejectsInvalidArguments() {
    assertThatThrownBy(() -> new HostClockObserver(null, Instant::now, Duration.ofMillis(100)))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new HostClockObserver(System::nanoTime, null, Duration.ofMillis(100)))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new HostClockObserver(System::nanoTime, Instant::now, Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void startRunsSamplesOnADaemonThreadAndCloseStopsThem() throws InterruptedException {
    CountDownLatch threeSamples = new CountDownLatch(3);
    AtomicInteger samples = new AtomicInteger();
    AtomicBoolean daemon = new AtomicBoolean(false);
    HostClockObserver started =
        new HostClockObserver(
            System::nanoTime,
            () -> {
              daemon.set(Thread.currentThread().isDaemon());
              samples.incrementAndGet();
              threeSamples.countDown();
              return Instant.now();
            },
            Duration.ofMillis(5));

    started.start();
    try {
      assertThat(threeSamples.await(5, TimeUnit.SECONDS)).isTrue();
    } finally {
      started.close();
    }

    assertThat(daemon).isTrue();
    int afterClose = samples.get();
    // Bounded pause of 10 sampling intervals: nothing may run once close() has returned.
    assertThat(new CountDownLatch(1).await(50, TimeUnit.MILLISECONDS)).isFalse();
    assertThat(samples.get()).isEqualTo(afterClose);
  }

  @Test
  void startTwiceIsRejected() {
    HostClockObserver started =
        new HostClockObserver(System::nanoTime, Instant::now, Duration.ofSeconds(1));
    started.start();
    try {
      assertThatThrownBy(started::start).isInstanceOf(IllegalStateException.class);
    } finally {
      started.close();
    }
  }

  /** Scripted monotonic and wall clocks; the wall clock is a function of the monotonic one. */
  private static final class FakeClocks {
    private static final long WALL_ORIGIN_NANOS = 1_767_225_600L * 1_000_000_000L;
    private static final long MONO_ORIGIN = 5_000_000_000_000L;

    private long mono = MONO_ORIGIN;
    private long wallStepNanos;
    private double slewPpm;
    private long stallNextWallRead;
    private long rewindOnWallRead;
    private long pendingAdvance;

    long mono() {
      mono += pendingAdvance;
      pendingAdvance = 0;
      return mono;
    }

    Instant wall() {
      if (rewindOnWallRead != 0) {
        mono -= rewindOnWallRead;
        rewindOnWallRead = 0;
      }
      if (stallNextWallRead != 0) {
        // The stall straddles the wall read: half before it, half after it.
        mono += stallNextWallRead / 2;
        pendingAdvance = stallNextWallRead / 2;
        stallNextWallRead = 0;
      }
      return wallAt(mono);
    }

    Instant wallAt(long monoNanos) {
      long elapsed = monoNanos - MONO_ORIGIN;
      long nanos = WALL_ORIGIN_NANOS + wallStepNanos + Math.round(elapsed * (1 + slewPpm / 1e6));
      return Instant.ofEpochSecond(0, nanos);
    }

    void advance(long nanos) {
      mono += nanos;
    }

    void stepWall(long nanos) {
      wallStepNanos += nanos;
    }

    void setWallSlewPpm(double ppm) {
      slewPpm = ppm;
    }

    void stallNextWallRead(long nanos) {
      stallNextWallRead = nanos;
    }

    void rewindOnWallRead(long nanos) {
      rewindOnWallRead = nanos;
    }
  }
}
