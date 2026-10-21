package io.webrtc.signaling.localplatform;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Measures the host wall clock against the host monotonic clock instead of declaring its quality.
 *
 * <p>LOCAL_TEST_ONLY / NOT_QUALIFIED. The observer only sees the wall-versus-monotonic drift inside
 * this process; it cannot see a drift that moves both clocks together, nor the real time error
 * against any external reference. Every figure is therefore conservative: without a qualifying
 * measurement window (cold start, stale samples, clock regression) the report is {@code
 * continuous=false} with unsafe figures, and it is never optimistic by default.
 *
 * <p>Each {@link #sample()} reads {@code n0 = mono; w = wall; n1 = mono}. The pair uncertainty is
 * {@code ceil((n1 - n0) / 2 in microseconds) + 1}; the offset is {@code w - midpoint(n0, n1)}.
 * Samples are kept for the last five seconds of monotonic time.
 */
public final class HostClockObserver implements AutoCloseable {

  /** Result of the most recent measurement. All figures are unsafe unless {@code continuous}. */
  public record Measurement(
      Instant observedAt, int pairUncertaintyMicros, int relativeRateErrorPpm, boolean continuous) {
    public Measurement {
      Objects.requireNonNull(observedAt, "observedAt");
      if (pairUncertaintyMicros < 0 || relativeRateErrorPpm < 0) {
        throw new IllegalArgumentException("measurement figures must not be negative");
      }
    }
  }

  private static final long NANOS_PER_MICRO = 1_000L;
  private static final long WINDOW_NANOS = Duration.ofSeconds(5).toNanos();
  private static final long MIN_SPAN_NANOS = Duration.ofSeconds(2).toNanos();
  private static final long MAX_AGE_NANOS = Duration.ofMillis(500).toNanos();
  private static final long MAX_JUMP_NANOS = Duration.ofMillis(50).toNanos();
  private static final int UNSAFE_FIGURE = 1_000_000;
  private static final Instant NEVER_OBSERVED = Instant.EPOCH;

  private record Sample(long monoMidNanos, long offsetNanos, int uncertaintyMicros) {}

  /** Immutable result of one sample, with the monotonic time at which it finished. */
  private record Published(Measurement measurement, long newestMonoNanos) {}

  private final LongSupplier monotonicNanos;
  private final Supplier<Instant> wall;
  private final Duration interval;
  private final Object lock = new Object();
  private final ArrayDeque<Sample> window = new ArrayDeque<>();
  private volatile Published latest;

  private ScheduledExecutorService scheduler;
  private boolean closed;

  public HostClockObserver(LongSupplier monotonicNanos, Supplier<Instant> wall, Duration interval) {
    this.monotonicNanos = Objects.requireNonNull(monotonicNanos, "monotonicNanos");
    this.wall = Objects.requireNonNull(wall, "wall");
    this.interval = Objects.requireNonNull(interval, "interval");
    if (interval.isNegative() || interval.isZero()) {
      throw new IllegalArgumentException("interval must be positive");
    }
  }

  /** Takes one bracketed reading. Intended to be called from a single thread at a time. */
  public void sample() {
    long n0 = monotonicNanos.getAsLong();
    Instant w = wall.get();
    long n1 = monotonicNanos.getAsLong();
    synchronized (lock) {
      record(n0, w, n1);
    }
  }

  /** Returns the latest measurement, downgraded to unsafe when it is stale. */
  public Measurement snapshot() {
    Published published = latest;
    if (published == null) {
      return unsafe(NEVER_OBSERVED);
    }
    long age = monotonicNanos.getAsLong() - published.newestMonoNanos();
    if (age < 0 || age > MAX_AGE_NANOS) {
      return unsafe(published.measurement().observedAt());
    }
    return published.measurement();
  }

  /** Starts sampling every {@code interval} on one daemon thread. */
  public synchronized void start() {
    if (closed) {
      throw new IllegalStateException("observer is closed");
    }
    if (scheduler != null) {
      throw new IllegalStateException("observer is already started");
    }
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            task -> {
              Thread thread = new Thread(task, "host-clock-observer");
              thread.setDaemon(true);
              return thread;
            });
    scheduler.scheduleAtFixedRate(
        this::sampleOrDiscard, 0, interval.toNanos(), TimeUnit.NANOSECONDS);
  }

  @Override
  public synchronized void close() {
    closed = true;
    if (scheduler == null) {
      return;
    }
    scheduler.shutdownNow();
    try {
      if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
        throw new IllegalStateException("host clock observer thread did not stop");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while stopping the host clock observer", e);
    }
  }

  /** A failing clock read must not kill the schedule; it invalidates the window instead. */
  private void sampleOrDiscard() {
    try {
      sample();
    } catch (RuntimeException e) {
      synchronized (lock) {
        discardWindow();
      }
    }
  }

  private void record(long n0, Instant w, long n1) {
    long wallNanos;
    try {
      wallNanos =
          Math.addExact(Math.multiplyExact(w.getEpochSecond(), 1_000_000_000L), w.getNano());
    } catch (ArithmeticException e) {
      discardWindow();
      return;
    }
    long gap = n1 - n0;
    if (gap < 0) {
      discardWindow();
      return;
    }
    long mid = n0 + gap / 2;
    Sample previous = window.peekLast();
    if (previous != null && mid - previous.monoMidNanos() < 0) {
      // The monotonic clock moved backwards between samples: nothing in the window is comparable.
      window.clear();
    }
    int uncertainty =
        capToInt(Math.floorDiv(gap + 2 * NANOS_PER_MICRO - 1, 2 * NANOS_PER_MICRO) + 1);
    window.addLast(new Sample(mid, wallNanos - mid, uncertainty));
    while (window.peekFirst().monoMidNanos() - (mid - WINDOW_NANOS) < 0) {
      window.removeFirst();
    }
    latest = new Published(evaluate(w), n1);
  }

  private void discardWindow() {
    window.clear();
    latest = null;
  }

  private Measurement evaluate(Instant observedAt) {
    Sample first = window.peekFirst();
    Sample last = window.peekLast();
    long span = last.monoMidNanos() - first.monoMidNanos();
    if (span < MIN_SPAN_NANOS) {
      return unsafe(observedAt);
    }
    int maxUncertainty = 0;
    boolean jumped = false;
    Sample before = null;
    for (Sample sample : window) {
      maxUncertainty = Math.max(maxUncertainty, sample.uncertaintyMicros());
      if (before != null && absDiff(sample.offsetNanos(), before.offsetNanos()) > MAX_JUMP_NANOS) {
        jumped = true;
      }
      before = sample;
    }
    double drift = (double) absDiff(last.offsetNanos(), first.offsetNanos());
    int ppm = capToInt((long) Math.min(Math.ceil(drift / span * 1e6), (double) Integer.MAX_VALUE));
    return new Measurement(observedAt, maxUncertainty, ppm, !jumped);
  }

  private static Measurement unsafe(Instant observedAt) {
    return new Measurement(observedAt, UNSAFE_FIGURE, UNSAFE_FIGURE, false);
  }

  private static long absDiff(long a, long b) {
    try {
      return Math.abs(Math.subtractExact(a, b));
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  private static int capToInt(long value) {
    return (int) Math.min(value, Integer.MAX_VALUE);
  }
}
