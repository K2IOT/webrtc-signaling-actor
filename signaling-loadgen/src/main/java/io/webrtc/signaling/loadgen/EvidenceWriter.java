package io.webrtc.signaling.loadgen;

import java.io.IOException;
import java.nio.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.HdrHistogram.*;

/**
 * Actual intended-arrival samples, including failure latency; exports lossless compressed HDR data.
 */
public final class EvidenceWriter {
  private static final long MAX_MICROS = 86_400_000_000L;

  public enum Operation {
    CONTROL,
    CONNECT,
    AUTH,
    REFRESH,
    INVITE,
    ACCEPT,
    NEGOTIATE,
    RELAY,
    MEDIA,
    SYNC,
    RECONNECT,
    HANGUP,
    SECURITY
  }

  private final ConcurrentHistogram latency = new ConcurrentHistogram(MAX_MICROS, 3);
  private final EnumMap<Operation, ConcurrentHistogram> phases = new EnumMap<>(Operation.class);
  private final AtomicLong attempts = new AtomicLong(),
      successes = new AtomicLong(),
      missed = new AtomicLong(),
      lateDispatch = new AtomicLong();

  public EvidenceWriter() {
    for (var operation : Operation.values())
      phases.put(operation, new ConcurrentHistogram(MAX_MICROS, 3));
  }

  public void record(long intended, long finished, boolean success) {
    record(Operation.CONTROL, intended, finished, success);
  }

  public void record(Operation operation, long intended, long finished, boolean success) {
    long elapsed = finished - intended;
    if (elapsed < 0 || elapsed / 1000 > MAX_MICROS)
      throw new IllegalArgumentException("Latency outside measured monotonic range");
    long micros = Math.max(1, elapsed / 1000);
    latency.recordValue(micros);
    phases.get(operation).recordValue(micros);
    attempts.incrementAndGet();
    if (success) successes.incrementAndGet();
  }

  public void missed(long intended, long finished) {
    missed(Operation.CONTROL, intended, finished);
  }

  public void missed(Operation phase, long intended, long finished) {
    missed.incrementAndGet();
    record(phase, intended, finished, false);
  }

  public void dispatched(long intended, long actual) {
    if (actual - intended > 5_000_000) lateDispatch.incrementAndGet();
  }

  public long attempts() {
    return attempts.get();
  }

  public long successes() {
    return successes.get();
  }

  public double percentileMillis(double percentile) {
    return latency.getValueAtPercentile(percentile) / 1000.0;
  }

  public Map<String, Object> snapshot() {
    var result = new LinkedHashMap<String, Object>();
    result.put("attempts", attempts());
    result.put("successes", successes());
    result.put("failures", attempts() - successes());
    result.put("missedIntendedArrivals", missed.get());
    result.put("lateDispatches", lateDispatch.get());
    var perPhase = new LinkedHashMap<String, Object>();
    phases.forEach(
        (op, h) ->
            perPhase.put(
                op.name(),
                Map.of(
                    "samples",
                    h.getTotalCount(),
                    "p50Ms",
                    h.getValueAtPercentile(50) / 1000.0,
                    "p95Ms",
                    h.getValueAtPercentile(95) / 1000.0,
                    "p99Ms",
                    h.getValueAtPercentile(99) / 1000.0,
                    "p999Ms",
                    h.getValueAtPercentile(99.9) / 1000.0)));
    result.put("latencies", perPhase);
    return result;
  }

  public void export(Path path) throws IOException {
    write(path, latency.copy());
  }

  public void exportPhases(Path directory) throws IOException {
    Files.createDirectories(directory);
    for (var e : phases.entrySet())
      write(
          directory.resolve(e.getKey().name().toLowerCase(Locale.ROOT) + ".hdr"),
          e.getValue().copy());
  }

  private static void write(Path path, AbstractHistogram histogram) throws IOException {
    var buffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
    int size = histogram.encodeIntoCompressedByteBuffer(buffer);
    Files.write(path, Arrays.copyOf(buffer.array(), size), StandardOpenOption.CREATE_NEW);
  }

  public static Histogram read(Path path) throws Exception {
    if (Files.size(path) > 4_194_304) throw new IOException("Histogram exceeds bound");
    return Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(Files.readAllBytes(path)), 0);
  }
}
