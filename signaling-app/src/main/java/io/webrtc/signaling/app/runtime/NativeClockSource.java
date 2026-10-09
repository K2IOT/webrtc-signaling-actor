package io.webrtc.signaling.app.runtime;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.rpc.RpcOperation;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import javax.net.ssl.*;

/**
 * One enrolled HTTPS clock source. No connection pool, redirects, worker threads or implicit trust.
 */
public final class NativeClockSource implements AutoCloseable {
  private static final ObjectMapper JSON =
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
  private final NativeSourceHttp http;
  private final ClockSafetyMonitor monitor;
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private boolean active, draining;
  private CompletableFuture<Void> admittedPhysical = CompletableFuture.completedFuture(null);

  public NativeClockSource(
      URI endpoint, SSLContext tls, ClockSafetyMonitor monitor, UUID podUid, UUID processBoot) {
    http = new NativeSourceHttp(endpoint, tls, podUid, processBoot);
    this.monitor = Objects.requireNonNull(monitor);
  }

  /** Cached authorization and polling must use the exact same attestation owner. */
  public boolean monitors(ClockSafetyMonitor expected) {
    return monitor == expected;
  }

  /**
   * Called only off event loops. Its one socket is synchronously closed before physical retirement.
   */
  public boolean poll(Duration budget) {
    return pollTracked(budget).logical().toCompletableFuture().join();
  }

  /**
   * Each invocation exposes its own original receipt, even if a completion callback admits another
   * poll.
   */
  public RpcOperation<Boolean> pollTracked(Duration budget) {
    final CompletableFuture<Void> physical;
    synchronized (this) {
      if (draining || active)
        return new RpcOperation<>(
            CompletableFuture.completedFuture(false), CompletableFuture.completedFuture(null));
      active = true;
      admittedPhysical = physical = new CompletableFuture<>();
    }
    long started = System.nanoTime();
    boolean accepted = false;
    try {
      if (budget == null || budget.isZero() || budget.isNegative())
        throw new IllegalArgumentException("Positive source budget required");
      long end = started + Math.min(budget.toNanos(), Duration.ofSeconds(1).toNanos());
      var payload = http.fetch(end, null);
      var decoder =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT);
      var value = JSON.readTree(decoder.decode(ByteBuffer.wrap(payload)).toString());
      if (!value.isObject()
          || value.size() != 2
          || !value.has("report")
          || !value.path("signature").isTextual()) throw new IOException("Invalid clock response");
      var report = JSON.treeToValue(value.get("report"), ClockSafetyMonitor.Report.class);
      if (System.nanoTime() - end >= 0) throw new SocketTimeoutException("Clock source deadline");
      accepted = monitor.observe(report, value.get("signature").asText(), started);
    } catch (Exception invalid) {
      accepted = false;
    } finally {
      synchronized (this) {
        if (!accepted || draining) monitor.invalidate();
        if (http.physicallySettled()) {
          active = false;
          physical.complete(null);
          if (draining && !active) drained.complete(null);
        }
      }
    }
    synchronized (this) {
      return new RpcOperation<>(CompletableFuture.completedFuture(accepted && !draining), physical);
    }
  }

  public NativeWorkerScheduler.Job job(Duration period) {
    if (period == null || period.compareTo(Duration.ofSeconds(1)) > 0)
      throw new IllegalArgumentException("Clock source poll must run at least once per second");
    return new NativeWorkerScheduler.Job(
        "clock_source",
        NativeWorkerScheduler.Priority.SAFETY,
        period,
        budget -> {
          var original = pollTracked(budget);
          return new RpcOperation<>(
              original
                  .logical()
                  .thenApply(
                      valid -> {
                        if (!valid) throw new IllegalStateException("Clock source unavailable");
                        return true;
                      }),
              original.physicalCompletion());
        });
  }

  public synchronized CompletionStage<Void> settleAdmitted() {
    return admittedPhysical.minimalCompletionStage();
  }

  public synchronized CompletionStage<Void> drain() {
    draining = true;
    monitor.invalidate();
    if (!active) drained.complete(null);
    return drained.minimalCompletionStage();
  }

  @Override
  public void close() {
    drain();
  }
}
