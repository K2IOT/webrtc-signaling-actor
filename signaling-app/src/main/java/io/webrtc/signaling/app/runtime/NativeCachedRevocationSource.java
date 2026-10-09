package io.webrtc.signaling.app.runtime;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import io.webrtc.signaling.rpc.RpcOperation;
import io.webrtc.signaling.storage.worker.RevocationReconciler;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import javax.net.ssl.SSLContext;

/**
 * One original signed edge-source socket per bounded worker invocation; never polled on event
 * loops.
 */
public final class NativeCachedRevocationSource implements AutoCloseable {
  private static final ObjectMapper JSON =
      new ObjectMapper(
              JsonFactory.builder()
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNestingDepth(8)
                          .maxStringLength(8192)
                          .maxNumberLength(20)
                          .build())
                  .build())
          .findAndRegisterModules()
          .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
          .enable(
              DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
              DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES,
              DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private final NativeSourceHttp http;
  private final NativeCachedSecurity cache;
  private boolean active, draining;
  private CompletableFuture<Void> admitted = CompletableFuture.completedFuture(null);
  private final CompletableFuture<Void> drained = new CompletableFuture<>();

  public NativeCachedRevocationSource(
      URI endpoint, SSLContext tls, NativeCachedSecurity cache, UUID podUid, UUID processBoot) {
    http = new NativeSourceHttp(endpoint, tls, podUid, processBoot);
    this.cache = Objects.requireNonNull(cache);
  }

  public synchronized boolean usable() {
    return !draining && cache.usable();
  }

  public RpcOperation<Long> poll(Duration budget) {
    if (budget == null
        || budget.isNegative()
        || budget.isZero()
        || budget.compareTo(Duration.ofSeconds(2)) > 0)
      throw new IllegalArgumentException("Source poll budget outside bound");
    final CompletableFuture<Void> physical;
    synchronized (this) {
      if (active || draining)
        return new RpcOperation<>(
            CompletableFuture.failedFuture(
                new RejectedExecutionException("Scoped source unavailable")),
            CompletableFuture.completedFuture(null));
      active = true;
      admitted = physical = new CompletableFuture<>();
    }
    long started = System.nanoTime(), end = started + budget.toNanos();
    CompletionStage<Long> logical;
    try {
      var bytes = http.fetch(end, cache.offset());
      var decoder =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT);
      var batch =
          JSON.readValue(
              decoder.decode(ByteBuffer.wrap(bytes)).toString(), RevocationReconciler.Batch.class);
      synchronized (this) {
        if (draining || System.nanoTime() - end >= 0 || !cache.apply(batch, started))
          throw new IllegalStateException("Invalid scoped source page");
        logical = CompletableFuture.completedFuture(batch.highWater());
      }
    } catch (Exception unavailable) {
      cache.invalidate();
      logical =
          CompletableFuture.failedFuture(new IllegalStateException("Scoped source unavailable"));
    }
    synchronized (this) {
      // An unsuccessful close is UNKNOWN: preserve the original receipt and admission.
      if (http.physicallySettled()) {
        active = false;
        physical.complete(null);
        if (draining && !active) drained.complete(null);
      }
    }
    return new RpcOperation<>(logical, physical);
  }

  public NativeWorkerScheduler.Job job(Duration period) {
    if (period == null || period.compareTo(Duration.ofSeconds(1)) > 0)
      throw new IllegalArgumentException("Scoped source must poll at least once per second");
    return new NativeWorkerScheduler.Job(
        "scoped_security_source", NativeWorkerScheduler.Priority.SAFETY, period, this::poll);
  }

  public synchronized CompletionStage<Void> settleAdmitted() {
    return admitted.minimalCompletionStage();
  }

  public synchronized CompletionStage<Void> drain() {
    draining = true;
    cache.invalidate();
    if (!active) drained.complete(null);
    return drained.minimalCompletionStage();
  }

  @Override
  public void close() {
    drain();
  }
}
