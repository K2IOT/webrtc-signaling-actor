package io.webrtc.signaling.app.runtime;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.webrtc.signaling.rpc.RpcOperation;
import io.webrtc.signaling.storage.DbOperation;
import io.webrtc.signaling.storage.worker.RevocationReconciler;
import io.webrtc.signaling.storage.worker.RevocationSourceVerifier;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * One source page per original bounded poll. SQL commit remains the security authority.
 */
public final class NativeRevocationSource implements AutoCloseable {
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
    private final RevocationSourceVerifier verifier;
    private final RevocationReconciler reconciler;
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private boolean active, draining;
    private long checkedNanos;
    private Instant checkedAt;
    private Instant observedCheckedAt;
    private long observedNanos;
    private CompletableFuture<Void> admitted = CompletableFuture.completedFuture(null);

    public NativeRevocationSource(
            URI endpoint,
            SSLContext tls,
            RevocationSourceVerifier verifier,
            RevocationReconciler reconciler,
            UUID podUid,
            UUID processBoot) {
        http = new NativeSourceHttp(endpoint, tls, podUid, processBoot);
        this.verifier = Objects.requireNonNull(verifier);
        this.reconciler = Objects.requireNonNull(reconciler);
    }

    private static Duration remaining(long end) throws TimeoutException {
        long nanos = end - System.nanoTime();
        if (nanos <= 0) throw new TimeoutException("Original revocation source deadline");
        return Duration.ofNanos(nanos);
    }

    private static <T> T tracked(DbOperation<T> work, long end, List<CompletableFuture<?>> receipts)
            throws Exception {
        receipts.add(work.physicalCompletion().toCompletableFuture());
        return work.logical().toCompletableFuture().get(remaining(end).toNanos(), TimeUnit.NANOSECONDS);
    }

    public synchronized boolean usable() {
        long age = System.nanoTime() - checkedNanos;
        Instant now = Instant.now();
        return !draining
                && checkedAt != null
                && age >= 0
                && age < reconciler.freshness().toNanos()
                && !checkedAt.isAfter(now)
                && now.isBefore(checkedAt.plus(reconciler.freshness()));
    }

    /**
     * Age of the last committed caught-up source page; invalid/absent observations are unknown.
     */
    public synchronized long freshnessAgeNanos() {
        if (observedCheckedAt == null) return -1;
        long monotonic = System.nanoTime() - observedNanos;
        long wall = Duration.between(observedCheckedAt, Instant.now()).toNanos();
        return monotonic < 0 || wall < 0 ? -1 : Math.max(monotonic, wall);
    }

    /**
     * Blocking HTTP and bounded native waits run only on an admitted worker, never event loops.
     */
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
                                new RejectedExecutionException("Revocation source unavailable")),
                        CompletableFuture.completedFuture(null));
            active = true;
            admitted = physical = new CompletableFuture<>();
        }
        long start = System.nanoTime(), end = start + budget.toNanos();
        var receipts = new ArrayList<CompletableFuture<?>>(2);
        CompletionStage<Long> logical;
        try {
            var cursor = tracked(reconciler.progress(remaining(end)), end, receipts);
            var bytes = http.fetch(end, cursor.offset());
            var decoder =
                    StandardCharsets.UTF_8
                            .newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT);
            var batch =
                    JSON.readValue(
                            decoder.decode(ByteBuffer.wrap(bytes)).toString(), RevocationReconciler.Batch.class);
            if (batch.fromOffset() != cursor.offset() || !verifier.test(batch))
                throw new IllegalStateException("Invalid revocation page");
            long offset = tracked(reconciler.apply(batch, remaining(end)), end, receipts);
            if (offset != batch.highWater())
                throw new IllegalStateException("Revocation cursor mismatch");
            synchronized (this) {
                checkedAt = !draining && batch.caughtUp() ? batch.checkedAt() : null;
                checkedNanos = start;
                if (checkedAt != null) {
                    observedCheckedAt = checkedAt;
                    observedNanos = start;
                }
            }
            logical = CompletableFuture.completedFuture(offset);
        } catch (Throwable invalid) {
            synchronized (this) {
                checkedAt = null;
            }
            logical = CompletableFuture.failedFuture(invalid);
        }
        if (!http.physicallySettled()) receipts.add(new CompletableFuture<Void>());
        CompletableFuture.allOf(receipts.toArray(CompletableFuture[]::new))
                .whenComplete(
                        (v, error) -> {
                            synchronized (this) {
                                if (error != null) {
                                    checkedAt = null;
                                    return;
                                }
                                active = false;
                                physical.complete(null);
                                if (draining && !active) drained.complete(null);
                            }
                        });
        return new RpcOperation<>(logical, physical);
    }

    public NativeWorkerScheduler.Job job(Duration period) {
        if (period == null
                || period.compareTo(Duration.ofSeconds(1)) > 0
                || period.compareTo(reconciler.freshness().dividedBy(2)) > 0)
            throw new IllegalArgumentException("Source poll period exceeds freshness margin");
        return new NativeWorkerScheduler.Job(
                "revocation_source", NativeWorkerScheduler.Priority.SAFETY, period, this::poll);
    }

    public synchronized CompletionStage<Void> settleAdmitted() {
        return admitted.minimalCompletionStage();
    }

    public synchronized CompletionStage<Void> drain() {
        draining = true;
        checkedAt = null;
        if (!active) drained.complete(null);
        return drained.minimalCompletionStage();
    }

    @Override
    public void close() {
        drain();
    }
}
