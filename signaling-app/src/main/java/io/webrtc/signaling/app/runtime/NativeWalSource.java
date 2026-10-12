package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.rpc.RpcOperation;
import io.webrtc.signaling.storage.PostgresWalFacts;
import java.math.BigInteger;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** One immutable cached writer observation and one original physical SQL owner. */
public final class NativeWalSource implements AutoCloseable {
  private final PostgresWalFacts observer;
  private final NativeWalEnrollment policy;
  private PostgresWalFacts.Facts cached;
  private boolean active, draining;
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  public NativeWalSource(PostgresWalFacts observer, NativeWalEnrollment policy) {
    this.observer = Objects.requireNonNull(observer); this.policy = Objects.requireNonNull(policy);
  }
  public synchronized boolean usable() {
    if (freshnessAgeNanos() < 0) return false;
    if (!policy.cell().equals(cached.cell()) || policy.storageEpoch() != cached.epoch()
        || !policy.systemIdentifier().equals(cached.systemIdentifier()) || policy.timeline() != cached.timeline()) return false;
    var configured = cached.synchronousPolicy();
    // quorum marks a candidate, not a required acknowledgement. Bind the actual writer's
    // configured threshold before counting available candidates/priority-selected standbys.
    if (configured.required() != policy.synchronousReplicas()
        || !new HashSet<>(configured.identities()).equals(policy.replicas())
        || (policy.localSingleWriter() ? !"NONE".equals(configured.mode())
            : !("ANY".equals(configured.mode()) || "FIRST".equals(configured.mode())))) return false;
    Set<String> seen = new HashSet<>(); int synchronous = 0;
    for (var replica : cached.replicas()) {
      if (!policy.replicas().contains(replica.application()) || !seen.add(replica.application())
          || !"streaming".equals(replica.state()) || replica.flush() == null || replica.replay() == null
          || replica.replyAt() == null || replica.replay().compareTo(replica.flush()) > 0) return false;
      long replyAge;
      try { replyAge = Duration.between(replica.replyAt(), cached.checkedAt()).toNanos() + freshnessAgeNanos(); }
      catch (ArithmeticException invalid) { return false; }
      var bytes = cached.flush().subtract(replica.flush());
      if (replyAge < 0 || replyAge > policy.maximumReplyAge().toNanos() || bytes.signum() < 0
          || bytes.compareTo(BigInteger.valueOf(policy.maximumLagBytes())) > 0) return false;
      if ("ANY".equals(configured.mode())) {
        if (!"quorum".equals(replica.syncState())) return false;
        synchronous++;
      } else if ("FIRST".equals(configured.mode())) {
        if ("sync".equals(replica.syncState())) synchronous++;
        else if (!"potential".equals(replica.syncState())) return false;
      }
    }
    return freshnessAgeNanos() >= 0 && seen.equals(policy.replicas())
        && ("FIRST".equals(configured.mode()) ? synchronous == configured.required()
            : synchronous >= configured.required());
  }
  /** -1 is unknown; source age is never reported as replication duration. */
  public synchronized long freshnessAgeNanos() {
    if (draining || cached == null) return -1;
    long age = System.nanoTime() - cached.sampledAtNanos();
    return age < 0 || age >= policy.freshness().toNanos() ? -1 : age;
  }
  public synchronized long maximumFlushLagBytes() {
    if (freshnessAgeNanos() < 0 || cached.replicas().isEmpty()
        || cached.replicas().stream().anyMatch(r -> r.flush() == null || r.flush().compareTo(cached.flush()) > 0)) return -1;
    return cached.replicas().stream().map(r -> cached.flush().subtract(r.flush())).max(BigInteger::compareTo).orElseThrow().min(BigInteger.valueOf(Long.MAX_VALUE)).longValue();
  }
  public synchronized long maximumObservedFlushDurationNanos() {
    if (freshnessAgeNanos() < 0 || cached.replicas().isEmpty() || cached.replicas().stream().anyMatch(r -> r.flushDuration() == null)) return -1;
    return cached.replicas().stream().mapToLong(r -> r.flushDuration().toNanos()).max().orElse(-1);
  }
  public RpcOperation<PostgresWalFacts.Facts> poll(Duration budget) {
    final CompletableFuture<Void> physical = new CompletableFuture<>();
    synchronized (this) {
      if (active || draining) return new RpcOperation<>(CompletableFuture.failedFuture(new RejectedExecutionException("WAL owner unavailable")), CompletableFuture.completedFuture(null));
      active = true;
    }
    try {
      var operation = observer.poll(budget);
      var logical = operation.logical().handle((facts, failure) -> {
        synchronized (this) { cached = failure == null && !draining ? facts : null; }
        if (failure != null) throw new CompletionException(failure);
        return facts;
      });
      operation.physicalCompletion().thenCombine(logical.handle((v,e) -> null), (a,b) -> null).whenComplete((v,e) -> {
        synchronized (this) {
          if (e != null) { cached = null; return; }
          active = false; physical.complete(null); if (draining) drained.complete(null);
        }
      });
      return new RpcOperation<>(logical, physical);
    } catch (Throwable unknown) {
      synchronized (this) { cached = null; }
      // A synchronous failure without a returned owner cannot prove physical retirement.
      return new RpcOperation<>(CompletableFuture.failedFuture(unknown), physical);
    }
  }
  public NativeWorkerScheduler.Job job() {
    return new NativeWorkerScheduler.Job("cell_wal", NativeWorkerScheduler.Priority.SAFETY,
        policy.freshness().dividedBy(2), this::poll);
  }
  public synchronized CompletionStage<Void> drain() {
    draining = true; cached = null; if (!active) drained.complete(null); return drained.minimalCompletionStage();
  }
  @Override public void close() { drain(); }
}
