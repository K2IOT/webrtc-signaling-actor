package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.actors.call.CallRecoveryService;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.worker.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Shipped concrete worker composition, sharing the hosting native authority and DB admission. */
public final class NativeActorMaintenance {
  public static final Set<String> REQUIRED_JOBS =
      Set.of("call_recovery", "control_outbox", "retention", "privacy_deletion");
  private final NativeActorComposition actors;
  private final CallRecoveryService recovery;
  private final OutboxDispatcher outbox;
  private final OutboxHints outboxHints;
  private final AtomicLong nextScan = new AtomicLong(), hintVersion = new AtomicLong();
  private long idleMillis = 20;
  private final RetentionWorker retention;
  private final PrivacyDeletionWorker privacy;
  private final List<NativeWorkerScheduler.Job> jobs;
  private volatile boolean draining;

  public NativeActorMaintenance(
      NativeActorComposition actors,
      NativeActorMaintenanceEnrollment enrollment,
      CellRpcClient homeRpc,
      GatewayRelayRpcClient gateways) {
    this.actors = Objects.requireNonNull(actors);
    var sql = actors.transactions();
    outboxHints = sql.outboxHints();
    recovery = actors.recoveryService();
    var workerIncarnation = UUID.randomUUID();
    var delivery = actors.outboxDelivery(homeRpc, gateways, actors.ownerNode(), workerIncarnation);
    outbox =
        new OutboxDispatcher(
            new OutboxRepository(sql, actors.cell(), actors.storageEpoch()),
            actors.ownerNode(),
            workerIncarnation,
            delivery,
            delivery::verified);
    retention = new RetentionWorker(sql, actors.cell(), actors.storageEpoch());
    privacy =
        new PrivacyDeletionWorker(
            sql,
            actors.cell(),
            actors.storageEpoch(),
            enrollment.privacyPseudonymKey(),
            enrollment.detachment());
    jobs =
        List.of(
            job(
                "call_recovery",
                Duration.ofMillis(100),
                budget -> rpc(recovery.sweepTracked(budget))),
            job("control_outbox", Duration.ofMillis(20), budget -> outboxScan()),
            job(
                "retention",
                Duration.ofMillis(100),
                budget -> rpc(retention.pruneDue(512, budget))),
            job(
                "privacy_deletion",
                Duration.ofSeconds(1),
                budget -> rpc(privacy.processTracked(128))));
  }

  /** Post-COMMIT hints wake the concrete native scanner, never an arbitrary delivery callback. */
  public void bindWake(NativeWorkerScheduler scheduler) {
    outboxHints.bind(
        () -> {
          hintVersion.incrementAndGet();
          nextScan.set(0);
          scheduler.wake("control_outbox");
        });
  }

  private RpcOperation<?> outboxScan() {
    if (System.nanoTime() < nextScan.get())
      return new RpcOperation<>(
          CompletableFuture.completedFuture(null), CompletableFuture.completedFuture(null));
    long version = hintVersion.get();
    var operation = outbox.pollTracked(128);
    var logical =
        operation
            .logical()
            .whenComplete(
                (report, error) -> {
                  boolean active = error != null || report.pending();
                  idleMillis = active ? 20 : Math.min(1000, idleMillis * 2);
                  nextScan.set(
                      active ? 0 : System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(idleMillis));
                  // A hint concurrent with scan completion must retain its immediate wake.
                  if (hintVersion.get() != version || outboxHints.pending()) nextScan.set(0);
                });
    return new RpcOperation<>(logical, operation.physicalCompletion());
  }

  private NativeWorkerScheduler.Job job(
      String name, Duration period, java.util.function.Function<Duration, RpcOperation<?>> run) {
    return new NativeWorkerScheduler.Job(
        name,
        NativeWorkerScheduler.Priority.MAINTENANCE,
        period,
        budget -> {
          if (draining)
            return new RpcOperation<>(
                CompletableFuture.failedFuture(new DbOverloadedException()),
                CompletableFuture.completedFuture(null));
          return run.apply(budget);
        });
  }

  private static <T> RpcOperation<T> rpc(DbOperation<T> operation) {
    return new RpcOperation<>(operation.logical(), operation.physicalCompletion());
  }

  public List<NativeWorkerScheduler.Job> jobs() {
    return jobs;
  }

  public boolean installed() {
    return !draining && actors.participantRenewalInstalled();
  }

  public CallRecoveryService recovery() {
    return recovery;
  }

  public PrivacyDeletionWorker privacy() {
    return privacy;
  }

  /** Scheduler invokes this only after every admitted invocation's physical receipt. */
  public CompletionStage<Void> drain() {
    draining = true;
    outboxHints.unbind();
    return CompletableFuture.allOf(
        recovery.drain().toCompletableFuture(), outbox.drain().toCompletableFuture());
  }
}
