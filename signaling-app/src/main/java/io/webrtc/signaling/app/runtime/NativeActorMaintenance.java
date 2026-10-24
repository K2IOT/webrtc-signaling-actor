package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.actors.call.CallRecoveryService;
import io.webrtc.signaling.rpc.CellRpcClient;
import io.webrtc.signaling.rpc.GatewayRelayRpcClient;
import io.webrtc.signaling.rpc.NativeActorComposition;
import io.webrtc.signaling.rpc.RpcOperation;
import io.webrtc.signaling.storage.DbOperation;
import io.webrtc.signaling.storage.DbOverloadedException;
import io.webrtc.signaling.storage.OutboxHints;
import io.webrtc.signaling.storage.OutboxRepository;
import io.webrtc.signaling.storage.worker.OutboxDispatcher;
import io.webrtc.signaling.storage.worker.PrivacyDeletionWorker;
import io.webrtc.signaling.storage.worker.RetentionWorker;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shipped concrete worker composition, sharing the hosting native authority and DB admission.
 */
public final class NativeActorMaintenance {
    public static final Set<String> REQUIRED_JOBS =
            Set.of("call_recovery", "control_outbox", "retention", "privacy_deletion");
    private final NativeActorComposition actors;
    private final CallRecoveryService recovery;
    private final OutboxDispatcher outbox;
    private final OutboxHints outboxHints;
    private final AtomicLong nextScan = new AtomicLong(), hintVersion = new AtomicLong();
    private final RetentionWorker retention;
    private final PrivacyDeletionWorker privacy;
    private final List<NativeWorkerScheduler.Job> jobs;
    private long idleMillis = 20;
    // Expired rows wait at most a second more; an idle cell no longer scans five tables 10x/s.
    private final IdleBackoff retentionBackoff = new IdleBackoff(100, 1000);
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
                        job("retention", Duration.ofMillis(100), this::retentionScan),
                        job(
                                "privacy_deletion",
                                Duration.ofSeconds(1),
                                budget -> rpc(privacy.processTracked(128))));
    }

    private static <T> RpcOperation<T> rpc(DbOperation<T> operation) {
        return new RpcOperation<>(operation.logical(), operation.physicalCompletion());
    }

    /**
     * Post-COMMIT hints wake the concrete native scanner, never an arbitrary delivery callback.
     */
    public void bindWake(NativeWorkerScheduler scheduler) {
        outboxHints.bind(
                () -> {
                    hintVersion.incrementAndGet();
                    nextScan.set(0);
                    scheduler.wake("control_outbox");
                });
    }

    private RpcOperation<?> retentionScan(Duration budget) {
        if (!retentionBackoff.due(System.nanoTime()))
            return new RpcOperation<>(
                    CompletableFuture.completedFuture(null), CompletableFuture.completedFuture(null));
        var operation = retention.pruneDue(512, budget);
        operation
                .logical()
                .whenComplete(
                        (report, error) ->
                                retentionBackoff.completed(
                                        error != null || report.deleted() > 0, System.nanoTime()));
        return rpc(operation);
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

    /**
     * Scheduler invokes this only after every admitted invocation's physical receipt.
     */
    public CompletionStage<Void> drain() {
        draining = true;
        outboxHints.unbind();
        return CompletableFuture.allOf(
                recovery.drain().toCompletableFuture(), outbox.drain().toCompletableFuture());
    }
}
