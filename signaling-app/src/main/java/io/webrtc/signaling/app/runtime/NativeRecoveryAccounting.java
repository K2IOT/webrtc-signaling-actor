package io.webrtc.signaling.app.runtime;

import io.micrometer.core.instrument.Gauge;
import io.webrtc.signaling.observability.SignalingMetrics;
import io.webrtc.signaling.observability.SignalingMetrics.RecoverySli;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import org.springframework.context.SmartLifecycle;

/** Bounded asynchronous native producer. Metric failure cannot change an authority result. */
public final class NativeRecoveryAccounting implements RecoveryOutcomeSink, SmartLifecycle {
  private final DurableRecoveryAccounting journal;
  private final NativeRecoveryEnrollment enrollment;
  private final NativeWorkerScheduler worker;

  private record Pending(DurableRecoveryAccounting.Observation observation, Set<UUID> cohorts) {}

  private static final class Scope {
    final DurableRecoveryAccounting.Cohort cohort;
    final Map<CallId, DurableRecoveryAccounting.Baseline> members;
    boolean captured, faultResolved, started;
    Instant faultAt;
    DbOperation<UUID> captureOperation, faultOperation;

    Scope(DurableRecoveryAccounting.Cohort cohort) {
      this.cohort = cohort;
      members = new HashMap<>();
      for (var baseline : cohort.completeRoster()) members.put(baseline.call(), baseline);
    }

    boolean eligible(DurableRecoveryAccounting.Observation observation) {
      var baseline = members.get(observation.call());
      if (!captured
          || faultAt == null
          || baseline == null
          || observation.observedAt().isBefore(faultAt)) return false;
      long seconds =
          switch (observation.sli()) {
            case WORKFLOW_CONVERGENCE, MEDIA_CONTINUITY -> 30;
            case SIGNALING_REATTACHMENT ->
                cohort.faultKind() == DurableRecoveryAccounting.FaultKind.AZ ? 180 : 60;
            case ACTIVE_CALL_PRESERVATION -> 60;
          };
      if (observation.observedAt().isAfter(faultAt.plusSeconds(seconds))) return false;
      return switch (observation.sli()) {
        case WORKFLOW_CONVERGENCE -> true;
        case ACTIVE_CALL_PRESERVATION -> baseline.active();
        case MEDIA_CONTINUITY -> baseline.media();
        case SIGNALING_REATTACHMENT ->
            observation.participant() != null
                && baseline.disconnected().contains(observation.participant().incarnation());
      };
    }

    boolean open(Instant now) {
      return captured
          && faultAt != null
          && !now.isBefore(faultAt)
          && !now.isAfter(
              faultAt.plusSeconds(
                  cohort.faultKind() == DurableRecoveryAccounting.FaultKind.AZ ? 180 : 60));
    }
  }

  // Same lifetime bounds as the durable journal; never evict unresolved enrollment evidence.
  private final Map<UUID, Scope> scopes = new HashMap<>();
  private final ArrayDeque<Pending> pending = new ArrayDeque<>();
  private final Set<CompletableFuture<?>> originalOwners = new HashSet<>();
  private final Set<UUID> lost = new HashSet<>();
  private volatile DurableRecoveryAccounting.Totals totals;
  private volatile long checkedNanos;
  private boolean running, draining;

  public NativeRecoveryAccounting(
      NativeActorComposition actors,
      NativeRecoveryEnrollment enrollment,
      SignalingMetrics metrics,
      NativeTelemetry telemetry) {
    this.enrollment = enrollment;
    journal =
        new DurableRecoveryAccounting(
            actors.transactions(),
            actors.cell(),
            actors.storageEpoch(),
            enrollment.privacyKeyId(),
            enrollment.privacyKey(),
            actors::recoveryAuthority);
    for (var sli : RecoverySli.values()) {
      // Shared DB cumulative values are gauges: aggregate replicas with max by(cell,sli), never
      // sum.
      Gauge.builder("signaling.recovery.durable.eligible", () -> metric(sli, 0))
          .tag("sli", sli.name())
          .register(metrics.registry());
      Gauge.builder("signaling.recovery.durable.successful", () -> metric(sli, 1))
          .tag("sli", sli.name())
          .register(metrics.registry());
      Gauge.builder("signaling.recovery.durable.ratio", () -> metric(sli, 2))
          .tag("sli", sli.name())
          .register(metrics.registry());
      Gauge.builder("signaling.recovery.durable.known", () -> metric(sli, 3))
          .tag("sli", sli.name())
          .register(metrics.registry());
    }
    worker =
        new NativeWorkerScheduler(
                List.of(
                    new NativeWorkerScheduler.Job(
                        "recovery_accounting_outcomes",
                        NativeWorkerScheduler.Priority.MAINTENANCE,
                        Duration.ofMillis(100),
                        budget -> process()),
                    new NativeWorkerScheduler.Job(
                        "recovery_accounting_totals",
                        NativeWorkerScheduler.Priority.MAINTENANCE,
                        Duration.ofSeconds(1),
                        budget -> refresh())),
                event -> {
                  if (event.status() != NativeWorkerScheduler.Status.COMPLETED
                      && event.name().equals("recovery_accounting_totals")) checkedNanos = 0;
                },
                List.of(this::drainOwners))
            .observe(telemetry::workerStarted);
    actors.installRecoveryOutcomes(this);
  }

  private double metric(RecoverySli sli, int kind) {
    var snapshot = totals;
    var value = snapshot == null ? null : snapshot.get(sli);
    if (value == null || System.nanoTime() - checkedNanos > Duration.ofSeconds(5).toNanos())
      return Double.NaN;
    return switch (kind) {
      case 0 -> value.eligible();
      case 1 -> value.successful();
      case 2 ->
          hasLoss() || !value.known() || value.eligible() == 0
              ? Double.NaN
              : (double) value.successful() / value.eligible();
      default -> value.known() && !hasLoss() ? 1 : 0;
    };
  }

  /** Operator/provider owns the complete pre-fault roster; <=128 calls per admitted fault scope. */
  public synchronized DbOperation<UUID> capture(DurableRecoveryAccounting.Cohort cohort) {
    if (!enrollment.sourceHash().equals(cohort.sourceHash()))
      throw new AuthoritySql.FencedException();
    var scope = scopes.get(cohort.id());
    if (scope != null && !scope.cohort.equals(cohort)) throw new AuthoritySql.FencedException();
    if (scope == null) {
      if (scopes.size() >= 64) throw new DbOverloadedException();
      scope = new Scope(cohort);
      scopes.put(cohort.id(), scope);
    }
    if (inFlight(scope.captureOperation)) return scope.captureOperation;
    final Scope admitted = scope;
    var original = track(() -> journal.capture(cohort));
    // Publish membership only after proven COMMIT and original physical cleanup, not a timeout
    // wrapper.
    var logical =
        original
            .logical()
            .thenCombine(
                original.physicalCompletion(),
                (id, physical) -> {
                  synchronized (this) {
                    admitted.captured = true;
                  }
                  return id;
                });
    scope.captureOperation = new DbOperation<>(logical, original.physicalCompletion());
    return scope.captureOperation;
  }

  private static boolean inFlight(DbOperation<?> operation) {
    return operation != null
        && (!operation.logical().toCompletableFuture().isDone()
            || !operation.physicalCompletion().toCompletableFuture().isDone());
  }

  public synchronized DbOperation<UUID> fault(UUID cohort, Instant actualFaultAt) {
    Objects.requireNonNull(actualFaultAt);
    var scope = scopes.get(cohort);
    if (scope == null || !scope.captured) throw new AuthoritySql.FencedException();
    Instant faultAt = actualFaultAt.truncatedTo(java.time.temporal.ChronoUnit.MICROS);
    if (scope.faultAt != null && !scope.faultAt.equals(faultAt))
      throw new AuthoritySql.FencedException();
    if (inFlight(scope.faultOperation)) return scope.faultOperation;
    // Arm before SQL submission: genuine callbacks while fault COMMIT is in flight retain
    // admission.
    scope.faultAt = faultAt;
    try {
      var original = track(() -> journal.start(cohort, faultAt));
      var logical =
          original
              .logical()
              .thenCombine(original.physicalCompletion(), (id, physical) -> id)
              .whenComplete(
                  (id, error) -> {
                    synchronized (this) {
                      scope.faultResolved = true;
                      if (error == null) scope.started = true;
                      else lost.add(cohort);
                    }
                  });
      scope.faultOperation = new DbOperation<>(logical, original.physicalCompletion());
      return scope.faultOperation;
    } catch (RuntimeException error) {
      scope.faultResolved = true;
      lost.add(cohort);
      throw error;
    }
  }

  @Override
  public void snapshot(CallSnapshotRepository.Snapshot snapshot) {
    if (snapshot.terminalAt() == null || snapshot.terminalReason() == null) return;
    enqueue(
        new DurableRecoveryAccounting.Observation(
            snapshot.callId(),
            snapshot.version(),
            snapshot.negotiationId(),
            RecoverySli.WORKFLOW_CONVERGENCE,
            null,
            null,
            Instant.now()));
  }

  @Override
  public void hydrated(CallId call, AuthoritySql.GroupToken group) {
    enqueue(
        new DurableRecoveryAccounting.Observation(
            call, 0, 0, RecoverySli.WORKFLOW_CONVERGENCE, null, group, Instant.now()));
  }

  @Override
  public void active(CallSnapshotRepository.Snapshot snapshot, AuthoritySql.GroupToken group) {
    enqueue(
        new DurableRecoveryAccounting.Observation(
            snapshot.callId(),
            snapshot.version(),
            snapshot.negotiationId(),
            RecoverySli.ACTIVE_CALL_PRESERVATION,
            null,
            group,
            Instant.now()));
    enqueue(
        new DurableRecoveryAccounting.Observation(
            snapshot.callId(),
            snapshot.version(),
            snapshot.negotiationId(),
            RecoverySli.MEDIA_CONTINUITY,
            null,
            group,
            Instant.now()));
  }

  @Override
  public void media(
      CallSnapshotRepository.Snapshot snapshot,
      AuthoritySql.GroupToken group,
      AuthenticatedSession sender) {
    enqueue(
        new DurableRecoveryAccounting.Observation(
            snapshot.callId(),
            snapshot.version(),
            snapshot.negotiationId(),
            RecoverySli.MEDIA_CONTINUITY,
            sender,
            group,
            Instant.now()));
  }

  @Override
  public void application(CallId call, long version, AuthenticatedSession recipient) {
    enqueue(
        new DurableRecoveryAccounting.Observation(
            call, version, 0, RecoverySli.SIGNALING_REATTACHMENT, recipient, null, Instant.now()));
  }

  private synchronized void enqueue(DurableRecoveryAccounting.Observation observation) {
    var targets = new HashSet<UUID>();
    for (var entry : scopes.entrySet())
      if (entry.getValue().eligible(observation)) targets.add(entry.getKey());
    if (targets.isEmpty()) return; // Unrelated traffic owns neither queue slots nor cohort loss.
    if (draining || !running || pending.size() >= 32) {
      lost.addAll(targets);
      return;
    }
    pending.addLast(new Pending(observation, Set.copyOf(targets)));
  }

  private synchronized boolean hasLoss() {
    return lost.stream().anyMatch(id -> scopes.get(id).started);
  }

  private synchronized Set<UUID> lossSnapshot() {
    return lost.stream()
        .filter(id -> scopes.get(id).faultResolved)
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
  }

  private synchronized void lost(Pending item) {
    lost.addAll(item.cohorts());
  }

  @Override
  public synchronized void unavailable() {
    Instant now = Instant.now();
    for (var entry : scopes.entrySet()) if (entry.getValue().open(now)) lost.add(entry.getKey());
  }

  private RpcOperation<?> process() {
    Pending next = null;
    synchronized (this) {
      // An unresolved fault COMMIT must not block ready observations for unrelated cohorts.
      var iterator = pending.iterator();
      while (iterator.hasNext()) {
        var item = iterator.next();
        var ready = new HashSet<UUID>();
        var waiting = new HashSet<UUID>();
        for (var id : item.cohorts()) {
          var scope = scopes.get(id);
          if (!scope.faultResolved) waiting.add(id);
          else if (scope.started) ready.add(id);
        }
        if (ready.isEmpty() && !waiting.isEmpty()) continue;
        iterator.remove();
        if (ready.isEmpty()) continue; // Failed fault admission never admits SQL outcomes.
        if (!waiting.isEmpty())
          pending.addLast(new Pending(item.observation(), Set.copyOf(waiting)));
        next = new Pending(item.observation(), Set.copyOf(ready));
        break;
      }
    }
    if (next == null)
      return new RpcOperation<>(
          CompletableFuture.completedFuture(null), CompletableFuture.completedFuture(null));
    final Pending admitted = next;
    try {
      var original = track(() -> journal.observe(admitted.observation(), admitted.cohorts()));
      original
          .physicalCompletion()
          .whenComplete(
              (physical, error) -> {
                if (error != null) lost(admitted);
              });
      return new RpcOperation<>(
          original
              .logical()
              .whenComplete(
                  (v, e) -> {
                    if (e != null) lost(admitted);
                  }),
          original.physicalCompletion());
    } catch (RuntimeException failure) {
      lost(admitted);
      throw failure;
    }
  }

  private RpcOperation<?> refresh() {
    final Set<UUID> losses = lossSnapshot();
    var original = track(() -> journal.totals(losses));
    return new RpcOperation<>(
        original
            .logical()
            .thenCombine(
                original.physicalCompletion(),
                (value, physical) -> {
                  synchronized (this) {
                    // The SQL transaction persisted these losses, or found all memberships already
                    // successful.
                    lost.removeAll(losses);
                    totals = value;
                    checkedNanos = System.nanoTime();
                  }
                  return (Void) null;
                }),
        original.physicalCompletion());
  }

  private synchronized <T> DbOperation<T> track(Supplier<DbOperation<T>> factory) {
    if (draining || originalOwners.size() >= 16) throw new DbOverloadedException();
    var original = factory.get();
    var owner = original.physicalCompletion().toCompletableFuture();
    originalOwners.add(owner);
    owner.whenComplete(
        (v, e) -> {
          synchronized (this) {
            if (e == null) originalOwners.remove(owner);
            else checkedNanos = 0;
          }
        });
    return original;
  }

  private synchronized CompletionStage<Void> drainOwners() {
    return CompletableFuture.allOf(originalOwners.toArray(CompletableFuture[]::new));
  }

  @Override
  public synchronized void start() {
    if (draining) throw new IllegalStateException("Recovery accounting drained");
    running = true;
    worker.start();
  }

  @Override
  public synchronized boolean isRunning() {
    return running;
  }

  @Override
  public int getPhase() {
    return Integer.MAX_VALUE;
  }

  @Override
  public void stop(Runnable callback) {
    synchronized (this) {
      running = false;
      draining = true;
      for (var item : pending) lost(item);
      pending.clear();
    }
    worker.drain().thenRun(callback);
  }

  @Override
  public void stop() {
    stop(() -> {});
  }
}
