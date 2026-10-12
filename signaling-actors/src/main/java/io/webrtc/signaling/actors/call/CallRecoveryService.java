package io.webrtc.signaling.actors.call;

import io.webrtc.signaling.observability.*;
import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.storage.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Fair owner/group keyset pages; a hint never manufactures participant or hosting authority. */
public final class CallRecoveryService implements AutoCloseable {
  public record Report(int examined, int hydrated, int unresolved, boolean cycleComplete) {}

  private record Hint(long hashVersion, int group, CallId call) {}

  private record Page(List<Hint> rows, Hint upper) {}

  private record Dispatch(int processed, int hydrated) {}

  @FunctionalInterface
  public interface Wake {
    DbOperation<Boolean> apply(CallId call, Duration budget);

    default void reconcile(CallId call, DbOperation<Boolean> original, Duration budget) {}
  }

  private static final class PendingWake {
    final Hint hint;
    final long admitted = System.nanoTime();
    long reconcileAfter = admitted + Duration.ofMillis(500).toNanos();
    DbOperation<Boolean> operation;
    CompletableFuture<?> settled;
    final java.util.concurrent.atomic.AtomicBoolean observationRecorded =
        new java.util.concurrent.atomic.AtomicBoolean();

    PendingWake(Hint hint) {
      this.hint = hint;
    }
  }

  private static final int MAXIMUM_WAKES = 128, WAKES_PER_GROUP = 4;
  private final Map<CallId, PendingWake> wakes = new LinkedHashMap<>();
  private final Map<Integer, Integer> groupWakes = new HashMap<>();

  private final SqlTransactions sql;
  private final String cell, owner;
  private final long epoch;
  private final Supplier<Set<Integer>> groups;
  private final Wake wake;
  private volatile OperationObservation observer = OperationObservation.NONE;

  public CallRecoveryService observe(OperationObservation observation) {
    observer = Objects.requireNonNull(observation);
    return this;
  }

  private record WakeResult(Boolean hydrated, Throwable failure) {}

  private void observeHydration(PendingWake pending, SignalingMetrics.HydrationOutcome outcome) {
    if (pending.observationRecorded.compareAndSet(false, true))
      OperationObservation.safeHydration(
          observer, outcome, Duration.ofNanos(Math.max(0, System.nanoTime() - pending.admitted)));
  }

  // The authority domain has exactly 1024 groups. Each holds two keyset positions, never a
  // per-call backlog; hash versions remain part of the SQL key and frozen boundary.
  private final Hint[] cursors = new Hint[1024], uppers = new Hint[1024];
  private final BitSet completedGroups = new BitSet(1024);
  private int nextGroup;
  private boolean closed, started, draining;
  private CompletableFuture<DbOperation.PhysicalCompletion> active;
  private ScheduledExecutorService worker;
  private CompletableFuture<Void> drained;
  private long cycleStarted = System.nanoTime(), lastCycleNanos;

  /** Legacy standalone adapter; native composition always supplies tracked wake and partitions. */
  public CallRecoveryService(
      SqlTransactions sql,
      String cell,
      long epoch,
      BiFunction<CallId, Duration, CompletionStage<Boolean>> wake) {
    this(
        sql,
        cell,
        epoch,
        null,
        () -> {
          var all = new HashSet<Integer>();
          for (int group = 0; group < 1024; group++) all.add(group);
          return Set.copyOf(all);
        },
        (call, budget) -> {
          var stage = wake.apply(call, budget);
          return new DbOperation<>(
              stage, stage.handle((v, e) -> DbOperation.PhysicalCompletion.FINISHED));
        });
  }

  public CallRecoveryService(
      SqlTransactions sql,
      String cell,
      long epoch,
      String owner,
      Supplier<Set<Integer>> groups,
      Wake wake) {
    this.sql = Objects.requireNonNull(sql);
    this.cell = Objects.requireNonNull(cell);
    this.epoch = epoch;
    this.owner = owner;
    this.groups = Objects.requireNonNull(groups);
    this.wake = Objects.requireNonNull(wake);
  }

  public synchronized void start() {
    if (closed || started) throw new IllegalStateException("Recovery already started or drained");
    started = true;
    worker =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("call-recovery").daemon().factory());
    worker.scheduleWithFixedDelay(
        () -> {
          if (!closed) sweep();
        },
        0,
        100,
        TimeUnit.MILLISECONDS);
  }

  public CompletionStage<Report> sweep() {
    return sweepTracked(Duration.ofSeconds(2)).logical();
  }

  public DbOperation<Report> sweepTracked(Duration budget) {
    reconcileWakes();
    CompletableFuture<DbOperation.PhysicalCompletion> physical;
    synchronized (this) {
      if (closed || active != null)
        return new DbOperation<>(
            CompletableFuture.failedFuture(new DbOverloadedException()),
            CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.NOT_STARTED));
      physical = new CompletableFuture<>();
      active = physical;
    }
    var retained = Collections.synchronizedList(new ArrayList<CompletableFuture<?>>());
    long end = System.nanoTime() + Math.min(budget.toNanos(), Duration.ofSeconds(2).toNanos());
    CompletionStage<Report> logical;
    try {
      var partition = Set.copyOf(groups.get());
      if (partition.stream().anyMatch(group -> group < 0 || group > 1023))
        throw new IllegalArgumentException("Invalid recovery partition");
      logical =
          partition.isEmpty()
              ? CompletableFuture.completedFuture(report(0, 0, true))
              : pages(partition, end, 0, 0, 0, retained);
    } catch (RuntimeException failed) {
      logical = CompletableFuture.failedFuture(failed);
    }
    logical.whenComplete(
        (value, error) -> {
          CompletableFuture<?>[] receipts;
          synchronized (retained) {
            receipts = retained.toArray(CompletableFuture[]::new);
          }
          CompletableFuture.allOf(receipts)
              .whenComplete(
                  (done, failure) -> {
                    if (failure != null) physical.completeExceptionally(failure);
                    else {
                      synchronized (this) {
                        active = null;
                      }
                      physical.complete(DbOperation.PhysicalCompletion.FINISHED);
                    }
                  });
        });
    return new DbOperation<>(logical, physical.minimalCompletionStage());
  }

  private CompletionStage<Report> pages(
      Set<Integer> partition,
      long end,
      int page,
      int examined,
      int hydrated,
      List<CompletableFuture<?>> retained) {
    if (closed || page == 32 || System.nanoTime() >= end)
      return CompletableFuture.completedFuture(report(examined, hydrated, false));
    Hint after, cycleUpper;
    int selectedGroup;
    synchronized (this) {
      // Yield immediately when every physical slot is occupied. No scanner owner awaits a wake;
      // the next invocation resumes the same group priority when any slot becomes available.
      if (wakes.size() >= MAXIMUM_WAKES)
        return CompletableFuture.completedFuture(report(examined, hydrated, false));
      int candidate = nextGroup;
      while (!partition.contains(candidate)) candidate = (candidate + 1) % 1024;
      selectedGroup = candidate;
      nextGroup = (selectedGroup + 1) % 1024;
      after = cursors[selectedGroup];
      cycleUpper = uppers[selectedGroup];
    }
    var scan =
        sql.submitTracked(
            DbClass.RECOVERY,
            remaining(end),
            c -> {
              AuthoritySql.cellBarrier(c, false);
              AuthoritySql.validateCell(c, cell, epoch);
              var selected = c.createArrayOf("integer", new Integer[] {selectedGroup});
              String ownership =
                  owner == null
                      ? ""
                      : " AND (g.group_id IS NULL OR g.owner_node=? OR g.status<>'OWNED'"
                          + " OR g.lease_until<=clock_timestamp())";
              String base =
                  "SELECT s.ownership_hash_version,s.ownership_group_id,s.call_id FROM call_state s"
                      + (owner == null
                          ? ""
                          : " LEFT JOIN group_owner g ON g.cell_id=?"
                              + " AND g.ownership_hash_version=s.ownership_hash_version"
                              + " AND g.group_id=s.ownership_group_id")
                      + " WHERE s.terminal_at IS NULL AND s.ownership_group_id=ANY(?)"
                      + ownership;
              Hint boundary = cycleUpper;
              try {
                if (boundary == null) {
                  try (var q =
                      c.prepareStatement(
                          base
                              + " ORDER BY s.ownership_hash_version DESC,s.ownership_group_id"
                              + " DESC,s.call_id DESC LIMIT 1")) {
                    bindPartition(q, selected);
                    try (var r = q.executeQuery()) {
                      if (r.next())
                        boundary = new Hint(r.getLong(1), r.getInt(2), new CallId(r.getString(3)));
                    }
                  }
                }
                if (boundary == null) return new Page(List.of(), null);
                var hints = new ArrayList<Hint>();
                String suffix =
                    after == null
                        ? ""
                        : " AND (s.ownership_hash_version,s.ownership_group_id,s.call_id)>(?,?,?)";
                try (var q =
                    c.prepareStatement(
                        base
                            + " AND (s.ownership_hash_version,s.ownership_group_id,s.call_id)<=(?,?,?)"
                            + suffix
                            + " ORDER BY s.ownership_hash_version,s.ownership_group_id,s.call_id"
                            + " LIMIT 512")) {
                  int index = bindPartition(q, selected);
                  index = bindHint(q, index, boundary);
                  if (after != null) bindHint(q, index, after);
                  try (var r = q.executeQuery()) {
                    while (r.next())
                      hints.add(new Hint(r.getLong(1), r.getInt(2), new CallId(r.getString(3))));
                  }
                }
                return new Page(List.copyOf(hints), boundary);
              } finally {
                selected.free();
              }
            });
    return track(scan, retained)
        .thenCompose(
            result -> {
              var hints = result.rows();
              return dispatch(hints, 0, end, retained)
                  .thenCompose(
                      dispatched -> {
                        boolean pageComplete = dispatched.processed() == hints.size();
                        boolean complete =
                            pageComplete
                                && (hints.size() < 512 || hints.getLast().equals(result.upper()));
                        synchronized (this) {
                          if (complete) cursors[selectedGroup] = null;
                          else if (dispatched.processed() > 0)
                            cursors[selectedGroup] = hints.get(dispatched.processed() - 1);
                          uppers[selectedGroup] = complete ? null : result.upper();
                          if (complete) completedGroups.set(selectedGroup);
                          complete = partition.stream().allMatch(completedGroups::get);
                          if (complete) {
                            completedGroups.clear();
                            lastCycleNanos = System.nanoTime() - cycleStarted;
                            cycleStarted = System.nanoTime();
                          }
                        }
                        int scanned = examined + hints.size(),
                            total = hydrated + dispatched.hydrated();
                        // Continue bounded pages without a one-second delay between each 512-row
                        // page.
                        if (complete)
                          return CompletableFuture.completedFuture(
                              report(scanned, total, complete));
                        return pages(partition, end, page + 1, scanned, total, retained);
                      });
            });
  }

  private int bindPartition(java.sql.PreparedStatement query, java.sql.Array selected)
      throws java.sql.SQLException {
    int index = 1;
    if (owner != null) query.setString(index++, cell);
    query.setArray(index++, selected);
    if (owner != null) query.setString(index++, owner);
    return index;
  }

  private static int bindHint(java.sql.PreparedStatement query, int index, Hint hint)
      throws java.sql.SQLException {
    query.setLong(index++, hint.hashVersion());
    query.setInt(index++, hint.group());
    query.setString(index++, hint.call().value());
    return index;
  }

  private CompletionStage<Dispatch> dispatch(
      List<Hint> hints, int offset, long end, List<CompletableFuture<?>> retained) {
    if (offset >= hints.size() || System.nanoTime() >= end)
      return CompletableFuture.completedFuture(new Dispatch(0, 0));
    int processed = 0, hydrated = 0;
    for (int index = offset; index < hints.size() && System.nanoTime() < end; index++) {
      CompletableFuture<Boolean> operation;
      synchronized (this) {
        Hint hint = hints.get(index);
        // An already owned original wake can be passed. A quota-refused hint cannot: retain
        // the preceding keyset position so the admitted prefix never jumps ahead of it again.
        if (!wakes.containsKey(hint.call())
            && (wakes.size() >= MAXIMUM_WAKES
                || groupWakes.getOrDefault(hint.group(), 0) >= WAKES_PER_GROUP)) break;
        Duration budget = remaining(end);
        operation = handoff(hint, budget);
      }
      processed++;
      if (operation.isDone() && operation.getNow(false)) hydrated++;
    }
    return CompletableFuture.completedFuture(new Dispatch(processed, hydrated));
  }

  private synchronized CompletableFuture<Boolean> handoff(Hint hint, Duration budget) {
    var existing = wakes.get(hint.call());
    if (existing != null) return CompletableFuture.completedFuture(false);
    if (closed
        || wakes.size() >= MAXIMUM_WAKES
        || groupWakes.getOrDefault(hint.group(), 0) >= WAKES_PER_GROUP)
      return CompletableFuture.completedFuture(false);
    var pending = new PendingWake(hint);
    wakes.put(hint.call(), pending);
    groupWakes.merge(hint.group(), 1, Integer::sum);
    try {
      pending.operation = wake.apply(hint.call(), budget);
      pending.settled =
          pending
              .operation
              .physicalCompletion()
              .thenCombine(
                  pending.operation.logical().handle(WakeResult::new),
                  (cleanup, result) -> {
                    if (cleanup == null)
                      throw new IllegalStateException("Missing wake cleanup proof");
                    var outcome =
                        result.failure() != null || result.hydrated() == null
                            ? SignalingMetrics.HydrationOutcome.OUTCOME_UNKNOWN
                            : cleanup == DbOperation.PhysicalCompletion.FINISHED
                                    && result.hydrated()
                                ? SignalingMetrics.HydrationOutcome.HYDRATED
                                : SignalingMetrics.HydrationOutcome.NOT_HYDRATED;
                    observeHydration(pending, outcome);
                    return cleanup;
                  })
              .toCompletableFuture();
      pending.settled.whenComplete(
          (done, failure) -> {
            if (failure != null)
              observeHydration(pending, SignalingMetrics.HydrationOutcome.OUTCOME_UNKNOWN);
            if (failure == null)
              synchronized (CallRecoveryService.this) {
                release(pending);
              }
          });
      // This logical deadline releases only scanner latency; ownership and credits remain retained.
      return pending
          .operation
          .logical()
          .toCompletableFuture()
          .copy()
          .completeOnTimeout(false, Math.max(1, budget.toNanos()), TimeUnit.NANOSECONDS)
          .exceptionally(error -> false);
    } catch (RuntimeException notAdmitted) {
      if (pending.operation == null
          && (notAdmitted instanceof DbOverloadedException
              || notAdmitted
                  instanceof io.webrtc.signaling.actors.admission.EntityAdmission.Overloaded
              || notAdmitted instanceof IllegalArgumentException)) release(pending);
      else if (pending.settled == null) pending.settled = new CompletableFuture<>();
      return CompletableFuture.completedFuture(false);
    }
  }

  private void release(PendingWake pending) {
    if (wakes.remove(pending.hint.call(), pending))
      groupWakes.compute(pending.hint.group(), (key, count) -> count == 1 ? null : count - 1);
  }

  private void reconcileWakes() {
    List<PendingWake> due;
    synchronized (this) {
      if (closed && !draining) return;
      long now = System.nanoTime();
      due =
          wakes.values().stream()
              .filter(p -> p.operation != null && now >= p.reconcileAfter)
              .sorted(Comparator.comparingLong(p -> p.reconcileAfter))
              .limit(8)
              .toList();
      for (var pending : due) pending.reconcileAfter = now + Duration.ofMillis(500).toNanos();
    }
    for (var pending : due) {
      try {
        wake.reconcile(pending.hint.call(), pending.operation, Duration.ofMillis(250));
      } catch (RuntimeException unavailable) {
        // Same original work remains quarantined; the bounded next query can repair a lost receipt.
      }
    }
  }

  private synchronized Report report(int examined, int hydrated, boolean cycleComplete) {
    return new Report(
        examined,
        hydrated,
        Math.max(examined - hydrated, wakes.size()),
        cycleComplete && wakes.isEmpty());
  }

  private static <T> CompletionStage<T> track(
      DbOperation<T> operation, List<CompletableFuture<?>> retained) {
    var settled =
        operation
            .physicalCompletion()
            .thenCombine(operation.logical().handle((v, e) -> null), (a, b) -> null)
            .toCompletableFuture();
    retained.add(settled);
    return operation.logical().thenCompose(v -> settled.thenApply(done -> v));
  }

  private static Duration remaining(long end) {
    return Duration.ofNanos(Math.max(1, end - System.nanoTime()));
  }

  public synchronized long oldestUnreconciledNanos() {
    long now = System.nanoTime();
    long age = Math.max(lastCycleNanos, now - cycleStarted);
    for (var pending : wakes.values()) age = Math.max(age, now - pending.admitted);
    return age;
  }

  /** Separate telemetry projection; startup without a native scan has no progress observation. */
  public synchronized long observedProgressAgeNanos() {
    return active == null && lastCycleNanos == 0 && wakes.isEmpty()
        ? -1
        : oldestUnreconciledNanos();
  }

  public synchronized CompletionStage<Void> drain() {
    if (drained != null) return drained.minimalCompletionStage();
    closed = true;
    if (worker != null) worker.shutdown();
    if (!wakes.isEmpty() && !draining) {
      draining = true;
      worker =
          Executors.newSingleThreadScheduledExecutor(
              Thread.ofPlatform().name("call-recovery-drain").daemon().factory());
      worker.scheduleWithFixedDelay(this::reconcileWakes, 0, 100, TimeUnit.MILLISECONDS);
    }
    var retained = new ArrayList<CompletableFuture<?>>();
    if (active != null) retained.add(active);
    for (var pending : wakes.values()) if (pending.settled != null) retained.add(pending.settled);
    var settled = CompletableFuture.allOf(retained.toArray(CompletableFuture[]::new));
    drained = settled;
    settled.whenComplete(
        (done, failure) -> {
          if (failure == null)
            synchronized (CallRecoveryService.this) {
              draining = false;
              if (worker != null) worker.shutdown();
            }
        });
    return settled.minimalCompletionStage();
  }

  @Override
  public void close() {
    drain();
  }
}
