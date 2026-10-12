package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.observability.OperationObservation;
import io.webrtc.signaling.observability.SignalingMetrics.Outcome;
import io.webrtc.signaling.rpc.RpcOperation;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * Fixed process jobs, one physically retained invocation each. No catch-up burst after a JVM pause.
 */
public final class NativeWorkerScheduler implements AutoCloseable {
  public enum Priority {
    SAFETY,
    MAINTENANCE
  }

  public enum Status {
    COMPLETED,
    UNKNOWN,
    OVERLOADED
  }

  public record Event(String name, Status status, long elapsedNanos) {}

  public record Job(
      String name, Priority priority, Duration period, Function<Duration, RpcOperation<?>> run) {
    public Job {
      if (name == null || !name.matches("[a-z][a-z0-9_-]{0,63}"))
        throw new IllegalArgumentException("Invalid worker name");
      Objects.requireNonNull(priority);
      Objects.requireNonNull(period);
      Objects.requireNonNull(run);
      if (period.compareTo(Duration.ofMillis(name.equals("control_outbox") ? 20 : 100)) < 0
          || period.compareTo(Duration.ofSeconds(30)) > 0)
        throw new IllegalArgumentException("Worker period outside bound");
    }
  }

  private static final long BUDGET_NANOS = Duration.ofSeconds(2).toNanos();

  private static final class Invocation {
    final long start = System.nanoTime();
    OperationObservation.Receipt observation;
    boolean executed;
    final CompletableFuture<Void> physical = new CompletableFuture<>();
    final CompletableFuture<Status> logical = new CompletableFuture<>();
  }

  private static final class Slot {
    final Job job;
    ScheduledFuture<?> periodic;
    ScheduledFuture<?> wakeTimer;
    boolean wakePending;
    Invocation active;

    Slot(Job job) {
      this.job = job;
    }
  }

  private final List<Slot> slots;
  private final Consumer<Event> events;
  private Function<String, OperationObservation.Receipt> observations =
      name -> OperationObservation.NONE.start(null);
  private final List<Supplier<CompletionStage<Void>>> ownerDrains;
  private final ScheduledThreadPoolExecutor timers =
      new ScheduledThreadPoolExecutor(
          1, Thread.ofPlatform().daemon().name("native-worker-timer").factory());
  private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
  private boolean started, shed, draining;
  private final CompletableFuture<Void> settled = new CompletableFuture<>();
  private final CompletableFuture<Void> drained = new CompletableFuture<>();

  public NativeWorkerScheduler(List<Job> jobs, Consumer<Event> events) {
    this(jobs, events, List.of());
  }

  public NativeWorkerScheduler(
      List<Job> jobs, Consumer<Event> events, List<Supplier<CompletionStage<Void>>> ownerDrains) {
    if (ownerDrains == null || ownerDrains.size() > 16)
      throw new IllegalArgumentException("Worker lifecycle owners exceed bound");
    this.ownerDrains = List.copyOf(ownerDrains);
    if (jobs == null
        || jobs.isEmpty()
        || jobs.size() > 16
        || jobs.stream().map(Job::name).distinct().count() != jobs.size())
      throw new IllegalArgumentException("Workers require 1..16 distinct fixed jobs");
    this.events = Objects.requireNonNull(events);
    slots = List.copyOf(jobs).stream().map(Slot::new).toList();
    timers.setRemoveOnCancelPolicy(true);
    settled.whenComplete(
        (ignored, failure) ->
            Thread.startVirtualThread(
                () -> {
                  long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                  try {
                    for (var executor : List.of(tasks, timers)) {
                      long left = end - System.nanoTime();
                      if (left <= 0 || !executor.awaitTermination(left, TimeUnit.NANOSECONDS))
                        throw new TimeoutException("Native worker executor termination unproven");
                    }
                    CompletableFuture.allOf(
                            this.ownerDrains.stream()
                                .map(
                                    owner ->
                                        Objects.requireNonNull(owner.get()).toCompletableFuture())
                                .toArray(CompletableFuture[]::new))
                        .whenComplete(
                            (v, error) -> {
                              if (error == null) drained.complete(null);
                              else drained.completeExceptionally(error);
                            });
                  } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    drained.completeExceptionally(interrupted);
                  } catch (Exception unknown) {
                    drained.completeExceptionally(unknown);
                  }
                }));
  }

  public synchronized NativeWorkerScheduler observe(
      Function<String, OperationObservation.Receipt> observer) {
    if (started) throw new IllegalStateException("Worker observations must precede start");
    observations = Objects.requireNonNull(observer);
    return this;
  }

  public synchronized boolean installedJobs(Set<String> required) {
    return started
        && !draining
        && slots.stream()
            .map(slot -> slot.job.name())
            .collect(java.util.stream.Collectors.toSet())
            .containsAll(required);
  }

  public synchronized void start() {
    if (started || draining) throw new IllegalStateException("Workers already started or drained");
    started = true;
    for (var slot : slots)
      slot.periodic =
          timers.scheduleWithFixedDelay(
              () -> tick(slot), 0, slot.job.period().toNanos(), TimeUnit.NANOSECONDS);
  }

  /**
   * One coalesced wake per fixed slot, retained behind that slot's original physical invocation.
   */
  public synchronized void wake(String name) {
    var slot =
        slots.stream()
            .filter(s -> s.job.name().equals(name))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown worker"));
    if (draining || shed && slot.job.priority() != Priority.SAFETY) return;
    slot.wakePending = true;
    scheduleWake(slot);
  }

  private void scheduleWake(Slot slot) {
    if (!started
        || draining
        || slot.active != null
        || slot.wakeTimer != null
        || shed && slot.job.priority() != Priority.SAFETY) return;
    slot.wakeTimer =
        timers.schedule(
            () -> {
              synchronized (this) {
                slot.wakeTimer = null;
                if (!slot.wakePending) return;
              }
              tick(slot);
            },
            0,
            TimeUnit.NANOSECONDS);
  }

  private void tick(Slot slot) {
    Invocation invocation;
    synchronized (this) {
      if (draining || shed && slot.job.priority() != Priority.SAFETY || slot.active != null) return;
      invocation = new Invocation();
      slot.active = invocation;
      slot.wakePending = false;
    }
    try {
      invocation.observation =
          OperationObservation.safeReceipt(observations.apply(slot.job.name()));
    } catch (RuntimeException ignored) {
      invocation.observation = OperationObservation.NONE.start(null);
    }
    invocation.logical.whenComplete(
        (status, error) -> {
          try {
            invocation.observation.logical(
                switch (status) {
                  case COMPLETED -> Outcome.READ;
                  case UNKNOWN -> Outcome.OUTCOME_UNKNOWN;
                  case OVERLOADED -> Outcome.BACKPRESSURE;
                });
            events.accept(
                new Event(
                    slot.job.name(), status, Math.max(0, System.nanoTime() - invocation.start)));
          } catch (RuntimeException ignored) {
            /* Metrics callback cannot replace native completion authority. */
          }
        });
    ScheduledFuture<?> expiry;
    try {
      expiry =
          timers.schedule(
              () -> invocation.logical.complete(Status.UNKNOWN),
              BUDGET_NANOS,
              TimeUnit.NANOSECONDS);
    } catch (RejectedExecutionException notStarted) {
      invocation.logical.complete(Status.OVERLOADED);
      retire(slot, invocation);
      return;
    }
    try {
      tasks.execute(
          () -> {
            try (var scope = invocation.observation.activate()) {
              invocation.observation.queue(Math.max(0, System.nanoTime() - invocation.start));
              long remaining = BUDGET_NANOS - (System.nanoTime() - invocation.start);
              if (remaining <= 0) {
                invocation.logical.complete(Status.OVERLOADED);
                expiry.cancel(false);
                retire(slot, invocation);
                return;
              }
              invocation.executed = true;
              var operation =
                  Objects.requireNonNull(slot.job.run().apply(Duration.ofNanos(remaining)));
              operation
                  .logical()
                  .whenComplete(
                      (value, error) -> {
                        invocation.logical.complete(
                            error == null ? Status.COMPLETED : Status.UNKNOWN);
                        expiry.cancel(false);
                      });
              operation
                  .physicalCompletion()
                  .thenCombine(operation.logical().handle((v, e) -> null), (a, b) -> null)
                  .whenComplete(
                      (v, error) -> {
                        if (error == null) retire(slot, invocation);
                        else invocation.logical.complete(Status.UNKNOWN);
                      });
            } catch (Throwable unknown) {
              invocation.logical.complete(Status.UNKNOWN);
              expiry.cancel(
                  false); /* A throwing factory supplies no proof that its effects never started. */
            }
          });
    } catch (RejectedExecutionException notStarted) {
      expiry.cancel(false);
      invocation.logical.complete(Status.OVERLOADED);
      retire(slot, invocation);
    }
  }

  private void retire(Slot slot, Invocation invocation) {
    boolean complete;
    synchronized (this) {
      if (slot.active != invocation) return;
      slot.active = null;
      complete = draining && slots.stream().allMatch(s -> s.active == null);
      if (slot.wakePending) scheduleWake(slot);
    }
    try {
      invocation.observation.physical(invocation.executed);
    } catch (RuntimeException ignored) {
      /* Independent observation only. */
    }
    invocation.physical.complete(null);
    if (complete) {
      timers.shutdown();
      settled.complete(null);
    }
  }

  public synchronized void shedNormal() {
    shed = true;
    for (var slot : slots)
      if (slot.job.priority() != Priority.SAFETY) {
        if (slot.periodic != null) slot.periodic.cancel(false);
        if (slot.wakeTimer != null) slot.wakeTimer.cancel(false);
        slot.wakePending = false;
      }
  }

  public synchronized CompletionStage<Void> settleAdmitted() {
    return CompletableFuture.allOf(
            slots.stream()
                .filter(s -> s.active != null)
                .map(s -> s.active.physical)
                .toArray(CompletableFuture[]::new))
        .minimalCompletionStage();
  }

  public CompletionStage<Void> drain() {
    boolean complete;
    synchronized (this) {
      draining = true;
      for (var slot : slots) {
        if (slot.periodic != null) slot.periodic.cancel(false);
        if (slot.wakeTimer != null) slot.wakeTimer.cancel(false);
        slot.wakePending = false;
      }
      tasks.shutdown();
      complete = slots.stream().allMatch(s -> s.active == null);
    }
    if (complete) {
      timers.shutdown();
      settled.complete(null);
    }
    return drained.minimalCompletionStage();
  }

  @Override
  public void close() {
    drain();
  }
}
