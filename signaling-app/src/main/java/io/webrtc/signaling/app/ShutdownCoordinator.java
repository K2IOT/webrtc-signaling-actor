package io.webrtc.signaling.app;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * Application-owned non-blocking drain. Hooks must observe real framework handoff and physical
 * cleanup.
 */
public final class ShutdownCoordinator implements AutoCloseable {
  public enum Status {
    COMPLETE,
    TIMED_OUT,
    FAILED
  }

  public interface Hooks {
    void readinessOff();

    void shedIngress();

    CompletionStage<Integer> reconnectBatch(int maximum);

    CompletionStage<Void> settleAdmitted();

    CompletionStage<Void> handoffAndRelease();

    CompletionStage<Void> leaveCluster();

    CompletionStage<Void> closeDatabase();
  }

  private final Hooks hooks;
  private final ScheduledExecutorService timer =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().daemon().name("signaling-drain").factory());
  private final CompletableFuture<Status> completion = new CompletableFuture<>();
  private final CompletionStage<Status> exposed = completion.minimalCompletionStage();
  private boolean started, closed;
  private long deadline;

  public ShutdownCoordinator(Hooks hooks) {
    this.hooks = Objects.requireNonNull(hooks);
  }

  public synchronized CompletionStage<Status> shutdown(
      SignalingApplication.Plane plane, Duration budget) {
    Objects.requireNonNull(plane);
    Duration maximum = Duration.ofSeconds(plane == SignalingApplication.Plane.GATEWAY ? 300 : 65);
    if (budget == null || budget.isZero() || budget.isNegative() || budget.compareTo(maximum) > 0)
      throw new IllegalArgumentException("Drain budget exceeds termination headroom");
    if (started) return exposed;
    if (closed) throw new IllegalStateException("Drain closed");
    started = true;
    deadline = System.nanoTime() + budget.toNanos();
    timer.schedule(
        () -> completion.complete(Status.TIMED_OUT), budget.toNanos(), TimeUnit.NANOSECONDS);
    try {
      hooks.readinessOff();
      hooks.shedIngress();
      CompletionStage<Void> work =
          plane == SignalingApplication.Plane.GATEWAY
              ? reconnect()
              : CompletableFuture.completedFuture(null);
      work =
          work.thenCompose(v -> advance(hooks::settleAdmitted))
              .thenCompose(v -> advance(hooks::handoffAndRelease))
              .thenCompose(v -> advance(hooks::leaveCluster))
              .thenCompose(v -> advance(hooks::closeDatabase));
      work.whenComplete(
          (v, e) -> {
            if (e == null) completion.complete(Status.COMPLETE);
            else if (e instanceof Expired || e.getCause() instanceof Expired)
              completion.complete(Status.TIMED_OUT);
            else completion.complete(Status.FAILED);
          });
    } catch (RuntimeException failed) {
      completion.complete(Status.FAILED);
    }
    return exposed;
  }

  private CompletionStage<Void> reconnect() {
    if (!active()) return CompletableFuture.failedFuture(new Expired());
    return hooks
        .reconnectBatch(128)
        .thenCompose(
            remaining -> {
              if (remaining == null || remaining < 0)
                throw new IllegalArgumentException("Invalid drain count");
              if (remaining == 0) return CompletableFuture.completedFuture(null);
              var next = new CompletableFuture<Void>();
              timer.schedule(
                  () -> {
                    try {
                      reconnect()
                          .whenComplete(
                              (v, e) -> {
                                if (e == null) next.complete(null);
                                else next.completeExceptionally(e);
                              });
                    } catch (RuntimeException failure) {
                      next.completeExceptionally(failure);
                    }
                  },
                  100,
                  TimeUnit.MILLISECONDS);
              return next;
            });
  }

  private boolean active() {
    return !completion.isDone() && System.nanoTime() - deadline < 0;
  }

  private CompletionStage<Void> advance(java.util.function.Supplier<CompletionStage<Void>> action) {
    return active()
        ? Objects.requireNonNull(action.get())
        : CompletableFuture.failedFuture(new Expired());
  }

  private static final class Expired extends RuntimeException {}

  @Override
  public synchronized void close() {
    closed = true;
    if (started && !completion.isDone()) completion.complete(Status.FAILED);
    timer.shutdown();
  }
}
