package io.webrtc.signaling.storage;

import io.webrtc.signaling.observability.*;
import io.webrtc.signaling.observability.SignalingMetrics.*;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.concurrent.*;

/** Logical expiry never cancels physical work or releases its credit. */
public final class DbBoundary implements AutoCloseable {
  private final DbAdmission admission;
  private volatile OperationObservation observer = OperationObservation.NONE;
  private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
  private final ScheduledThreadPoolExecutor deadlines =
      new ScheduledThreadPoolExecutor(
          1, Thread.ofPlatform().daemon().name("db-deadlines").factory());
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private boolean accepting = true;
  private int physicalTasks;
  private final java.util.Set<CompletableFuture<?>> activePhysical = new java.util.HashSet<>();

  public DbBoundary(DbAdmission admission) {
    this.admission = admission;
    deadlines.setRemoveOnCancelPolicy(true);
  }

  public void observe(OperationObservation observer) {
    this.observer = java.util.Objects.requireNonNull(observer);
  }

  public synchronized int physicalPending() {
    return physicalTasks;
  }

  public <T> CompletionStage<T> submit(DbClass clazz, Duration budget, Callable<T> tx) {
    return submitTracked(clazz, budget, tx).logical();
  }

  public DbCost maximumCharge(DbClass clazz) {
    return admission.maximumCharge(clazz);
  }

  public void requireProductionProfiles() {
    admission.requireProductionProfiles();
  }

  public <T> DbOperation<T> submitTracked(DbClass clazz, Duration budget, Callable<T> tx) {
    return submitTracked(clazz, budget, maximumCharge(clazz), tx);
  }

  public <T> DbOperation<T> submitTracked(
      DbClass clazz, Duration budget, DbCost cost, Callable<T> tx) {
    return submitReserved(clazz, budget, cost, tx, false);
  }

  static final class Validated<T> {
    private final T value;
    private final long bound;

    private Validated(T value, long bound) {
      this.value = value;
      this.bound = bound;
    }
  }

  static <T> Validated<T> beforeCommit(T value, long bound) throws ReflectiveOperationException {
    validateBytes(value, 0, new DtoBudget(bound));
    return new Validated<>(value, bound);
  }

  <T> DbOperation<T> submitTransactionTracked(
      DbClass clazz, Duration budget, DbCost cost, Callable<Validated<T>> tx) {
    return submitReserved(clazz, budget, cost, tx, true);
  }

  private <T> DbOperation<T> submitReserved(
      DbClass clazz, Duration budget, DbCost cost, Callable<?> tx, boolean validatedTransaction) {
    final long enqueued = System.nanoTime();
    var observation =
        OperationObservation.safeStart(
            observer, clazz != null && clazz.safety() ? Operation.DB_SAFETY : Operation.DB_CONTROL);
    if (clazz == null
        || cost == null
        || tx == null
        || budget == null
        || budget.isNegative()
        || budget.isZero()) {
      observation.logical(Outcome.BACKPRESSURE);
      observation.physical(false);
      return rejected();
    }
    final CompletableFuture<T> result;
    final CompletableFuture<DbOperation.PhysicalCompletion> physical;
    synchronized (this) {
      if (!accepting
          || !admission.acquire(clazz, cost, budget.toNanos() - (System.nanoTime() - enqueued))) {
        observation.logical(Outcome.BACKPRESSURE);
        observation.physical(false);
        return rejected();
      }
      result = new CompletableFuture<>();
      physical = new CompletableFuture<>();
      physicalTasks++;
      activePhysical.add(physical);
    }
    result.whenComplete(
        (value, error) -> {
          try {
            observation.logical(
                error == null
                    ? Outcome.COMMITTED
                    : error instanceof DbOutcomeUnknownException
                        ? Outcome.OUTCOME_UNKNOWN
                        : error instanceof DbOverloadedException
                            ? Outcome.BACKPRESSURE
                            : Outcome.INTERNAL_ERROR);
          } catch (RuntimeException ignored) {
            /* Completion belongs to the original operation. */
          }
        });
    physical.whenComplete(
        (value, error) -> {
          if (error == null)
            try {
              observation.physical(value == DbOperation.PhysicalCompletion.FINISHED);
            } catch (RuntimeException ignored) {
              /* Independent physical observation. */
            }
        });
    ScheduledFuture<?> expiry;
    try {
      expiry =
          deadlines.schedule(
              () -> result.completeExceptionally(new DbOutcomeUnknownException()),
              Math.max(1, budget.toNanos() - (System.nanoTime() - enqueued)),
              TimeUnit.NANOSECONDS);
    } catch (RuntimeException e) {
      admission.release(clazz, cost);
      physical.complete(DbOperation.PhysicalCompletion.NOT_STARTED);
      result.completeExceptionally(new DbOverloadedException());
      retired(physical);
      return rejected();
    }
    try {
      tasks.execute(
          () -> {
            try (var scope = observation.activate()) {
              observation.queue(Math.max(0, System.nanoTime() - enqueued));
              if (!result.isDone()) {
                Object returned = tx.call();
                Object value;
                if (validatedTransaction) {
                  if (!(returned instanceof Validated<?> checked)
                      || checked.bound != cost.resultBytes())
                    throw new IllegalArgumentException(
                        "Missing original pre-COMMIT result validation");
                  value = checked.value;
                } else {
                  validateBytes(returned, 0, new DtoBudget(cost.resultBytes()));
                  value = returned;
                }
                @SuppressWarnings("unchecked")
                T typed = (T) value;
                result.complete(typed);
              }
            } catch (Throwable e) {
              result.completeExceptionally(e);
            } finally {
              expiry.cancel(false);
              admission.release(clazz, cost);
              physical.complete(DbOperation.PhysicalCompletion.FINISHED);
              retired(physical);
            }
          });
    } catch (RejectedExecutionException e) {
      expiry.cancel(false);
      admission.release(clazz, cost);
      result.completeExceptionally(new DbOverloadedException());
      physical.complete(DbOperation.PhysicalCompletion.NOT_STARTED);
      retired(physical);
    }
    return new DbOperation<>(result.minimalCompletionStage(), physical.minimalCompletionStage());
  }

  private static <T> DbOperation<T> rejected() {
    return new DbOperation<>(
        CompletableFuture.<T>failedFuture(new DbOverloadedException()).minimalCompletionStage(),
        CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.NOT_STARTED)
            .minimalCompletionStage());
  }

  static void validate(Object value, int depth) throws ReflectiveOperationException {
    validateBytes(value, depth, new DtoBudget(32L * 1024 * 1024));
  }

  private static final class DtoBudget {
    private long remaining;

    private DtoBudget(long bytes) {
      if (bytes < 1) throw new IllegalArgumentException("Invalid DTO byte bound");
      remaining = bytes;
    }

    private void add(long bytes) {
      if (bytes < 0 || bytes > remaining)
        throw new IllegalArgumentException("DTO aggregate byte bound exceeded");
      remaining -= bytes;
    }
  }

  private static void validateBytes(Object value, int depth, DtoBudget budget)
      throws ReflectiveOperationException {
    budget.add(8); // Every value/reference consumes aggregate capacity, including nulls.
    if (value == null) return;
    if (depth > 16) throw new IllegalArgumentException("DTO nesting exceeds bound");
    Class<?> c = value.getClass();
    if (c == String.class) {
      // Count UTF-8 without allocating another full byte array. Java replaces malformed
      // surrogate code units with one byte in its UTF-8 encoder.
      String text = (String) value;
      for (int i = 0; i < text.length(); i++) {
        char ch = text.charAt(i);
        if (ch < 0x80) budget.add(1);
        else if (ch < 0x800) budget.add(2);
        else if (Character.isHighSurrogate(ch)
            && i + 1 < text.length()
            && Character.isLowSurrogate(text.charAt(i + 1))) {
          budget.add(4);
          i++;
        } else budget.add(Character.isSurrogate(ch) ? 1 : 3);
      }
      return;
    }
    if (c == java.math.BigInteger.class) {
      // PG WAL LSN is unsigned 64-bit. Exact class excludes mutable overriding subclasses.
      budget.add(32L + (((java.math.BigInteger) value).bitLength() + 7L) / 8L);
      return;
    }
    if (c == Boolean.class
        || c == Byte.class
        || c == Short.class
        || c == Integer.class
        || c == Long.class
        || c == Float.class
        || c == Double.class
        || c == java.util.UUID.class
        || c == java.time.Instant.class
        || c == Duration.class
        || c.isEnum()) {
      budget.add(32);
      return;
    }
    if (c.isRecord() && Modifier.isFinal(c.getModifiers())) {
      budget.add(64);
      for (var part : c.getRecordComponents()) {
        var accessor = part.getAccessor();
        if (!accessor.trySetAccessible()) throw new IllegalArgumentException("Inaccessible DTO");
        validateBytes(accessor.invoke(value), depth + 1, budget);
      }
      return;
    }
    if (c.getName().startsWith("java.util.ImmutableCollections$")
        && value instanceof java.util.Collection<?> list) {
      budget.add(32);
      for (Object item : list) validateBytes(item, depth + 1, budget);
      return;
    }
    if (value instanceof java.util.Optional<?> optional) {
      budget.add(16);
      validateBytes(optional.orElse(null), depth + 1, budget);
      return;
    }
    throw new IllegalArgumentException("Only immutable native DTOs may leave database boundary");
  }

  private synchronized void retired(CompletableFuture<?> physical) {
    activePhysical.remove(physical);
    physicalTasks--;
    if (!accepting && physicalTasks == 0) {
      deadlines.shutdown();
      drained.complete(null);
    }
  }

  public synchronized CompletionStage<Void> settleAdmitted() {
    return CompletableFuture.allOf(activePhysical.toArray(CompletableFuture[]::new))
        .minimalCompletionStage();
  }

  /** Call after framework handoff and native root release, before closing either database pool. */
  public synchronized CompletionStage<Void> drain() {
    if (accepting) {
      accepting = false;
      tasks.shutdown();
      if (physicalTasks == 0) {
        deadlines.shutdown();
        drained.complete(null);
      }
    }
    return drained.minimalCompletionStage();
  }

  @Override
  public void close() {
    drain();
  }
}
