package io.webrtc.signaling.rpc;

import io.webrtc.signaling.storage.*;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;

/** Bounded retry of pure native reconciliation reads; never repeats a mutation or RPC. */
final class NativeReadRetry {
  static <T> RpcOperation<T> execute(Function<Duration, DbOperation<T>> read, Duration budget) {
    Objects.requireNonNull(read);
    if (budget == null
        || budget.isZero()
        || budget.isNegative()
        || budget.compareTo(Duration.ofSeconds(2)) > 0)
      throw new IllegalArgumentException("Invalid native read budget");
    var result = new CompletableFuture<T>();
    var scope = new PhysicalScope();
    long end = System.nanoTime() + budget.toNanos();
    attempt(read, end, 0, result, scope);
    result.orTimeout(Math.max(1, end - System.nanoTime()), TimeUnit.NANOSECONDS);
    return scope.seal(result);
  }

  private static <T> void attempt(
      Function<Duration, DbOperation<T>> read,
      long end,
      int retry,
      CompletableFuture<T> result,
      PhysicalScope scope) {
    long left = end - System.nanoTime();
    if (result.isDone()) return;
    if (left <= 0) {
      result.completeExceptionally(new TimeoutException("Original native read deadline"));
      return;
    }
    var originalReceipt = new CompletableFuture<Void>();
    scope.track(new RpcOperation<>(CompletableFuture.completedFuture(null), originalReceipt));
    final DbOperation<T> original;
    try {
      original = Objects.requireNonNull(read.apply(Duration.ofNanos(left)));
    } catch (Throwable unknown) {
      result.completeExceptionally(unknown);
      return;
    }
    original
        .physicalCompletion()
        .whenComplete(
            (physical, error) -> {
              if (error == null && physical != null) originalReceipt.complete(null);
            });
    original
        .logical()
        .whenComplete(
            (value, error) -> {
              if (error == null) {
                result.complete(value);
                return;
              }
              var cause = error;
              while (cause instanceof CompletionException && cause.getCause() != null)
                cause = cause.getCause();
              if (retry >= 2
                  || !(cause instanceof AuthoritySql.RetryableConflict
                      || cause instanceof DbOverloadedException)) {
                result.completeExceptionally(cause);
                return;
              }
              originalReceipt.whenComplete(
                  (v, e) -> {
                    if (result.isDone()) return;
                    long delay = ThreadLocalRandom.current().nextLong(10L << retry, 20L << retry);
                    CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS)
                        .execute(() -> attempt(read, end, retry + 1, result, scope));
                  });
            });
  }
}
