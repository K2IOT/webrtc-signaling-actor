package io.webrtc.signaling.rpc;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.storage.*;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class NativeReadRetryTest {
  @Test
  void knownReadConflictRetriesOnlyAfterOriginalPhysicalCleanup() throws Exception {
    var starts = new AtomicInteger();
    var physical = new CompletableFuture<DbOperation.PhysicalCompletion>();
    var result =
        NativeReadRetry.execute(
            budget -> {
              if (starts.incrementAndGet() == 1)
                return new DbOperation<String>(
                    CompletableFuture.failedFuture(new AuthoritySql.RetryableConflict()), physical);
              return new DbOperation<>(
                  CompletableFuture.completedFuture("native-read"),
                  CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.FINISHED));
            },
            Duration.ofSeconds(1));
    Thread.sleep(80);
    assertThat(starts).hasValue(1);
    assertThat(result.logical().toCompletableFuture()).isNotDone();
    assertThat(result.physicalCompletion().toCompletableFuture()).isNotDone();
    physical.complete(DbOperation.PhysicalCompletion.FINISHED);
    assertThat(result.logical().toCompletableFuture().get(500, TimeUnit.MILLISECONDS))
        .isEqualTo("native-read");
    result.physicalCompletion().toCompletableFuture().get(500, TimeUnit.MILLISECONDS);
    assertThat(starts).hasValue(2);
  }

  @Test
  void unknownCleanupCannotCreateReplacementRead() throws Exception {
    var starts = new AtomicInteger();
    var physical = new CompletableFuture<DbOperation.PhysicalCompletion>();
    var result =
        NativeReadRetry.execute(
            budget -> {
              starts.incrementAndGet();
              return new DbOperation<String>(
                  CompletableFuture.failedFuture(new AuthoritySql.RetryableConflict()), physical);
            },
            Duration.ofMillis(60));
    assertThatThrownBy(() -> result.logical().toCompletableFuture().get(500, TimeUnit.MILLISECONDS))
        .hasCauseInstanceOf(TimeoutException.class);
    assertThat(starts).hasValue(1);
    assertThat(result.physicalCompletion().toCompletableFuture()).isNotDone();
    physical.completeExceptionally(new IllegalStateException("TEST_ONLY_UNKNOWN_CLEANUP"));
    assertThat(result.physicalCompletion().toCompletableFuture()).isNotDone();
  }

  @Test
  void onlyKnownReadPressureRetriesAtMostTwiceAndConsumesOriginalBudget() throws Exception {
    var starts = new AtomicInteger();
    var original = Duration.ofSeconds(1);
    var result =
        NativeReadRetry.execute(
            budget -> {
              assertThat(budget).isPositive().isLessThan(original);
              starts.incrementAndGet();
              return new DbOperation<String>(
                  CompletableFuture.failedFuture(new AuthoritySql.RetryableConflict()),
                  CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.FINISHED));
            },
            original);
    assertThatThrownBy(() -> result.logical().toCompletableFuture().get(500, TimeUnit.MILLISECONDS))
        .hasCauseInstanceOf(AuthoritySql.RetryableConflict.class);
    result.physicalCompletion().toCompletableFuture().get(500, TimeUnit.MILLISECONDS);
    assertThat(starts).hasValue(3);
    var deniedStarts = new AtomicInteger();
    var denied =
        NativeReadRetry.execute(
            budget -> {
              deniedStarts.incrementAndGet();
              return new DbOperation<String>(
                  CompletableFuture.failedFuture(new AuthoritySql.FencedException()),
                  CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.FINISHED));
            },
            original);
    assertThatThrownBy(() -> denied.logical().toCompletableFuture().get(500, TimeUnit.MILLISECONDS))
        .hasCauseInstanceOf(AuthoritySql.FencedException.class);
    assertThat(deniedStarts).hasValue(1);
  }
}
