package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.app.runtime.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class NativeCellHealthSourceIT {
  @Test
  void originalPhysicalCallbackCannotFinishDrainWhileItsNextNativePollStillOwnsSql()
      throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture();
        var lock = f.connection()) {
      var source = new NativeCellHealthSource(new PrimaryCellFacts(f.runtime.sql, "c001", 1));
      var violation = new java.util.concurrent.atomic.AtomicBoolean();
      var callback = new java.util.concurrent.CompletableFuture<Void>();
      var second =
          new java.util.concurrent.atomic.AtomicReference<
              io.webrtc.signaling.rpc.RpcOperation<PrimaryCellFacts.Facts>>();
      var first = source.poll(Duration.ofSeconds(2));
      first
          .physicalCompletion()
          .thenRun(
              () -> {
                try {
                  lock.setAutoCommit(false);
                  try (var query = lock.createStatement()) {
                    query.execute("LOCK TABLE cell_authority IN ACCESS EXCLUSIVE MODE");
                  }
                  var next = source.poll(Duration.ofSeconds(2));
                  second.set(next);
                  assertThat(next.physicalCompletion().toCompletableFuture()).isNotDone();
                  source
                      .drain()
                      .thenRun(
                          () -> {
                            if (!next.physicalCompletion().toCompletableFuture().isDone())
                              violation.set(true);
                          });
                  callback.complete(null);
                } catch (Throwable failure) {
                  callback.completeExceptionally(failure);
                }
              });
      callback.get(2, TimeUnit.SECONDS);
      first.physicalCompletion().toCompletableFuture().get(2, TimeUnit.SECONDS);
      lock.rollback();
      second.get().physicalCompletion().toCompletableFuture().get(2, TimeUnit.SECONDS);
      source.drain().toCompletableFuture().get(2, TimeUnit.SECONDS);
      assertThat(violation).isFalse();
    }
  }

  @Test
  void actualPrimaryWorkerFeedsOnlyFreshHealthFactsAndDrainIsAbsorbing() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var source = new NativeCellHealthSource(new PrimaryCellFacts(f.runtime.sql, "c001", 1));
      assertThat(source.usable()).isFalse();
      var poll = source.poll(Duration.ofSeconds(2));
      assertThat(
              poll.logical()
                  .toCompletableFuture()
                  .get(2, TimeUnit.SECONDS)
                  .usable(System.nanoTime()))
          .isTrue();
      poll.physicalCompletion().toCompletableFuture().get(2, TimeUnit.SECONDS);
      assertThat(source.usable()).isTrue();
      try (var c = f.connection();
          var update = c.createStatement()) {
        update.executeUpdate("UPDATE cell_authority SET status='RECOVERING' WHERE singleton_id=1");
      }
      var job = source.job(Duration.ofMillis(100));
      var fresh = job.run().apply(Duration.ofSeconds(2));
      assertThat(fresh.logical().toCompletableFuture().get(2, TimeUnit.SECONDS))
          .isInstanceOfSatisfying(
              PrimaryCellFacts.Facts.class,
              facts -> assertThat(facts.status()).isEqualTo("RECOVERING"));
      fresh.physicalCompletion().toCompletableFuture().get(2, TimeUnit.SECONDS);
      assertThat(source.usable()).isFalse();
      var events = new java.util.concurrent.ConcurrentLinkedQueue<NativeWorkerScheduler.Event>();
      try (var scheduler = new NativeWorkerScheduler(List.of(job), events::add)) {
        scheduler.start();
        org.awaitility.Awaitility.await()
            .atMost(Duration.ofSeconds(2))
            .until(
                () ->
                    events.stream()
                        .anyMatch(
                            event ->
                                event.name().equals("cell_primary")
                                    && event.status() == NativeWorkerScheduler.Status.COMPLETED));
        source.drain().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertThat(source.usable()).isFalse();
        assertThat(source.poll(Duration.ofSeconds(2)).logical().toCompletableFuture())
            .isCompletedExceptionally();
        scheduler.drain().toCompletableFuture().get(2, TimeUnit.SECONDS);
      }
    }
  }
}
