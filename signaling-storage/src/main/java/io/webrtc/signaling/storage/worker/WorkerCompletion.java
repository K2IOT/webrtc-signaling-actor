package io.webrtc.signaling.storage.worker;

import io.webrtc.signaling.storage.DbOperation;
import java.util.*;
import java.util.concurrent.*;

/** Worker credits include reply processing and independent physical cleanup. */
final class WorkerCompletion {
  private final List<CompletableFuture<?>> retained =
      Collections.synchronizedList(new ArrayList<>());

  <T> CompletionStage<T> track(DbOperation<T> operation) {
    retained.add(
        operation
            .physicalCompletion()
            .thenCombine(operation.logical().handle((v, e) -> null), (a, b) -> null)
            .toCompletableFuture());
    return operation
        .logical()
        .thenCompose(value -> operation.physicalCompletion().thenApply(cleanup -> value));
  }

  <T> CompletionStage<T> finish(CompletionStage<T> logical, Runnable release) {
    return finishTracked(logical, release).logical();
  }

  <T> DbOperation<T> finishTracked(CompletionStage<T> logical, Runnable release) {
    var physical = new CompletableFuture<DbOperation.PhysicalCompletion>();
    logical.whenComplete(
        (v, e) -> {
          CompletableFuture<?>[] cleanup;
          synchronized (retained) {
            cleanup = retained.toArray(CompletableFuture[]::new);
          }
          CompletableFuture.allOf(cleanup)
              .whenComplete(
                  (done, error) -> {
                    if (error != null) physical.completeExceptionally(error);
                    else {
                      release.run();
                      physical.complete(DbOperation.PhysicalCompletion.FINISHED);
                    }
                  });
        });
    return new DbOperation<>(logical, physical.minimalCompletionStage());
  }
}
