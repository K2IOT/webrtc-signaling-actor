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
            .handle((v, e) -> null)
            .thenCombine(operation.logical().handle((v, e) -> null), (a, b) -> null)
            .toCompletableFuture());
    return operation
        .logical()
        .thenCompose(value -> operation.physicalCompletion().thenApply(cleanup -> value));
  }

  <T> CompletionStage<T> finish(CompletionStage<T> logical, Runnable release) {
    var exposed = new CompletableFuture<T>();
    logical.whenComplete(
        (v, e) -> {
          CompletableFuture<?>[] cleanup;
          synchronized (retained) {
            cleanup = retained.toArray(CompletableFuture[]::new);
          }
          var all = CompletableFuture.allOf(cleanup);
          if (all.isDone()) release.run();
          else all.whenComplete((done, error) -> release.run());
          if (e == null) exposed.complete(v);
          else exposed.completeExceptionally(e);
        });
    return exposed;
  }
}
