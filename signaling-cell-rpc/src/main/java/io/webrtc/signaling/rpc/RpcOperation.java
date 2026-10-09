package io.webrtc.signaling.rpc;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Logical outcome and independent physical cleanup; only successful cleanup returns credit.
 * Exceptional cleanup remains UNKNOWN.
 */
public record RpcOperation<T>(CompletionStage<T> logical, CompletionStage<?> physicalCompletion) {
  public RpcOperation {
    Objects.requireNonNull(logical);
    Objects.requireNonNull(physicalCompletion);
    logical = logical.toCompletableFuture().minimalCompletionStage();
    physicalCompletion = physicalCompletion.toCompletableFuture().minimalCompletionStage();
  }
}
