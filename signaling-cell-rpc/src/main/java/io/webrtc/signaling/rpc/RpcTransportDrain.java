package io.webrtc.signaling.rpc;

import io.grpc.ManagedChannel;
import java.util.List;
import java.util.concurrent.*;

/**
 * One bounded process shutdown waiter; stream cleanup and channel termination are separate facts.
 */
final class RpcTransportDrain {
  private RpcTransportDrain() {}

  static void await(
      CompletionStage<Void> settled,
      List<ManagedChannel> channels,
      CompletableFuture<Void> drained,
      ExecutorService... executors) {
    settled.whenComplete(
        (ignored, failure) -> {
          if (failure != null) {
            drained.completeExceptionally(failure);
            return;
          }
          Thread.startVirtualThread(
              () -> {
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                try {
                  for (var channel : channels) {
                    long left = end - System.nanoTime();
                    if (left <= 0 || !channel.awaitTermination(left, TimeUnit.NANOSECONDS))
                      throw new TimeoutException("RPC transport termination unproven");
                  }
                  for (var executor : executors) executor.shutdown();
                  for (var executor : executors) {
                    long left = end - System.nanoTime();
                    if (left <= 0 || !executor.awaitTermination(left, TimeUnit.NANOSECONDS))
                      throw new TimeoutException("RPC callback termination unproven");
                  }
                  drained.complete(null);
                } catch (InterruptedException interrupted) {
                  Thread.currentThread().interrupt();
                  drained.completeExceptionally(interrupted);
                } catch (Exception unknown) {
                  drained.completeExceptionally(unknown);
                }
              });
        });
  }
}
