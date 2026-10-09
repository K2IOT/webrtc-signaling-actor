package io.webrtc.signaling.control;

import java.util.UUID;
import java.util.concurrent.*;

public final class BucketTransferService {
  public record FreezeReceipt(int bucket, String sourceCell, long sourceEpoch, UUID fenceId) {}

  public interface AuthorityTransfer {
    CompletionStage<FreezeReceipt> freeze(HomeRoute source);

    CompletionStage<Void> copyAndInstall(FreezeReceipt fence, HomeRoute target);
  }

  private final DirectoryRepository directory;
  private final AuthorityTransfer authorities;

  public BucketTransferService(DirectoryRepository directory, AuthorityTransfer authorities) {
    this.directory = directory;
    this.authorities = authorities;
  }

  public CompletionStage<HomeRoute> transfer(HomeRoute source, HomeRoute target) {
    if (source.bucket() != target.bucket()
        || target.epoch() != Math.addExact(source.epoch(), 1)
        || source.cell().equals(target.cell()))
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("invalid bucket transfer"));
    return authorities
        .freeze(source)
        .thenCompose(
            receipt -> {
              if (receipt == null
                  || receipt.fenceId() == null
                  || receipt.bucket() != source.bucket()
                  || !receipt.sourceCell().equals(source.cell())
                  || receipt.sourceEpoch() != source.epoch())
                return CompletableFuture.failedFuture(
                    new IllegalStateException("old authority not fenced"));
              return authorities.copyAndInstall(receipt, target);
            })
        .thenCompose(ignored -> directory.compareAndPublish(source, target))
        .thenApply(
            published -> {
              if (!published)
                throw new IllegalStateException("directory CAS conflict; source remains fenced");
              return target;
            });
  }
}
