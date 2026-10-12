package io.webrtc.signaling.control;

import io.webrtc.signaling.storage.DbOperation;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public final class BucketTransferService {
  public record FreezeReceipt(int bucket, String sourceCell, long sourceEpoch, UUID fenceId) {}

  public interface AuthorityTransfer {
    CompletionStage<FreezeReceipt> freeze(HomeRoute source);

    CompletionStage<Void> copyAndInstall(FreezeReceipt fence, HomeRoute target);

    default CompletionStage<FreezeReceipt> prepare(UUID id, HomeRoute source, HomeRoute target) {
      return freeze(source);
    }

    default CompletionStage<Void> publicationConfirmed(FreezeReceipt fence, HomeRoute target) {
      return CompletableFuture.completedFuture(null);
    }

    /** An abstract callback is not an installation receipt; native primaries verify both stores. */
    default CompletionStage<Void> verifyInstalled(FreezeReceipt fence, HomeRoute target) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("Native installation proof missing"));
    }
  }

  private final DirectoryRepository directory;
  private final AuthorityTransfer authorities;

  public BucketTransferService(DirectoryRepository directory, AuthorityTransfer authorities) {
    this.directory = Objects.requireNonNull(directory);
    this.authorities = Objects.requireNonNull(authorities);
  }

  /** A repeated convenience request keeps the same identity after an uncertain outcome. */
  public CompletionStage<HomeRoute> transfer(HomeRoute source, HomeRoute target) {
    UUID id =
        UUID.nameUUIDFromBytes(
            ("native-bucket-transfer-v1\n" + source + "\n" + target)
                .getBytes(StandardCharsets.UTF_8));
    return transfer(id, source, target);
  }

  public CompletionStage<HomeRoute> transfer(UUID id, HomeRoute source, HomeRoute target) {
    if (id == null
        || source.bucket() != target.bucket()
        || target.epoch() != Math.addExact(source.epoch(), 1)
        || source.cell().equals(target.cell()))
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("invalid bucket transfer"));
    return authorities
        .prepare(id, source, target)
        .thenCompose(
            receipt -> {
              if (receipt == null
                  || !id.equals(receipt.fenceId())
                  || receipt.bucket() != source.bucket()
                  || !source.cell().equals(receipt.sourceCell())
                  || receipt.sourceEpoch() != source.epoch())
                return CompletableFuture.failedFuture(
                    new IllegalStateException("Old authority not fenced"));
              return authorities
                  .copyAndInstall(receipt, target)
                  .thenCompose(v -> authorities.verifyInstalled(receipt, target))
                  .thenCompose(v -> publish(source, target))
                  .thenCompose(
                      published ->
                          authorities
                              .publicationConfirmed(receipt, target)
                              .thenApply(v -> published));
            });
  }

  private CompletionStage<HomeRoute> publish(HomeRoute source, HomeRoute target) {
    // Reading the exact previous/next directory identity reconciles both CAS conflicts and UNKNOWN.
    return settled(directory.compareAndPublishTracked(source, target))
        .thenCompose(published -> completed(directory.readTracked(source.bucket())))
        .thenApply(
            current -> {
              if (current.isPresent() && current.get().equals(target)) return target;
              throw new IllegalStateException(
                  "Directory publication unproven; resume the same transfer identity");
            });
  }

  private record Logical<T>(T value, Throwable failure) {}

  private static <T> CompletionStage<T> completed(DbOperation<T> operation) {
    return settled(operation)
        .thenApply(
            result -> {
              if (result.failure() != null) throw new CompletionException(result.failure());
              return result.value();
            });
  }

  private static <T> CompletionStage<Logical<T>> settled(DbOperation<T> operation) {
    return operation
        .logical()
        .handle((value, error) -> new Logical<>(value, error))
        .thenCompose(
            result ->
                operation
                    .physicalCompletion()
                    .thenApply(
                        completion -> {
                          if (completion != DbOperation.PhysicalCompletion.FINISHED)
                            throw new IllegalStateException(
                                "Native directory operation did not finish physically");
                          return result;
                        }));
  }
}
