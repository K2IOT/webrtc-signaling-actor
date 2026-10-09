package io.webrtc.signaling.control;

import io.webrtc.signaling.protocol.Identity.UserId;
import java.time.*;
import java.util.concurrent.*;

public final class DirectoryService {
  private record Cached(HomeRoute route, Instant checkedAt) {}

  private final DirectoryRepository repository;
  private final Duration refresh;
  private final ConcurrentHashMap<Integer, Cached> cache = new ConcurrentHashMap<>();

  public DirectoryService(DirectoryRepository repository, int maximumBuckets, Duration refresh) {
    if (maximumBuckets != 16384
        || refresh == null
        || refresh.isNegative()
        || refresh.isZero()
        || refresh.compareTo(Duration.ofSeconds(30)) > 0)
      throw new IllegalArgumentException("directory cache contract");
    this.repository = repository;
    this.refresh = refresh;
  }

  public CompletionStage<HomeRoute> resolveHome(UserId user) {
    return resolveHome(user, Instant.now());
  }

  public CompletionStage<HomeRoute> resolveHome(UserId user, Instant now) {
    int bucket = BucketHasher.bucket(user);
    var cached = cache.get(bucket);
    if (cached != null
        && !now.isBefore(cached.checkedAt())
        && now.isBefore(cached.checkedAt().plus(refresh)))
      return CompletableFuture.completedFuture(cached.route());
    return repository
        .read(bucket)
        .handle(
            (found, error) -> {
              if (error != null) {
                if (cached != null) return cached.route();
                throw new CompletionException(error);
              }
              var route =
                  found.orElseThrow(() -> new IllegalStateException("directory route unknown"));
              if (route.bucket() != bucket)
                throw new IllegalStateException("directory bucket mismatch");
              cache.compute(
                  bucket,
                  (k, old) ->
                      old != null && old.route().epoch() > route.epoch()
                          ? old
                          : new Cached(route, now));
              return cache.get(bucket).route();
            });
  }

  /**
   * Routing check only; every mutation independently checks local bucket authority inside its
   * transaction.
   */
  public CompletionStage<Void> requireLocal(UserId user, String cell, long epoch) {
    return repository
        .localActive(BucketHasher.bucket(user), cell, epoch)
        .thenApply(
            active -> {
              if (!active) throw new WrongCellException();
              return null;
            });
  }
}
