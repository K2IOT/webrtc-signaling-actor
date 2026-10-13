package io.webrtc.signaling.control;

import io.webrtc.signaling.storage.DbOperation;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Durable CP directory; implementations must use the admitted database transaction boundary.
 */
public interface DirectoryRepository {
    CompletionStage<Optional<HomeRoute>> read(int bucket);

    CompletionStage<Boolean> compareAndPublish(HomeRoute previous, HomeRoute next);

    default DbOperation<Optional<HomeRoute>> readTracked(int bucket) {
        return new DbOperation<>(
                CompletableFuture.failedFuture(
                        new UnsupportedOperationException("Tracked directory read missing")),
                CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.NOT_STARTED));
    }

    default DbOperation<Boolean> compareAndPublishTracked(HomeRoute previous, HomeRoute next) {
        return new DbOperation<>(
                CompletableFuture.failedFuture(
                        new UnsupportedOperationException("Tracked directory publication missing")),
                CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.NOT_STARTED));
    }

    CompletionStage<Boolean> localActive(int bucket, String cell, long epoch);
}
