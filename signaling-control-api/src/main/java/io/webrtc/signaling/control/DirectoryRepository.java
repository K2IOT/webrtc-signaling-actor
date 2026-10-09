package io.webrtc.signaling.control;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Durable CP directory; implementations must use the admitted database transaction boundary. */
public interface DirectoryRepository {
  CompletionStage<Optional<HomeRoute>> read(int bucket);

  CompletionStage<Boolean> compareAndPublish(HomeRoute previous, HomeRoute next);

  CompletionStage<Boolean> localActive(int bucket, String cell, long epoch);
}
