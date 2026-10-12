package io.webrtc.signaling.storage;

import io.webrtc.signaling.protocol.Identity.*;

/** Internal observations only; these callbacks never enroll fault cohorts or authorize work. */
public interface RecoveryOutcomeSink {
  RecoveryOutcomeSink NONE = new RecoveryOutcomeSink() {};

  default void snapshot(CallSnapshotRepository.Snapshot snapshot) {}

  default void hydrated(CallId call, AuthoritySql.GroupToken originalGroup) {}

  default void active(CallSnapshotRepository.Snapshot snapshot, AuthoritySql.GroupToken group) {}

  default void media(
      CallSnapshotRepository.Snapshot snapshot,
      AuthoritySql.GroupToken group,
      AuthenticatedSession sender) {}

  default void application(CallId call, long version, AuthenticatedSession recipient) {}

  default void unavailable() {}

  static void safely(RecoveryOutcomeSink sink, Runnable observation) {
    try {
      observation.run();
    } catch (RuntimeException failure) {
      try {
        sink.unavailable();
      } catch (RuntimeException ignored) {
      }
    }
  }
}
