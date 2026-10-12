package io.webrtc.signaling.actors.call;

import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.Snapshot;
import java.time.Duration;

/** Hosting actor owns one exact renewal cycle until independent physical completion. */
@FunctionalInterface
public interface ParticipantRenewal {
  record Result(int renewed, Duration retryAfter) {
    public Result(int renewed) {
      this(renewed, null);
    }

    public Result {
      if (renewed < 0
          || renewed > 2
          || retryAfter != null
              && (retryAfter.isNegative()
                  || retryAfter.isZero()
                  || retryAfter.compareTo(Duration.ofSeconds(10)) > 0))
        throw new IllegalArgumentException("Invalid native renewal outcome");
    }
  }

  @FunctionalInterface
  interface Reconciliation {
    DbOperation<Result> reconcile(
        Snapshot snapshot,
        AuthoritySql.GroupToken token,
        CallActor.Backend backend,
        Duration budget);
  }

  /** Retains at most the two original participant attempts, never a retry collector. */
  final class Uncertain extends RuntimeException {
    private final Reconciliation reconciliation;

    public Uncertain(Throwable cause, Reconciliation reconciliation) {
      super("Native renewal requires a fresh durable projection after physical settlement", cause);
      this.reconciliation = java.util.Objects.requireNonNull(reconciliation);
    }

    public Reconciliation reconciliation() {
      return reconciliation;
    }
  }

  DbOperation<Result> renew(
      Snapshot snapshot, AuthoritySql.GroupToken token, CallActor.Backend backend, Duration budget);
}
