package io.webrtc.signaling.localplatform;

import io.webrtc.signaling.auth.RevocationState;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Append-only, offset-ordered revocation log behind the local security source.
 *
 * <p>Offsets are gapless and committed in order: a reader never sees an offset ahead of a gap.
 * Every entry is a revoke event for a {@code (user, jti)} subject ({@code jti == null} is the user
 * scope). An event with epoch {@code e} revokes every token of the subject whose security epoch is
 * {@code <= e}, so the epoch of a revoke must be {@code >= 0}; the default epoch is one above the
 * latest of the subject, or {@code 0} when there is none.
 *
 * <p>Failures are three distinct, detail-free <em>unchecked</em> types (no message, no cause, no
 * suppressed exceptions, no stack trace) so that nothing from the database - SQL, parameters,
 * connection details - can leak through them: {@link EpochConflict} and {@link CursorAhead} are
 * contract outcomes (the HTTP layer maps both to 409), {@link Unavailable} means the store could
 * not answer (503). Invalid arguments are {@link IllegalArgumentException} / {@link
 * NullPointerException}, thrown before the store is touched.
 *
 * <p>Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
public interface RevocationLog {

  /**
   * Issuer carried by the events a log returns. The log is issuer-agnostic; the page service
   * replaces it with the configured issuer before an event is signed.
   */
  String UNSCOPED_ISSUER = "unscoped";

  /**
   * Appends one revoke event atomically and returns its offset and epoch.
   *
   * @param userId the user, NFC-normalised and validated like {@code UserId}
   * @param jtiOrNull the session id, or {@code null} for the user scope
   * @param epoch an explicit epoch ({@code >= 0}), or empty for latest-of-subject plus one
   * @throws EpochConflict if an explicit epoch is not above the latest of the subject
   * @throws Unavailable if the store could not complete the append (nothing was appended)
   */
  Appended append(String userId, String jtiOrNull, OptionalLong epoch);

  /**
   * Reads one consistent snapshot: at most {@code limit} events with offset {@code > from}, in
   * offset order, together with the high-water mark of that same snapshot.
   *
   * @throws CursorAhead if {@code from} is above the high-water mark
   * @throws Unavailable if the store could not answer
   */
  Snapshot read(long from, int limit);

  /** The offset and epoch the append was assigned. */
  record Appended(long offset, long epoch) {}

  /**
   * A consistent view of the log.
   *
   * @param events the page, with {@link #UNSCOPED_ISSUER} as issuer
   * @param currentHighWater the high-water mark of the snapshot the page was read from
   * @param newestCommittedAt the newest commit time among {@code events}; {@link Instant#EPOCH}
   *     when the page is empty
   */
  record Snapshot(
      List<RevocationState.Event> events, long currentHighWater, Instant newestCommittedAt) {
    public Snapshot {
      events = List.copyOf(Objects.requireNonNull(events, "events"));
      Objects.requireNonNull(newestCommittedAt, "newestCommittedAt");
      if (currentHighWater < 0) {
        throw new IllegalArgumentException();
      }
    }
  }

  /** An explicit epoch was not above the latest epoch of the subject. */
  final class EpochConflict extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public EpochConflict() {
      super(null, null, false, false);
    }
  }

  /** The reader's cursor is above the high-water mark. */
  final class CursorAhead extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public CursorAhead() {
      super(null, null, false, false);
    }
  }

  /** The store could not answer; the cause is deliberately dropped. */
  final class Unavailable extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public Unavailable() {
      super(null, null, false, false);
    }
  }
}
