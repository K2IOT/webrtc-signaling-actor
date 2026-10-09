package io.webrtc.signaling.loadgen;

import java.util.Objects;

/** Current call identity and native version; late events cannot overwrite a later call. */
final class CallEventCursor {
  private String call, closedCall;
  private long version;

  synchronized void bind(String next, long value) {
    Objects.requireNonNull(next);
    if (next.isBlank() || value < 1)
      throw new IllegalArgumentException("Native call binding required");
    if (!canBind(next))
      throw new IllegalStateException("Call is closed or a different call is still active");
    call = next;
    version = Math.max(version, value);
  }

  synchronized boolean accept(String eventCall, long value) {
    if (value < 1) throw new IllegalArgumentException("Native call version required");
    if (call == null || !call.equals(eventCall) || value < version) return false;
    version = value;
    return true;
  }

  synchronized void clear(String expected) {
    if (Objects.equals(call, expected) && call != null) {
      closedCall = call;
      call = null;
      version = 0;
    }
  }

  synchronized boolean canBind(String next) {
    return next != null && !Objects.equals(next, closedCall) && (call == null || call.equals(next));
  }

  synchronized String call() {
    return call;
  }

  synchronized long version() {
    return version;
  }
}
