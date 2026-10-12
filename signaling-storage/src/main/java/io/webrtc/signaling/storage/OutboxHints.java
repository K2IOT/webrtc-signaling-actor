package io.webrtc.signaling.storage;

import java.util.*;

/** Bounded, process-local post-COMMIT hints. Persisted queue scans remain the repair authority. */
public final class OutboxHints {
  private final LinkedHashSet<UUID> pending = new LinkedHashSet<>();
  private final int capacity;
  private Runnable wake;

  public OutboxHints(int capacity) {
    if (capacity < 1 || capacity > 4096)
      throw new IllegalArgumentException("Hint capacity outside bound");
    this.capacity = capacity;
  }

  public void offer(UUID eventId) {
    Runnable signal;
    synchronized (this) {
      if (pending.size() < capacity) pending.add(Objects.requireNonNull(eventId));
      signal = wake;
    }
    if (signal != null) {
      try {
        signal.run();
      } catch (RuntimeException ignored) {
        /* A lost hint is repaired from the committed queue. */
      }
    }
  }

  public synchronized List<UUID> take(int limit) {
    if (limit < 1 || limit > 128) throw new IllegalArgumentException("Hint batch outside bound");
    var result = new ArrayList<UUID>();
    var iterator = pending.iterator();
    while (iterator.hasNext() && result.size() < limit) {
      result.add(iterator.next());
      iterator.remove();
    }
    return List.copyOf(result);
  }

  public synchronized boolean pending() {
    return !pending.isEmpty();
  }

  public void bind(Runnable signal) {
    boolean ready;
    synchronized (this) {
      if (wake != null) throw new IllegalStateException("Outbox wake already installed");
      wake = Objects.requireNonNull(signal);
      ready = !pending.isEmpty();
    }
    if (ready) signal.run();
  }

  public synchronized void unbind() {
    wake = null;
  }
}
