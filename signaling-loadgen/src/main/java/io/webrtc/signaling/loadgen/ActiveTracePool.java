package io.webrtc.signaling.loadgen;

import java.util.*;

/** Only live local traces occupy this bounded, constant-time source selection index. */
final class ActiveTracePool<K, V> {
  record Entry<K, V>(K key, V value) {}

  private final int maximum;
  private final List<Entry<K, V>> entries = new ArrayList<>();
  private final Map<K, Integer> positions = new HashMap<>();
  private int cursor;

  ActiveTracePool(int maximum) {
    if (maximum < 1 || maximum > 200000) throw new IllegalArgumentException("Invalid trace bound");
    this.maximum = maximum;
  }

  synchronized void put(K key, V value) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    var position = positions.get(key);
    if (position != null) {
      entries.set(position, new Entry<>(key, value));
      return;
    }
    if (entries.size() == maximum) throw new IllegalStateException("Trace source bound exhausted");
    positions.put(key, entries.size());
    entries.add(new Entry<>(key, value));
  }

  synchronized boolean remove(K key, V expected) {
    var position = positions.get(key);
    if (position == null || entries.get(position).value() != expected) return false;
    int last = entries.size() - 1;
    var moved = entries.remove(last);
    positions.remove(key);
    if (position != last) {
      entries.set(position, moved);
      positions.put(moved.key(), position);
    }
    if (cursor >= entries.size()) cursor = 0;
    return true;
  }

  synchronized Optional<Entry<K, V>> next() {
    if (entries.isEmpty()) return Optional.empty();
    var entry = entries.get(cursor++);
    if (cursor == entries.size()) cursor = 0;
    return Optional.of(entry);
  }

  synchronized int size() {
    return entries.size();
  }
}
