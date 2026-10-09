package io.webrtc.signaling.rpc;

import java.util.concurrent.RejectedExecutionException;

/** Shared pending delivery bound; relay cannot borrow reserved control credits. */
public final class DeliveryCreditController {
  public static final class Overloaded extends RejectedExecutionException {
    public Overloaded() {
      super("DELIVERY_OVERLOADED");
    }
  }

  private final int maxCount, maxBytes, reservedCount, reservedBytes;
  private int count, bytes;

  public DeliveryCreditController(
      int maxCount, int maxBytes, int reservedCount, int reservedBytes) {
    if (maxCount < 2
        || maxCount > 65536
        || maxBytes < 1
        || maxBytes > 268435456
        || reservedCount < 1
        || reservedCount >= maxCount
        || reservedBytes < 1
        || reservedBytes >= maxBytes) throw new IllegalArgumentException("Unsafe delivery credits");
    this.maxCount = maxCount;
    this.maxBytes = maxBytes;
    this.reservedCount = reservedCount;
    this.reservedBytes = reservedBytes;
  }

  public synchronized Ticket acquire(boolean control, int retainedBytes) {
    if (retainedBytes < 1
        || retainedBytes > 98304
        || count >= (control ? maxCount : maxCount - reservedCount)
        || retainedBytes > (control ? maxBytes : maxBytes - reservedBytes) - bytes)
      throw new Overloaded();
    count++;
    bytes += retainedBytes;
    return new Ticket(retainedBytes);
  }

  public final class Ticket implements AutoCloseable {
    private final int size;
    private boolean closed;

    private Ticket(int size) {
      this.size = size;
    }

    public void close() {
      synchronized (DeliveryCreditController.this) {
        if (!closed) {
          closed = true;
          count--;
          bytes -= size;
        }
      }
    }
  }

  public synchronized int retainedBytes() {
    return bytes;
  }

  public synchronized int count() {
    return count;
  }
}
