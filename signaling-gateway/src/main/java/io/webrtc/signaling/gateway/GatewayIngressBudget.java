package io.webrtc.signaling.gateway;

import java.util.concurrent.RejectedExecutionException;

/**
 * A process-wide count/byte bound. Tickets retain credits until owned buffers and work are
 * released.
 */
public final class GatewayIngressBudget {
  private final int maximumCount, maximumBytes;
  private int count, bytes;

  public GatewayIngressBudget(int count, int bytes) {
    if (count < 1 || count > 4096 || bytes < 1 || bytes > 33554432)
      throw new IllegalArgumentException("Bounded process ingress required");
    maximumCount = count;
    maximumBytes = bytes;
  }

  public synchronized Ticket acquire(int encodedBytes) {
    if (encodedBytes < 1 || count >= maximumCount || encodedBytes > maximumBytes - bytes)
      throw new RejectedExecutionException("INGRESS_OVERLOADED");
    count++;
    bytes += encodedBytes;
    return new Ticket(encodedBytes);
  }

  public final class Ticket implements AutoCloseable {
    private int retained;
    private boolean closed;

    private Ticket(int bytes) {
      retained = bytes;
    }

    public void grow(int more) {
      synchronized (GatewayIngressBudget.this) {
        if (closed || more < 1 || more > maximumBytes - bytes)
          throw new RejectedExecutionException("FRAGMENT_OVERLOADED");
        bytes += more;
        retained += more;
      }
    }

    @Override
    public void close() {
      synchronized (GatewayIngressBudget.this) {
        if (!closed) {
          closed = true;
          count--;
          bytes -= retained;
        }
      }
    }
  }

  public synchronized int bytes() {
    return bytes;
  }

  public synchronized int count() {
    return count;
  }
}
