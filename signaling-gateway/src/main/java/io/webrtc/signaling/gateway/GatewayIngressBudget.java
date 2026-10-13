package io.webrtc.signaling.gateway;

import java.util.concurrent.RejectedExecutionException;

/**
 * Process count/byte bounds with a protected critical lane; credits follow physical ownership.
 */
public final class GatewayIngressBudget {
    private final int maximumCount, maximumBytes, reservedCount, reservedBytes;
    private int count, bytes, normalCount, normalBytes, fragmentCount, fragmentBytes;

    public GatewayIngressBudget(int count, int bytes) {
        this(count, bytes, 0, 0);
    }

    public GatewayIngressBudget(int count, int bytes, int criticalCount, int criticalBytes) {
        if (count < 1
                || count > 4096
                || bytes < 1
                || bytes > 33554432
                || criticalCount < 0
                || 2L * criticalCount >= count
                || criticalBytes < 0
                || 2L * criticalBytes >= bytes
                || (criticalCount == 0) != (criticalBytes == 0))
            throw new IllegalArgumentException("Bounded process ingress required");
        maximumCount = count;
        maximumBytes = bytes;
        reservedCount = criticalCount;
        reservedBytes = criticalBytes;
    }

    public Ticket acquire(int encodedBytes) {
        return acquire(encodedBytes, false);
    }

    public synchronized Ticket acquire(int encodedBytes, boolean critical) {
        boolean protectedLane = critical && reservedCount > 0;
        int normalMaximumCount = maximumCount - 2 * reservedCount;
        int normalMaximumBytes = maximumBytes - 2 * reservedBytes;
        if (encodedBytes < 1
                || count >= maximumCount
                || encodedBytes > maximumBytes - bytes
                || (protectedLane
                ? count - normalCount - fragmentCount >= reservedCount
                || encodedBytes > reservedBytes - (bytes - normalBytes - fragmentBytes)
                : normalCount >= normalMaximumCount || encodedBytes > normalMaximumBytes - normalBytes))
            throw new RejectedExecutionException("INGRESS_OVERLOADED");
        count++;
        bytes += encodedBytes;
        if (!protectedLane) {
            normalCount++;
            normalBytes += encodedBytes;
        }
        return new Ticket(encodedBytes, protectedLane, false);
    }

    /**
     * Unknown partial messages have separate credits; they cannot occupy completed critical input.
     */
    public synchronized Ticket acquireFragment(int encodedBytes) {
        if (reservedCount == 0) return acquire(encodedBytes);
        if (encodedBytes < 1
                || fragmentCount >= reservedCount
                || encodedBytes > reservedBytes - fragmentBytes
                || count >= maximumCount
                || encodedBytes > maximumBytes - bytes)
            throw new RejectedExecutionException("FRAGMENT_OVERLOADED");
        fragmentCount++;
        fragmentBytes += encodedBytes;
        count++;
        bytes += encodedBytes;
        return new Ticket(encodedBytes, false, true);
    }

    public synchronized int bytes() {
        return bytes;
    }

    public synchronized int count() {
        return count;
    }

    public final class Ticket implements AutoCloseable {
        private final boolean critical, fragment;
        private int retained;
        private boolean closed;

        private Ticket(int bytes, boolean critical, boolean fragment) {
            retained = bytes;
            this.critical = critical;
            this.fragment = fragment;
        }

        public void grow(int more) {
            synchronized (GatewayIngressBudget.this) {
                if (closed
                        || more < 1
                        || more > maximumBytes - bytes
                        || (fragment
                        ? more > reservedBytes - fragmentBytes
                        : critical
                        ? more > reservedBytes - (bytes - normalBytes - fragmentBytes)
                        : more > maximumBytes - 2 * reservedBytes - normalBytes))
                    throw new RejectedExecutionException("FRAGMENT_OVERLOADED");
                bytes += more;
                retained += more;
                if (fragment) fragmentBytes += more;
                else if (!critical) normalBytes += more;
            }
        }

        @Override
        public void close() {
            synchronized (GatewayIngressBudget.this) {
                if (!closed) {
                    closed = true;
                    count--;
                    bytes -= retained;
                    if (fragment) {
                        fragmentCount--;
                        fragmentBytes -= retained;
                    } else if (!critical) {
                        normalCount--;
                        normalBytes -= retained;
                    }
                }
            }
        }
    }
}
