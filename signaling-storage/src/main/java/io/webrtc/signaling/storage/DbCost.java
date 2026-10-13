package io.webrtc.signaling.storage;

/**
 * Declared admission charge, not a measurement of an opaque work closure or JDBC heap.
 */
public record DbCost(long inputBytes, long retainedBytes, long resultBytes, long serviceNanos) {
    public DbCost {
        if (inputBytes < 0 || retainedBytes < inputBytes || resultBytes < 1 || serviceNanos < 0)
            throw new IllegalArgumentException("Invalid DB resource charge");
    }
}
