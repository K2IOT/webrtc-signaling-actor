package io.webrtc.signaling.actors.admission;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;

/**
 * Credits precede entity enqueue and remain owned through physical work, with reserved safety
 * capacity.
 */
public final class EntityAdmission {
    private final int maximumCount,
            maximumBytes,
            reservedCount,
            reservedBytes,
            processCount,
            processBytes;
    private final int recoveryCount, recoveryBytes, recoveryProcessCount, recoveryProcessBytes;
    private final Map<String, Usage> entities = new HashMap<>();
    private int count, bytes, retainedRecoveryCount, retainedRecoveryBytes;
    public EntityAdmission(
            int maximumCount,
            int maximumBytes,
            int reservedCount,
            int reservedBytes,
            int processCount,
            int processBytes) {
        this(
                maximumCount, maximumBytes, reservedCount, reservedBytes, processCount, processBytes, 0, 0);
    }
    public EntityAdmission(
            int maximumCount,
            int maximumBytes,
            int reservedCount,
            int reservedBytes,
            int processCount,
            int processBytes,
            int recoveryCount,
            int recoveryBytes) {
        if (maximumCount < 2
                || maximumCount > 64
                || maximumBytes < 1024
                || maximumBytes > 262144
                || reservedCount < 1
                || reservedCount >= maximumCount
                || reservedBytes < 1
                || reservedBytes >= maximumBytes
                || processCount < maximumCount
                || processCount > 65536
                || processBytes < maximumBytes
                || processBytes > 268435456
                || recoveryCount < 0
                || recoveryCount + reservedCount > maximumCount
                || recoveryBytes < 0
                || recoveryBytes + reservedBytes > maximumBytes
                || (recoveryCount == 0) != (recoveryBytes == 0))
            throw new IllegalArgumentException("Unsafe entity admission limits");
        this.maximumCount = maximumCount;
        this.maximumBytes = maximumBytes;
        this.reservedCount = reservedCount;
        this.reservedBytes = reservedBytes;
        this.processCount = processCount;
        this.processBytes = processBytes;
        this.recoveryCount = recoveryCount;
        this.recoveryBytes = recoveryBytes;
        this.recoveryProcessCount =
                recoveryCount == 0 ? 0 : Math.min(128, Math.max(recoveryCount, processCount / 8));
        this.recoveryProcessBytes =
                recoveryBytes == 0
                        ? 0
                        : Math.min(4 * 1024 * 1024, Math.max(recoveryBytes, processBytes / 8));
    }

    public static EntityAdmission forIngressProducers(int maximumActorProcesses) {
        if (maximumActorProcesses < 1 || maximumActorProcesses > 16)
            throw new IllegalArgumentException("Bounded cluster producer envelope required");
        return new EntityAdmission(
                64 / maximumActorProcesses,
                262144 / maximumActorProcesses,
                Math.max(1, 16 / maximumActorProcesses),
                Math.max(1, 32768 / maximumActorProcesses),
                4096,
                33554432,
                1,
                Math.max(512, 32768 / maximumActorProcesses));
    }

    private int countLimit(Priority priority, boolean process, Usage usage) {
        int maximum = process ? processCount : maximumCount;
        int recovery =
                (process ? recoveryProcessCount : recoveryCount)
                        - (process ? retainedRecoveryCount : usage == null ? 0 : usage.recoveryCount);
        return switch (priority) {
            case RECOVERY -> maximum;
            case SAFETY -> maximum - recovery;
            case NORMAL -> maximum - recovery - reservedCount;
        };
    }

    private int byteLimit(Priority priority, boolean process, Usage usage) {
        int maximum = process ? processBytes : maximumBytes;
        int recovery =
                (process ? recoveryProcessBytes : recoveryBytes)
                        - (process ? retainedRecoveryBytes : usage == null ? 0 : usage.recoveryBytes);
        return switch (priority) {
            case RECOVERY -> maximum;
            case SAFETY -> maximum - recovery;
            case NORMAL -> maximum - recovery - reservedBytes;
        };
    }

    public synchronized Ticket acquire(String entity, Priority priority, int encodedBytes) {
        if (entity == null
                || entity.isBlank()
                || entity.length() > 384
                || priority == null
                || encodedBytes < 1
                || encodedBytes > 98304)
            throw new IllegalArgumentException("Invalid admitted entity request");
        var usage = entities.get(entity);
        int n = usage == null ? 0 : usage.count;
        int b = usage == null ? 0 : usage.bytes;
        if (n >= countLimit(priority, false, usage)
                || encodedBytes > byteLimit(priority, false, usage) - b
                || count >= countLimit(priority, true, usage)
                || encodedBytes > byteLimit(priority, true, usage) - bytes
                || priority == Priority.RECOVERY
                && ((usage != null
                && (usage.recoveryCount >= recoveryCount
                || encodedBytes > recoveryBytes - usage.recoveryBytes))
                || encodedBytes > recoveryBytes
                || retainedRecoveryCount >= recoveryProcessCount
                || encodedBytes > recoveryProcessBytes - retainedRecoveryBytes))
            throw new Overloaded();
        if (usage == null) {
            usage = new Usage();
            entities.put(entity, usage);
        }
        usage.count++;
        usage.bytes += encodedBytes;
        count++;
        bytes += encodedBytes;
        if (priority == Priority.RECOVERY) {
            usage.recoveryCount++;
            usage.recoveryBytes += encodedBytes;
            retainedRecoveryCount++;
            retainedRecoveryBytes += encodedBytes;
        }
        return new Ticket(entity, usage, encodedBytes, priority);
    }

    public synchronized int count(String entity) {
        var usage = entities.get(entity);
        return usage == null ? 0 : usage.count;
    }

    public synchronized int retainedBytes() {
        return bytes;
    }

    public synchronized int entities() {
        return entities.size();
    }

    public enum Priority {
        NORMAL,
        SAFETY,
        RECOVERY
    }

    public static final class Overloaded extends RejectedExecutionException {
        public Overloaded() {
            super("ENTITY_OVERLOADED");
        }
    }

    private static final class Usage {
        int count, bytes, recoveryCount, recoveryBytes;
    }

    public final class Ticket implements AutoCloseable {
        private final String entity;
        private final Usage usage;
        private final Priority priority;
        private int size;
        private boolean closed;

        private Ticket(String entity, Usage usage, int size, Priority priority) {
            this.entity = entity;
            this.usage = usage;
            this.size = size;
            this.priority = priority;
        }

        public void charge(int encodedBytes) {
            synchronized (EntityAdmission.this) {
                if (closed || encodedBytes < 1 || encodedBytes > 98304)
                    throw new IllegalArgumentException("Invalid full envelope charge");
                int delta = encodedBytes - size;
                if (delta <= 0) return;
                if (delta > byteLimit(priority, false, usage) - usage.bytes
                        || delta > byteLimit(priority, true, usage) - bytes
                        || priority == Priority.RECOVERY
                        && (delta > recoveryBytes - usage.recoveryBytes
                        || delta > recoveryProcessBytes - retainedRecoveryBytes))
                    throw new Overloaded();
                usage.bytes += delta;
                bytes += delta;
                if (priority == Priority.RECOVERY) {
                    usage.recoveryBytes += delta;
                    retainedRecoveryBytes += delta;
                }
                size = encodedBytes;
            }
        }

        public void releaseAfter(CompletionStage<?> physical) {
            Objects.requireNonNull(physical)
                    .whenComplete(
                            (done, failure) -> {
                                if (failure == null) close();
                            });
        }

        public void close() {
            synchronized (EntityAdmission.this) {
                if (!closed) {
                    closed = true;
                    usage.count--;
                    usage.bytes -= size;
                    count--;
                    bytes -= size;
                    if (priority == Priority.RECOVERY) {
                        usage.recoveryCount--;
                        usage.recoveryBytes -= size;
                        retainedRecoveryCount--;
                        retainedRecoveryBytes -= size;
                    }
                    if (usage.count == 0) entities.remove(entity, usage);
                }
            }
        }
    }
}
