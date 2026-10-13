package io.webrtc.signaling.gateway;

import io.webrtc.signaling.protocol.CallCommand;
import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.protocol.internal.ControlEvent;
import io.webrtc.signaling.rpc.ControlApplicationReceipt;
import io.webrtc.signaling.rpc.RpcOperation;
import io.webrtc.signaling.storage.SessionRepository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Bounded issued receipts. Timeout never retires an outstanding original socket write.
 */
public final class NativeControlReceiptTracker {
    private final Map<Key, Pending> pending = new HashMap<>();
    private final Map<UUID, Integer> connections = new HashMap<>();
    private final int maximumCount, maximumBytes;
    private int bytes;
    private boolean draining;

    public NativeControlReceiptTracker(int maximumCount, int maximumBytes) {
        if (maximumCount < 1 || maximumCount > 65536 || maximumBytes < 8192)
            throw new IllegalArgumentException("Bounded control receipt enrollment required");
        this.maximumCount = maximumCount;
        this.maximumBytes = maximumBytes;
    }

    public synchronized Pending issue(
            ControlEvent event,
            SessionRepository.Route route,
            long remainingNanos,
            BooleanSupplier current) {
        var key =
                new Key(
                        UUID.fromString(event.getEventId()),
                        new CallId(event.getCallId()),
                        event.getCallVersion(),
                        route);
        int size = event.getSerializedSize() + 2048;
        int perConnection = connections.getOrDefault(route.connectionId(), 0);
        if (draining
                || remainingNanos <= 0
                || pending.containsKey(key)
                || pending.size() >= maximumCount
                || perConnection >= 8
                || size > maximumBytes - bytes)
            throw new RejectedExecutionException("CONTROL_RECEIPT_OVERLOADED");
        var issued = new Pending(key, size, remainingNanos, current);
        pending.put(key, issued);
        connections.put(route.connectionId(), perConnection + 1);
        bytes += size;
        issued.result.orTimeout(Math.max(1, remainingNanos), TimeUnit.NANOSECONDS);
        return issued;
    }

    public boolean acknowledge(CallCommand command, SessionRepository.Route route, UUID event) {
        final List<Pending> matching;
        synchronized (this) {
            // The public receipt has eventId and callId only. Version comes from the issued receipt.
            matching =
                    pending.entrySet().stream()
                            .filter(
                                    e ->
                                            e.getKey().event().equals(event)
                                                    && e.getKey().call().equals(command.callId())
                                                    && e.getKey().route().equals(route))
                            .map(Map.Entry::getValue)
                            .toList();
        }
        return matching.size() == 1 && matching.getFirst().acknowledge();
    }

    private synchronized void retire(Pending value) {
        if (!pending.remove(value.key, value)) return;
        bytes -= value.size;
        connections.computeIfPresent(
                value.key.route().connectionId(), (key, count) -> count == 1 ? null : count - 1);
    }

    public synchronized CompletionStage<Void> drain() {
        draining = true;
        var admitted = List.copyOf(pending.values());
        admitted.forEach(p -> p.failed(new IllegalStateException("Gateway control draining")));
        return CompletableFuture.allOf(
                admitted.stream()
                        .map(p -> p.physical.toCompletableFuture())
                        .toArray(CompletableFuture[]::new));
    }

    private record Key(UUID event, CallId call, long version, SessionRepository.Route route) {
    }

    public final class Pending {
        private final Key key;
        private final int size;
        private final long deadline;
        private final BooleanSupplier current;
        private final CompletableFuture<ControlApplicationReceipt> result = new CompletableFuture<>();
        private final CompletableFuture<Void> sent = new CompletableFuture<>();
        private final CompletionStage<Void> physical;
        private boolean received, written, cleaned;

        private Pending(Key key, int size, long nanos, BooleanSupplier current) {
            this.key = key;
            this.size = size;
            this.deadline = System.nanoTime() + nanos;
            this.current = current;
            physical = sent.thenCombine(result.handle((v, e) -> null), (a, b) -> null);
            physical.thenRun(() -> retire(this));
        }

        public RpcOperation<ControlApplicationReceipt> operation() {
            return new RpcOperation<>(result.minimalCompletionStage(), physical);
        }

        public void failed(Throwable error) {
            result.completeExceptionally(error);
        }

        public void sendFinished() {
            sent.complete(null);
        }

        public synchronized void writeSucceeded() {
            written = true;
            complete();
        }

        public synchronized void writeCleaned() {
            cleaned = true;
            complete();
        }

        private synchronized boolean acknowledge() {
            if (result.isDone() || deadline - System.nanoTime() <= 0 || !current.getAsBoolean())
                return false;
            received = true;
            complete();
            return true;
        }

        private void complete() {
            if (received && written && cleaned && !result.isDone()) {
                if (deadline - System.nanoTime() > 0 && current.getAsBoolean())
                    result.complete(
                            new ControlApplicationReceipt(key.event(), key.call(), key.version(), key.route()));
                else failed(new IllegalStateException("STALE_BINDING"));
            }
        }
    }
}
