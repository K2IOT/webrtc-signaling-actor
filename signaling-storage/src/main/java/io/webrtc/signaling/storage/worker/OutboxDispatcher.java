package io.webrtc.signaling.storage.worker;

import io.webrtc.signaling.storage.DbOperation;
import io.webrtc.signaling.storage.DbOverloadedException;
import io.webrtc.signaling.storage.OutboxRepository;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

/**
 * Scans and delivery waits have independent, physically retained bounded owners.
 */
public final class OutboxDispatcher {
    private static final int MAX_DELIVERIES = 512, MAX_PER_DESTINATION = 8;
    private static final long MAX_BYTES = 8 * 1024 * 1024;
    private static final long BUDGET = Duration.ofSeconds(2).toNanos();
    private final OutboxRepository repository;
    private final String worker;
    private final UUID incarnation;
    private final Delivery delivery;
    private final BiPredicate<OutboxRepository.Claim, Receipt> authenticatedReceipt;
    private final AtomicBoolean busy = new AtomicBoolean();
    private final AtomicInteger completed = new AtomicInteger();
    private final Map<UUID, Flight> active = new HashMap<>();
    private final Map<String, Integer> destinations = new HashMap<>();
    private final ExecutorService deliveries =
            new ThreadPoolExecutor(
                    8,
                    8,
                    0,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(MAX_DELIVERIES),
                    Thread.ofPlatform().daemon().name("native-outbox-delivery-", 0).factory(),
                    new ThreadPoolExecutor.AbortPolicy());
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private long retainedBytes;
    private boolean draining;
    private CompletableFuture<?> scanPhysical = CompletableFuture.completedFuture(null);
    public OutboxDispatcher(
            OutboxRepository repository,
            String worker,
            UUID incarnation,
            Delivery delivery,
            BiPredicate<OutboxRepository.Claim, Receipt> authenticatedReceipt) {
        this.repository = Objects.requireNonNull(repository);
        this.worker = Objects.requireNonNull(worker);
        this.incarnation = Objects.requireNonNull(incarnation);
        this.delivery = Objects.requireNonNull(delivery);
        this.authenticatedReceipt = Objects.requireNonNull(authenticatedReceipt);
    }

    public CompletionStage<Report> poll(int limit) {
        return pollTracked(limit).logical();
    }

    public DbOperation<Report> pollTracked(int limit) {
        WorkerFence.limit(limit, 128);
        final Set<UUID> excluded;
        final CompletableFuture<Void> scan = new CompletableFuture<>();
        synchronized (this) {
            if (draining || !busy.compareAndSet(false, true))
                return new DbOperation<>(
                        CompletableFuture.failedFuture(new DbOverloadedException()),
                        CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.NOT_STARTED));
            scanPhysical = scan;
            excluded = Set.copyOf(active.keySet());
        }
        var work = new WorkerCompletion();
        CompletionStage<Report> logical;
        try {
            logical =
                    work.track(repository.reclaimExpired(limit))
                            .thenCompose(
                                    recovered ->
                                            work.track(
                                                    repository.scanOutboxBatchTracked(
                                                            worker,
                                                            incarnation,
                                                            limit,
                                                            repository.outboxHints().take(limit),
                                                            excluded)))
                            .thenCompose(
                                    batch -> {
                                        var rejected = new ArrayList<CompletableFuture<?>>();
                                        for (var event : batch.claims()) {
                                            var flight = reserve(event);
                                            if (flight == null) {
                                                rejected.add(
                                                        work.track(repository.defer(event, worker, incarnation, false))
                                                                .exceptionally(error -> null)
                                                                .toCompletableFuture());
                                                continue;
                                            }
                                            try {
                                                deliveries.execute(() -> send(flight));
                                            } catch (RejectedExecutionException notStarted) {
                                                // The known executor rejection has no socket effects; the flight still owns
                                                // SQL cleanup.
                                                settle(flight, new Receipt(event.eventId(), Kind.UNAVAILABLE));
                                            }
                                        }
                                        return CompletableFuture.allOf(rejected.toArray(CompletableFuture[]::new))
                                                .thenApply(
                                                        v ->
                                                                new Report(
                                                                        batch.claims().size(),
                                                                        completed.getAndSet(0),
                                                                        batch.pending()
                                                                                || activeDeliveries() != 0
                                                                                || repository.outboxHints().pending()));
                                    });
        } catch (RuntimeException error) {
            logical = CompletableFuture.failedFuture(error);
        }
        var operation = work.finishTracked(logical, () -> busy.set(false));
        operation
                .physicalCompletion()
                .whenComplete(
                        (v, e) -> {
                            if (e == null) scan.complete(null);
                            else scan.completeExceptionally(e);
                        });
        return operation;
    }

    private synchronized Flight reserve(OutboxRepository.Claim event) {
        long bytes =
                512L
                        + event.destination().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                        + event.payloadBase64().length();
        int count = destinations.getOrDefault(event.destination(), 0);
        if (draining
                || active.containsKey(event.eventId())
                || active.size() >= MAX_DELIVERIES
                || count >= MAX_PER_DESTINATION
                || bytes > MAX_BYTES - retainedBytes) return null;
        var flight = new Flight(event, bytes);
        active.put(event.eventId(), flight);
        destinations.put(event.destination(), count + 1);
        retainedBytes += bytes;
        return flight;
    }

    private void send(Flight flight) {
        long remaining = flight.deadline - System.nanoTime();
        if (remaining <= 0) {
            settle(flight, new Receipt(flight.event.eventId(), Kind.UNAVAILABLE));
            return;
        }
        try {
            var operation =
                    Objects.requireNonNull(delivery.send(flight.event, Duration.ofNanos(remaining)));
            var logical =
                    operation
                            .logical()
                            .toCompletableFuture()
                            .copy()
                            .orTimeout(Math.max(1, flight.deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
                            .handle(
                                    (receipt, error) ->
                                            error == null && receipt != null
                                                    ? receipt
                                                    : new Receipt(flight.event.eventId(), Kind.UNAVAILABLE));
            operation
                    .physicalCompletion()
                    .thenCombine(logical, (physical, receipt) -> receipt)
                    .whenComplete(
                            (receipt, error) -> {
                                if (error == null) settle(flight, receipt);
                                // Failed physical proof remains quarantined, including through drain.
                            });
        } catch (Throwable unknown) {
            // A throwing factory provides no proof its original effects were not started. Retain the
            // owner.
        }
    }

    private void settle(Flight flight, Receipt receipt) {
        try {
            boolean verified =
                    receipt.kind() == Kind.APPLICATION_RECEIVED
                            && flight.event.eventId().equals(receipt.eventId())
                            && flight.deadline > System.nanoTime()
                            && authenticatedReceipt.test(flight.event, receipt);
            var operation =
                    verified
                            ? repository.completeTracked(flight.event, worker, incarnation)
                            : repository.defer(
                            flight.event, worker, incarnation, receipt.kind() == Kind.MALFORMED);
            operation
                    .physicalCompletion()
                    .thenCombine(
                            operation
                                    .logical()
                                    .handle(
                                            (value, error) -> verified && error == null && Boolean.TRUE.equals(value)),
                            (physical, delivered) -> delivered)
                    .whenComplete(
                            (delivered, error) -> {
                                if (error != null) return;
                                if (Boolean.TRUE.equals(delivered)) completed.incrementAndGet();
                                synchronized (this) {
                                    if (!active.remove(flight.event.eventId(), flight)) return;
                                    retainedBytes -= flight.bytes;
                                    destinations.computeIfPresent(
                                            flight.event.destination(), (key, count) -> count == 1 ? null : count - 1);
                                }
                                flight.physical.complete(null);
                            });
        } catch (Throwable unknown) {
            // SQL factory failure supplies no original physical proof either.
        }
    }

    public synchronized int activeDeliveries() {
        return active.size();
    }

    public synchronized CompletionStage<Void> drain() {
        if (!draining) {
            draining = true;
            deliveries.shutdown();
            var owners = new ArrayList<CompletableFuture<?>>();
            owners.add(scanPhysical);
            active.values().forEach(flight -> owners.add(flight.physical));
            CompletableFuture.allOf(owners.toArray(CompletableFuture[]::new))
                    .whenComplete(
                            (v, error) -> {
                                if (error != null) {
                                    drained.completeExceptionally(error);
                                    return;
                                }
                                Thread.startVirtualThread(
                                        () -> {
                                            try {
                                                if (!deliveries.awaitTermination(2, TimeUnit.SECONDS))
                                                    throw new TimeoutException("Outbox delivery CPU termination unproven");
                                                drained.complete(null);
                                            } catch (InterruptedException interrupted) {
                                                Thread.currentThread().interrupt();
                                                drained.completeExceptionally(interrupted);
                                            } catch (TimeoutException unknown) {
                                                drained.completeExceptionally(unknown);
                                            }
                                        });
                            });
        }
        return drained.minimalCompletionStage();
    }

    public enum Kind {
        WRITE_COMPLETED,
        APPLICATION_RECEIVED,
        MALFORMED,
        UNAVAILABLE
    }

    @FunctionalInterface
    public interface Delivery {
        DbOperation<Receipt> send(OutboxRepository.Claim event, Duration budget);
    }

    public record Receipt(UUID eventId, Kind kind) {
        public Receipt {
            Objects.requireNonNull(eventId);
            Objects.requireNonNull(kind);
        }
    }

    public record Report(int claimed, int delivered, boolean pending) {
        public Report(int claimed, int delivered) {
            this(claimed, delivered, claimed != 0);
        }
    }

    private static final class Flight {
        final OutboxRepository.Claim event;
        final long bytes, deadline = System.nanoTime() + BUDGET;
        final CompletableFuture<Void> physical = new CompletableFuture<>();

        Flight(OutboxRepository.Claim event, long bytes) {
            this.event = event;
            this.bytes = bytes;
        }
    }
}
