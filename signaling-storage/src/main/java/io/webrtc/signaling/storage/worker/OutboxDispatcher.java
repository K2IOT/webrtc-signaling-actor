package io.webrtc.signaling.storage.worker;

import io.webrtc.signaling.storage.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BiPredicate;

/**
 * A verified application receipt is distinct from a physical socket write. Hints only accelerate
 * polling.
 */
public final class OutboxDispatcher {
  public enum Kind {
    WRITE_COMPLETED,
    APPLICATION_RECEIVED,
    MALFORMED,
    UNAVAILABLE
  }

  public record Receipt(UUID eventId, Kind kind) {
    public Receipt {
      Objects.requireNonNull(eventId);
      Objects.requireNonNull(kind);
    }
  }

  public record Report(int claimed, int delivered) {}

  @FunctionalInterface
  public interface Delivery {
    DbOperation<Receipt> send(OutboxRepository.Claim event, Duration budget);
  }

  private final OutboxRepository repository;
  private final String worker;
  private final UUID incarnation;
  private final Delivery delivery;
  private final BiPredicate<OutboxRepository.Claim, Receipt> authenticatedReceipt;
  private final AtomicBoolean busy = new AtomicBoolean();

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
    WorkerFence.limit(limit, 128);
    if (!busy.compareAndSet(false, true))
      return CompletableFuture.failedFuture(new DbOverloadedException());
    var work = new WorkerCompletion();
    CompletionStage<Report> logical;
    try {
      logical =
          work.track(repository.reclaimExpired(limit))
              .thenCompose(
                  recovered ->
                      work.track(repository.claimOutboxBatchTracked(worker, incarnation, limit)))
              .thenCompose(
                  claims -> {
                    var sends = new ArrayList<CompletableFuture<Boolean>>();
                    for (var event : claims) {
                      try {
                        var receipt =
                            work.track(delivery.send(event, Duration.ofSeconds(2)))
                                .toCompletableFuture()
                                .copy()
                                .orTimeout(2, TimeUnit.SECONDS);
                        sends.add(
                            receipt
                                .handle(
                                    (value, error) ->
                                        error == null
                                            ? value
                                            : new Receipt(event.eventId(), Kind.UNAVAILABLE))
                                .thenCompose(
                                    value -> {
                                      boolean verified =
                                          value.kind() == Kind.APPLICATION_RECEIVED
                                              && event.eventId().equals(value.eventId())
                                              && authenticatedReceipt.test(event, value);
                                      return verified
                                          ? work.track(
                                              repository.completeTracked(
                                                  event, worker, incarnation))
                                          : work.track(
                                                  repository.defer(
                                                      event,
                                                      worker,
                                                      incarnation,
                                                      value.kind() == Kind.MALFORMED))
                                              .thenApply(ignored -> false);
                                    })
                                .exceptionally(error -> false)
                                .toCompletableFuture());
                      } catch (RuntimeException unavailable) {
                        sends.add(CompletableFuture.completedFuture(false));
                      }
                    }
                    return CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
                        .thenApply(
                            done ->
                                new Report(
                                    claims.size(),
                                    (int) sends.stream().filter(CompletableFuture::join).count()));
                  });
    } catch (RuntimeException error) {
      logical = CompletableFuture.failedFuture(error);
    }
    return work.finish(logical, () -> busy.set(false));
  }
}
