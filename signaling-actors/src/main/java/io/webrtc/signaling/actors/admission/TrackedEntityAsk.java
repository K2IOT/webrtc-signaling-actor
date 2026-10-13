package io.webrtc.signaling.actors.admission;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.MailboxSelector;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Bounded callers own this collector until both logical outcome and independent cleanup arrive.
 */
public final class TrackedEntityAsk {
    private TrackedEntityAsk() {
    }

    public static <T> ActorOperation<T> ask(
            ActorSystem<?> system,
            Duration budget,
            Class<T> resultType,
            BiConsumer<ActorRef<T>, CompletionReceipt> send) {
        return ask(system, budget, resultType, send, witness -> {
        });
    }

    public static <T> ActorOperation<T> ask(
            ActorSystem<?> system,
            Duration budget,
            Class<T> resultType,
            BiConsumer<ActorRef<T>, CompletionReceipt> send,
            Consumer<CompletionReceipt.PhysicalOwner> witness) {
        Objects.requireNonNull(witness);
        if (budget == null
                || budget.isNegative()
                || budget.isZero()
                || budget.compareTo(Duration.ofSeconds(2)) > 0)
            throw new IllegalArgumentException("Invalid tracked ask budget");
        var logical = new CompletableFuture<T>();
        var physical = new CompletableFuture<Void>();
        UUID operation = UUID.randomUUID();
        long end = System.nanoTime() + budget.toNanos();
        Behavior<Object> behavior =
                Behaviors.withTimers(
                        timers ->
                                Behaviors.setup(
                                        ctx -> {
                                            timers.startSingleTimer(
                                                    DeadlineExpired.INSTANCE,
                                                    Duration.ofNanos(Math.max(1, end - System.nanoTime())));
                                            return Behaviors.receiveMessage(
                                                    value -> {
                                                        if (value instanceof CompletionReceipt.PhysicalDone done) {
                                                            if (operation.equals(done.operation()))
                                                                physical.complete(null);
                                                        } else if (value instanceof CompletionReceipt.PhysicalOwner owner) {
                                                            if (operation.equals(owner.operation()))
                                                                witness.accept(owner);
                                                        } else if (value == DeadlineExpired.INSTANCE)
                                                            logical.completeExceptionally(
                                                                    new TimeoutException("OUTCOME_UNKNOWN"));
                                                        else if (resultType.isInstance(value))
                                                            logical.complete(resultType.cast(value));
                                                        return logical.isDone() && physical.isDone()
                                                                ? Behaviors.stopped()
                                                                : Behaviors.same();
                                                    });
                                        }));
        var collector =
                system.systemActorOf(
                        behavior, "physical-completion-" + operation, MailboxSelector.bounded(16));
        var receipt = new CompletionReceipt(operation, collector.narrow());
        logical.orTimeout(budget.toNanos(), TimeUnit.NANOSECONDS);
        try {
            send.accept(collector.narrow(), receipt);
        } catch (RuntimeException failed) {
            logical.completeExceptionally(failed);
            if (failed instanceof NotEnqueued)
                receipt
                        .signal(); /* Other dispatch failures remain quarantined until an explicit physical receipt. */
        }
        var retained = CompletableFuture.allOf(physical, logical.handle((value, failure) -> null));
        return new ActorOperation<>(
                logical.minimalCompletionStage(), retained.minimalCompletionStage());
    }

    private enum DeadlineExpired {
        INSTANCE
    }

    public static final class NotEnqueued extends RuntimeException {
        public NotEnqueued(Throwable cause) {
            super("Rejected before entity enqueue", cause);
        }
    }
}
