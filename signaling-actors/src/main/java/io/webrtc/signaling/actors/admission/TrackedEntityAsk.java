package io.webrtc.signaling.actors.admission;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.*;

/** Bounded callers own this collector until both logical outcome and independent cleanup arrive. */
public final class TrackedEntityAsk {
  public static final class NotEnqueued extends RuntimeException {
    public NotEnqueued(Throwable cause) {
      super("Rejected before entity enqueue", cause);
    }
  }

  private enum DeadlineExpired {
    INSTANCE
  }

  private TrackedEntityAsk() {}

  public static <T> ActorOperation<T> ask(
      ActorSystem<?> system,
      Duration budget,
      Class<T> resultType,
      BiConsumer<ActorRef<T>, CompletionReceipt> send) {
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
                              if (operation.equals(done.operation())) physical.complete(null);
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
        system.systemActorOf(behavior, "physical-completion-" + operation, Props.empty());
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
}
