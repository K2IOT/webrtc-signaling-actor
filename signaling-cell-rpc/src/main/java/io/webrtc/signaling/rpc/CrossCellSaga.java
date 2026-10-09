package io.webrtc.signaling.rpc;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Coordinator transitions and home effects are separate committed operations; never a network call
 * inside SQL.
 */
public final class CrossCellSaga {
  public enum Phase {
    RESERVE_HOME,
    RING_COORDINATOR,
    CLAIM_HOME,
    ACCEPT_COORDINATOR,
    ACTIVATE_COORDINATOR,
    CONFIRM_CALLER,
    CONFIRM_CALLEE,
    READY_COORDINATOR,
    RELEASE_HOME
  }

  @FunctionalInterface
  public interface Effects {
    CompletionStage<String> apply(Phase phase, UUID originalOperation, Duration remaining);

    default RpcOperation<String> applyTracked(
        Phase phase, UUID originalOperation, Duration remaining) {
      var stage = apply(phase, originalOperation, remaining);
      return new RpcOperation<>(stage, stage);
    }
  }

  public record Outcome(UUID operation, String code, boolean reconcileHomeWinner) {}

  private final Effects effects;

  public CrossCellSaga(Effects effects) {
    this.effects = Objects.requireNonNull(effects);
  }

  public CompletionStage<Outcome> invite(UUID operation, Duration budget) {
    return inviteTracked(operation, budget).logical();
  }

  public CompletionStage<Outcome> accept(UUID operation, Duration budget) {
    return acceptTracked(operation, budget).logical();
  }

  public CompletionStage<Outcome> activate(UUID operation, Duration budget) {
    return activateTracked(operation, budget).logical();
  }

  public CompletionStage<Outcome> release(UUID operation, Duration budget) {
    return releaseTracked(operation, budget).logical();
  }

  public RpcOperation<Outcome> inviteTracked(UUID operation, Duration budget) {
    return runTracked(
        operation, budget, List.of(Phase.RESERVE_HOME, Phase.RING_COORDINATOR), false);
  }

  public RpcOperation<Outcome> acceptTracked(UUID operation, Duration budget) {
    return runTracked(operation, budget, List.of(Phase.CLAIM_HOME, Phase.ACCEPT_COORDINATOR), true);
  }

  public RpcOperation<Outcome> activateTracked(UUID operation, Duration budget) {
    return runTracked(
        operation,
        budget,
        List.of(
            Phase.ACTIVATE_COORDINATOR,
            Phase.CONFIRM_CALLER,
            Phase.CONFIRM_CALLEE,
            Phase.READY_COORDINATOR),
        true);
  }

  public RpcOperation<Outcome> releaseTracked(UUID operation, Duration budget) {
    return runTracked(operation, budget, List.of(Phase.RELEASE_HOME), false);
  }

  private RpcOperation<Outcome> runTracked(
      UUID operation, Duration budget, List<Phase> phases, boolean winner) {
    Objects.requireNonNull(operation);
    if (budget == null || budget.isNegative() || budget.isZero())
      return new RpcOperation<>(
          CompletableFuture.completedFuture(new Outcome(operation, "OUTCOME_UNKNOWN", winner)),
          CompletableFuture.completedFuture(null));
    long end = System.nanoTime() + Math.min(budget.toNanos(), Duration.ofSeconds(2).toNanos());
    var scope = new PhysicalScope();
    var logical =
        step(operation, end, phases, 0, winner, scope)
            .handle(
                (code, error) ->
                    new Outcome(
                        operation,
                        error == null ? code : "OUTCOME_UNKNOWN",
                        winner
                            && (error != null
                                || "OUTCOME_UNKNOWN".equals(code)
                                || "UNKNOWN".equals(code))));
    return scope.seal(logical);
  }

  private static boolean succeeded(Phase phase, String code) {
    if ("COMMITTED".equals(code))
      return true; // Legacy Effects adapters; concrete native effects return typed phase codes.
    return switch (phase) {
      case RESERVE_HOME -> "RESERVED".equals(code);
      case RING_COORDINATOR -> "RINGING".equals(code);
      case CLAIM_HOME -> "CLAIMED".equals(code);
      case ACCEPT_COORDINATOR -> "ACCEPTED_PENDING_ACTIVATION".equals(code);
      case ACTIVATE_COORDINATOR -> "ACTIVATING".equals(code);
      case CONFIRM_CALLER, CONFIRM_CALLEE -> "CONFIRMED".equals(code);
      case READY_COORDINATOR -> "CALL_READY".equals(code);
      case RELEASE_HOME -> "RELEASED".equals(code);
    };
  }

  private CompletionStage<String> step(
      UUID operation,
      long end,
      List<Phase> phases,
      int index,
      boolean winner,
      PhysicalScope scope) {
    long remaining = end - System.nanoTime();
    if (remaining <= 0) return CompletableFuture.failedFuture(new TimeoutException());
    CompletionStage<String> stage;
    try {
      stage =
          scope.track(
              effects.applyTracked(phases.get(index), operation, Duration.ofNanos(remaining)));
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    return stage
        .toCompletableFuture()
        .orTimeout(remaining, TimeUnit.NANOSECONDS)
        .thenCompose(
            code -> {
              if (!succeeded(phases.get(index), code) || index == phases.size() - 1)
                return CompletableFuture.completedFuture(code);
              return step(operation, end, phases, index + 1, winner, scope);
            });
  }
}
