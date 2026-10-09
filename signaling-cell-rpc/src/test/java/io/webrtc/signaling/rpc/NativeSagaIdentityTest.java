package io.webrtc.signaling.rpc;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.actors.user.UserCommand;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class NativeSagaIdentityTest {
  @Test
  void readyHasStableDistinctInternalReplayIdentityWhileAcceptKeepsPublicIdentity() {
    var call = CallId.create("c001", 1);
    var original = UUID.randomUUID();
    var token =
        new AuthoritySql.GroupToken(
            "c001", 1, 1, HomeParticipationService.group(call), 1, "TEST_ONLY", UUID.randomUUID());
    var cleanup = new CompletableFuture<Void>();
    var seen = new ArrayList<UUID>();
    var actors =
        new RpcBusinessHandler.ActorIngress() {
          public CompletionStage<UserCommand.Result> user(
              UserCommand.Operation o, Instant d, int b) {
            throw new AssertionError();
          }

          public CompletionStage<CallCommandService.Outcome> call(
              CallCommand c, String p, Instant d, int b) {
            throw new AssertionError();
          }

          public CompletionStage<CallWorkflowService.Outcome> progress(
              CallWorkflowService.Transition t, Instant d, int b) {
            seen.add(t.operation());
            return CompletableFuture.completedFuture(
                new CallWorkflowService.Outcome("TEST_ONLY", null, List.of()));
          }

          public RpcOperation<CallWorkflowService.Outcome> progressTracked(
              CallWorkflowService.Transition t, Instant d, int b) {
            return new RpcOperation<>(progress(t, d, b), cleanup);
          }
        };
    var effects =
        new NativeSagaEffects(
            actors,
            (o, w, b) -> {
              throw new AssertionError();
            },
            phase ->
                new NativeSagaEffects.CoordinatorStep(
                    new CallWorkflowService.Transition(
                        call,
                        token,
                        1,
                        1,
                        original,
                        phase == CrossCellSaga.Phase.ACCEPT_COORDINATOR
                            ? CallWorkflowService.Step.ACCEPT
                            : CallWorkflowService.Step.READY,
                        null,
                        List.of(),
                        UUID.randomUUID(),
                        Instant.now().plusSeconds(5),
                        null,
                        "TEST_ONLY",
                        null)),
            Clock.systemUTC());
    effects
        .apply(CrossCellSaga.Phase.ACCEPT_COORDINATOR, original, Duration.ofSeconds(2))
        .toCompletableFuture()
        .join();
    var pending =
        effects.applyTracked(
            CrossCellSaga.Phase.READY_COORDINATOR, original, Duration.ofSeconds(2));
    pending.logical().toCompletableFuture().join();
    assertThat(pending.physicalCompletion().toCompletableFuture()).isNotDone();
    effects
        .apply(CrossCellSaga.Phase.READY_COORDINATOR, original, Duration.ofSeconds(2))
        .toCompletableFuture()
        .join();
    assertThat(seen.getFirst()).isEqualTo(original);
    assertThat(seen.get(1)).isNotEqualTo(original).isEqualTo(seen.get(2));
    cleanup.complete(null);
    assertThat(pending.physicalCompletion().toCompletableFuture()).isCompleted();
  }

  @Test
  void activationCommitsCoordinatorIntentBeforeConfirmingEitherHome() {
    var phases = new ArrayList<String>();
    var saga =
        new CrossCellSaga(
            (phase, op, budget) -> {
              phases.add(phase.name());
              return CompletableFuture.completedFuture("COMMITTED");
            });
    saga.activate(UUID.randomUUID(), Duration.ofSeconds(2)).toCompletableFuture().join();
    assertThat(phases)
        .containsExactly(
            "ACTIVATE_COORDINATOR", "CONFIRM_CALLER", "CONFIRM_CALLEE", "READY_COORDINATOR");
  }

  @Test
  void outerSagaUnknownWaitsForOriginalPhasePhysicalCleanup() {
    var cleanup = new CompletableFuture<Void>();
    var phases = new ArrayList<CrossCellSaga.Phase>();
    var effects =
        new CrossCellSaga.Effects() {
          public CompletionStage<String> apply(
              CrossCellSaga.Phase phase, UUID original, Duration budget) {
            phases.add(phase);
            return CompletableFuture.failedFuture(new DbOutcomeUnknownException());
          }

          public RpcOperation<String> applyTracked(
              CrossCellSaga.Phase phase, UUID original, Duration budget) {
            return new RpcOperation<>(apply(phase, original, budget), cleanup);
          }
        };
    var operation =
        new CrossCellSaga(effects).acceptTracked(UUID.randomUUID(), Duration.ofSeconds(2));
    assertThat(operation.logical().toCompletableFuture().join().code())
        .isEqualTo("OUTCOME_UNKNOWN");
    assertThat(phases).containsExactly(CrossCellSaga.Phase.CLAIM_HOME);
    assertThat(operation.physicalCompletion().toCompletableFuture()).isNotDone();
    cleanup.complete(null);
    assertThat(operation.physicalCompletion().toCompletableFuture()).isCompleted();
  }
}
