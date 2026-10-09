package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class AcceptCompletionIT {
  static <T> T finish(DbOperation<T> work) {
    try {
      return work.logical().toCompletableFuture().join();
    } finally {
      work.physicalCompletion().toCompletableFuture().join();
    }
  }

  static CallCommand accept(AuthenticatedSession sender, CallId call) {
    return new CallCommand(
        SignalEnvelope.Type.ACCEPT,
        sender,
        new RequestId(UUID.randomUUID()),
        call,
        CommandScope.call(call),
        null,
        null,
        null,
        "{}",
        "b".repeat(64));
  }

  @Test
  void homeWinnerCommitFinalizesOriginalAcceptResultAndReplayRetainsRecordedOutcome()
      throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("accept-caller");
      var callee = f.sender("accept-callee");
      var commands = f.service();
      var invite =
          commands
              .executeCallCommand(f.invite(caller, callee.userId()))
              .toCompletableFuture()
              .join();
      var call = invite.callId();
      var token = f.token(call);
      var command = accept(callee, call);
      var context = new CallCommandService.Authority(call, token, 1, "TEST_ONLY_VERIFIED", 1);
      assertThat(
              finish(commands.executeUnderAuthorityTracked(command, context, Duration.ofSeconds(2)))
                  .status())
          .isEqualTo("PENDING");
      var winner =
          new Participant(
              callee.userId(), callee.key(), callee.incarnation(), callee.connectionGeneration());
      var transition =
          new CallWorkflowService.Transition(
              call,
              token,
              1,
              1,
              command.requestId().value(),
              CallWorkflowService.Step.ACCEPT,
              winner,
              List.of(),
              null,
              Instant.now().plusSeconds(5),
              Instant.now().plusSeconds(30),
              "TEST_ONLY_VERIFIED",
              null);
      var workflow =
          new CallWorkflowService(
              f.runtime.sql,
              "c001",
              1,
              "TEST_ONLY_LOCAL_OWNER",
              (t, s) -> t.proof().equals("TEST_ONLY_VERIFIED"));
      var committed = finish(workflow.apply(transition, Duration.ofSeconds(2)));
      var recorded =
          commands
              .getCommandResult(callee, command.scope(), command.requestId())
              .toCompletableFuture()
              .join()
              .orElseThrow();
      assertThat(recorded.status()).isEqualTo("FINAL");
      assertThat(recorded.code()).isEqualTo("ACCEPTED_PENDING_ACTIVATION");
      assertThat(recorded.version()).isEqualTo(2);
      assertThat(recorded.eventIds()).isEqualTo(committed.eventIds());
      assertThat(
              finish(
                  commands.executeUnderAuthorityTracked(command, context, Duration.ofSeconds(2))))
          .isEqualTo(recorded);
    }
  }

  @Test
  void subsequentAcceptReturnsImmutableWinnerDecisionInsteadOfLeavingAnotherPendingResult()
      throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("repeat-caller");
      var callee = f.sender("repeat-callee");
      var other = f.sender("repeat-callee");
      var commands = f.service();
      var call =
          commands
              .executeCallCommand(f.invite(caller, callee.userId()))
              .toCompletableFuture()
              .join()
              .callId();
      var token = f.token(call);
      var winner = new Participant(callee.userId(), callee.key(), callee.incarnation(), 1);
      var workflow =
          new CallWorkflowService(
              f.runtime.sql, "c001", 1, "TEST_ONLY_LOCAL_OWNER", (t, s) -> true);
      finish(
          workflow.apply(
              new CallWorkflowService.Transition(
                  call,
                  token,
                  1,
                  1,
                  UUID.randomUUID(),
                  CallWorkflowService.Step.ACCEPT,
                  winner,
                  List.of(),
                  null,
                  Instant.now().plusSeconds(5),
                  Instant.now().plusSeconds(30),
                  "TEST_ONLY_VERIFIED",
                  null),
              Duration.ofSeconds(2)));
      var context = new CallCommandService.Authority(call, token, 1, "TEST_ONLY_VERIFIED", 2);
      assertThat(
              finish(
                      commands.executeUnderAuthorityTracked(
                          accept(callee, call), context, Duration.ofSeconds(2)))
                  .code())
          .isEqualTo("ACCEPTED_PENDING_ACTIVATION");
      assertThat(
              finish(
                      commands.executeUnderAuthorityTracked(
                          accept(other, call), context, Duration.ofSeconds(2)))
                  .code())
          .isEqualTo("ANSWERED_ELSEWHERE");
    }
  }
}
