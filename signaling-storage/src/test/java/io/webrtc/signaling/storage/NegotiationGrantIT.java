package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class NegotiationGrantIT {
  static CallCommand request(AuthenticatedSession sender, CallId call) {
    return new CallCommand(
        SignalEnvelope.Type.NEGOTIATE_REQUEST,
        sender,
        new RequestId(UUID.randomUUID()),
        call,
        CommandScope.call(call),
        null,
        null,
        null,
        "{}",
        "c".repeat(64));
  }

  static <T> T done(DbOperation<T> operation) {
    try {
      return operation.logical().toCompletableFuture().join();
    } finally {
      operation.physicalCompletion().toCompletableFuture().join();
    }
  }

  static CallCommandService.Outcome contention(DbOperation<CallCommandService.Outcome> operation) {
    try {
      return done(operation);
    } catch (java.util.concurrent.CompletionException error) {
      if (error.getCause() instanceof AuthoritySql.RetryableConflict) return null;
      throw error;
    }
  }

  static CallId ready(
      LocalInviteAtomicIT.Fixture f, AuthenticatedSession caller, AuthenticatedSession callee)
      throws Exception {
    var call =
        f.service()
            .executeCallCommand(f.invite(caller, callee.userId()))
            .toCompletableFuture()
            .join()
            .callId();
    var group = f.token(call);
    var workflow =
        new CallWorkflowService(f.runtime.sql, "c001", 1, "TEST_ONLY_LOCAL_OWNER", (t, s) -> true);
    var winner = new Participant(callee.userId(), callee.key(), callee.incarnation(), 1);
    var activation = UUID.randomUUID();
    long version = 1;
    for (var step :
        List.of(
            CallWorkflowService.Step.ACCEPT,
            CallWorkflowService.Step.ACTIVATE,
            CallWorkflowService.Step.READY))
      done(
          workflow.apply(
              new CallWorkflowService.Transition(
                  call,
                  group,
                  1,
                  version++,
                  UUID.randomUUID(),
                  step,
                  winner,
                  List.of(),
                  activation,
                  Instant.now().plusSeconds(5),
                  Instant.now().plusSeconds(30),
                  "TEST_ONLY",
                  null),
              Duration.ofSeconds(2)));
    return call;
  }

  static CallCommandService commands(
      LocalInviteAtomicIT.Fixture f, AuthenticatedSession recipient) {
    return new CallCommandService(
            f.runtime.sql,
            "c001",
            1,
            c -> {
              throw new AssertionError();
            },
            (c, s, p) -> p.equals("TEST_ONLY"),
            (c, command, s, p) ->
                new CallCommandService.NegotiationEvidence(
                    recipient, Instant.now().plusSeconds(4), Instant.now().plusSeconds(30)))
        .businessAdmission(() -> true);
  }

  @Test
  void firstRoundIsCallerOnlyAndCommittedMetadataHasNoSdpBody() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("neg-caller");
      var callee = f.sender("neg-callee");
      var call = ready(f, caller, callee);
      var context = new CallCommandService.Authority(call, f.token(call), 1, "TEST_ONLY", 4);
      assertThatThrownBy(
              () ->
                  done(
                      commands(f, caller)
                          .executeUnderAuthorityTracked(
                              request(callee, call), context, Duration.ofSeconds(2))))
          .hasCauseInstanceOf(CallCommandService.AuthorizationRejected.class);
      var command = request(caller, call);
      var service = commands(f, callee);
      var outcome =
          done(service.executeUnderAuthorityTracked(command, context, Duration.ofSeconds(2)));
      assertThat(outcome.code()).isEqualTo("NEGOTIATION_GRANTED");
      assertThat(outcome.version()).isEqualTo(5);
      assertThat(
              done(service.executeUnderAuthorityTracked(command, context, Duration.ofSeconds(2))))
          .isEqualTo(outcome);
      var snapshot = service.loadCallSnapshot(caller, call).toCompletableFuture().join();
      assertThat(snapshot.negotiationId()).isEqualTo(1);
      assertThat(snapshot.deadlines())
          .contains("iceGeneration", "negotiationUntil", "offerer")
          .doesNotContain("sdp", "candidate");
      assertThat(
              done(service.executeUnderAuthorityTracked(
                      request(caller, call),
                      new CallCommandService.Authority(call, f.token(call), 1, "TEST_ONLY", 5),
                      Duration.ofSeconds(2)))
                  .code())
          .isEqualTo("NEGOTIATION_BUSY");
    }
  }

  @Test
  void simultaneousRestartGrantsOnlyOneRoundAndTakeoverRejectsOldRoot() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("restart-caller");
      var callee = f.sender("restart-callee");
      var call = ready(f, caller, callee);
      try (var c = f.connection();
          var q =
              c.prepareStatement(
                  "UPDATE call_state SET state='ESTABLISHED',negotiation_id=1,deadlines=deadlines||'{\"iceGeneration\":\"1\",\"negotiationState\":\"COMPLETE\"}'::jsonb WHERE call_id=?")) {
        q.setString(1, call.value());
        q.executeUpdate();
      }
      var context = new CallCommandService.Authority(call, f.token(call), 1, "TEST_ONLY");
      var first = request(caller, call);
      var second = request(callee, call);
      var a =
          commands(f, callee).executeUnderAuthorityTracked(first, context, Duration.ofSeconds(2));
      var b =
          commands(f, caller).executeUnderAuthorityTracked(second, context, Duration.ofSeconds(2));
      var resultA = contention(a);
      var resultB = contention(b);
      if (resultA == null)
        resultA =
            done(
                commands(f, callee)
                    .executeUnderAuthorityTracked(first, context, Duration.ofSeconds(2)));
      if (resultB == null)
        resultB =
            done(
                commands(f, caller)
                    .executeUnderAuthorityTracked(second, context, Duration.ofSeconds(2)));
      var outcomes = List.of(resultA.code(), resultB.code());
      assertThat(outcomes).containsExactlyInAnyOrder("NEGOTIATION_GRANTED", "NEGOTIATION_BUSY");
      var token = f.token(call);
      f.groups.releaseTracked(token).logical().toCompletableFuture().join();
      assertThatThrownBy(
              () ->
                  done(
                      commands(f, callee)
                          .executeUnderAuthorityTracked(
                              request(caller, call), context, Duration.ofSeconds(2))))
          .hasCauseInstanceOf(AuthoritySql.FencedException.class);
    }
  }

  @Test
  void absentNativeTwoHomeEvidenceFailsClosed() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("no-proof-caller");
      var callee = f.sender("no-proof-callee");
      var call = ready(f, caller, callee);
      assertThatThrownBy(
              () ->
                  done(
                      f.service()
                          .executeUnderAuthorityTracked(
                              request(caller, call),
                              new CallCommandService.Authority(
                                  call, f.token(call), 1, "TEST_ONLY_VERIFIED"),
                              Duration.ofSeconds(2))))
          .hasCauseInstanceOf(CallCommandService.AuthorizationRejected.class);
    }
  }
}
