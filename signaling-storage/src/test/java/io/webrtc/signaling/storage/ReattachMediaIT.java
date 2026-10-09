package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class ReattachMediaIT {
  static <T> T done(DbOperation<T> operation) {
    try {
      return operation.logical().toCompletableFuture().join();
    } finally {
      operation.physicalCompletion().toCompletableFuture().join();
    }
  }

  static CallCommand command(
      SignalEnvelope.Type type, AuthenticatedSession sender, CallId call, long round, long ice) {
    return new CallCommand(
        type,
        sender,
        new RequestId(UUID.randomUUID()),
        call,
        CommandScope.call(call),
        null,
        round == 0 ? null : new NegotiationId(round),
        ice == 0 ? null : new IceGeneration(ice),
        "{}",
        Integer.toHexString(type.ordinal() % 16).repeat(64));
  }

  static CallCommandService.Authority context(LocalInviteAtomicIT.Fixture f, CallId call) {
    return new CallCommandService.Authority(call, f.token(call), 1, "TEST_ONLY");
  }

  @Test
  void onlyBothCurrentMatchingRoundMediaConnectedObservationsEstablishTheCall() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("media-caller");
      var callee = f.sender("media-winner");
      var call = NegotiationGrantIT.ready(f, caller, callee);
      var callerCommands = NegotiationGrantIT.commands(f, callee);
      var calleeCommands = NegotiationGrantIT.commands(f, caller);
      done(
          callerCommands.executeUnderAuthorityTracked(
              NegotiationGrantIT.request(caller, call), context(f, call), Duration.ofSeconds(2)));
      var first = command(SignalEnvelope.Type.MEDIA_CONNECTED, caller, call, 1, 1);
      assertThat(
              done(callerCommands.executeUnderAuthorityTracked(
                      first, context(f, call), Duration.ofSeconds(2)))
                  .code())
          .isEqualTo("MEDIA_RECORDED");
      assertThat(callerCommands.loadCallSnapshot(caller, call).toCompletableFuture().join().state())
          .isEqualTo("CONNECTING");
      assertThatThrownBy(
              () ->
                  done(
                      calleeCommands.executeUnderAuthorityTracked(
                          command(SignalEnvelope.Type.MEDIA_CONNECTED, callee, call, 1, 2),
                          context(f, call),
                          Duration.ofSeconds(2))))
          .hasCauseInstanceOf(CallCommandService.AuthorizationRejected.class);
      var established =
          done(
              calleeCommands.executeUnderAuthorityTracked(
                  command(SignalEnvelope.Type.MEDIA_CONNECTED, callee, call, 1, 1),
                  context(f, call),
                  Duration.ofSeconds(2)));
      assertThat(established.state()).isEqualTo("ESTABLISHED");
      assertThat(established.code()).isEqualTo("ESTABLISHED");
      assertThat(
              done(callerCommands.executeUnderAuthorityTracked(
                      first, context(f, call), Duration.ofSeconds(2)))
                  .code())
          .isEqualTo("MEDIA_RECORDED");
      assertThat(
              callerCommands
                  .loadCallSnapshot(caller, call)
                  .toCompletableFuture()
                  .join()
                  .deadlines())
          .contains("COMPLETE");
    }
  }

  @Test
  void resumeCommitsNewBindingWithoutResettingEstablishedMediaAndOldDisconnectCannotCloseIt()
      throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("resume-caller");
      var callee = f.sender("resume-winner");
      var call = NegotiationGrantIT.ready(f, caller, callee);
      try (var c = f.connection();
          var q =
              c.prepareStatement(
                  "UPDATE call_state SET state='ESTABLISHED',negotiation_id=1,deadlines=deadlines||'{\"iceGeneration\":\"1\",\"negotiationState\":\"COMPLETE\"}'::jsonb WHERE call_id=?")) {
        q.setString(1, call.value());
        q.executeUpdate();
      }
      SessionRepository.Route old;
      try (var c = f.connection()) {
        old = new SessionRepository().find(c, caller.key());
      }
      var principal = SessionAuthReadIT.principal(old);
      var nativeRoute =
          f.sessions
              .registerSession(principal, f.boot, UUID.randomUUID(), 1)
              .toCompletableFuture()
              .join();
      var newer =
          new AuthenticatedSession(
              nativeRoute.user(),
              nativeRoute.key(),
              nativeRoute.incarnation(),
              2,
              nativeRoute.connectionId());
      var commands = NegotiationGrantIT.commands(f, callee);
      var request = command(SignalEnvelope.Type.RESUME, newer, call, 0, 0);
      var rebound =
          done(
              commands.executeUnderAuthorityTracked(
                  request, context(f, call), Duration.ofSeconds(2)));
      assertThat(rebound.state()).isEqualTo("ESTABLISHED");
      assertThat(rebound.code()).isEqualTo("RESUMED");
      assertThat(f.sessions.closeSessionIfGeneration(old, 1).toCompletableFuture().join())
          .isFalse();
      var snapshot = commands.loadCallSnapshot(newer, call).toCompletableFuture().join();
      assertThat(snapshot.caller().generation()).isEqualTo(2);
      assertThat(snapshot.negotiationId()).isEqualTo(1);
      assertThat(
              done(
                  commands.executeUnderAuthorityTracked(
                      request, context(f, call), Duration.ofSeconds(2))))
          .isEqualTo(rebound);
      assertThatThrownBy(
              () ->
                  done(
                      commands.executeUnderAuthorityTracked(
                          command(SignalEnvelope.Type.RESUME, caller, call, 0, 0),
                          context(f, call),
                          Duration.ofSeconds(2))))
          .hasCauseInstanceOf(CallCommandService.AuthorizationRejected.class);
    }
  }

  @Test
  void coldHydrationInvalidatesIncompleteVolatileRoundButKeepsItsDurableIdentities()
      throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("cold-caller");
      var callee = f.sender("cold-winner");
      var call = NegotiationGrantIT.ready(f, caller, callee);
      done(
          NegotiationGrantIT.commands(f, callee)
              .executeUnderAuthorityTracked(
                  NegotiationGrantIT.request(caller, call),
                  context(f, call),
                  Duration.ofSeconds(2)));
      var workflow =
          new CallWorkflowService(
              f.runtime.sql, "c001", 1, "TEST_ONLY_LOCAL_OWNER", (t, s) -> true);
      var recovered =
          done(workflow.loadCold(call, f.token(call), Duration.ofSeconds(2))).orElseThrow();
      assertThat(recovered.negotiationId()).isEqualTo(1);
      assertThat(recovered.deadlines()).contains("INVALIDATED");
      assertThat(recovered.state()).isEqualTo("CONNECTING");
      assertThat(
              done(NegotiationGrantIT.commands(f, callee)
                      .executeUnderAuthorityTracked(
                          NegotiationGrantIT.request(caller, call),
                          context(f, call),
                          Duration.ofSeconds(2)))
                  .code())
          .isEqualTo("NEGOTIATION_GRANTED");
    }
  }

  @Test
  void establishmentWorkflowCannotBypassBothMatchingMediaObservations() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("bypass-caller");
      var callee = f.sender("bypass-winner");
      var call = NegotiationGrantIT.ready(f, caller, callee);
      done(
          NegotiationGrantIT.commands(f, callee)
              .executeUnderAuthorityTracked(
                  NegotiationGrantIT.request(caller, call),
                  context(f, call),
                  Duration.ofSeconds(2)));
      var workflow =
          new CallWorkflowService(
              f.runtime.sql, "c001", 1, "TEST_ONLY_LOCAL_OWNER", (t, s) -> true);
      var snapshot = done(workflow.load(call, f.token(call), Duration.ofSeconds(2))).orElseThrow();
      var t =
          new CallWorkflowService.Transition(
              call,
              f.token(call),
              1,
              snapshot.version(),
              UUID.randomUUID(),
              CallWorkflowService.Step.ESTABLISH,
              snapshot.winner(),
              List.of(),
              snapshot.activationId(),
              Instant.now().plusSeconds(4),
              Instant.now().plusSeconds(10),
              "TEST_ONLY",
              null);
      assertThatThrownBy(() -> done(workflow.apply(t, Duration.ofSeconds(2))))
          .hasCauseInstanceOf(AuthoritySql.FencedException.class);
    }
  }

  @Test
  void nativeRouteLossGraceNeverExtendsAndExpiredResumeCannotReviveTheCall() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("grace-caller");
      var callee = f.sender("grace-winner");
      var call = NegotiationGrantIT.ready(f, caller, callee);
      var observed = Instant.now().minusSeconds(80);
      var workflow =
          new CallWorkflowService(
              f.runtime.sql, "c001", 1, "TEST_ONLY_LOCAL_OWNER", (t, s) -> true, loss -> observed);
      var loss =
          new CallWorkflowService.RouteLoss(
              call, f.token(call), caller, "TEST_ONLY_NATIVE_CLOSE_PROOF");
      var first = done(workflow.routeLost(loss, Duration.ofSeconds(2)));
      var duplicate = done(workflow.routeLost(loss, Duration.ofSeconds(2)));
      assertThat(duplicate.version()).isEqualTo(first.version());
      assertThat(DurableDeadlines.due(first)).isEqualTo(observed.plusSeconds(75));
      var resumed =
          done(
              NegotiationGrantIT.commands(f, callee)
                  .executeUnderAuthorityTracked(
                      command(SignalEnvelope.Type.RESUME, caller, call, 0, 0),
                      context(f, call),
                      Duration.ofSeconds(2)));
      assertThat(resumed.state()).isEqualTo("TERMINAL");
      assertThat(resumed.code()).isEqualTo("RECONNECT_TIMEOUT");
    }
  }

  @Test
  void durableMediaWindowSurvivesColdLoadAndClientReportsCannotExtendIt() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("window-caller");
      var callee = f.sender("window-winner");
      var call = NegotiationGrantIT.ready(f, caller, callee);
      var commands = NegotiationGrantIT.commands(f, callee);
      done(
          commands.executeUnderAuthorityTracked(
              NegotiationGrantIT.request(caller, call), context(f, call), Duration.ofSeconds(2)));
      done(
          commands.executeUnderAuthorityTracked(
              command(SignalEnvelope.Type.MEDIA_CONNECTED, caller, call, 1, 1),
              context(f, call),
              Duration.ofSeconds(2)));
      done(
          NegotiationGrantIT.commands(f, caller)
              .executeUnderAuthorityTracked(
                  command(SignalEnvelope.Type.MEDIA_CONNECTED, callee, call, 1, 1),
                  context(f, call),
                  Duration.ofSeconds(2)));
      var lost =
          done(
              commands.executeUnderAuthorityTracked(
                  command(SignalEnvelope.Type.MEDIA_DISCONNECTED, caller, call, 1, 1),
                  context(f, call),
                  Duration.ofSeconds(2)));
      assertThat(lost.state()).isEqualTo("ESTABLISHED");
      var snapshot = commands.loadCallSnapshot(caller, call).toCompletableFuture().join();
      var until = DurableDeadlines.due(snapshot);
      assertThat(until).isAfter(Instant.now()).isBefore(Instant.now().plusSeconds(31));
      done(
          commands.executeUnderAuthorityTracked(
              command(SignalEnvelope.Type.MEDIA_DISCONNECTED, caller, call, 1, 1),
              context(f, call),
              Duration.ofSeconds(2)));
      assertThat(
              DurableDeadlines.due(
                  commands.loadCallSnapshot(caller, call).toCompletableFuture().join()))
          .isEqualTo(until);
      try (var c = f.connection();
          var q =
              c.prepareStatement(
                  "UPDATE call_state SET deadlines=jsonb_set(deadlines,'{mediaRecoveryUntil}',to_jsonb(?::text)) WHERE call_id=?")) {
        q.setString(1, Instant.now().minusSeconds(1).toString());
        q.setString(2, call.value());
        q.executeUpdate();
      }
      var workflow =
          new CallWorkflowService(
              f.runtime.sql, "c001", 1, "TEST_ONLY_LOCAL_OWNER", (t, s) -> true);
      var current =
          done(workflow.loadCold(call, f.token(call), Duration.ofSeconds(2))).orElseThrow();
      assertThat(
              done(workflow.expire(call, f.token(call), current.version(), Duration.ofSeconds(2)))
                  .snapshot()
                  .state())
          .isEqualTo("TERMINAL");
    }
  }
}
