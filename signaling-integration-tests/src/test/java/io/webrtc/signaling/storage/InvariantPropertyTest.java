package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.util.*;
import net.jqwik.api.*;
import net.jqwik.api.constraints.*;

/**
 * Generated schedules exercise native SQL, rather than a second implementation of the state
 * machine.
 */
class InvariantPropertyTest {
  @Property(tries = 16, seed = "2026100401")
  void duplicateReorderedAndLostRepliesKeepOneCallAndAnAbsorbingTerminalSnapshot(
      @ForAll @Size(min = 8, max = 24) List<@IntRange(min = 0, max = 5) Integer> schedule)
      throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("property-caller");
      var callee = f.sender("property-callee");
      var stranger = f.sender("property-stranger");
      var invite = f.invite(caller, callee.userId());
      var commands = f.service();
      var first = commands.executeCallCommand(invite).toCompletableFuture().join();
      var call = first.callId();
      var token = f.token(call);
      var cancel = command(SignalEnvelope.Type.CANCEL, caller, call, "b");
      var decline = command(SignalEnvelope.Type.DECLINE_ALL, callee, call, "c");
      var context = new CallCommandService.Authority(call, token, 1, "TEST_ONLY_VERIFIED");
      CallSnapshotRepository.Snapshot terminal = null;
      long previousVersion = 1;
      for (int action : schedule) {
        switch (action) {
          case 0 ->
              assertThat(commands.executeCallCommand(invite).toCompletableFuture().join())
                  .isEqualTo(first);
          case 1 ->
              CoordinatorGrantIT.done(
                  commands.executeUnderAuthorityTracked(cancel, context, Duration.ofSeconds(2)));
          case 2 ->
              CoordinatorGrantIT.done(
                  commands.executeUnderAuthorityTracked(decline, context, Duration.ofSeconds(2)));
          case 3 -> { // Drop one already committed response, then query the original native result.
            var lost =
                CoordinatorGrantIT.done(
                    commands.executeUnderAuthorityTracked(cancel, context, Duration.ofSeconds(2)));
            var stored =
                CoordinatorGrantIT.done(
                    f.runtime.sql.submitTracked(
                        DbClass.RECOVERY,
                        Duration.ofSeconds(2),
                        c ->
                            new CommandResultRepository()
                                .find(c, caller.key(), cancel.scope(), cancel.requestId())));
            assertThat(stored.outcome()).isEqualTo(lost);
          }
          case 4 -> {
            var borrowed =
                new AuthoritySql.GroupToken(
                    token.cell(),
                    token.storageEpoch(),
                    token.hashVersion(),
                    token.group(),
                    token.epoch(),
                    "TEST_ONLY_BORROWED_OWNER",
                    token.incarnation());
            assertThatThrownBy(
                    () ->
                        CoordinatorGrantIT.done(
                            commands.executeUnderAuthorityTracked(
                                cancel,
                                new CallCommandService.Authority(
                                    call, borrowed, 1, "TEST_ONLY_VERIFIED"),
                                Duration.ofSeconds(2))))
                .hasCauseInstanceOf(AuthoritySql.FencedException.class);
          }
          case 5 ->
              assertThatThrownBy(
                      () ->
                          CoordinatorGrantIT.done(
                              commands.executeUnderAuthorityTracked(
                                  command(SignalEnvelope.Type.CANCEL, stranger, call, "d"),
                                  context,
                                  Duration.ofSeconds(2))))
                  .hasCauseInstanceOf(CallCommandService.AuthorizationRejected.class);
          default -> throw new AssertionError();
        }
        var snapshot = commands.loadCallSnapshot(caller, call).toCompletableFuture().join();
        assertThat(snapshot.version()).isGreaterThanOrEqualTo(previousVersion);
        previousVersion = snapshot.version();
        if (terminal != null) assertThat(snapshot).isEqualTo(terminal);
        if (snapshot.terminalAt() != null) terminal = snapshot;
        assertThat(CrashScheduleIT.count(f, "call_state")).isEqualTo(1);
        assertThat(CrashScheduleIT.count(f, "control_outbox")).isLessThanOrEqualTo(3);
        try (var c = f.connection();
            var q = c.createStatement();
            var r =
                q.executeQuery(
                    "SELECT count(*) FROM (SELECT user_id FROM user_reservation GROUP BY user_id HAVING count(*)>1) duplicates")) {
          r.next();
          assertThat(r.getInt(1)).isZero();
        }
      }
    }
  }

  @Property(tries = 16, seed = "2026100402")
  void delayedOldGenerationCloseAndReadCannotAffectAnyNewNativeRoute(
      @ForAll @IntRange(min = 1, max = 5) int replacements) throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var sender = f.sender("property-reconnect");
      SessionRepository.Route current = SessionAuthReadIT.route(f, sender);
      var original = current;
      var registry = new SessionRegistryService(f.runtime.sql, "c001", 1, (c, p) -> true);
      for (int n = 0; n < replacements; n++) {
        var old = current;
        current =
            registry
                .registerSession(SessionAuthReadIT.principal(old), f.boot, UUID.randomUUID(), 1)
                .toCompletableFuture()
                .join();
        assertThat(current.connectionGeneration()).isEqualTo(old.connectionGeneration() + 1);
        assertThat(current.incarnation()).isEqualTo(original.incarnation());
        assertThat(registry.closeSessionIfGeneration(old, 1).toCompletableFuture().join())
            .isFalse();
        assertThatThrownBy(
                () ->
                    CoordinatorGrantIT.done(
                        registry.readCurrentSessionTracked(
                            old, SessionAuthReadIT.principal(old), 1, Duration.ofSeconds(2))))
            .hasCauseInstanceOf(AuthoritySql.FencedException.class);
        assertThat(
                CoordinatorGrantIT.done(
                        registry.readCurrentSessionTracked(
                            current,
                            SessionAuthReadIT.principal(current),
                            1,
                            Duration.ofSeconds(2)))
                    .route())
            .isEqualTo(current);
      }
    }
  }

  @Property(tries = 8, seed = "2026100901")
  void everyNativeLifecyclePhaseSurvivesGeneratedDuplicateLossAndStaleReordering(
      @ForAll @Size(min = 3, max = 8) List<@IntRange(min = 0, max = 2) Integer> schedule)
      throws Exception {
    // Every generated run terminates from each live phase, including ESTABLISHED.
    for (int terminalPhase = 0; terminalPhase <= 4; terminalPhase++) {
      try (var f = new LocalInviteAtomicIT.Fixture()) {
        var caller = f.sender("lifecycle-caller");
        var callee = f.sender("lifecycle-winner");
        var call =
            f.service()
                .executeCallCommand(f.invite(caller, callee.userId()))
                .toCompletableFuture()
                .join()
                .callId();
        var group = f.token(call);
        var workflow =
            new CallWorkflowService(
                f.runtime.sql, "c001", 1, "TEST_ONLY_LOCAL_OWNER", (t, snapshot) -> true);
        var winner =
            new CallSnapshotRepository.Participant(
                callee.userId(), callee.key(), callee.incarnation(), 1);
        var activation = UUID.randomUUID();
        var expectedStates =
            List.of("RINGING", "ACCEPTED", "ACTIVATING", "CONNECTING", "ESTABLISHED");
        var steps =
            List.of(
                CallWorkflowService.Step.ACCEPT,
                CallWorkflowService.Step.ACTIVATE,
                CallWorkflowService.Step.READY);
        CallWorkflowService.Transition previous = null;
        CallWorkflowService.Outcome committed = null;
        for (int phase = 0; phase <= terminalPhase; phase++) {
          var snapshot =
              CoordinatorGrantIT.done(workflow.load(call, group, Duration.ofSeconds(2)))
                  .orElseThrow();
          assertThat(snapshot.state()).isEqualTo(expectedStates.get(phase));
          var reordered =
              transition(
                  call,
                  group,
                  snapshot.version() == 1 ? 2 : snapshot.version() - 1,
                  CallWorkflowService.Step.READY,
                  winner,
                  activation);
          for (int action : schedule) {
            if (action == 0 && previous != null) {
              // Reconcile an original operation even after losing its first application reply.
              var replay = CoordinatorGrantIT.done(workflow.apply(previous, Duration.ofSeconds(2)));
              assertThat(replay.code()).isEqualTo(committed.code());
              assertThat(replay.eventIds()).isEqualTo(committed.eventIds());
            } else if (action == 1) {
              assertThat(
                      CoordinatorGrantIT.done(workflow.apply(reordered, Duration.ofSeconds(2)))
                          .code())
                  .isEqualTo("STALE_VERSION");
            } else if (action == 2) {
              var borrowed =
                  new AuthoritySql.GroupToken(
                      group.cell(),
                      group.storageEpoch(),
                      group.hashVersion(),
                      group.group(),
                      group.epoch(),
                      "TEST_ONLY_OLD_ACTOR",
                      group.incarnation());
              assertThatThrownBy(
                      () ->
                          CoordinatorGrantIT.done(
                              workflow.load(call, borrowed, Duration.ofSeconds(2))))
                  .hasCauseInstanceOf(AuthoritySql.FencedException.class);
            }
            assertThat(
                    CoordinatorGrantIT.done(workflow.load(call, group, Duration.ofSeconds(2)))
                        .orElseThrow())
                .isEqualTo(snapshot);
          }
          if (phase == terminalPhase) break;
          if (phase < 3) {
            previous =
                transition(call, group, snapshot.version(), steps.get(phase), winner, activation);
            committed = CoordinatorGrantIT.done(workflow.apply(previous, Duration.ofSeconds(2)));
            assertThat(committed.snapshot().version()).isEqualTo(snapshot.version() + 1);
          } else {
            // Establish through real negotiation and both matching native media observations.
            var callerCommands = NegotiationGrantIT.commands(f, callee);
            var calleeCommands = NegotiationGrantIT.commands(f, caller);
            CoordinatorGrantIT.done(
                callerCommands.executeUnderAuthorityTracked(
                    NegotiationGrantIT.request(caller, call),
                    ReattachMediaIT.context(f, call),
                    Duration.ofSeconds(2)));
            CoordinatorGrantIT.done(
                callerCommands.executeUnderAuthorityTracked(
                    ReattachMediaIT.command(
                        SignalEnvelope.Type.MEDIA_CONNECTED, caller, call, 1, 1),
                    ReattachMediaIT.context(f, call),
                    Duration.ofSeconds(2)));
            var media =
                CoordinatorGrantIT.done(
                    calleeCommands.executeUnderAuthorityTracked(
                        ReattachMediaIT.command(
                            SignalEnvelope.Type.MEDIA_CONNECTED, callee, call, 1, 1),
                        ReattachMediaIT.context(f, call),
                        Duration.ofSeconds(2)));
            assertThat(media.state()).isEqualTo("ESTABLISHED");
          }
        }
        var before =
            CoordinatorGrantIT.done(workflow.load(call, group, Duration.ofSeconds(2)))
                .orElseThrow();
        var end =
            transition(
                call,
                group,
                before.version(),
                CallWorkflowService.Step.TERMINATE,
                winner,
                activation);
        var terminal =
            CoordinatorGrantIT.done(workflow.apply(end, Duration.ofSeconds(2))).snapshot();
        assertThat(terminal.state()).isEqualTo("TERMINAL");
        assertThat(terminal.version()).isEqualTo(before.version() + 1);
        assertThat(terminal.winner()).isEqualTo(before.winner());
        long outbox = CrashScheduleIT.count(f, "control_outbox");
        for (var step : CallWorkflowService.Step.values()) {
          var absorbed =
              CoordinatorGrantIT.done(
                  workflow.apply(
                      transition(call, group, terminal.version(), step, winner, activation),
                      Duration.ofSeconds(2)));
          assertThat(absorbed.code()).isEqualTo("ALREADY_TERMINAL");
          assertThat(absorbed.snapshot()).isEqualTo(terminal);
        }
        assertThat(CrashScheduleIT.count(f, "control_outbox")).isEqualTo(outbox);
        assertThat(CrashScheduleIT.count(f, "call_state")).isEqualTo(1);
      }
    }
  }

  private static CallWorkflowService.Transition transition(
      CallId call,
      AuthoritySql.GroupToken group,
      long version,
      CallWorkflowService.Step step,
      CallSnapshotRepository.Participant winner,
      UUID activation) {
    return new CallWorkflowService.Transition(
        call,
        group,
        1,
        version,
        UUID.randomUUID(),
        step,
        winner,
        List.of(),
        activation,
        Instant.now().plusMillis(4500),
        Instant.now().plusSeconds(30),
        "TEST_ONLY_VERIFIED",
        step == CallWorkflowService.Step.TERMINATE ? "CANCELED" : null);
  }

  private static CallCommand command(
      SignalEnvelope.Type type, AuthenticatedSession sender, CallId call, String hash) {
    return new CallCommand(
        type,
        sender,
        new RequestId(UUID.randomUUID()),
        call,
        CommandScope.call(call),
        null,
        null,
        null,
        "{}",
        hash.repeat(64));
  }
}
