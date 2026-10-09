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
