package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.rpc.*;
import java.security.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class NativeTerminalReadIT {
  static <T> T done(DbOperation<T> operation) {
    try {
      return operation.logical().toCompletableFuture().join();
    } finally {
      operation.physicalCompletion().toCompletableFuture().join();
    }
  }

  @Test
  void terminalSyncAndResultUseCurrentAuthOnlyNativeProofAfterParticipationRelease()
      throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var caller = f.sender("terminal-sync-caller");
      var winner = f.sender("terminal-sync-winner");
      var call = NegotiationGrantIT.ready(f, caller, winner);
      var hangup = ReattachMediaIT.command(SignalEnvelope.Type.HANGUP, caller, call, 0, 0);
      var committed =
          done(
              f.service()
                  .executeUnderAuthorityTracked(
                      hangup,
                      new CallCommandService.Authority(
                          call, f.token(call), 1, "TEST_ONLY_VERIFIED"),
                      Duration.ofSeconds(2)));
      assertThat(committed.state()).isEqualTo("TERMINAL");
      try (var c = f.connection();
          var q =
              c.prepareStatement(
                  "UPDATE home_participation SET phase='RELEASED',terminal_at=clock_timestamp(),expires_at=clock_timestamp()+interval '24 hours 5 seconds' WHERE call_id=?")) {
        q.setString(1, call.value());
        q.executeUpdate();
      }
      try (var c = f.connection();
          var q = c.prepareStatement("DELETE FROM user_reservation WHERE call_id=?")) {
        q.setString(1, call.value());
        q.executeUpdate();
      }
      f.groups.releaseTracked(f.token(call)).logical().toCompletableFuture().join();
      var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
      var proofs =
          new HomeAuthorizationProof(
              "c001", "test", keys.getPrivate(), Map.of("c001/test", keys.getPublic()));
      var bindings = new ProofBindings(proofs, Clock.systemUTC());
      var commands =
          new CallCommandService(
                  f.runtime.sql,
                  "c001",
                  1,
                  c -> {
                    throw new AssertionError();
                  },
                  bindings.commandVerifier(
                      "c001", u -> new ProofBindings.TrustedHome("c001", 1, 1)))
              .businessAdmission(() -> true);
      var registry = new SessionRegistryService(f.runtime.sql, "c001", 1, (c, p) -> true);
      SessionRepository.Route route;
      try (var c = f.connection()) {
        route = new SessionRepository().find(c, caller.key());
      }
      var view =
          done(
              registry.readCurrentSessionTracked(
                  route, SessionAuthReadIT.principal(route), 1, Duration.ofSeconds(2)));
      var sync = ReattachMediaIT.command(SignalEnvelope.Type.SYNC_CALL, caller, call, 0, 0);
      String syncProof = proofs.sessionProofs().issue(view, sync);
      var snapshot =
          done(commands.loadCallSnapshotAuthorized(sync, syncProof, Duration.ofSeconds(2)));
      assertThat(snapshot.state()).isEqualTo("TERMINAL");
      assertThat(snapshot.version()).isEqualTo(committed.version());
      var adapter = new NativeSnapshotReads(commands);
      var tracked =
          adapter.read(
              new RpcBusinessHandler.SyncRead(
                  caller, call, sync.requestId(), syncProof, sync.intentHash()),
              Duration.ofSeconds(2));
      assertThat(tracked.logical().toCompletableFuture().join().state()).isEqualTo("TERMINAL");
      tracked.physicalCompletion().toCompletableFuture().join();
      var query =
          new CallCommand(
              SignalEnvelope.Type.GET_COMMAND_RESULT,
              caller,
              hangup.requestId(),
              call,
              CommandScope.call(call),
              null,
              null,
              null,
              "{}",
              "d".repeat(64));
      String queryProof = proofs.sessionProofs().issue(view, query);
      assertThat(
              done(commands.getCommandResultAuthorized(query, queryProof, Duration.ofSeconds(2)))
                  .orElseThrow())
          .isEqualTo(committed);
      var bad =
          new AuthenticatedSession(
              caller.userId(),
              caller.key(),
              caller.incarnation(),
              caller.connectionGeneration(),
              UUID.randomUUID());
      var forged =
          new CallCommand(
              sync.type(),
              bad,
              sync.requestId(),
              call,
              sync.scope(),
              null,
              null,
              null,
              "{}",
              sync.intentHash());
      assertThatThrownBy(
              () ->
                  done(
                      commands.loadCallSnapshotAuthorized(
                          forged, syncProof, Duration.ofSeconds(2))))
          .hasCauseInstanceOf(CallCommandService.AuthorizationRejected.class);
    }
  }
}
