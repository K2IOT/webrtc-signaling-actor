package io.webrtc.signaling.rpc;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import java.security.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class SessionAuthorizationProofTest {
  @Test
  void nativeAuthOnlyProofBindsFullConnectionRequestAndHomeWithoutInventingAGroupTenure()
      throws Exception {
    var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    var codec =
        new SessionAuthorizationProof(
            "c001", "test", keys.getPrivate(), Map.of("c001/test", keys.getPublic()));
    Instant now = Instant.now();
    var route =
        new SessionRepository.Route(
            new UserId("caller"),
            new SessionKey("TEST_ONLY", "j"),
            new SessionIncarnation(UUID.randomUUID()),
            2,
            "TEST_ONLY_GW",
            UUID.randomUUID(),
            UUID.randomUUID(),
            now.plusSeconds(30),
            "TEST_ONLY",
            1);
    var view =
        new SessionRegistryService.SessionProofView(route, now, now.plusSeconds(5), "c001", 1, 1);
    var call = CallId.create("c002", 1);
    var sender =
        new AuthenticatedSession(
            route.user(), route.key(), route.incarnation(), 2, route.connectionId());
    var command =
        new CallCommand(
            SignalEnvelope.Type.RESUME,
            sender,
            new RequestId(UUID.randomUUID()),
            call,
            CommandScope.call(call),
            null,
            null,
            null,
            "{}",
            "a".repeat(64));
    var signed = codec.issue(view, command);
    assertThat(codec.verify(signed, command, new ProofBindings.TrustedHome("c001", 1, 1), now))
        .isTrue();
    var fakeConnection =
        new AuthenticatedSession(
            sender.userId(), sender.key(), sender.incarnation(), 2, UUID.randomUUID());
    var forged =
        new CallCommand(
            command.type(),
            fakeConnection,
            command.requestId(),
            call,
            command.scope(),
            null,
            null,
            null,
            "{}",
            command.intentHash());
    assertThat(codec.verify(signed, forged, new ProofBindings.TrustedHome("c001", 1, 1), now))
        .isFalse();
    assertThat(codec.verify(signed, command, new ProofBindings.TrustedHome("c001", 2, 1), now))
        .isFalse();
    assertThat(
            codec.verify(
                signed, command, new ProofBindings.TrustedHome("c001", 1, 1), now.plusSeconds(5)))
        .isFalse();
    assertThat(codec.decode(signed, "c002", now)).isEmpty();
    assertThat(codec.decode(signed, "c001", now).orElseThrow().toString()).doesNotContain("jti=j");
  }

  @Test
  void authOnlyProofCanAuthorizeTerminalSnapshotReadsButIsNotAnActiveParticipantProof()
      throws Exception {
    var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    var home =
        new HomeAuthorizationProof(
            "c001", "test", keys.getPrivate(), Map.of("c001/test", keys.getPublic()));
    var codec = home.sessionProofs();
    Instant now = Instant.now();
    var route =
        new SessionRepository.Route(
            new UserId("caller"),
            new SessionKey("TEST_ONLY", "j"),
            new SessionIncarnation(UUID.randomUUID()),
            1,
            "TEST_ONLY_GW",
            UUID.randomUUID(),
            UUID.randomUUID(),
            now.plusSeconds(30),
            "TEST_ONLY",
            1);
    var command =
        new CallCommand(
            SignalEnvelope.Type.SYNC_CALL,
            new AuthenticatedSession(
                route.user(), route.key(), route.incarnation(), 1, route.connectionId()),
            new RequestId(UUID.randomUUID()),
            CallId.create("c001", 1),
            CommandScope.call(new CallId("c001.e1.00000000-0000-0000-0000-000000000001")),
            null,
            null,
            null,
            "{}",
            "b".repeat(64));
    command =
        new CallCommand(
            command.type(),
            command.sender(),
            command.requestId(),
            command.callId(),
            CommandScope.call(command.callId()),
            null,
            null,
            null,
            "{}",
            command.intentHash());
    var signed =
        codec.issue(
            new SessionRegistryService.SessionProofView(
                route, now, now.plusSeconds(5), "c001", 1, 1),
            command);
    var bindings = new ProofBindings(home, Clock.systemUTC());
    assertThat(
            bindings
                .commandVerifier("c001", u -> new ProofBindings.TrustedHome("c001", 1, 1))
                .verify(command, null, signed))
        .isTrue();
    assertThat(home.decode(signed, "c001", now)).isEmpty();
  }
}
