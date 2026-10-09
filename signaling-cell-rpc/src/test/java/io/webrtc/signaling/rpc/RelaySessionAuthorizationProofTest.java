package io.webrtc.signaling.rpc;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import java.security.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

/**
 * TEST_ONLY source keys/views; purpose and binding arithmetic, never a native authority
 * attestation.
 */
class RelaySessionAuthorizationProofTest {
  final Instant now = Instant.parse("2026-10-05T00:00:00Z");
  final CallId call = CallId.create("c002", 1);
  final SessionRepository.Route route =
      new SessionRepository.Route(
          new UserId("TEST_ONLY_USER"),
          new SessionKey("TEST_ONLY_ISSUER", "TEST_ONLY_JTI"),
          new SessionIncarnation(UUID.randomUUID()),
          2,
          "TEST_ONLY_GW",
          UUID.randomUUID(),
          UUID.randomUUID(),
          now.plusSeconds(30),
          "TEST_ONLY_KEY",
          1);
  final AuthenticatedSession sender =
      new AuthenticatedSession(
          route.user(), route.key(), route.incarnation(), 2, route.connectionId());
  final SessionRegistryService.SessionProofView view =
      new SessionRegistryService.SessionProofView(route, now, now.plusSeconds(5), "c001", 1, 1);
  final ProofBindings.TrustedHome home = new ProofBindings.TrustedHome("c001", 1, 1);

  CallCommand command(
      SignalEnvelope.Type type,
      AuthenticatedSession own,
      CallId id,
      long round,
      long ice,
      String body) {
    return new CallCommand(
        type,
        own,
        new RequestId(UUID.randomUUID()),
        id,
        CommandScope.call(id),
        null,
        new NegotiationId(round),
        new IceGeneration(ice),
        body,
        "a".repeat(64));
  }

  @Test
  void authOnlyRoundProofCanServeDifferentVolatileFramesWithoutRestartingOriginalExpiry()
      throws Exception {
    var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    var codec =
        new RelaySessionAuthorizationProof(
            "c001", "test", key.getPrivate(), Map.of("c001/test", key.getPublic()));
    var first = command(SignalEnvelope.Type.OFFER, sender, call, 1, 1, "{}");
    var signed = codec.issue(view, first);
    var verifyOnly = new RelaySessionAuthorizationProof(Map.of("c001/test", key.getPublic()));
    var bindings =
        new ProofBindings(
            new HomeAuthorizationProof(
                "c001", "test", key.getPrivate(), Map.of("c001/test", key.getPublic())),
            Clock.fixed(now, ZoneOffset.UTC));
    assertThat(bindings.commandVerifier("c002", u -> home).verify(first, null, signed)).isTrue();
    for (var type :
        List.of(
            SignalEnvelope.Type.OFFER,
            SignalEnvelope.Type.ANSWER,
            SignalEnvelope.Type.ICE_CANDIDATES,
            SignalEnvelope.Type.END_OF_CANDIDATES)) {
      assertThat(
              verifyOnly.verify(
                  signed,
                  command(type, sender, call, 1, 1, "{\"newFrame\":true}"),
                  home,
                  now.plusSeconds(1)))
          .isTrue();
    }
    assertThat(
            verifyOnly
                .decode(signed, "c001", now.plusSeconds(1))
                .orElseThrow()
                .nativeView()
                .proofUntil())
        .isEqualTo(view.proofUntil());
    assertThat(verifyOnly.verify(signed, first, home, now.plusSeconds(5))).isFalse();
    assertThat(verifyOnly.verify(signed, first, home, now.plusMillis(4745))).isFalse();
  }

  @Test
  void criticalS1AndActiveHomeNamespacesCannotUseAReusableRelayProof() throws Exception {
    var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    var trusted = Map.of("c001/test", key.getPublic());
    var codec = new RelaySessionAuthorizationProof("c001", "test", key.getPrivate(), trusted);
    var original = command(SignalEnvelope.Type.OFFER, sender, call, 1, 1, "{}");
    var signed = codec.issue(view, original);
    assertThat(
            new SessionAuthorizationProof("c001", "test", key.getPrivate(), trusted)
                .verify(signed, original, home, now))
        .isFalse();
    assertThat(
            new HomeAuthorizationProof("c001", "test", key.getPrivate(), trusted)
                .decode(signed, "c001", now))
        .isEmpty();
    for (var type :
        List.of(
            SignalEnvelope.Type.NEGOTIATE_REQUEST,
            SignalEnvelope.Type.MEDIA_CONNECTED,
            SignalEnvelope.Type.SYNC_CALL,
            SignalEnvelope.Type.HANGUP)) {
      var critical = command(type, sender, call, 1, 1, "{}");
      assertThat(codec.verify(signed, critical, home, now)).isFalse();
      assertThatThrownBy(() -> codec.issue(view, critical))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void otherCallsRoundsConnectionsHomesAndForgedSignaturesCannotReuseTheProof() throws Exception {
    var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    var codec =
        new RelaySessionAuthorizationProof(
            "c001", "test", key.getPrivate(), Map.of("c001/test", key.getPublic()));
    var original = command(SignalEnvelope.Type.OFFER, sender, call, 1, 1, "{}");
    var signed = codec.issue(view, original);
    assertThat(
            codec.verify(
                signed,
                command(SignalEnvelope.Type.OFFER, sender, CallId.create("c002", 1), 1, 1, "{}"),
                home,
                now))
        .isFalse();
    assertThat(
            codec.verify(
                signed, command(SignalEnvelope.Type.OFFER, sender, call, 2, 1, "{}"), home, now))
        .isFalse();
    assertThat(
            codec.verify(
                signed, command(SignalEnvelope.Type.OFFER, sender, call, 1, 2, "{}"), home, now))
        .isFalse();
    var changed =
        new AuthenticatedSession(
            sender.userId(), sender.key(), sender.incarnation(), 3, UUID.randomUUID());
    assertThat(
            codec.verify(
                signed, command(SignalEnvelope.Type.OFFER, changed, call, 1, 1, "{}"), home, now))
        .isFalse();
    assertThat(codec.verify(signed, original, new ProofBindings.TrustedHome("c001", 2, 1), now))
        .isFalse();
    assertThat(codec.verify(signed, original, new ProofBindings.TrustedHome("c001", 1, 2), now))
        .isFalse();
    assertThat(codec.decode(signed, "c002", now)).isEmpty();
    assertThat(
            codec.decode(
                signed.substring(0, signed.lastIndexOf('.') + 1) + "a".repeat(86), "c001", now))
        .isEmpty();
  }
}
