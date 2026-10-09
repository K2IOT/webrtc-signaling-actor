package io.webrtc.signaling.rpc;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import java.security.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class PortableProofExpiryTest {
  final Instant now = Instant.parse("2026-10-04T00:00:00Z");

  HomeAuthorizationProof signer() throws Exception {
    var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    return new HomeAuthorizationProof(
        "c001", "TEST_ONLY", keys.getPrivate(), Map.of("c001/TEST_ONLY", keys.getPublic()));
  }

  HomeAuthorizationProof.Claims claims(Instant issued, Instant until) {
    return new HomeAuthorizationProof.Claims(
        1,
        "COORDINATOR_GRANT",
        "c001",
        "c002",
        UUID.randomUUID(),
        CallId.create("c001", 1),
        new UserId("TEST_ONLY"),
        null,
        null,
        0,
        1,
        1,
        1,
        1,
        1,
        1,
        UUID.randomUUID(),
        null,
        0,
        null,
        1,
        0,
        "a".repeat(64),
        issued,
        until,
        1,
        null);
  }

  @Test
  void homeProofReservesPairUncertaintyAndFiveSecondDriftAtStrictExpiryBoundary() throws Exception {
    var signer = signer();
    assertThat(signer.decode(signer.issue(claims(now, now.plusMillis(255))), "c001", now))
        .isEmpty();
    assertThat(signer.decode(signer.issue(claims(now, now.plusMillis(256))), "c001", now))
        .isPresent();
  }

  @Test
  void homeProofRejectsFutureIssueBeyondPairBoundEvenWithAValidSignature() throws Exception {
    var signer = signer();
    assertThat(
            signer.decode(
                signer.issue(claims(now.plusMillis(251), now.plusSeconds(5))), "c001", now))
        .isEmpty();
    assertThat(
            signer.decode(
                signer.issue(claims(now.plusMillis(250), now.plusSeconds(5))), "c001", now))
        .isPresent();
  }

  @Test
  void sessionProofUsesSameConservativeTimeDomainWithoutJwtSkew() throws Exception {
    var codec = signer().sessionProofs();
    var route =
        new SessionRepository.Route(
            new UserId("TEST_ONLY_user"),
            new SessionKey("TEST_ONLY", "j"),
            new SessionIncarnation(UUID.randomUUID()),
            1,
            "TEST_ONLY_gateway",
            UUID.randomUUID(),
            UUID.randomUUID(),
            now.plusSeconds(60),
            "TEST_ONLY",
            1);
    var call = CallId.create("c002", 1);
    var command =
        new CallCommand(
            SignalEnvelope.Type.RESUME,
            new AuthenticatedSession(
                route.user(), route.key(), route.incarnation(), 1, route.connectionId()),
            new RequestId(UUID.randomUUID()),
            call,
            CommandScope.call(call),
            null,
            null,
            null,
            "{}",
            "b".repeat(64));
    var shortProof =
        codec.issue(
            new SessionRegistryService.SessionProofView(
                route, now, now.plusMillis(255), "c001", 1, 1),
            command);
    assertThat(codec.decode(shortProof, "c001", now)).isEmpty();
    var good =
        codec.issue(
            new SessionRegistryService.SessionProofView(
                route, now, now.plusMillis(256), "c001", 1, 1),
            command);
    assertThat(codec.decode(good, "c001", now)).isPresent();
    var future =
        codec.issue(
            new SessionRegistryService.SessionProofView(
                route, now.plusMillis(251), now.plusSeconds(5), "c001", 1, 1),
            command);
    assertThat(codec.decode(future, "c001", now)).isEmpty();
  }
}
