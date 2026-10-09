package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.webrtc.signaling.app.SignalingApplication;
import io.webrtc.signaling.app.runtime.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.SessionReply;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.worker.*;
import java.net.URI;
import java.security.*;
import java.security.interfaces.RSAPublicKey;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.FileSystemResource;

/**
 * Main's selected rule with original RS256 verification, signed sources and guarded native
 * PostgreSQL.
 */
class NativeCallingPolicyIT {
  static final Duration BUDGET = Duration.ofSeconds(2);

  static RevocationReconciler.Batch signed(
      KeyPair key, long from, long to, List<RevocationState.Event> events) throws Exception {
    var page = new RevocationReconciler.Batch(from, to, events, Instant.now());
    var signature = Signature.getInstance("Ed25519");
    signature.initSign(key.getPrivate());
    signature.update(RevocationSourceVerifier.signingBytes("c001", "https://issuer.test", page));
    return new RevocationReconciler.Batch(
        from,
        to,
        events,
        page.checkedAt(),
        "TEST_ONLY_SOURCE."
            + Base64.getUrlEncoder().withoutPadding().encodeToString(signature.sign()));
  }

  static SessionReply proof(
      NativeSessionOperations operations,
      SessionRepository.Route route,
      String token,
      UserId target)
      throws Exception {
    var sender =
        new AuthenticatedSession(
            route.user(),
            route.key(),
            route.incarnation(),
            route.connectionGeneration(),
            route.connectionId());
    // The home proof binds the candidate call assigned by the coordinator before reservation.
    var command =
        new CallCommand(
            SignalEnvelope.Type.INVITE,
            sender,
            new RequestId(UUID.randomUUID()),
            CallId.create("c001", 1),
            CommandScope.invite(),
            target,
            null,
            null,
            "{}",
            "a".repeat(64));
    var gateway =
        new NativeSessionHandler.GatewayIdentity(
            route.gatewayId(), route.bootId(), "c001", 1, "TEST_ONLY_REGION");
    var request =
        new NativeSessionHandler.Request(
            "READ_PROOF", gateway, token, route, null, 1, 0, command.requestId().value(), command);
    var original = operations.execute(request, BUDGET);
    try {
      return original.logical().toCompletableFuture().get(2, java.util.concurrent.TimeUnit.SECONDS);
    } finally {
      original
          .physicalCompletion()
          .toCompletableFuture()
          .get(2, java.util.concurrent.TimeUnit.SECONDS);
    }
  }

  @Test
  void selectedOpenRuleStillRequiresOriginalJwtCurrentSessionClockAndDurableRevocation()
      throws Exception {
    var rsa = KeyPairGenerator.getInstance("RSA");
    rsa.initialize(2048);
    var identityKeys = rsa.generateKeyPair();
    var sourceKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    var identity =
        new IdentitySecurityContract(
            "https://issuer.test",
            "control-test",
            Duration.ofMinutes(15),
            Duration.ofSeconds(30),
            Duration.ofSeconds(2),
            Duration.ofSeconds(5),
            true,
            "TEST_ONLY_SOURCE");
    var endpoint =
        new NativeActorSourceEnrollment.Endpoint(
            URI.create("https://localhost:1/v1/source"),
            NativeClockSourceIT.clientTls(true),
            Map.of("TEST_ONLY_SOURCE", sourceKeys.getPublic()));
    var sources =
        new NativeActorSourceEnrollment(
            "c001", 1, UUID.randomUUID(), UUID.randomUUID(), identity, endpoint, endpoint);
    var defaults =
        new YamlPropertySourceLoader()
            .load(
                "TEST_ONLY_defaults", new FileSystemResource("../config/production-defaults.yaml"));
    var selected = new AtomicReference<CallAuthorizationPolicy>();
    new ApplicationContextRunner()
        .withUserConfiguration(SignalingApplication.class)
        .withBean(NativeActorSourceEnrollment.class, () -> sources)
        .withInitializer(
            context -> {
              defaults.forEach(
                  value -> context.getEnvironment().getPropertySources().addLast(value));
              context.getEnvironment().setActiveProfiles("actor");
            })
        .withPropertyValues(
            "signaling.identity.issuer=https://issuer.test",
            "signaling.identity.audience=control-test")
        .run(
            context -> {
              assertThat(context).hasNotFailed().hasSingleBean(CallAuthorizationPolicy.class);
              selected.set(context.getBean(CallAuthorizationPolicy.class));
            });
    try (var f = new LocalInviteAtomicIT.Fixture();
        var clock = new NativeGatewayCommandIT.MainGateway(sourceKeys);
        var tokens =
            new BoundedTokenVerifier(
                new Rs256TokenVerifier(
                    identity,
                    new TrustedRsaKeys(
                        Map.of("test", (RSAPublicKey) identityKeys.getPublic()),
                        null,
                        Duration.ofSeconds(1)),
                    8192),
                1,
                8,
                Duration.ofSeconds(1))) {
      clock.refreshClock();
      var source =
          new RevocationReconciler(
              f.runtime.sql,
              "c001",
              1,
              Duration.ofSeconds(5),
              new RevocationSourceVerifier(
                  "c001", identity.issuer(), Map.of("TEST_ONLY_SOURCE", sourceKeys.getPublic())));
      SessionAuthReadIT.done(source.apply(signed(sourceKeys, 0, 0, List.of())));
      var security = new NativeActorSecurityPolicies(source, clock.clock::valid);
      var registry = new SessionRegistryService(f.runtime.sql, "c001", 1, security);
      var now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
      var token = NativeControlMainIT.token(identityKeys, now);
      var principal = tokens.verify(token, Instant.now()).toCompletableFuture().join();
      var route =
          SessionAuthReadIT.done(
              registry.registerSessionTracked(principal, f.boot, UUID.randomUUID(), 1, BUDGET));
      var proofs =
          new HomeAuthorizationProof(
              "c001", "test", sourceKeys.getPrivate(), Map.of("c001/test", sourceKeys.getPublic()));
      var operations =
          new NativeSessionOperations(
              registry,
              tokens,
              Clock.systemUTC(),
              proofs.sessionProofs(),
              proofs.relaySessionProofs(),
              clock.clock::valid,
              selected.get());
      var target = new UserId("bob");
      var permitted = proof(operations, route, token, target);
      assertThat(permitted.getStatus())
          .as("native proof rejection: %s", permitted.getErrorCode())
          .isEqualTo("READ");
      assertThat(permitted.getAckCommitted()).isFalse();
      var sealed = new ObjectMapper().readValue(permitted.getResult().toByteArray(), String.class);
      var claims = proofs.sessionProofs().decode(sealed, "c001", Instant.now()).orElseThrow();
      assertThat(claims.nativeView().route()).isEqualTo(route);
      assertThat(claims.nativeView().proofUntil())
          .isBeforeOrEqualTo(principal.expiresAt())
          .isBeforeOrEqualTo(claims.nativeView().checkedAt().plusSeconds(5));
      assertThat(proof(operations, route, token, principal.userId()).getErrorCode())
          .isEqualTo("FORBIDDEN");
      int signatureAt = token.lastIndexOf('.') + 1;
      var forged =
          token.substring(0, signatureAt)
              + (token.charAt(signatureAt) == 'x' ? 'y' : 'x')
              + token.substring(signatureAt + 1);
      assertThat(proof(operations, route, forged, target).getErrorCode()).isEqualTo("UNAUTHORIZED");
      assertThat(
              proof(
                      operations,
                      route,
                      NativeControlMainIT.token(identityKeys, now.minusSeconds(601)),
                      target)
                  .getErrorCode())
          .isEqualTo("FORBIDDEN");
      clock.clock.invalidate();
      assertThat(proof(operations, route, token, target).getStatus()).isEqualTo("REJECTED");
      clock.refreshClock();
      try (var c = f.connection();
          var q = c.createStatement()) {
        q.execute(
            "UPDATE security_progress SET source_checked_at=clock_timestamp()-interval '6 seconds'");
      }
      assertThat(proof(operations, route, token, target).getErrorCode()).isEqualTo("UNAUTHORIZED");
      SessionAuthReadIT.done(source.apply(signed(sourceKeys, 0, 0, List.of())));
      var replacement =
          SessionAuthReadIT.done(
              registry.registerSessionTracked(principal, f.boot, UUID.randomUUID(), 1, BUDGET));
      assertThat(proof(operations, route, token, target).getErrorCode()).isEqualTo("UNAUTHORIZED");
      assertThat(proof(operations, replacement, token, target).getStatus()).isEqualTo("READ");
      var revoked =
          new RevocationState.Event(
              principal.key().issuer(),
              principal.userId(),
              principal.key().jti(),
              1,
              1,
              Instant.now().minusMillis(1));
      SessionAuthReadIT.done(source.apply(signed(sourceKeys, 0, 1, List.of(revoked))));
      assertThat(proof(operations, replacement, token, target).getErrorCode())
          .isEqualTo("UNAUTHORIZED");
    }
  }
}
