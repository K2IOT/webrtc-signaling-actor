package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;

import io.webrtc.signaling.auth.RevocationState;
import io.webrtc.signaling.storage.worker.RevocationReconciler;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** TEST_ONLY enrollment; policy reads and revocation commits use actual PostgreSQL. */
class NativeActorSecurityPoliciesIT {
  private static RevocationReconciler source(LocalInviteAtomicIT.Fixture f) {
    return new RevocationReconciler(
        f.runtime.sql,
        "c001",
        1,
        Duration.ofSeconds(5),
        b -> b.sourceProof().equals("TEST_ONLY_SOURCE"));
  }

  private static void fresh(RevocationReconciler source) {
    SessionAuthReadIT.done(
        source.apply(
            new RevocationReconciler.Batch(0, 0, List.of(), Instant.now(), "TEST_ONLY_SOURCE")));
  }

  private static List<Boolean> read(
      LocalInviteAtomicIT.Fixture f,
      NativeActorSecurityPolicies policies,
      SessionRepository.Route route) {
    return SessionAuthReadIT.done(
        f.runtime.sql.submitTracked(
            DbClass.CRITICAL,
            Duration.ofSeconds(2),
            c ->
                List.of(
                    policies.allowed(c, SessionAuthReadIT.principal(route)),
                    policies.allowed(c, route),
                    policies.current(c, route.user()))));
  }

  @Test
  void bindsNativeSessionRouteAndHomeFreshnessToOriginalDurableSource() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var route = SessionAuthReadIT.route(f, f.sender("native-policy-fresh"));
      var source = source(f);
      var policies = new NativeActorSecurityPolicies(source, () -> true);
      assertThat(read(f, policies, route)).containsExactly(false, false, false);
      fresh(source);
      assertThat(read(f, policies, route)).containsExactly(true, true, true);
    }
  }

  @Test
  void cachedHealthySourceCannotBypassClockInvalidation() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var route = SessionAuthReadIT.route(f, f.sender("native-policy-clock"));
      var source = source(f);
      fresh(source);
      var clock = new AtomicBoolean(true);
      var policies = new NativeActorSecurityPolicies(source, clock::get);
      assertThat(read(f, policies, route)).containsExactly(true, true, true);
      clock.set(false);
      assertThat(read(f, policies, route)).containsExactly(false, false, false);
    }
  }

  @Test
  void futureExpiredAndIncompleteSourceCannotAuthorizeAnyPolicy() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var route = SessionAuthReadIT.route(f, f.sender("native-policy-progress"));
      var source = source(f);
      fresh(source);
      var policies = new NativeActorSecurityPolicies(source, () -> true);
      for (var stamp :
          List.of(
              "clock_timestamp()+interval '1 second'", "clock_timestamp()-interval '6 seconds'")) {
        try (var c = f.connection();
            var q = c.createStatement()) {
          q.execute(
              "UPDATE security_progress SET checked_at=" + stamp + ",source_checked_at=" + stamp);
        }
        assertThat(read(f, policies, route)).containsExactly(false, false, false);
      }
      fresh(source);
      SessionAuthReadIT.done(
          source.apply(
              new RevocationReconciler.Batch(
                  0, 1, List.of(), Instant.now(), "TEST_ONLY_SOURCE", List.of(), 2)));
      assertThat(read(f, policies, route)).containsExactly(false, false, false);
    }
  }

  @Test
  void scopedJtiRevocationDoesNotTurnHomeFreshnessIntoIdentityAuthorization() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var route = SessionAuthReadIT.route(f, f.sender("native-policy-revoked"));
      var source = source(f);
      fresh(source);
      var now = Instant.now();
      var event =
          new RevocationState.Event(
              route.key().issuer(),
              route.user(),
              route.key().jti(),
              route.securityEpoch(),
              1,
              now.minusMillis(1));
      SessionAuthReadIT.done(
          source.apply(
              new RevocationReconciler.Batch(0, 1, List.of(event), now, "TEST_ONLY_SOURCE")));
      assertThat(read(f, new NativeActorSecurityPolicies(source, () -> true), route))
          .containsExactly(false, false, true);
    }
  }

  @Test
  void signingKeyRetirementDeniesStillCurrentNativeRoute() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var route = SessionAuthReadIT.route(f, f.sender("native-policy-retired"));
      var source = source(f);
      fresh(source);
      var now = Instant.now();
      var key =
          new RevocationReconciler.KeyRetirement(
              route.key().issuer(), route.signingKeyId(), 1, now.minusMillis(1));
      SessionAuthReadIT.done(
          source.apply(
              new RevocationReconciler.Batch(
                  0, 1, List.of(), now, "TEST_ONLY_SOURCE", List.of(key))));
      try (var c = f.connection()) {
        assertThat(new SessionRepository().requireCurrent(c, route)).isEqualTo(route);
      }
      assertThat(read(f, new NativeActorSecurityPolicies(source, () -> true), route))
          .containsExactly(false, false, true);
    }
  }

  @Test
  void wrongNativeCellCannotReuseAnotherCellsFreshProgress() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var route = SessionAuthReadIT.route(f, f.sender("native-policy-cell"));
      var source = source(f);
      fresh(source);
      var wrong =
          new RevocationReconciler(f.runtime.sql, "c002", 1, Duration.ofSeconds(5), b -> false);
      assertThatThrownBy(() -> read(f, new NativeActorSecurityPolicies(wrong, () -> true), route))
          .hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
    }
  }

  @Test
  void freshSourceCannotAuthorizeExpiredPrincipalOrNativeRoute() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var route = SessionAuthReadIT.route(f, f.sender("native-policy-expired"));
      var source = source(f);
      fresh(source);
      var policies = new NativeActorSecurityPolicies(source, () -> true);
      var principal = SessionAuthReadIT.principal(route);
      var expired =
          new io.webrtc.signaling.auth.AuthPrincipal(
              principal.userId(),
              principal.key(),
              Instant.now().minusSeconds(1),
              principal.issuedAt(),
              principal.signingKeyId(),
              principal.securityEpoch());
      boolean expiredAllowed =
          SessionAuthReadIT.done(
              f.runtime.sql.submitTracked(
                  DbClass.CRITICAL, Duration.ofSeconds(2), c -> policies.allowed(c, expired)));
      assertThat(expiredAllowed).isFalse();
      try (var c = f.connection();
          var q = c.createStatement()) {
        q.execute("UPDATE session_registry SET token_exp=clock_timestamp()-interval '1 second'");
      }
      var old =
          SessionAuthReadIT.route(
              f,
              new io.webrtc.signaling.protocol.Identity.AuthenticatedSession(
                  route.user(),
                  route.key(),
                  route.incarnation(),
                  route.connectionGeneration(),
                  route.connectionId()));
      assertThat(read(f, policies, old)).containsExactly(false, false, true);
    }
  }

  @Test
  void legacyRouteWithoutSigningKeyCannotBypassRetirementPolicy() throws Exception {
    try (var f = new LocalInviteAtomicIT.Fixture()) {
      var sender = f.sender("native-policy-no-kid");
      var source = source(f);
      fresh(source);
      try (var c = f.connection();
          var q = c.createStatement()) {
        q.execute("UPDATE session_registry SET signing_key_id=NULL");
      }
      var route = SessionAuthReadIT.route(f, sender);
      var policies = new NativeActorSecurityPolicies(source, () -> true);
      try (var c = f.connection()) {
        assertThat(new SessionRepository().requireCurrent(c, route)).isEqualTo(route);
      }
      boolean routeAllowed =
          SessionAuthReadIT.done(
              f.runtime.sql.submitTracked(
                  DbClass.CRITICAL, Duration.ofSeconds(2), c -> policies.allowed(c, route)));
      assertThat(routeAllowed).isFalse();
    }
  }
}
