package io.webrtc.signaling.auth;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
class AuthorizationRevocationTest {
    final Instant now=Rs256TokenVerifierTest.NOW;
    final AuthPrincipal principal=new AuthPrincipal(new UserId("alice"),new SessionKey("https://issuer.test","j-a"),now.plusSeconds(900),now,"key-a",0);
    @Test void requiresActualContractValuesAndRejectsUnsafeFreshness() {
        assertThatThrownBy(()->new IdentitySecurityContract("","",Duration.ZERO,Duration.ofSeconds(30),Duration.ofSeconds(2),Duration.ofSeconds(5),true,"")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->new IdentitySecurityContract("issuer","aud",Duration.ofSeconds(5),Duration.ZERO,Duration.ofSeconds(5),Duration.ofSeconds(6),true,"log")).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void policyDenyAndUnknownFailClosed() {
        var request=new CallAuthorizationRequest(principal,new UserId("bob"),now);
        assertThat(CallAuthorizationPolicy.denyAll().authorize(request).toCompletableFuture().join().allowed()).isFalse();
        var unknown=CallAuthorizationPolicy.guarded(r->CompletableFuture.failedFuture(new IllegalStateException("dependency unavailable")));
        assertThat(unknown.authorize(request).toCompletableFuture().join().allowed()).isFalse();
        assertThat(CallAuthorizationPolicy.openAuthenticated("test-open-v1",Duration.ofSeconds(2)).authorize(request).toCompletableFuture().join().allowed()).isTrue();
    }
    @Test void revocationIsDurableMonotonicIdempotentAndClosesBoundSession() {
        var durable=new TestSecurityStore();var closures=new ArrayList<SessionKey>();
        var state=new RevocationState(durable,Duration.ofSeconds(5));state.markReconciled(10,now);
        var consumer=new RevocationConsumer(state,e -> closures.add(new SessionKey(e.issuer(),e.jti())));
        var event=new RevocationState.Event(principal.key().issuer(),principal.userId(),principal.key().jti(),2,11,now);
        consumer.apply(event);consumer.apply(event);consumer.apply(new RevocationState.Event(event.issuer(),event.userId(),event.jti(),1,10,now));
        assertThat(closures).containsExactly(principal.key());
        assertThat(new RevocationState(durable,Duration.ofSeconds(5)).checkRevocation(principal,now)).isEqualTo(AuthorizationStatus.REVOKED);
    }
    @Test void staleHighWaterDoesNotAuthorizeAuthenticationOrNewCommands() {
        var state=new RevocationState(new TestSecurityStore(),Duration.ofSeconds(5));
        assertThat(state.checkRevocation(principal,now)).isEqualTo(AuthorizationStatus.FRESHNESS_UNKNOWN);
        state.markReconciled(10,now);assertThat(state.checkRevocation(principal,now)).isEqualTo(AuthorizationStatus.ALLOWED);
        assertThat(state.checkRevocation(principal,now.plusSeconds(6))).isEqualTo(AuthorizationStatus.FRESHNESS_UNKNOWN);
    }
}
