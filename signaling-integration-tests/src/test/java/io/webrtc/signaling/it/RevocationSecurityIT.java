package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.storage.worker.RevocationReconciler;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Required authenticated-source fixtures exercise native security gates; no production source is invented. */
class RevocationSecurityIT {
    static final String SOURCE="TEST_ONLY_ENROLLED_COMPLETE_SOURCE_RANGE";
    static RevocationReconciler worker(LocalInviteAtomicIT.Fixture f,AtomicBoolean source){return new RevocationReconciler(f.runtime.sql,"c001",1,Duration.ofSeconds(5),batch->source.get()&&batch.sourceProof().equals(SOURCE));}
    static RevocationReconciler.Batch batch(long from,long high,List<RevocationState.Event> events,Instant now){return new RevocationReconciler.Batch(from,high,events,now,SOURCE);}
    @Test void missingUntrustedLaggedAndFutureSourceCannotAuthorizeAnyNativeSessionRead()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var route=SessionAuthReadIT.route(f,f.sender("security-source"));var principal=SessionAuthReadIT.principal(route);var source=new AtomicBoolean(false);var worker=worker(f,source);var registry=new SessionRegistryService(f.runtime.sql,"c001",1,worker);
            assertThatThrownBy(()->CoordinatorGrantIT.done(registry.readCurrentSessionTracked(route,principal,1,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThatThrownBy(()->CoordinatorGrantIT.done(worker.apply(batch(0,0,List.of(),Instant.now())))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            source.set(true);
            assertThatThrownBy(()->CoordinatorGrantIT.done(worker.apply(new RevocationReconciler.Batch(0,0,List.of(),Instant.now(),"TEST_ONLY_UNKNOWN_SOURCE")))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThatThrownBy(()->CoordinatorGrantIT.done(worker.apply(batch(0,0,List.of(),Instant.now().minusSeconds(6))))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThatThrownBy(()->CoordinatorGrantIT.done(worker.apply(batch(0,0,List.of(),Instant.now().plusSeconds(6))))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            CoordinatorGrantIT.done(worker.apply(batch(0,0,List.of(),Instant.now())));
            assertThat(CoordinatorGrantIT.done(registry.readCurrentSessionTracked(route,principal,1,Duration.ofSeconds(2))).route()).isEqualTo(route);
            try(var c=f.connection();var q=c.createStatement()){q.execute("UPDATE security_progress SET source_checked_at=clock_timestamp()-interval '6 seconds'");}
            assertThatThrownBy(()->CoordinatorGrantIT.done(registry.readCurrentSessionTracked(route,principal,1,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        }
    }
    @Test void replayedOldSourcePageCannotRefreshAuthorizationAndCursorGapsCannotAdvance()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var route=SessionAuthReadIT.route(f,f.sender("security-replay"));var principal=SessionAuthReadIT.principal(route);var worker=worker(f,new AtomicBoolean(true));var now=Instant.now();
            CoordinatorGrantIT.done(worker.apply(batch(0,1,List.of(),now)));
            assertThatThrownBy(()->CoordinatorGrantIT.done(worker.apply(batch(2,3,List.of(),Instant.now())))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            try(var c=f.connection();var q=c.createStatement()){q.execute("UPDATE security_progress SET checked_at=clock_timestamp()-interval '6 seconds',source_checked_at=clock_timestamp()-interval '6 seconds'");}
            assertThat(CoordinatorGrantIT.done(worker.apply(batch(0,1,List.of(),Instant.now())))).isEqualTo(1);
            try(var c=f.connection()){assertThat(worker.allowed(c,principal)).isFalse();}
            assertThat(CoordinatorGrantIT.done(worker.progress()).offset()).isEqualTo(1);
        }
    }
    @Test void committedRevocationClosesTheNativeRouteAndBlocksRefreshReadAndReregistration()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var route=SessionAuthReadIT.route(f,f.sender("security-revoked"));var principal=SessionAuthReadIT.principal(route);var worker=worker(f,new AtomicBoolean(true));CoordinatorGrantIT.done(worker.apply(batch(0,0,List.of(),Instant.now())));
            var registry=new SessionRegistryService(f.runtime.sql,"c001",1,worker);var now=Instant.now();var event=new RevocationState.Event(route.key().issuer(),route.user(),route.key().jti(),principal.securityEpoch(),1,now);
            CoordinatorGrantIT.done(worker.apply(batch(0,1,List.of(event),now)));
            assertThatThrownBy(()->CoordinatorGrantIT.done(registry.readCurrentSessionTracked(route,principal,1,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThatThrownBy(()->registry.refreshSession(route,principal,1).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThatThrownBy(()->registry.registerSession(principal,f.boot,UUID.randomUUID(),1).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThat(f.sessions.lookupLiveRoutes(route.user(),1).toCompletableFuture().join()).isEmpty();
        }
    }
    @Test void signingKeyRetirementBlocksExistingNativeProofsEvenWithFreshSourceProgress()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var route=SessionAuthReadIT.route(f,f.sender("security-key-retired"));var principal=SessionAuthReadIT.principal(route);var worker=worker(f,new AtomicBoolean(true));var now=Instant.now();
            var retirement=new RevocationReconciler.KeyRetirement(route.key().issuer(),route.signingKeyId(),1,now);
            CoordinatorGrantIT.done(worker.apply(new RevocationReconciler.Batch(0,1,List.of(),now,SOURCE,List.of(retirement))));
            var registry=new SessionRegistryService(f.runtime.sql,"c001",1,worker);
            assertThatThrownBy(()->CoordinatorGrantIT.done(registry.readCurrentSessionTracked(route,principal,1,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThat(CoordinatorGrantIT.done(worker.progress()).offset()).isEqualTo(1);
        }
    }
    @Test void transientNativeAuthContentionIsOverloadWhileAReplacedRouteRemainsUnauthorized()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var sender=f.sender("security-contention");var route=SessionAuthReadIT.route(f,sender);var principal=SessionAuthReadIT.principal(route);
            var registry=new SessionRegistryService(f.runtime.sql,"c001",1,(c,p)->true);
            try(var verifier=new BoundedTokenVerifier((token,now)->principal,1,8,Duration.ofSeconds(1))){
                var operations=new io.webrtc.signaling.rpc.NativeSessionOperations(registry,verifier,Clock.systemUTC());
                var command=new io.webrtc.signaling.protocol.CallCommand(io.webrtc.signaling.protocol.SignalEnvelope.Type.GET_COMMAND_RESULT,sender,new io.webrtc.signaling.protocol.Identity.RequestId(UUID.randomUUID()),null,io.webrtc.signaling.protocol.Identity.CommandScope.invite(),null,null,null,"{}","a".repeat(64));
                var gateway=new io.webrtc.signaling.rpc.NativeSessionHandler.GatewayIdentity(route.gatewayId(),route.bootId(),"c001",1,"TEST_ONLY");
                var request=new io.webrtc.signaling.rpc.NativeSessionHandler.Request("READ_INVITE_RESULT",gateway,"TEST_ONLY",route,null,1,0,command.requestId().value(),command);
                try(var c=f.connection();var q=c.prepareStatement("SELECT user_id FROM user_guard WHERE user_id=? FOR UPDATE")){
                    c.setAutoCommit(false);q.setString(1,route.user().value());try(var r=q.executeQuery()){assertThat(r.next()).isTrue();}
                    var work=operations.execute(request,Duration.ofSeconds(2));assertThat(work.logical().toCompletableFuture().join().getErrorCode()).isEqualTo("OVERLOADED");work.physicalCompletion().toCompletableFuture().join();c.rollback();
                }
                var recovered=operations.execute(request,Duration.ofSeconds(2));assertThat(recovered.logical().toCompletableFuture().join().getStatus()).isEqualTo("READ");recovered.physicalCompletion().toCompletableFuture().join();
                registry.registerSession(principal,f.boot,UUID.randomUUID(),1).toCompletableFuture().join();
                var stale=operations.execute(request,Duration.ofSeconds(2));assertThat(stale.logical().toCompletableFuture().join().getErrorCode()).isEqualTo("UNAUTHORIZED");stale.physicalCompletion().toCompletableFuture().join();
            }
        }
    }

    @Test void authenticatedPartialCatchUpInvalidatesRevokedRoutesWithoutAdvertisingCurrentSourceFreshness() throws Exception {
        try (var f = new LocalInviteAtomicIT.Fixture()) {
            var revoked = SessionAuthReadIT.route(f, f.sender("security-partial-revoked"));
            var untouched = SessionAuthReadIT.route(f, f.sender("security-partial-untouched"));
            var worker = worker(f, new AtomicBoolean(true)); var now = Instant.now();
            var event = new RevocationState.Event(revoked.key().issuer(), revoked.user(), revoked.key().jti(), 1, 1, now);
            CoordinatorGrantIT.done(worker.apply(batch(0, 0, List.of(), now)));
            try (var c = f.connection()) { assertThat(worker.allowed(c, SessionAuthReadIT.principal(untouched))).isTrue(); }
            var partial = new RevocationReconciler.Batch(0, 1, List.of(event), now, SOURCE, List.of(), 2);
            assertThat(CoordinatorGrantIT.done(worker.apply(partial))).isEqualTo(1);
            assertThat(f.sessions.lookupLiveRoutes(revoked.user(), 1).toCompletableFuture().join()).isEmpty();
            try (var c = f.connection()) { assertThat(worker.allowed(c, SessionAuthReadIT.principal(untouched))).isFalse(); }
            assertThat(CoordinatorGrantIT.done(worker.progress()).checkedAt()).isEqualTo(Instant.EPOCH);
            var completed = new RevocationReconciler.Batch(1, 2, List.of(), Instant.now(), SOURCE, List.of(), 2);
            assertThat(CoordinatorGrantIT.done(worker.apply(completed))).isEqualTo(2);
            try (var c = f.connection()) { assertThat(worker.allowed(c, SessionAuthReadIT.principal(untouched))).isTrue(); }
            assertThatThrownBy(() -> new RevocationReconciler.Batch(2, 3, List.of(), Instant.now(), SOURCE, List.of(), 2)).isInstanceOf(IllegalArgumentException.class);
        }
    }

}
