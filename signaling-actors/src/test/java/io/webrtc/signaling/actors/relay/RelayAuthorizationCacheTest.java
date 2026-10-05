package io.webrtc.signaling.actors.relay;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.AuthoritySql;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class RelayAuthorizationCacheTest {
    final AtomicLong now=new AtomicLong();final AtomicBoolean trusted=new AtomicBoolean(true);
    final CallId call=new CallId("c001.e1.00000000-0000-0000-0000-000000000001");
    final AuthenticatedSession sender=new AuthenticatedSession(new UserId("sender"),new SessionKey("TEST_ONLY","s"),new SessionIncarnation(UUID.randomUUID()),1,UUID.randomUUID());
    final AuthenticatedSession recipient=new AuthenticatedSession(new UserId("receiver"),new SessionKey("TEST_ONLY","r"),new SessionIncarnation(UUID.randomUUID()),1,UUID.randomUUID());
    final AtomicReference<AuthoritySql.GroupToken> root=new AtomicReference<>(new AuthoritySql.GroupToken("c001",1,1,685,1,"TEST_ONLY_OWNER",UUID.randomUUID()));
    RelayAuthorizationCache cache(){return new RelayAuthorizationCache(4,1,now::get,trusted::get,c->Optional.of(root.get()));}
    RelayAuthorizationCache.Snapshot snapshot(long tokenUntil,long reservationUntil,long groupUntil){return new RelayAuthorizationCache.Snapshot(call,UUID.randomUUID(),2,1,1,"CONNECTING",sender,recipient,root.get(),now.get(),tokenUntil,reservationUntil,groupUntil,Duration.ofSeconds(30).toNanos());}
    @Test void ageIsAtMostFiveSecondsAndNeverOutlivesAnyNativeAuthorityDeadline(){
        for(long expiry:new long[]{Duration.ofSeconds(2).toNanos(),Duration.ofSeconds(8).toNanos()}){now.set(0);var cache=cache();cache.put(snapshot(expiry,Duration.ofSeconds(20).toNanos(),Duration.ofSeconds(20).toNanos()));assertThat(cache.get(call,sender,1,1)).isPresent();now.set(Math.min(expiry,Duration.ofSeconds(5).toNanos()));assertThat(cache.get(call,sender,1,1)).isEmpty();}
        now.set(0);var cache=cache();cache.put(snapshot(Duration.ofSeconds(20).toNanos(),Duration.ofSeconds(1).toNanos(),Duration.ofSeconds(20).toNanos()));now.set(Duration.ofSeconds(1).toNanos());assertThat(cache.get(call,sender,1,1)).isEmpty();
    }
    @Test void takeoverGenerationRoundAndClockUncertaintyInvalidateRelay(){var cache=cache();cache.put(snapshot(30000000000L,30000000000L,30000000000L));assertThat(cache.get(call,sender,2,1)).isEmpty();var newer=new AuthenticatedSession(sender.userId(),sender.key(),sender.incarnation(),2,UUID.randomUUID());assertThat(cache.get(call,newer,1,1)).isEmpty();trusted.set(false);assertThat(cache.get(call,sender,1,1)).isEmpty();trusted.set(true);root.set(new AuthoritySql.GroupToken("c001",1,1,685,2,"TEST_ONLY_NEW_OWNER",UUID.randomUUID()));assertThat(cache.get(call,sender,1,1)).isEmpty();}
    @Test void trafficRefreshIsSingleFlightAndBoundedWithNoPeriodicIdleRefresh(){var cache=cache();var primary=new CompletableFuture<RelayAuthorizationCache.Snapshot>();var loads=new AtomicInteger();var a=cache.refresh(call,sender,1,1,Duration.ofSeconds(1),()->{loads.incrementAndGet();return new io.webrtc.signaling.actors.admission.ActorOperation<>(primary,primary.thenApply(value->null));});var b=cache.refresh(call,sender,1,1,Duration.ofSeconds(1),()->{loads.incrementAndGet();throw new AssertionError();});assertThat(loads).hasValue(1);primary.complete(snapshot(30000000000L,30000000000L,30000000000L));assertThat(a.toCompletableFuture().join()).isEqualTo(b.toCompletableFuture().join());assertThat(cache.pending()).isZero();assertThat(cache.size()).isEqualTo(1);}
    @Test void unknownRefreshRetainsAdmissionUntilIndependentCleanup(){var cache=cache();var logical=new CompletableFuture<RelayAuthorizationCache.Snapshot>();var physical=new CompletableFuture<Void>();var operation=cache.refresh(call,sender,1,1,Duration.ofSeconds(1),()->new io.webrtc.signaling.actors.admission.ActorOperation<>(logical,physical));logical.completeExceptionally(new io.webrtc.signaling.storage.DbOutcomeUnknownException());assertThat(operation.toCompletableFuture()).isCompletedExceptionally();assertThat(cache.pending()).isEqualTo(1);physical.complete(null);assertThat(cache.pending()).isZero();}
    @Test void exceptionalCleanupDoesNotProveRefreshCreditCanBeReused(){
        var cache=cache();var physical=new CompletableFuture<Void>();
        var operation=cache.refresh(call,sender,1,1,Duration.ofSeconds(1),()->new io.webrtc.signaling.actors.admission.ActorOperation<>(CompletableFuture.failedFuture(new io.webrtc.signaling.storage.DbOutcomeUnknownException()),physical));
        assertThat(operation.toCompletableFuture()).isCompletedExceptionally();physical.completeExceptionally(new IllegalStateException("TEST_ONLY_CLEANUP_UNKNOWN"));
        assertThat(cache.pending()).isEqualTo(1);
        assertThat(cache.refresh(call,sender,2,2,Duration.ofSeconds(1),()->{throw new AssertionError("Unknown physical work cannot admit replacement");}).toCompletableFuture()).isCompletedExceptionally();
    }
    @Test void throwingFactoryDoesNotProveNativeWorkNeverStarted(){
        var cache=cache();var started=new AtomicInteger();
        var operation=cache.refresh(call,sender,1,1,Duration.ofSeconds(1),()->{started.incrementAndGet();throw new IllegalStateException("TEST_ONLY_THROW_AFTER_START");});
        assertThat(operation.toCompletableFuture()).isCompletedExceptionally();assertThat(started).hasValue(1);assertThat(cache.pending()).isEqualTo(1);
        assertThat(cache.refresh(call,sender,2,2,Duration.ofSeconds(1),()->{throw new AssertionError("Physical start was not disproven");}).toCompletableFuture()).isCompletedExceptionally();
    }
}
