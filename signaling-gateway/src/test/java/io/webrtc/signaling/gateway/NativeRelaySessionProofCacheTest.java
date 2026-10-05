package io.webrtc.signaling.gateway;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
/** TEST_ONLY views/keys test cache arithmetic and original physical ownership. */
class NativeRelaySessionProofCacheTest {
    final Instant now=Instant.parse("2026-10-05T00:00:00Z");
    final AtomicReference<Instant> wall=new AtomicReference<>(now);
    final Clock clock=new Clock(){public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId zone){return this;}public Instant instant(){return wall.get();}};
    final AtomicLong ticks=new AtomicLong();final AtomicBoolean healthy=new AtomicBoolean(true);
    final CallId call=CallId.create("c002",1);
    final SessionRepository.Route route=new SessionRepository.Route(new UserId("TEST_ONLY_USER"),new SessionKey("TEST_ONLY_ISSUER","TEST_ONLY_JTI"),new SessionIncarnation(UUID.randomUUID()),1,"TEST_ONLY_GW",UUID.randomUUID(),UUID.randomUUID(),now.plusSeconds(60),"TEST_ONLY_KEY",1);
    final AuthenticatedSession sender=new AuthenticatedSession(route.user(),route.key(),route.incarnation(),1,route.connectionId());
    final ProofBindings.TrustedHome home=new ProofBindings.TrustedHome("c001",1,1);
    CallCommand command(long round){return new CallCommand(SignalEnvelope.Type.OFFER,sender,new RequestId(UUID.randomUUID()),call,CommandScope.call(call),null,new NegotiationId(round),new IceGeneration(round),"{}","a".repeat(64));}
    RelaySessionAuthorizationProof codec()throws Exception{var k=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();return new RelaySessionAuthorizationProof("c001","test",k.getPrivate(),Map.of("c001/test",k.getPublic()));}
    String issue(RelaySessionAuthorizationProof codec,CallCommand c){return codec.issue(new SessionRegistryService.SessionProofView(route,now,now.plusSeconds(5),"c001",1,1),c);}
    @Test void sameRoundFramesUseOneNativeProofUntilItsOriginalMonotonicExpiry()throws Exception{
        var codec=codec();var cache=new NativeRelaySessionProofCache(2,2,clock,ticks::get,healthy::get,codec);
        var count=new AtomicInteger();var command=command(1);String proof=issue(codec,command);
        java.util.function.Supplier<RpcOperation<String>> load=()->{count.incrementAndGet();return new RpcOperation<>(CompletableFuture.completedFuture(proof),CompletableFuture.completedFuture(null));};
        assertThat(cache.get(command,home,Duration.ofSeconds(1),load).logical().toCompletableFuture().join()).isEqualTo(proof);
        assertThat(cache.get(command(1),home,Duration.ofSeconds(1),load).logical().toCompletableFuture().join()).isEqualTo(proof);assertThat(count).hasValue(1);
        ticks.set(4_745_000_000L);wall.set(now.plusMillis(4745));assertThat(cache.get(command(1),home,Duration.ofSeconds(1),load).logical().toCompletableFuture()).isCompletedExceptionally();assertThat(count).hasValue(2);
        wall.set(now);
        cache.get(command(2),home,Duration.ofSeconds(1),()->new RpcOperation<>(CompletableFuture.completedFuture(issue(codec,command(2))),CompletableFuture.completedFuture(null))).logical().toCompletableFuture().join();
        assertThat(cache.size()).isLessThanOrEqualTo(2);
    }
    @Test void originalPhysicalCleanupHoldsSingleFlightAndCapacityThroughUnknownOutcome()throws Exception{
        var codec=codec();var cache=new NativeRelaySessionProofCache(1,1,clock,ticks::get,healthy::get,codec);
        var original=new CompletableFuture<String>();var cleanup=new CompletableFuture<Void>();var count=new AtomicInteger();
        java.util.function.Supplier<RpcOperation<String>> load=()->{count.incrementAndGet();return new RpcOperation<>(original,cleanup);};
        var a=cache.get(command(1),home,Duration.ofSeconds(1),load);var b=cache.get(command(1),home,Duration.ofSeconds(1),load);
        original.completeExceptionally(new TimeoutException("TEST_ONLY_UNKNOWN"));assertThat(a.logical().toCompletableFuture()).isCompletedExceptionally();assertThat(b.logical().toCompletableFuture()).isCompletedExceptionally();
        assertThat(a.physicalCompletion().toCompletableFuture()).isNotDone();assertThat(cache.pending()).isEqualTo(1);assertThat(count).hasValue(1);
        assertThat(cache.get(command(2),home,Duration.ofSeconds(1),load).logical().toCompletableFuture()).isCompletedExceptionally();assertThat(count).hasValue(1);
        var drain=cache.drain();var allReceiptsClosed=new AtomicBoolean();drain.whenComplete((v,e)->allReceiptsClosed.set(a.physicalCompletion().toCompletableFuture().isDone()));assertThat(drain.toCompletableFuture()).isNotDone();cleanup.complete(null);assertThat(allReceiptsClosed).isTrue();assertThat(drain.toCompletableFuture()).isDone();assertThat(cache.pending()).isZero();
    }
    @Test void clockOrSecurityLossInvalidatesReadyProofAndCannotRestartOriginalTtl()throws Exception{
        var codec=codec();var cache=new NativeRelaySessionProofCache(2,1,clock,ticks::get,healthy::get,codec);var c=command(1);var count=new AtomicInteger();
        java.util.function.Supplier<RpcOperation<String>> load=()->{count.incrementAndGet();return new RpcOperation<>(CompletableFuture.completedFuture(issue(codec,c)),CompletableFuture.completedFuture(null));};
        cache.get(c,home,Duration.ofSeconds(1),load).logical().toCompletableFuture().join();healthy.set(false);
        assertThat(cache.get(c,home,Duration.ofSeconds(1),load).logical().toCompletableFuture()).isCompletedExceptionally();healthy.set(true);
        cache.get(c,home,Duration.ofSeconds(1),load).logical().toCompletableFuture().join();assertThat(count).hasValue(2);
        var wrongHome=new ProofBindings.TrustedHome("c001",2,1);
        assertThat(cache.get(c,wrongHome,Duration.ofSeconds(1),load).logical().toCompletableFuture()).isCompletedExceptionally();
    }
    @Test void coalescedCallerRetainsItsOwnShorterLogicalDeadline()throws Exception{
        var codec=codec();var cache=new NativeRelaySessionProofCache(2,1,clock,ticks::get,healthy::get,codec);
        var original=new CompletableFuture<String>();var cleanup=new CompletableFuture<Void>();
        var first=cache.get(command(1),home,Duration.ofSeconds(1),()->new RpcOperation<>(original,cleanup));
        var shortCall=cache.get(command(1),home,Duration.ofMillis(40),()->{throw new AssertionError("Single flight required");});
        try{shortCall.logical().toCompletableFuture().get(250,TimeUnit.MILLISECONDS);fail("Expected original shorter deadline");}
        catch(ExecutionException timedOut){assertThat(timedOut).hasCauseInstanceOf(TimeoutException.class);}
        assertThat(first.logical().toCompletableFuture()).isNotDone();assertThat(shortCall.physicalCompletion().toCompletableFuture()).isNotDone();
        original.completeExceptionally(new TimeoutException("TEST_ONLY_ORIGINAL"));cleanup.complete(null);
    }
    @Test void producerFactoryOrCleanupFailureRetainsUnknownOwnership()throws Exception{
        var codec=codec();var cache=new NativeRelaySessionProofCache(1,1,clock,ticks::get,healthy::get,codec);
        var work=cache.get(command(1),home,Duration.ofSeconds(1),()->{throw new IllegalStateException("TEST_ONLY_UNKNOWN_START");});
        assertThat(work.logical().toCompletableFuture()).isCompletedExceptionally();assertThat(work.physicalCompletion().toCompletableFuture()).isNotDone();assertThat(cache.drain().toCompletableFuture()).isNotDone();
        var other=new NativeRelaySessionProofCache(1,1,clock,ticks::get,healthy::get,codec);var cleanup=new CompletableFuture<Void>();
        var c=command(1);var admitted=other.get(c,home,Duration.ofSeconds(1),()->new RpcOperation<>(CompletableFuture.completedFuture(issue(codec,c)),cleanup));
        cleanup.completeExceptionally(new IllegalStateException("TEST_ONLY_UNKNOWN_CLOSE"));assertThat(admitted.physicalCompletion().toCompletableFuture()).isNotDone();assertThat(other.drain().toCompletableFuture()).isNotDone();
    }
}
