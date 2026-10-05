package io.webrtc.signaling.actors.relay;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.actors.admission.ActorOperation;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.AuthoritySql;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class NegotiationRelayTest {
    final AtomicLong now=new AtomicLong();
    final UUID activation=UUID.randomUUID();
    final CallId call=CallId.create("c001",1);
    final AuthenticatedSession caller=session("caller"),callee=session("callee");
    final AuthoritySql.GroupToken group=new AuthoritySql.GroupToken("c001",1,1,685,1,"TEST_ONLY_OWNER",UUID.randomUUID());
    final AtomicInteger loads=new AtomicInteger(),writes=new AtomicInteger();
    final RelayBufferBudget memory=new RelayBufferBudget(262144);
    static AuthenticatedSession session(String user){return new AuthenticatedSession(new UserId(user),new SessionKey("TEST_ONLY",user),new SessionIncarnation(UUID.randomUUID()),1,UUID.randomUUID());}
    RelayAuthorizationCache.Snapshot snapshot(AuthenticatedSession sender,long round,long ice){return new RelayAuthorizationCache.Snapshot(call,activation,3,round,ice,"CONNECTING",sender,sender.equals(caller)?callee:caller,group,now.get(),30000000000L,30000000000L,30000000000L,30000000000L);}
    RelayAuthorizationCache cacheForTest(){return new RelayAuthorizationCache(4,2,now::get,()->true,c->Optional.of(group));}
    NegotiationRelay relay(){var cache=cacheForTest();return new NegotiationRelay(4,now::get,memory,cache,(c,s,r,i,b)->{loads.incrementAndGet();return new ActorOperation<>(CompletableFuture.completedFuture(snapshot(s,r,i)),CompletableFuture.completedFuture(null));},message->{writes.incrementAndGet();return new ActorOperation<>(CompletableFuture.completedFuture(null),CompletableFuture.completedFuture(null));});}
    NegotiationRelay.Grant grant(long round,long ice){return new NegotiationRelay.Grant(call,activation,3,round,ice,caller,callee,now.get()+20000000000L,group);}
    NegotiationRelay.Description offer(UUID request,String body,long round,long ice){return new NegotiationRelay.Description(call,round,ice,caller,request,NegotiationRelay.Kind.OFFER,body);}
    @Test void serverIssuedRoundRejectsConcurrentGrantAndOldGeneration(){var relay=relay();assertThat(relay.install(grant(1,1))).isTrue();assertThat(relay.install(grant(2,2))).isFalse();relay.recoverVolatileLoss(call);assertThat(relay.install(grant(2,2))).isTrue();assertThatThrownBy(()->relay.send(offer(UUID.randomUUID(),"v=0",1,1),Duration.ofSeconds(1)).toCompletableFuture().join()).hasCauseInstanceOf(IllegalStateException.class);assertThat(writes).hasValue(0);}
    @Test void retryReusesRequestAndPayloadWithoutAllocatingANewRound(){var relay=relay();relay.install(grant(1,1));var request=UUID.randomUUID();var offer=offer(request,"v=0",1,1);relay.send(offer,Duration.ofSeconds(1)).toCompletableFuture().join();relay.send(offer,Duration.ofSeconds(1)).toCompletableFuture().join();assertThat(writes).hasValue(1);assertThatThrownBy(()->relay.send(offer(request,"changed",1,1),Duration.ofSeconds(1)).toCompletableFuture().join()).hasCauseInstanceOf(IllegalArgumentException.class);assertThat(loads).hasValue(1);relay.close();assertThat(memory.retainedBytes()).isZero();}
    @Test void onlyGrantedOffererSendsOfferAndAnswerFollowsOffer(){var relay=relay();relay.install(grant(1,1));var answer=new NegotiationRelay.Description(call,1,1,callee,UUID.randomUUID(),NegotiationRelay.Kind.ANSWER,"v=0");assertThatThrownBy(()->relay.send(answer,Duration.ofSeconds(1)).toCompletableFuture().join()).hasCauseInstanceOf(IllegalStateException.class);var wrong=new NegotiationRelay.Description(call,1,1,callee,UUID.randomUUID(),NegotiationRelay.Kind.OFFER,"v=0");assertThatThrownBy(()->relay.send(wrong,Duration.ofSeconds(1)).toCompletableFuture().join()).hasCauseInstanceOf(IllegalStateException.class);relay.send(offer(UUID.randomUUID(),"v=0",1,1),Duration.ofSeconds(1)).toCompletableFuture().join();relay.send(answer,Duration.ofSeconds(1)).toCompletableFuture().join();assertThat(writes).hasValue(2);relay.close();}
    @Test void expiryCannotBeExtendedByTrafficAndPayloadNeverAppearsInToString(){var relay=relay();relay.install(grant(1,1));var offer=offer(UUID.randomUUID(),"SECRET_SDP",1,1);assertThat(offer.toString()).doesNotContain("SECRET_SDP");relay.send(offer,Duration.ofSeconds(1)).toCompletableFuture().join();now.set(20000000000L);assertThatThrownBy(()->relay.send(offer,Duration.ofSeconds(1)).toCompletableFuture().join()).hasCauseInstanceOf(IllegalStateException.class);assertThat(memory.retainedBytes()).isZero();}
    @Test void oversizeBodyRejectedBeforeAuthorizationOrTransport(){assertThatThrownBy(()->offer(UUID.randomUUID(),"x".repeat(65537),1,1)).isInstanceOf(IllegalArgumentException.class);assertThat(writes).hasValue(0);assertThat(loads).hasValue(0);}
    @Test void exceptionalTransportCleanupCannotRetireRetainedSdpAfterClose(){
        var physical=new CompletableFuture<Void>();var cache=cacheForTest();
        var relay=new NegotiationRelay(4,now::get,memory,cache,(c,s,r,i,b)->new ActorOperation<>(CompletableFuture.completedFuture(snapshot(s,r,i)),CompletableFuture.completedFuture(null)),message->new ActorOperation<>(CompletableFuture.completedFuture(null),physical));
        relay.install(grant(1,1));relay.send(offer(UUID.randomUUID(),"v=0",1,1),Duration.ofSeconds(1)).toCompletableFuture().join();relay.close();
        long retained=memory.retainedBytes();assertThat(retained).isPositive();physical.completeExceptionally(new IllegalStateException("TEST_ONLY_CLEANUP_UNKNOWN"));
        assertThat(memory.retainedBytes()).isEqualTo(retained);
    }
    @Test void throwingTransportCannotAssertPhysicalRetirementOrDuplicateItsAttempt(){
        var cache=cacheForTest();var started=new AtomicInteger();
        var relay=new NegotiationRelay(4,now::get,memory,cache,(c,s,r,i,b)->new ActorOperation<>(CompletableFuture.completedFuture(snapshot(s,r,i)),CompletableFuture.completedFuture(null)),message->{started.incrementAndGet();throw new IllegalStateException("TEST_ONLY_THROW_AFTER_START");});
        relay.install(grant(1,1));var offer=offer(UUID.randomUUID(),"v=0",1,1);
        assertThat(relay.send(offer,Duration.ofSeconds(1)).toCompletableFuture()).isCompletedExceptionally();
        assertThat(relay.send(offer,Duration.ofSeconds(1)).toCompletableFuture()).isCompletedExceptionally();assertThat(started).hasValue(1);
        long retained=memory.retainedBytes();relay.close();assertThat(retained).isPositive();assertThat(memory.retainedBytes()).isEqualTo(retained);
    }
    @Test void trackedSdpWaitsForBothOriginalAuthorizationAndTransportCleanup(){
        var authorizationCleanup=new CompletableFuture<Void>();var writeCleanup=new CompletableFuture<Void>();var cache=cacheForTest();
        var relay=new NegotiationRelay(4,now::get,memory,cache,(c,s,r,i,b)->new ActorOperation<>(CompletableFuture.completedFuture(snapshot(s,r,i)),authorizationCleanup),m->new ActorOperation<>(CompletableFuture.completedFuture(null),writeCleanup));
        relay.install(grant(1,1));var message=offer(UUID.randomUUID(),"v=0",1,1);var work=relay.sendTracked(message,Duration.ofSeconds(1));
        assertThat(work.logical().toCompletableFuture()).isDone();assertThat(work.physicalCompletion().toCompletableFuture()).isNotDone();writeCleanup.complete(null);assertThat(work.physicalCompletion().toCompletableFuture()).isNotDone();
        authorizationCleanup.complete(null);assertThat(work.physicalCompletion().toCompletableFuture()).isDone();relay.close();assertThat(memory.retainedBytes()).isZero();
    }
    @Test void trackedUnknownSdpFactoryNeverInventsAWriteCleanupReceipt(){
        var relay=new NegotiationRelay(4,now::get,memory,cacheForTest(),(c,s,r,i,b)->new ActorOperation<>(CompletableFuture.completedFuture(snapshot(s,r,i)),CompletableFuture.completedFuture(null)),m->{throw new IllegalStateException("TEST_ONLY_UNKNOWN_START");});
        relay.install(grant(1,1));var work=relay.sendTracked(offer(UUID.randomUUID(),"v=0",1,1),Duration.ofSeconds(1));
        assertThat(work.logical().toCompletableFuture()).isCompletedExceptionally();assertThat(work.physicalCompletion().toCompletableFuture()).isNotDone();relay.close();assertThat(memory.retainedBytes()).isPositive();
    }
    @Test void duplicateSdpDuringPendingAuthorizationUsesOneOriginalWrite(){
        var authorization=new CompletableFuture<RelayAuthorizationCache.Snapshot>();var cleanup=new CompletableFuture<Void>();var sends=new AtomicInteger();
        var relay=new NegotiationRelay(4,now::get,memory,cacheForTest(),(c,s,r,i,b)->new ActorOperation<>(authorization,cleanup),m->{sends.incrementAndGet();return new ActorOperation<>(CompletableFuture.completedFuture(null),CompletableFuture.completedFuture(null));});
        relay.install(grant(1,1));var message=offer(UUID.randomUUID(),"v=0",1,1);var first=relay.sendTracked(message,Duration.ofSeconds(1));var duplicate=relay.sendTracked(message,Duration.ofSeconds(1));
        authorization.complete(snapshot(caller,1,1));assertThat(sends).hasValue(1);assertThat(first.physicalCompletion().toCompletableFuture()).isNotDone();assertThat(duplicate.physicalCompletion().toCompletableFuture()).isNotDone();cleanup.complete(null);relay.close();assertThat(memory.retainedBytes()).isZero();
    }
    @Test void authorizationCleanupAlsoRetainsTheDescriptionCreditAfterLogicalWrite(){
        var cleanup=new CompletableFuture<Void>();var relay=new NegotiationRelay(4,now::get,memory,cacheForTest(),(c,s,r,i,b)->new ActorOperation<>(CompletableFuture.completedFuture(snapshot(s,r,i)),cleanup),m->new ActorOperation<>(CompletableFuture.completedFuture(null),CompletableFuture.completedFuture(null)));
        relay.install(grant(1,1));var work=relay.sendTracked(offer(UUID.randomUUID(),"v=0",1,1),Duration.ofSeconds(1));relay.close();
        assertThat(memory.retainedBytes()).isPositive();assertThat(work.physicalCompletion().toCompletableFuture()).isNotDone();cleanup.complete(null);assertThat(memory.retainedBytes()).isZero();
    }
    @Test void transportReceivesOnlyTheRemainingOriginalBudget(){var pending=new CompletableFuture<RelayAuthorizationCache.Snapshot>();var budget=new AtomicReference<Duration>();var relay=new NegotiationRelay(4,now::get,memory,cacheForTest(),(c,s,r,i,b)->new ActorOperation<>(pending,CompletableFuture.completedFuture(null)),new NegotiationRelay.Transport(){public ActorOperation<Void> send(NegotiationRelay.Description d){throw new AssertionError("Must use remaining budget");}public ActorOperation<Void> send(NegotiationRelay.Description d,Duration remaining){budget.set(remaining);return new ActorOperation<>(CompletableFuture.completedFuture(null),CompletableFuture.completedFuture(null));}});relay.install(grant(1,1));var work=relay.sendTracked(offer(UUID.randomUUID(),"v=0",1,1),Duration.ofMillis(200));now.addAndGet(Duration.ofMillis(100).toNanos());pending.complete(snapshot(caller,1,1));assertThat(work.logical().toCompletableFuture()).isDone();assertThat(budget.get()).isPositive().isLessThanOrEqualTo(Duration.ofMillis(100));relay.close();}
    @Test void nativeSdpChargesTheOriginalCommandPayloadBeforeTransport(){var bounded=new RelayBufferBudget(70000);var count=new AtomicInteger();var body="x".repeat(65536);var request=UUID.randomUUID();var command=new io.webrtc.signaling.protocol.CallCommand(io.webrtc.signaling.protocol.SignalEnvelope.Type.OFFER,caller,new RequestId(request),call,CommandScope.call(call),null,new NegotiationId(1),new IceGeneration(1),"{\"sdp\":\""+body+"\"}","a".repeat(64));var description=new NegotiationRelay.Description(call,1,1,caller,request,NegotiationRelay.Kind.OFFER,body,command);assertThat(description.original()).isSameAs(command);var relay=new NegotiationRelay(4,now::get,bounded,cacheForTest(),(c,s,r,i,b)->new ActorOperation<>(CompletableFuture.completedFuture(snapshot(s,r,i)),CompletableFuture.completedFuture(null)),message->{count.incrementAndGet();return new ActorOperation<>(CompletableFuture.completedFuture(null),CompletableFuture.completedFuture(null));});relay.install(grant(1,1));assertThat(relay.sendTracked(description,Duration.ofSeconds(1)).logical().toCompletableFuture()).isCompletedExceptionally();assertThat(count).hasValue(0);assertThat(bounded.retainedBytes()).isZero();relay.close();}

    @Test void retainedNativeCommandCannotBeReboundToAnotherDescriptor(){var request=UUID.randomUUID();var body="v=0";var original=new io.webrtc.signaling.protocol.CallCommand(io.webrtc.signaling.protocol.SignalEnvelope.Type.OFFER,caller,new RequestId(request),call,CommandScope.call(call),null,new NegotiationId(1),new IceGeneration(1),"{\"sdp\":\"v=0\"}","a".repeat(64));for(var changed:List.of(new NegotiationRelay.Description(call,1,1,caller,request,NegotiationRelay.Kind.OFFER,body,original))){assertThat(changed.original()).isSameAs(original);}assertThatThrownBy(()->new NegotiationRelay.Description(CallId.create("c001",1),1,1,caller,request,NegotiationRelay.Kind.OFFER,body,original)).isInstanceOf(IllegalArgumentException.class);assertThatThrownBy(()->new NegotiationRelay.Description(call,1,1,callee,request,NegotiationRelay.Kind.OFFER,body,original)).isInstanceOf(IllegalArgumentException.class);assertThatThrownBy(()->new NegotiationRelay.Description(call,1,1,caller,UUID.randomUUID(),NegotiationRelay.Kind.OFFER,body,original)).isInstanceOf(IllegalArgumentException.class);assertThatThrownBy(()->new NegotiationRelay.Description(call,2,1,caller,request,NegotiationRelay.Kind.OFFER,body,original)).isInstanceOf(IllegalArgumentException.class);assertThatThrownBy(()->new NegotiationRelay.Description(call,1,2,caller,request,NegotiationRelay.Kind.OFFER,body,original)).isInstanceOf(IllegalArgumentException.class);assertThatThrownBy(()->new NegotiationRelay.Description(call,1,1,caller,request,NegotiationRelay.Kind.ANSWER,body,original)).isInstanceOf(IllegalArgumentException.class);assertThatThrownBy(()->new NegotiationRelay.Description(call,1,1,caller,request,NegotiationRelay.Kind.OFFER,"different",original)).isInstanceOf(IllegalArgumentException.class);}

}
