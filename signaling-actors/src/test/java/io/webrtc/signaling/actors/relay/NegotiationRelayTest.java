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
}
