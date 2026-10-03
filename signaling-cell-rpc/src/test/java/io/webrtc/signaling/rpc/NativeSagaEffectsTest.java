package io.webrtc.signaling.rpc;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.actors.call.CallActor;
import io.webrtc.signaling.actors.user.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class NativeSagaEffectsTest {
    @Test void typedHomeEffectRequestsSealedGrantFromCurrentEntityAndCannotSendUnsignedWork()throws Exception {
        Instant now=Instant.now();var call=new CallId(CrossCellSagaIT.CALL);UUID op=UUID.randomUUID();var request=new Request(new UserId("bob"),call,UUID.randomUUID(),"a".repeat(64),1,Phase.RINGING,new Grant("c001",1,1,685,2,1,op,now,now.plusSeconds(5),"UNSIGNED"));var seen=new AtomicInteger();
        RpcBusinessHandler.ActorIngress actors=new RpcBusinessHandler.ActorIngress(){
            public CompletionStage<CallActor.GrantReply> grant(Request r,AuthorizationIntent action,String destination,Instant deadline,int bytes){seen.incrementAndGet();assertThat(destination).isEqualTo("c002");assertThat(action).isEqualTo(AuthorizationIntent.reserve());return CompletableFuture.completedFuture(new CallActor.GrantReply("UNKNOWN",null,null));}
            public CompletionStage<UserCommand.Result> user(UserCommand.Operation r,Instant d,int b){throw new AssertionError();}
            public CompletionStage<CallCommandService.Outcome> call(CallCommand c,String p,Instant d,int b){throw new AssertionError();}
            public CompletionStage<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition t,Instant d,int b){throw new AssertionError();}
        };
        var effects=new NativeSagaEffects(actors,(operation,c,budget)->{throw new AssertionError("No sealed native grant, no network effect");},phase->new NativeSagaEffects.HomeStep("c002",new UserCommand.Reserve(request),null),Clock.systemUTC());
        assertThat(effects.apply(CrossCellSaga.Phase.RESERVE_HOME,op,Duration.ofSeconds(2)).toCompletableFuture().join()).isEqualTo("UNKNOWN");assertThat(seen).hasValue(1);
        assertThatThrownBy(()->effects.apply(CrossCellSaga.Phase.RESERVE_HOME,UUID.randomUUID(),Duration.ofSeconds(2))).isInstanceOf(IllegalArgumentException.class);
    }
}
