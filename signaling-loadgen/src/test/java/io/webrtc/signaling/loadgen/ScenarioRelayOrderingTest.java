package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContextBuilder;
import java.net.*;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Local worker ordering only; no native or production delivery evidence. */
class ScenarioRelayOrderingTest {
    private static final ObjectMapper JSON=new ObjectMapper();
    @Test void staleAnswerCannotCompleteCurrentIceTrace() throws Exception {
        try(var fixture=new Fixture()) {
            fixture.frame("ANSWER","17","29");
            assertThat(fixture.trace.seal(Long.MAX_VALUE)).isEmpty();
            fixture.frame("ANSWER","18","30");
            assertThat(fixture.trace.seal(Long.MAX_VALUE)).hasValue(0);
        }
    }
    @Test void mismatchedIceAnswerCannotCompleteCurrentTrace() throws Exception {
        try(var fixture=new Fixture()) {
            fixture.frame("ANSWER","18","29");
            assertThat(fixture.trace.seal(Long.MAX_VALUE)).isEmpty();
        }
    }
    @Test void staleOfferCannotReplaceCurrentRoundOrDispatchAnswer() throws Exception {
        try(var fixture=new Fixture()) {
            fixture.frame("OFFER","17","29");
            assertThat(get(fixture.state,"round")).isSameAs(fixture.round);
            assertThat(((AtomicLong)get(fixture.state,"operations")).get()).isZero();
        }
    }
    @Test void mismatchedIceOfferCannotReplaceCurrentRound() throws Exception {
        try(var fixture=new Fixture()) {
            fixture.frame("OFFER","18","29");
            assertThat(get(fixture.state,"round")).isSameAs(fixture.round);
            assertThat(((AtomicLong)get(fixture.state,"operations")).get()).isZero();
        }
    }
    @Test void duplicateOfferPreservesOriginalTraceAndAnswerRequestIdentity() throws Exception {
        try(var fixture=new Fixture()) {
            fixture.frame("OFFER","18","30");
            var original=get(fixture.state,"round");long count=((AtomicLong)get(fixture.state,"operations")).get();
            fixture.frame("OFFER","18","30");
            assertThat(get(fixture.state,"round")).isSameAs(original);
            assertThat(((AtomicLong)get(fixture.state,"operations")).get()).isEqualTo(count);
        }
    }
    private static Object get(Object target,String name)throws Exception {var field=target.getClass().getDeclaredField(name);field.setAccessible(true);return field.get(target);}
    private static void set(Object target,String name,Object value)throws Exception {var field=target.getClass().getDeclaredField(name);field.setAccessible(true);field.set(target,value);}
    private static final class Fixture implements AutoCloseable {
        final NioEventLoopGroup loops=new NioEventLoopGroup(1);
        final ScenarioRunner runner=new ScenarioRunner();final VirtualClient client;
        final Object state,round;final NegotiationTrace trace=new NegotiationTrace(0,1_000_000_000L,256);
        Fixture()throws Exception {
            client=new VirtualClient(0,"TEST_ONLY_user","c001",URI.create("wss://localhost/ws"),new InetSocketAddress("127.0.0.1",0),SslContextBuilder.forClient().build(),loops,new VirtualClient.Credits(10,819200),new EvidenceWriter(),()->"TEST_ONLY",(c,f)->{});
            var stateType=Class.forName(ScenarioRunner.class.getName()+"$State");var constructor=stateType.getDeclaredConstructor(VirtualClient.class);constructor.setAccessible(true);state=constructor.newInstance(client);set(state,"call","TEST_ONLY_call");
            var roundType=Class.forName(ScenarioRunner.class.getName()+"$Round");var roundConstructor=roundType.getDeclaredConstructor(String.class,String.class,String.class,NegotiationTrace.class);roundConstructor.setAccessible(true);round=roundConstructor.newInstance("TEST_ONLY_call","18","30",trace);set(state,"round",round);set(runner,"answer","v=0\r\n");
            @SuppressWarnings("unchecked") var states=(Map<Long,Object>)get(runner,"states");states.put(0L,state);
        }
        void frame(String type,String negotiation,String ice)throws Exception {
            ObjectNode event=JSON.createObjectNode().put("type",type).put("callId","TEST_ONLY_call").put("negotiationId",negotiation).put("iceGeneration",ice);
            var method=ScenarioRunner.class.getDeclaredMethod("frame",VirtualClient.class,com.fasterxml.jackson.databind.JsonNode.class);method.setAccessible(true);method.invoke(runner,client,event);
        }
        public void close()throws Exception {loops.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();}
    }
}
