package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.*;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.Identity.CallId;
import java.net.*;
import java.security.*;
import java.security.interfaces.RSAPublicKey;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/** Actual WSS/RS256 wire generation with explicit TEST_ONLY snapshot authority. */
class ScenarioRunnerOfferWssTest {
    private static final ObjectMapper JSON=new ObjectMapper();
    private static void set(Object target,String field,Object value)throws Exception {var f=target.getClass().getDeclaredField(field);f.setAccessible(true);f.set(target,value);}
    @Test void snapshotGrantedOfferPreservesBothNativeRoundIdsOnActualWss()throws Exception {run(false,false,true);}
    @Test void invalidatedSnapshotRequestsCommittedIceRestartBeforeFreshOfferOnActualWss()throws Exception {run(true,false,true);}
    @Test void scopedResyncReplyRetiresOldTraceAndRequestsFreshRoundOnActualWss()throws Exception {run(false,true,true);}
    @Test void unknownNegotiationOutcomeCannotAuthorizeFreshOfferOnActualWss()throws Exception {run(true,false,false);}
    @Test void snapshotOlderThanCommittedRestartCannotTriggerAnotherNegotiationOnActualWss()throws Exception {run(true,false,true,true);}
    private void run(boolean invalidated,boolean resync,boolean granted)throws Exception {run(invalidated,resync,granted,false);}
    private void run(boolean invalidated,boolean resync,boolean granted,boolean staleAfterGrant)throws Exception {
        var keygen=KeyPairGenerator.getInstance("RSA");keygen.initialize(2048);var keys=keygen.generateKeyPair();String token=LoadGeneratorTlsTest.token(keys);
        var verifier=new Rs256TokenVerifier(new IdentitySecurityContract("TEST_ONLY_ISSUER","TEST_ONLY_AUDIENCE",Duration.ofHours(1),Duration.ZERO,Duration.ofSeconds(4),Duration.ofSeconds(5),true,"TEST_ONLY_SOURCE"),new TrustedRsaKeys(Map.of("test",(RSAPublicKey)keys.getPublic()),null,Duration.ofSeconds(1)),8192);
        var principal=verifier.validate(token,Instant.now());String incarnation=UUID.randomUUID().toString(),connection=UUID.randomUUID().toString(),call=CallId.create("c001",1).value();
        var offered=new CompletableFuture<JsonNode>();var negotiations=new java.util.concurrent.atomic.AtomicInteger();var offerCount=new java.util.concurrent.atomic.AtomicInteger();var rejectedOldOffer=new java.util.concurrent.atomic.AtomicBoolean();var boss=new NioEventLoopGroup(1);var children=new NioEventLoopGroup(1);var loops=new NioEventLoopGroup(1);Channel server=null;VirtualClient client=null;
        try {
            var serverTls=SslContextBuilder.forServer(LoadGeneratorTlsTest.cert("server.crt"),LoadGeneratorTlsTest.cert("server.key")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
            server=new ServerBootstrap().group(boss,children).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<Channel>(){protected void initChannel(Channel channel){channel.pipeline().addLast(serverTls.newHandler(channel.alloc()),new HttpServerCodec(),new HttpObjectAggregator(81920),new WebSocketServerProtocolHandler(WebSocketServerProtocolConfig.newBuilder().websocketPath("/ws").subprotocols("webrtc-signaling.v1").build()),new SimpleChannelInboundHandler<TextWebSocketFrame>(){protected void channelRead0(ChannelHandlerContext ctx,TextWebSocketFrame wire)throws Exception {
                var request=JSON.readTree(wire.text());String type=request.path("type").asText();
                if(type.equals("AUTH")){assertThat(verifier.validate(request.path("payload").path("token").asText(),Instant.now()).key()).isEqualTo(principal.key());ctx.writeAndFlush(new TextWebSocketFrame(JSON.createObjectNode().put("v",1).put("type","AUTH_OK").put("sessionIncarnation",incarnation).put("connectionGeneration","1").toString()));}
                else if(type.equals("NEGOTIATE_REQUEST")){assertThat(invalidated||resync).isTrue();assertThat(request.path("payload").path("iceRestart").asBoolean()).isTrue();negotiations.incrementAndGet();var committed=JSON.createObjectNode().put("v",1).put("type",granted?"ACK_COMMITTED":"COMMAND_RESULT").put("ackCommitted",granted).put("requestId",request.path("requestId").asText()).put("callId",call).put("callVersion","9");committed.putObject("result").put("status",granted?"FINAL":"UNKNOWN").put("code",granted?"NEGOTIATION_GRANTED":"OUTCOME_UNKNOWN");ctx.writeAndFlush(new TextWebSocketFrame(committed.toString()));}
                else if(type.equals("SYNC_CALL")){
                    boolean fresh=!staleAfterGrant&&(invalidated||resync)&&negotiations.get()>0;String round=fresh?"18":"17",ice=fresh?"30":"29";
                    var reply=JSON.createObjectNode().put("v",1).put("type","CALL_SNAPSHOT").put("requestId",request.path("requestId").asText()).put("callId",call).put("callVersion",fresh?"9":"8").put("negotiationId",round).put("iceGeneration",ice);
                    var result=reply.putObject("result").put("code","SNAPSHOT").put("state","CONNECTING");var sender=result.putObject("caller").put("userId",principal.userId().value()).put("issuer",principal.key().issuer()).put("jti",principal.key().jti()).put("sessionIncarnation",incarnation).put("connectionGeneration","1").put("connectionId",connection);
                    result.putObject("negotiation").put("state",(invalidated||rejectedOldOffer.get())&&!fresh?"INVALIDATED":"OFFER_GRANTED").put("negotiationId",round).put("iceGeneration",ice).set("offerer",sender.deepCopy());ctx.writeAndFlush(new TextWebSocketFrame(reply.toString()));
                }else if(type.equals("OFFER")){offerCount.incrementAndGet();if(resync&&request.path("negotiationId").asText().equals("17")){rejectedOldOffer.set(true);var error=JSON.createObjectNode().put("v",1).put("type","ERROR").put("requestId",request.path("requestId").asText());error.putObject("error").put("code","RESYNC_REQUIRED");ctx.writeAndFlush(new TextWebSocketFrame(error.toString()));return;}offered.complete(request);ctx.writeAndFlush(new TextWebSocketFrame(JSON.createObjectNode().put("v",1).put("type","COMMAND_RESULT").put("requestId",request.path("requestId").asText()).put("ackCommitted",false).toString()));}
            }});}}).bind("127.0.0.1",0).sync().channel();int port=((InetSocketAddress)server.localAddress()).getPort();
            var tls=SslContextBuilder.forClient().trustManager(LoadGeneratorTlsTest.cert("ca.crt")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
            client=new VirtualClient(0,principal.userId().value(),"c001",URI.create("wss://localhost:"+port+"/ws"),new InetSocketAddress("127.0.0.1",0),tls,loops,new VirtualClient.Credits(100,819200),new EvidenceWriter(),()->token,(c,event)->{});client.connect(System.nanoTime()).toCompletableFuture().get(5,TimeUnit.SECONDS);
            var runner=new ScenarioRunner();set(runner,"offer","v=0\r\n");var stateType=Class.forName(ScenarioRunner.class.getName()+"$State");var constructor=stateType.getDeclaredConstructor(VirtualClient.class);constructor.setAccessible(true);var state=constructor.newInstance(client);set(state,"call",call);set(state,"outgoing",true);set(state,"session",new SnapshotOffer.Session(principal.userId().value(),principal.key().issuer(),principal.key().jti(),incarnation,"1"));
            var cursorField=stateType.getDeclaredField("cursor");cursorField.setAccessible(true);((CallEventCursor)cursorField.get(state)).bind(call,7);var sync=ScenarioRunner.class.getDeclaredMethod("sync",stateType,long.class);sync.setAccessible(true);sync.invoke(runner,state,System.nanoTime());
            if(staleAfterGrant){var failures=ScenarioRunner.class.getDeclaredField("failures");failures.setAccessible(true);org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(()->assertThat(((java.util.List<?>)failures.get(runner)).contains("RECOVERY_SNAPSHOT_BEFORE_GRANT")).isTrue());assertThat(offered).isNotDone();assertThat(negotiations).hasValue(1);return;}
            if(!granted){var failures=ScenarioRunner.class.getDeclaredField("failures");failures.setAccessible(true);org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(()->assertThat(((java.util.List<?>)failures.get(runner)).contains("RECOVERY_NEGOTIATION_NOT_COMMITTED")).isTrue());assertThat(offered).isNotDone();assertThat(offerCount).hasValue(0);return;}
            var frame=offered.get(3,TimeUnit.SECONDS);assertThat(frame.path("callId").asText()).isEqualTo(call);assertThat(frame.path("negotiationId").asText()).isEqualTo(invalidated||resync?"18":"17");assertThat(frame.path("iceGeneration").asText()).isEqualTo(invalidated||resync?"30":"29");assertThat(negotiations).hasValue(invalidated||resync?1:0);assertThat(frame.path("payload").path("sdp").asText()).isEqualTo("v=0\r\n");
        }finally{if(client!=null)client.close().toCompletableFuture().get(5,TimeUnit.SECONDS);if(server!=null)server.close().sync();loops.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();children.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();boss.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();}
    }
}
