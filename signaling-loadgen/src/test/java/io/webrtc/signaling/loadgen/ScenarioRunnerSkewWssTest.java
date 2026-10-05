package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.*;
import io.webrtc.signaling.auth.*;
import java.net.*;
import java.security.*;
import java.security.interfaces.RSAPublicKey;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/** Actual TEST_ONLY WSS target dispatch; declared global population is a unit fixture, not a capacity run. */
class ScenarioRunnerSkewWssTest {
    final ObjectMapper json=new ObjectMapper();
    static void set(Object object,String name,Object value)throws Exception {var field=object.getClass().getDeclaredField(name);field.setAccessible(true);field.set(object,value);}
    @Test void hotDestinationSelectionChangesOriginalInviteOnActualWss()throws Exception {run(false);}
    @Test void hotBucketSelectionChangesOriginalInviteOnActualWss()throws Exception {run(true);}
    void run(boolean hotBucket)throws Exception {
        long seed=20261005;String prefix="TEST_ONLY_user";var directory=SkewTargetSelectorTest.directory(50);var selector=new SkewTargetSelector(seed,200000,prefix,directory,5,5);
        long ordinal=0,selected=0;for(;ordinal<1000000;ordinal++){selected=selector.target(ordinal);int bucket=SkewTargetSelectorTest.bucket(prefix+selected);if(hotBucket?bucket==selector.hotBucket():directory[bucket].equals(selector.destinationCell()))break;}assertThat(ordinal).isLessThan(1000000);
        long arrival=System.nanoTime();var assignment=new CallSchedule.Assignment(0,selected==100000?100001:100000,ordinal,arrival);
        var keys=KeyPairGenerator.getInstance("RSA");keys.initialize(2048);var keypair=keys.generateKeyPair();String token=LoadGeneratorTlsTest.token(keypair);
        var verifier=new Rs256TokenVerifier(new IdentitySecurityContract("TEST_ONLY_ISSUER","TEST_ONLY_AUDIENCE",Duration.ofHours(1),Duration.ZERO,Duration.ofSeconds(4),Duration.ofSeconds(5),true,"TEST_ONLY_SOURCE"),new TrustedRsaKeys(Map.of("test",(RSAPublicKey)keypair.getPublic()),null,Duration.ofSeconds(1)),8192);
        var principal=verifier.validate(token,Instant.now());var received=new CompletableFuture<JsonNode>();var boss=new NioEventLoopGroup(1);var children=new NioEventLoopGroup(1);var loops=new NioEventLoopGroup(1);Channel server=null;VirtualClient client=null;
        try {
            var tls=SslContextBuilder.forServer(LoadGeneratorTlsTest.cert("server.crt"),LoadGeneratorTlsTest.cert("server.key")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
            server=new ServerBootstrap().group(boss,children).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<Channel>(){protected void initChannel(Channel c){c.pipeline().addLast(tls.newHandler(c.alloc()),new HttpServerCodec(),new HttpObjectAggregator(81920),new WebSocketServerProtocolHandler(WebSocketServerProtocolConfig.newBuilder().websocketPath("/ws").subprotocols("webrtc-signaling.v1").build()),new SimpleChannelInboundHandler<TextWebSocketFrame>(){protected void channelRead0(ChannelHandlerContext ctx,TextWebSocketFrame wire)throws Exception {
                var frame=json.readTree(wire.text());if(frame.path("type").asText().equals("AUTH")){assertThat(verifier.validate(frame.path("payload").path("token").asText(),Instant.now()).key()).isEqualTo(principal.key());ctx.writeAndFlush(new TextWebSocketFrame(json.createObjectNode().put("type","AUTH_OK").put("sessionIncarnation",UUID.randomUUID().toString()).put("connectionGeneration","1").toString()));}
                else if(frame.path("type").asText().equals("INVITE"))received.complete(frame);
            }});}}).bind("127.0.0.1",0).sync().channel();
            int port=((InetSocketAddress)server.localAddress()).getPort();client=new VirtualClient(0,principal.userId().value(),directory[SkewTargetSelectorTest.bucket(principal.userId().value())],URI.create("wss://localhost:"+port+"/ws"),new InetSocketAddress("127.0.0.1",0),SslContextBuilder.forClient().trustManager(LoadGeneratorTlsTest.cert("ca.crt")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build(),loops,new VirtualClient.Credits(100,819200),new EvidenceWriter(),()->token,(c,event)->{});client.connect(System.nanoTime()).toCompletableFuture().get(3,TimeUnit.SECONDS);
            var runner=new ScenarioRunner();var scenario=json.createObjectNode();scenario.putObject("burst").put("hotDestinationMultiplier",5).put("hotBucketMultiplier",5);set(runner,"scenario",scenario);set(runner,"seed",seed);set(runner,"users",200000L);set(runner,"config",json.createObjectNode().put("userPrefix",prefix));set(runner,"buckets",directory);runner.configureSkew();
            var stateType=Class.forName(ScenarioRunner.class.getName()+"$State");var constructor=stateType.getDeclaredConstructor(VirtualClient.class);constructor.setAccessible(true);var state=constructor.newInstance(client);var statesField=ScenarioRunner.class.getDeclaredField("states");statesField.setAccessible(true);@SuppressWarnings("unchecked") var states=(Map<Long,Object>)statesField.get(runner);states.put(0L,state);
            var invite=ScenarioRunner.class.getDeclaredMethod("invite",CallSchedule.Assignment.class);invite.setAccessible(true);invite.invoke(runner,assignment);
            var wire=received.get(2,TimeUnit.SECONDS);assertThat(wire.path("payload").path("targetUserId").asText()).isEqualTo(prefix+selected);
            assertThat(wire.path("requestId").asText()).isEqualTo(DistributedLoadGenerator.operation(seed,0,"INVITE",ordinal).toString());
            var observed=runner.skewSnapshot();assertThat(observed.get("totalAttempts")).isEqualTo(1L);assertThat(observed.get(hotBucket?"bucketAttempts":"destinationAttempts")).isEqualTo(1L);assertThat(observed.get("hotDestinationCell")).isEqualTo(selector.destinationCell());assertThat(observed.get("hotBucket")).isEqualTo(selector.hotBucket());
        }finally{if(client!=null)client.close().toCompletableFuture().get(3,TimeUnit.SECONDS);if(server!=null)server.close().sync();loops.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();children.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();boss.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();}
    }
}
