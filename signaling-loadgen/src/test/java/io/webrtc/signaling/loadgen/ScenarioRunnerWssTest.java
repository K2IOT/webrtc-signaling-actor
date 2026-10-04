package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.*;
import io.webrtc.signaling.auth.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import java.net.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.interfaces.RSAPublicKey;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Two actual WSS sockets, not a capacity qualification or production collector. */
class ScenarioRunnerWssTest {
    @TempDir Path scratch;
    private static final ObjectMapper JSON=new ObjectMapper();
    private String token(KeyPair keys,int index)throws Exception {
        var encode=Base64.getUrlEncoder().withoutPadding();long now=Instant.now().getEpochSecond();String header=encode.encodeToString("{\"alg\":\"RS256\",\"kid\":\"test\"}".getBytes(StandardCharsets.UTF_8));String body=encode.encodeToString(JSON.writeValueAsBytes(Map.of("iss","TEST_ONLY_ISSUER","aud","TEST_ONLY_AUDIENCE","userId","TEST_ONLY_user"+index,"jti","TEST_ONLY_jti"+index,"iat",now,"exp",now+600)));var signature=Signature.getInstance("SHA256withRSA");signature.initSign(keys.getPrivate());signature.update((header+"."+body).getBytes(StandardCharsets.US_ASCII));return header+"."+body+"."+encode.encodeToString(signature.sign());
    }
    private Path input(String name,String content)throws Exception {var path=scratch.resolve(name);Files.writeString(path,content);return path;}
    @Test void actualWorkerAuthenticatesAssignedRangeAndExportsMeasuredHistogramsAndHeadroom()throws Exception {
        var keygen=KeyPairGenerator.getInstance("RSA");keygen.initialize(2048);var keys=keygen.generateKeyPair();
        var verifier=new Rs256TokenVerifier(new IdentitySecurityContract("TEST_ONLY_ISSUER","TEST_ONLY_AUDIENCE",Duration.ofHours(1),Duration.ZERO,Duration.ofSeconds(4),Duration.ofSeconds(5),true,"TEST_ONLY_SOURCE"),new TrustedRsaKeys(Map.of("test",(RSAPublicKey)keys.getPublic()),null,Duration.ofSeconds(1)),8192);
        var authenticated=new ConcurrentHashMap<String,Boolean>();var unexpectedCommands=new AtomicInteger();var boss=new NioEventLoopGroup(1);var children=new NioEventLoopGroup(1);Channel server=null;
        try {
            var tls=SslContextBuilder.forServer(LoadGeneratorTlsTest.cert("server.crt"),LoadGeneratorTlsTest.cert("server.key")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
            server=new ServerBootstrap().group(boss,children).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<Channel>(){protected void initChannel(Channel c){c.pipeline().addLast(tls.newHandler(c.alloc()),new HttpServerCodec(),new HttpObjectAggregator(81920),new WebSocketServerProtocolHandler("/ws"),new SimpleChannelInboundHandler<TextWebSocketFrame>(){protected void channelRead0(ChannelHandlerContext ctx,TextWebSocketFrame frame)throws Exception {var body=JSON.readTree(frame.text());if(body.path("type").asText().equals("AUTH")){var principal=verifier.validate(body.path("payload").path("token").asText(),Instant.now());authenticated.put(principal.userId().value(),true);ctx.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"AUTH_OK\",\"connectionGeneration\":\"1\"}"));}else unexpectedCommands.incrementAndGet();}});}}).bind("127.0.0.1",0).sync().channel();int port=((InetSocketAddress)server.localAddress()).getPort();
            String candidate="20261004-0000-aaaaaaa-ffffffffffff";var directory=JSON.createObjectNode().put("candidateId",candidate);var buckets=directory.putArray("buckets");for(int i=0;i<16384;i++)buckets.add("c001");var directoryFile=input("directory.json",directory.toString());
            var tokens=new StringBuilder();for(int i=0;i<2;i++)tokens.append(JSON.createObjectNode().put("socketIndex",i).put("token",token(keys,i))).append('\n');var inventory=input("identities.jsonl",tokens.toString());
            var publicKey=input("rsa.pem","-----BEGIN PUBLIC KEY-----\n"+Base64.getMimeEncoder(64,new byte[]{'\n'}).encodeToString(keys.getPublic().getEncoded())+"\n-----END PUBLIC KEY-----\n");
            var config=JSON.createObjectNode().put("candidateId",candidate).put("gitCommit","a".repeat(40)).put("configurationFingerprint","b".repeat(64)).put("compatibilityFingerprint","c".repeat(64)).put("testOnly",true).put("seed",20261004).put("workerIndex",0).put("workerCount",1).put("localSocketLimit",2).put("eventLoops",1).put("maxPendingOperations",100).put("maxPendingBytes",819200).put("maxIdentityBytes",16384).put("connectsPerSecond",10).put("tlsCaFile",LoadGeneratorTlsTest.cert("ca.crt").getAbsolutePath()).put("issuer","TEST_ONLY_ISSUER").put("audience","TEST_ONLY_AUDIENCE").put("maximumJwtLifetimeSeconds",3600).put("identitiesFile",inventory.toString()).put("refreshIdentitiesFile",inventory.toString()).put("userPrefix","TEST_ONLY_user").put("generatorCpuLimit",.8).put("workerHostId","TEST_ONLY_LOCAL").put("nicCapacityBytesPerSecond",1_000_000_000).put("stageSockets",2).put("directoryFile",directoryFile.toString()).put("directorySnapshotHash",HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(directoryFile)))).put("offerSdpFile",input("offer.sdp","v=0\r\n").toString()).put("answerSdpFile",input("answer.sdp","v=0\r\n").toString()).put("iceCandidatesFile",input("ice.json","[\"candidate:TEST_ONLY\"]").toString()).put("mediaMode","SIGNALING_EMULATED").put("scheduledStartAt",Instant.now().plusSeconds(5).toString());
            config.putArray("sourceIps").add("127.0.0.1");config.putObject("imageDigests").put("actor","sha256:"+"f".repeat(64)).put("gateway","sha256:"+"1".repeat(64)).put("control","sha256:"+"2".repeat(64));config.putObject("publicKeys").put("test",publicKey.toString());config.putArray("endpoints").addObject().put("cell","c001").put("url","wss://localhost:"+port+"/ws");
            var scenario=JSON.createObjectNode().put("scenarioVersion",1).put("name","TEST_ONLY_TWO_SOCKET_P0").put("durationSeconds",2);scenario.putObject("targets").put("sockets",2).put("distinctUsers",2).put("callAttemptsPerSecond",0).put("establishedCalls",0).put("meanCallSeconds",300).put("inboundSetupFramesPerSecond",0).put("registrationsPerSecond",0).put("crossCellRatio",.98).put("heartbeatSeconds",30).put("refreshSeconds",300);
            var evidence=scratch.resolve("TEST_ONLY_evidence");try{new ScenarioRunner().run(input("scenario.yaml",scenario.toString()),input("config.json",config.toString()),evidence);}catch(IllegalStateException failed){throw new AssertionError(Files.readString(evidence.resolve("summary.json"))+Files.readString(evidence.resolve("generator.jsonl")),failed);}
            var summary=JSON.readTree(Files.readString(evidence.resolve("summary.json")));assertThat(summary.path("status").asText()).isEqualTo("PASSED");assertThat(summary.path("testOnly").asBoolean()).isTrue();assertThat(summary.path("observed").path("peakAuthenticatedSockets").asLong()).isEqualTo(2);assertThat(summary.path("observed").path("callAttempts").asLong()).isZero();assertThat(authenticated.keySet()).containsExactlyInAnyOrder("TEST_ONLY_user0","TEST_ONLY_user1");assertThat(unexpectedCommands).hasValue(0);assertThat(EvidenceWriter.read(evidence.resolve("latency.hdr")).getTotalCount()).isEqualTo(4);
            var samples=Files.readAllLines(evidence.resolve("generator.jsonl"));assertThat(samples).isNotEmpty();for(var line:samples){var sample=JSON.readTree(line);assertThat(sample.path("sourceInterface").asText()).isEqualTo("lo");assertThat(sample.path("fdSoftLimit").asLong()).isPositive();assertThat(sample.path("sampleIntervalNanos").asLong()).isPositive();assertThat(sample.path("headroom").size()).isEqualTo(6);}
            assertThat(Files.readString(evidence.resolve("summary.json"))).doesNotContain("TEST_ONLY_jti",tokens.toString());
        }finally{if(server!=null)server.close().sync();children.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();boss.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();}
    }
}
