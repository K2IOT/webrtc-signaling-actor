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
    @Test void actualWorkerExportsAssignedWssAuthAndMeasuredHeadroomOrExplicitGeneratorLimitation()throws Exception {runWorker(false);}
    @Test void calleeAnswerPreservesOfferRoundAndIceOnActualWss()throws Exception {runWorker(true);}
    @Test void malformedProfileSchedulesNativeWireRejectionAndExportsSeparateCounters()throws Exception {runWorker(false,"malformed");}
    @Test void oversizedProfileSchedulesNativeWireRejectionAndExportsSeparateCounters()throws Exception {runWorker(false,"oversized");}
    @Test void staleGenerationProfileSchedulesNativeReplacementInsideDeclaredSocketBudget()throws Exception {runWorker(false,"staleGeneration");}
    @Test void slowConsumerWorkerRetainsOriginalReadObservationBeyondWorkloadStop()throws Exception {runWorker(false,"slowConsumer");}
    private void runWorker(boolean relay)throws Exception {runWorker(relay,null);}
    private void runWorker(boolean relay,String abuse)throws Exception {
        var keygen=KeyPairGenerator.getInstance("RSA");keygen.initialize(2048);var keys=keygen.generateKeyPair();
        var verifier=new Rs256TokenVerifier(new IdentitySecurityContract("TEST_ONLY_ISSUER","TEST_ONLY_AUDIENCE",Duration.ofHours(1),Duration.ZERO,Duration.ofSeconds(4),Duration.ofSeconds(5),true,"TEST_ONLY_SOURCE"),new TrustedRsaKeys(Map.of("test",(RSAPublicKey)keys.getPublic()),null,Duration.ofSeconds(1)),8192);
        var mutableSource=new AtomicReference<Path>();var sourceMutated=new AtomicBoolean();var authenticated=new ConcurrentHashMap<String,Boolean>();var unexpectedCommands=new AtomicInteger();var received=new ConcurrentLinkedQueue<JsonNode>();var boss=new NioEventLoopGroup(1);var children=new NioEventLoopGroup(1);Channel server=null;
        boolean stale="staleGeneration".equals(abuse),slow="slowConsumer".equals(abuse);
        var nativePeers=ConcurrentHashMap.<Channel>newKeySet();var pressureStop=new AtomicBoolean();var runner=new ScenarioRunner();Thread pressureObserver=null;
        var boot=UUID.randomUUID();var nativeGeneration=new AtomicLong();var incarnation=new io.webrtc.signaling.protocol.Identity.SessionIncarnation(UUID.randomUUID());
        var registry=new io.webrtc.signaling.gateway.ConnectionRegistry("TEST_ONLY_gw",boot,8);
        var nativeServices=new io.webrtc.signaling.gateway.GatewayServices(){
            public boolean currentBoot(){return true;}
            public CompletionStage<AuthPrincipal> verify(String value,Instant at){try{return CompletableFuture.completedFuture(verifier.validate(value,at));}catch(AuthException denied){return CompletableFuture.failedFuture(denied);}}
            public AuthorizationStatus cachedSecurity(AuthPrincipal principal,Instant at){return AuthorizationStatus.ALLOWED;}
            public CompletionStage<io.webrtc.signaling.storage.SessionRepository.Route> register(AuthPrincipal principal,UUID connection,Duration budget){
                authenticated.put(principal.userId().value(),true);
                try{if(sourceMutated.compareAndSet(false,true))Files.writeString(mutableSource.get(),"{}");}catch(Exception failure){return CompletableFuture.failedFuture(failure);}
                return CompletableFuture.completedFuture(new io.webrtc.signaling.storage.SessionRepository.Route(principal.userId(),principal.key(),incarnation,nativeGeneration.incrementAndGet(),"TEST_ONLY_gw",boot,connection,principal.expiresAt(),principal.signingKeyId(),1));
            }
            public CompletionStage<io.webrtc.signaling.storage.SessionRepository.Route> refresh(io.webrtc.signaling.storage.SessionRepository.Route route,AuthPrincipal principal,Duration budget){throw new AssertionError("TEST_ONLY unexpected refresh");}
            public CompletionStage<Void> close(io.webrtc.signaling.storage.SessionRepository.Route route){return CompletableFuture.completedFuture(null);}
            public CompletionStage<String> command(io.webrtc.signaling.protocol.CallCommand command,Duration budget){throw new AssertionError("TEST_ONLY unexpected business command");}
        };
        try {
            var tls=SslContextBuilder.forServer(LoadGeneratorTlsTest.cert("server.crt"),LoadGeneratorTlsTest.cert("server.key")).sslProvider(SslProvider.JDK).protocols("TLSv1.3").build();
            server=new ServerBootstrap().group(boss,children).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<Channel>(){protected void initChannel(Channel c){c.pipeline().addLast(tls.newHandler(c.alloc()),new HttpServerCodec(),new HttpObjectAggregator(81920),new WebSocketServerProtocolHandler(WebSocketServerProtocolConfig.newBuilder().websocketPath("/ws").subprotocols("webrtc-signaling.v1").closeOnProtocolViolation(false).build()),new SimpleChannelInboundHandler<TextWebSocketFrame>(){protected void channelRead0(ChannelHandlerContext ctx,TextWebSocketFrame frame)throws Exception {var body=JSON.readTree(frame.text());if(body.path("type").asText().equals("AUTH")){var principal=verifier.validate(body.path("payload").path("token").asText(),Instant.now());authenticated.put(principal.userId().value(),true);if(sourceMutated.compareAndSet(false,true))Files.writeString(mutableSource.get(),"{}");ctx.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"AUTH_OK\",\"connectionGeneration\":\"1\"}"));if(relay){String call=io.webrtc.signaling.protocol.Identity.CallId.create("c001",1).value();ctx.writeAndFlush(new TextWebSocketFrame(JSON.createObjectNode().put("v",1).put("type","RINGING").put("callId",call).put("callVersion","7").toString()));ctx.writeAndFlush(new TextWebSocketFrame(JSON.createObjectNode().put("v",1).put("type","OFFER").put("callId",call).put("negotiationId","17").put("iceGeneration","29").set("payload",JSON.createObjectNode().put("sdp","v=0\r\n")).toString()));}}else if(relay){received.add(body);ctx.writeAndFlush(new TextWebSocketFrame(JSON.createObjectNode().put("v",1).put("type","COMMAND_RESULT").put("requestId",body.path("requestId").asText()).put("ackCommitted",false).toString()));}else unexpectedCommands.incrementAndGet();}});
                if(abuse!=null){
                    c.pipeline().removeLast();
                    c.pipeline().addBefore(c.pipeline().context(WebSocketServerProtocolHandler.class).name(),"native_decoder_rejection",new io.webrtc.signaling.gateway.DecoderRejectionHandler());
                    c.pipeline().addLast(new io.webrtc.signaling.gateway.FrameAdmissionHandler(new io.webrtc.signaling.protocol.ProtocolValidator(io.webrtc.signaling.protocol.ProtocolLimits.v1()),Runnable::run,new io.webrtc.signaling.gateway.GatewayIngressBudget(8,1048576)),new SimpleChannelInboundHandler<io.webrtc.signaling.gateway.FrameAdmissionHandler.Admitted>(){
                        protected void channelRead0(ChannelHandlerContext ctx,io.webrtc.signaling.gateway.FrameAdmissionHandler.Admitted admitted)throws Exception {
                            try {
                                assertThat(admitted.envelope().type()).isEqualTo(io.webrtc.signaling.protocol.SignalEnvelope.Type.AUTH);
                                var body=JSON.readTree(admitted.envelope().payloadJson());var principal=verifier.validate(body.path("token").asText(),Instant.now());authenticated.put(principal.userId().value(),true);
                                if(sourceMutated.compareAndSet(false,true))Files.writeString(mutableSource.get(),"{}");
                                ctx.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"AUTH_OK\",\"connectionGeneration\":\"1\"}"));
                            }finally{admitted.release().run();}
                        }
                    });
                    if(stale||slow){c.pipeline().removeLast();c.pipeline().addLast(new io.webrtc.signaling.gateway.AuthHandler(registry,nativeServices,Clock.systemUTC(),Runnable::run),new io.webrtc.signaling.gateway.HeartbeatHandler(registry,nativeServices,Clock.systemUTC()));nativePeers.add(c);}
                }
            }}).bind("127.0.0.1",0).sync().channel();int port=((InetSocketAddress)server.localAddress()).getPort();
            String candidate="20261004-0000-aaaaaaa-ffffffffffff";var directory=JSON.createObjectNode().put("candidateId",candidate);var buckets=directory.putArray("buckets");for(int i=0;i<16384;i++)buckets.add("c001");var directoryFile=input("directory.json",directory.toString());
            var tokens=new StringBuilder();for(int i=0;i<2;i++)tokens.append(JSON.createObjectNode().put("socketIndex",i).put("token",token(keys,i))).append('\n');var inventory=input("identities.jsonl",tokens.toString());
            var publicKey=input("rsa.pem","-----BEGIN PUBLIC KEY-----\n"+Base64.getMimeEncoder(64,new byte[]{'\n'}).encodeToString(keys.getPublic().getEncoded())+"\n-----END PUBLIC KEY-----\n");
            var config=JSON.createObjectNode().put("candidateId",candidate).put("gitCommit","a".repeat(40)).put("configurationFingerprint","b".repeat(64)).put("compatibilityFingerprint","c".repeat(64)).put("testOnly",true).put("seed",20261004).put("workerIndex",0).put("workerCount",1).put("localSocketLimit",2).put("eventLoops",1).put("maxPendingOperations",100).put("maxPendingBytes",819200).put("maxIdentityBytes",16384).put("connectsPerSecond",10).put("tlsCaFile",LoadGeneratorTlsTest.cert("ca.crt").getAbsolutePath()).put("issuer","TEST_ONLY_ISSUER").put("audience","TEST_ONLY_AUDIENCE").put("maximumJwtLifetimeSeconds",3600).put("identitiesFile",inventory.toString()).put("refreshIdentitiesFile",inventory.toString()).put("userPrefix","TEST_ONLY_user").put("generatorCpuLimit",.8).put("workerHostId","TEST_ONLY_LOCAL").put("nicCapacityBytesPerSecond",1_000_000_000).put("stageSockets",2).put("directoryFile",directoryFile.toString()).put("directorySnapshotHash",HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(directoryFile)))).put("offerSdpFile",input("offer.sdp","v=0\r\n").toString()).put("answerSdpFile",input("answer.sdp","v=0\r\n").toString()).put("iceCandidatesFile",input("ice.json","[\"candidate:TEST_ONLY\"]").toString()).put("mediaMode","SIGNALING_EMULATED").put("scheduledStartAt",Instant.now().plusSeconds(5).toString());
            for(String binding:List.of("identityContractFingerprint","topologyFingerprint","hardwareFingerprint"))config.put(binding,"d".repeat(64));
            if(stale)config.put("localSocketLimit",3);
            config.putArray("sourceIps").add("127.0.0.1");config.putObject("imageDigests").put("actor","sha256:"+"f".repeat(64)).put("gateway","sha256:"+"1".repeat(64)).put("control","sha256:"+"2".repeat(64));config.putObject("publicKeys").put("test",publicKey.toString());config.putArray("endpoints").addObject().put("cell","c001").put("url","wss://localhost:"+port+"/ws");
            var scenario=JSON.createObjectNode().put("scenarioVersion",1).put("name","TEST_ONLY_TWO_SOCKET_P0").put("durationSeconds",2);scenario.putObject("targets").put("sockets",2).put("distinctUsers",2).put("callAttemptsPerSecond",0).put("establishedCalls",0).put("meanCallSeconds",300).put("inboundSetupFramesPerSecond",0).put("registrationsPerSecond",0).put("crossCellRatio",.98).put("heartbeatSeconds",30).put("refreshSeconds",300);
            if(abuse!=null){scenario.putArray("abuse").add(abuse);scenario.put("abuseFraction",1);scenario.withObject("targets").put("inboundSetupFramesPerSecond",1);}
            var configFile=input("config.json",config.toString());var scenarioFile=input("scenario.yaml",scenario.toString());mutableSource.set(configFile);
            var evidence=scratch.resolve("TEST_ONLY_evidence");
            if(slow){
                pressureObserver=new Thread(()->{
                    var charged=new java.util.HashSet<Channel>();long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
                    try{
                        var stateField=ScenarioRunner.class.getDeclaredField("states");stateField.setAccessible(true);
                        var clientField=Class.forName(ScenarioRunner.class.getName()+"$State").getDeclaredField("client");clientField.setAccessible(true);
                        var channelField=VirtualClient.class.getDeclaredField("channel");channelField.setAccessible(true);
                        while(!pressureStop.get()&&System.nanoTime()<deadline){
                            for(var state:((java.util.Map<?,?>)stateField.get(runner)).values()){
                                var socket=(Channel)channelField.get(clientField.get(state));
                                if(socket==null||!socket.isActive()||socket.config().isAutoRead()||!charged.add(socket))continue;
                                for(var peer:nativePeers)if(java.util.Objects.equals(peer.remoteAddress(),socket.localAddress()))peer.eventLoop().execute(()->{
                                    peer.pipeline().addBefore(peer.pipeline().context(io.webrtc.signaling.gateway.FrameAdmissionHandler.class).name(),"TEST_ONLY_writability_fault",new io.webrtc.signaling.gateway.OutboundAdmissionHandler(new io.webrtc.signaling.rpc.DeliveryCreditController(64,1048576,8,65536)));
                                    peer.unsafe().outboundBuffer().setUserDefinedWritability(1,false);
                                    for(int i=0;i<64;i++)peer.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"ICE\",\"payload\":{\"candidate\":\"TEST_ONLY\"}}"));
                                });
                            }
                            Thread.sleep(10);
                        }
                    }catch(Exception failure){throw new AssertionError("TEST_ONLY native pressure observer failed",failure);}
                },"TEST_ONLY_native_pressure");pressureObserver.start();
            }
            try{runner.run(scenarioFile,configFile,evidence);}catch(IllegalStateException failed){var report=JSON.readTree(Files.readString(evidence.resolve("summary.json")));assertThat(report.path("status").asText()).isEqualTo("GENERATOR_LIMITED");assertThat(report.path("failures").toString()).contains("GENERATOR_HEADROOM_EXHAUSTED");}
            var summary=JSON.readTree(Files.readString(evidence.resolve("summary.json")));
            if(abuse!=null){
                var security=summary.path("observed").path("security");
                assertThat(security.path("scope").asText()).isEqualTo("offeredSetupFrameArrivals");
                assertThat(security.path("attempted").asLong()).isPositive();
                assertThat(security.path("verifiedRejected").asLong()).isEqualTo(security.path("attempted").asLong());
                assertThat(security.path("pendingPhysical").asLong()).isZero();assertThat(security.path("unknown").asLong()).isZero();
                if(slow){
                    assertThat(security.path("modes").path("SLOW_CONSUMER").path("readPauseVerified").asLong()).isEqualTo(security.path("attempted").asLong());
                    assertThat(security.path("modes").path("SLOW_CONSUMER").path("outcomes").path("BACKPRESSURE_REJECTED").asLong()).isEqualTo(security.path("attempted").asLong());
                    assertThat(Duration.between(Instant.parse(summary.path("finishedAt").asText()),Instant.parse(summary.path("cleanupFinishedAt").asText())).toSeconds()).isGreaterThanOrEqualTo(7);
                    assertThat(summary.path("observed").path("durationSeconds").asLong()).isEqualTo(2);
                }
                if(stale){
                    assertThat(security.path("modes").path("STALE_GENERATION").path("outcomes").path("REPLACED").asLong()).isEqualTo(security.path("attempted").asLong());
                    assertThat(summary.path("observed").path("securityReplacementSlots").path("capacity").asInt()).isEqualTo(1);
                    assertThat(summary.path("observed").path("securityReplacementSlots").path("peak").asInt()).isEqualTo(1);
                    assertThat(summary.path("observed").path("securityReplacementSlots").path("pending").asInt()).isZero();
                    assertThat(summary.path("observed").path("registrations").asLong()).isGreaterThanOrEqualTo(2+security.path("attempted").asLong());
                }
                assertThat(summary.path("observed").path("relayFrames").asLong()).isZero();
                assertThat(summary.path("observed").path("successes").asLong()).isEqualTo(summary.path("observed").path("attempts").asLong());
                assertThat(summary.path("status").asText()).isIn("PASSED","GENERATOR_LIMITED");
                assertThat(Files.readString(evidence.resolve("source-scenario.yaml"))).isEqualTo(scenario.toString());
                assertThat(Files.readString(evidence.resolve("summary.json"))).doesNotContain("TEST_ONLY_jti",tokens.toString());
                return;
            }
            assertThat(sourceMutated).isTrue();assertThat(Files.readString(configFile)).isEqualTo("{}");
            assertThat(evidence.resolve("source-config.json")).isRegularFile();assertThat(Files.readString(evidence.resolve("source-config.json"))).isEqualTo(config.toString());assertThat(Files.readString(evidence.resolve("source-scenario.yaml"))).isEqualTo(scenario.toString());
            assertThat(summary.path("configHash").asText()).isEqualTo(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(config.toString().getBytes(StandardCharsets.UTF_8))));
            assertThat(summary.path("scenarioHash").asText()).isEqualTo(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(scenario.toString().getBytes(StandardCharsets.UTF_8))));
            var finished=Instant.parse(summary.path("finishedAt").asText());var cleanupFinished=Instant.parse(summary.path("cleanupFinishedAt").asText());
            assertThat(finished).isBeforeOrEqualTo(cleanupFinished);assertThat(summary.path("observed").path("workloadDurationNanos").asLong(-1)).isBetween(0L,2_500_000_000L);
            assertThat(summary.path("observed").path("durationSeconds").asLong(-1)).isEqualTo(Math.max(0,Duration.between(Instant.parse(config.path("scheduledStartAt").asText()),finished).toSeconds()));
            assertThat(summary.path("sourceConfig").asText()).isEqualTo("source-config.json");assertThat(summary.path("sourceScenario").asText()).isEqualTo("source-scenario.yaml");
            assertThat(summary.path("status").asText()).isIn("PASSED","GENERATOR_LIMITED");assertThat(summary.path("testOnly").asBoolean()).isTrue();assertThat(summary.path("sourceIp").asText()).isEqualTo("127.0.0.1");for(String binding:List.of("identityContractFingerprint","topologyFingerprint","hardwareFingerprint"))assertThat(summary.path(binding).asText()).isEqualTo("d".repeat(64));assertThat(summary.path("observed").path("peakAuthenticatedSockets").asLong()).isEqualTo(2);assertThat(summary.path("observed").path("callAttempts").asLong()).isZero();assertThat(authenticated.keySet()).containsExactlyInAnyOrder("TEST_ONLY_user0","TEST_ONLY_user1");assertThat(unexpectedCommands).hasValue(0);if(relay){var answers=received.stream().filter(f->f.path("type").asText().equals("ANSWER")).toList();assertThat(answers).hasSize(2);for(var answerFrame:answers){assertThat(answerFrame.path("callId").asText()).isNotBlank();assertThat(answerFrame.path("negotiationId").asText()).isEqualTo("17");assertThat(answerFrame.path("iceGeneration").asText()).isEqualTo("29");}}else assertThat(EvidenceWriter.read(evidence.resolve("latency.hdr")).getTotalCount()).isEqualTo(4);
            var samples=Files.readAllLines(evidence.resolve("generator.jsonl"));assertThat(samples).isNotEmpty();boolean observedLimited=false;for(var line:samples){var sample=JSON.readTree(line);assertThat(sample.path("sourceInterface").asText()).isEqualTo("lo");assertThat(sample.path("fdSoftLimit").asLong()).isPositive();assertThat(sample.path("maxPendingOperations").asLong()).isEqualTo(100);assertThat(sample.path("maxPendingBytes").asLong()).isEqualTo(819200);assertThat(sample.path("sampleIntervalNanos").asLong()).isPositive();assertThat(sample.path("headroom").size()).isEqualTo(6);var workload=sample.path("workload");assertThat(workload.path("authenticatedSockets").asLong(-1)).isEqualTo(2);assertThat(workload.path("establishedCallerCalls").asLong(-1)).isZero();assertThat(workload.path("registrations").asLong(-1)).isEqualTo(2);assertThat(workload.path("callAttempts").asLong(-1)).isZero();assertThat(workload.path("crossCellAttempts").asLong(-1)).isZero();assertThat(workload.path("relayFrames").asLong(-1)).isZero();var flags=sample.path("headroom").elements();while(flags.hasNext())observedLimited|=!flags.next().asBoolean();}
            assertThat(observedLimited).isEqualTo(summary.path("status").asText().equals("GENERATOR_LIMITED"));
            assertThat(Files.readString(evidence.resolve("summary.json"))).doesNotContain("TEST_ONLY_jti",tokens.toString());
        }finally{pressureStop.set(true);if(pressureObserver!=null)pressureObserver.join(2000);if(server!=null)server.close().sync();children.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();boss.shutdownGracefully(0,2,TimeUnit.SECONDS).sync();}
    }
}
