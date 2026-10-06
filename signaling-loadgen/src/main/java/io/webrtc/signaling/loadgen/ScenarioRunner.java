package io.webrtc.signaling.loadgen;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.*;
import io.webrtc.signaling.auth.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;


/** Worker orchestration: bounded open-loop arrivals, genuine issuer inventory and measured source headroom. */
public final class ScenarioRunner {
    private static final ObjectMapper JSON=new ObjectMapper(JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(16).maxStringLength(131072).build()).build()).findAndRegisterModules();
    private static final ObjectMapper YAML=new ObjectMapper(YAMLFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(16).maxStringLength(65536).maxNumberLength(20).build()).build()).findAndRegisterModules();
    private record Token(String text,AuthPrincipal principal){@Override public String toString(){return "Token[redacted]";}}
    private record Round(String call,String negotiation,String ice,NegotiationTrace trace){Round(String call,String negotiation,String ice){this(call,negotiation,ice,new NegotiationTrace(System.nanoTime(),1_000_000_000L,256));}}
    private static final class State {final VirtualClient client;final AtomicLong operations=new AtomicLong();final CallEventCursor cursor=new CallEventCursor();volatile boolean outgoing,closed,established,accepting,lookup,recovering,negotiating;volatile String call;volatile UUID invite;volatile Round round;volatile SnapshotOffer.Session session;volatile long lastOfferedRound,lastAnsweredRound,lastObservedIce,recoveryGrantVersion;volatile long nextHeartbeat,nextRefresh,reconnectAt,hangupAt,nextLookup;long scheduledReconnectGeneration=-1;int reconnectAttempt;State(VirtualClient c){client=c;}}
    public record RoundIds(String negotiation,String ice){}
    public static Optional<RoundIds> roundIds(JsonNode snapshot){
        String round=snapshot.path("negotiationId").asText("0"),ice=snapshot.path("iceGeneration").asText("0");
        if(round.equals("0")&&ice.equals("0"))return Optional.empty();
        if(!round.matches("[1-9][0-9]{0,18}")||!ice.matches("[1-9][0-9]{0,18}"))throw new IllegalArgumentException("Native round IDs required");
        Long.parseLong(round);Long.parseLong(ice);return Optional.of(new RoundIds(round,ice));
    }
    static long effectiveUsers(long stageSockets,long requestedSockets,long distinctUsers){
        if(stageSockets<1||requestedSockets<1||requestedSockets>10000000||stageSockets>requestedSockets||distinctUsers<1||distinctUsers>requestedSockets)throw new IllegalArgumentException("Invalid native source population");
        return (Math.multiplyExact(stageSockets,distinctUsers)+requestedSockets-1)/requestedSockets;
    }
    public static long arrivalNanos(long start,long ordinal,long rate){if(ordinal<0||rate<1)throw new IllegalArgumentException("Invalid open-loop arrival");long seconds=ordinal/rate,remainder=ordinal%rate;return Math.addExact(start,Math.addExact(Math.multiplyExact(seconds,1_000_000_000L),Math.multiplyExact(remainder,1_000_000_000L)/rate));}
    public static long burstArrivalNanos(long start,long ordinal,long rate,long multiplier,long seconds){
        if(multiplier<1||seconds<0)throw new IllegalArgumentException("Invalid burst");long accelerated=Math.multiplyExact(rate,multiplier),first=Math.multiplyExact(accelerated,seconds);
        if(ordinal<first)return arrivalNanos(start,ordinal,accelerated);
        return arrivalNanos(Math.addExact(start,Math.multiplyExact(seconds,1_000_000_000L)),ordinal-first,rate);
    }
    public static final class SocketGauge {
        private final ConcurrentHashMap<Long,Long> generations=new ConcurrentHashMap<>();private final AtomicLong live=new AtomicLong(),peak=new AtomicLong();
        public void authenticated(long socket,long generation){if(socket<0||generation<1)throw new IllegalArgumentException("Invalid source socket identity");generations.compute(socket,(k,old)->{if(old==null){peak.accumulateAndGet(live.incrementAndGet(),Math::max);return generation;}return Math.max(old,generation);});}
        public void closed(long socket,long generation){generations.computeIfPresent(socket,(k,old)->{if(old==generation){live.decrementAndGet();return null;}return old;});}
        public long live(){return live.get();}public long peak(){return peak.get();}
    }
    private final SocketGauge socketGauge=new SocketGauge();
    private final ActiveTracePool<Long,Round> activeTraces=new ActiveTracePool<>(200000);
    private long planned(long start,long ordinal,long rate){return scenario.has("burst")?burstArrivalNanos(start,ordinal,rate,scenario.path("burst").path("multiplier").asLong(1),scenario.path("burst").path("seconds").asLong(0)):arrivalNanos(start,ordinal,rate);}
    private final EvidenceWriter evidence=new EvidenceWriter();
    private final ConcurrentHashMap<Long,State> states=new ConcurrentHashMap<>();
    private final AtomicLong peakSockets=new AtomicLong(),liveSockets=new AtomicLong(),established=new AtomicLong(),peakEstablished=new AtomicLong(),crossAttempts=new AtomicLong(),callAttempts=new AtomicLong(),relayFrames=new AtomicLong(),registrations=new AtomicLong(),reconnects=new AtomicLong();
    private final AtomicBoolean stopped=new AtomicBoolean(),limited=new AtomicBoolean();
    private SecurityWorkload security;
    private SkewTargetSelector skew;private final AtomicLong hotDestinationAttempts=new AtomicLong(),hotBucketAttempts=new AtomicLong();
    private JsonNode config,scenario,targets;private String[] buckets;private Map<String,List<URI>> endpoints;private String offer,answer;private JsonNode candidates;private long seed;private long users;private int worker,workers;
    private GeneratorResources resourceMonitor;private VirtualClient.Credits credits;private NioEventLoopGroup loops;private final AtomicReference<Map<Long,Token>> inventory=new AtomicReference<>(Map.of());
    private Rs256TokenVerifier verifier;private DistributedLoadGenerator.Range range;private final List<String> failures=new CopyOnWriteArrayList<>();private final AtomicLong eventLoopLag=new AtomicLong();
    /** Requested labels must never be substituted with ordinary traffic and reported as executed. */
    static void requireImplementedWorkload(JsonNode scenario){
        SecurityWorkload.parse(scenario);
        if(scenario.has("burst")){
            var burst=scenario.path("burst");if(!burst.isObject())throw new IllegalArgumentException("Invalid burst profile");
            for(var name:List.of("multiplier","seconds"))if(burst.has(name)&&(!burst.path(name).isIntegralNumber()||!burst.path(name).canConvertToLong()||burst.path(name).longValue()<(name.equals("multiplier")?1:0)))throw new IllegalArgumentException("Invalid burst arrival profile");
            for(var name:List.of("hotDestinationMultiplier","hotBucketMultiplier"))if(burst.has(name)&&(!burst.path(name).isIntegralNumber()||!burst.path(name).canConvertToLong()||!Set.of(1L,5L).contains(burst.path(name).longValue())))
                throw new IllegalArgumentException("SKEW_PROFILE_NOT_IMPLEMENTED");
        }
    }
    void configureSkew(){
        requireImplementedWorkload(scenario);var burst=scenario.path("burst");int destination=burst.path("hotDestinationMultiplier").asInt(1),bucket=burst.path("hotBucketMultiplier").asInt(1);
        skew=destination==1&&bucket==1?null:new SkewTargetSelector(seed,users,config.path("userPrefix").asText(),buckets,destination,bucket);
    }
    Map<String,Object> skewSnapshot(){
        if(skew==null)return Map.of();
        var details=new LinkedHashMap<String,Object>();details.put("hotDestinationCell",skew.destinationCell());details.put("hotBucket",skew.hotBucket()<0?null:skew.hotBucket());details.put("targetUsers",skew.targetUsers());details.put("destinationUsers",skew.destinationUsers());details.put("bucketUsers",skew.bucketUsers());details.put("destinationAttempts",hotDestinationAttempts.get());details.put("bucketAttempts",hotBucketAttempts.get());details.put("totalAttempts",callAttempts.get());return Collections.unmodifiableMap(details);
    }
    public void run(Path scenarioFile,Path configFile,Path output)throws Exception {
        if(Files.exists(output))throw new IllegalArgumentException("Evidence directory must be new");Files.createDirectories(output);
        byte[] configBytes=boundedBytes(configFile,524288),scenarioBytes=boundedBytes(scenarioFile,65536);
        config=JSON.readTree(configBytes);scenario=YAML.readTree(scenarioBytes);requireImplementedWorkload(scenario);security=SecurityWorkload.parse(scenario);targets=scenario.path("targets");
        try(var schema=getClass().getResourceAsStream("/config.schema.json")){if(schema==null)throw new IOException("Worker schema missing");validate(config,JSON.readTree(schema),"config");}
        Files.write(output.resolve("source-config.json"),configBytes,StandardOpenOption.CREATE_NEW);Files.write(output.resolve("source-scenario.yaml"),scenarioBytes,StandardOpenOption.CREATE_NEW);
        worker=config.path("workerIndex").asInt();workers=config.path("workerCount").asInt();seed=config.path("seed").asLong();long sockets=config.path("stageSockets").asLong();
        if(worker>=workers||config.path("sourceIps").size()!=workers||sockets>targets.path("sockets").asLong()||scenario.path("durationSeconds").asLong()<1||scenario.path("durationSeconds").asLong()>86400)throw new IllegalArgumentException("Invalid distributed stage");
        range=DistributedLoadGenerator.partition(sockets,worker,workers);if(range.size()>config.path("localSocketLimit").asLong())throw new IllegalArgumentException("Local socket budget insufficient");
        users=effectiveUsers(sockets,targets.path("sockets").asLong(),targets.path("distinctUsers").asLong());
        InetAddress source=InetAddress.getByName(config.path("sourceIps").get(worker).asText());if(NetworkInterface.getByInetAddress(source)==null||!config.path("testOnly").asBoolean()&&source.isLoopbackAddress())throw new IllegalArgumentException("Source IP must belong to approved worker");
        resourceMonitor=new GeneratorResources(NetworkInterface.getByInetAddress(source).getName(),config.path("nicCapacityBytesPerSecond").asLong());
        var seenIps=new HashSet<String>();for(var ip:config.path("sourceIps"))if(!seenIps.add(InetAddress.getByName(ip.asText()).getHostAddress()))throw new IllegalArgumentException("Duplicate source IP");
        byte[] directory=Files.readAllBytes(boundedPath(config.path("directoryFile").asText(),524288));if(!digest(directory).equals(config.path("directorySnapshotHash").asText()))throw new IllegalArgumentException("Directory snapshot hash mismatch");var directoryNode=JSON.readTree(directory);if(!directoryNode.path("candidateId").asText().equals(config.path("candidateId").asText())||directoryNode.path("buckets").size()!=16384)throw new IllegalArgumentException("Candidate directory incomplete");buckets=new String[16384];for(int i=0;i<buckets.length;i++)buckets[i]=directoryNode.path("buckets").get(i).asText();configureSkew();
        endpoints=new HashMap<>();for(var endpoint:config.path("endpoints")){var uri=URI.create(endpoint.path("url").asText());if(!uri.getScheme().equals("wss")||uri.getHost()==null||uri.getUserInfo()!=null)throw new IllegalArgumentException("WSS endpoint invalid");endpoints.computeIfAbsent(endpoint.path("cell").asText(),c->new ArrayList<>()).add(uri);}for(var cell:buckets)if(!endpoints.containsKey(cell))throw new IllegalArgumentException("Directory cell has no endpoint");
        var rsa=new HashMap<String,RSAPublicKey>();var fields=config.path("publicKeys").fields();while(fields.hasNext()){var key=fields.next();String pem=Files.readString(boundedPath(key.getValue().asText(),8192)).replace("-----BEGIN PUBLIC KEY-----","").replace("-----END PUBLIC KEY-----","").replaceAll("\\s","");rsa.put(key.getKey(),(RSAPublicKey)KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(pem))));}
        var contract=new IdentitySecurityContract(config.path("issuer").asText(),config.path("audience").asText(),Duration.ofSeconds(config.path("maximumJwtLifetimeSeconds").asLong()),Duration.ZERO,Duration.ofSeconds(4),Duration.ofSeconds(5),true,"approved-issuer-inventory");verifier=new Rs256TokenVerifier(contract,new TrustedRsaKeys(rsa,null,Duration.ofSeconds(1)),8192);
        inventory.set(loadInventory(Path.of(config.path("identitiesFile").asText()),null));offer=Files.readString(boundedPath(config.path("offerSdpFile").asText(),65536));answer=Files.readString(boundedPath(config.path("answerSdpFile").asText(),65536));candidates=read(Path.of(config.path("iceCandidatesFile").asText()),8192,JSON);if(!candidates.isArray()||candidates.isEmpty()||candidates.size()>20)throw new IllegalArgumentException("Approved ICE trace invalid");
        credits=new VirtualClient.Credits(config.path("maxPendingOperations").asInt(),config.path("maxPendingBytes").asLong());loops=new NioEventLoopGroup(config.path("eventLoops").asInt());var tls=SslContextBuilder.forClient().sslProvider(SslProvider.JDK).protocols("TLSv1.3").trustManager(boundedPath(config.path("tlsCaFile").asText(),1048576).toFile()).build();
        for(long index=range.start();index<range.end();index++){long identity=index;String user=user(DistributedLoadGenerator.userIndex(index,users)),cell=home(user);var urls=endpoints.get(cell);var client=new VirtualClient(index,user,cell,urls.get((int)Math.floorMod(index,urls.size())),new InetSocketAddress(source,0),tls,loops,credits,evidence,()->Objects.requireNonNull(inventory.get().get(identity)).text(),this::frame);states.put(index,new State(client));}
        Instant began=Instant.now();long warmup=System.nanoTime();Instant scheduled=Instant.parse(config.path("scheduledStartAt").asText());long untilStart=Duration.between(Instant.now(),scheduled).toNanos();if(untilStart<=0)throw new IllegalArgumentException("Common scheduled start must follow worker preparation");long start=Math.addExact(System.nanoTime(),untilStart),end=Math.addExact(start,Duration.ofSeconds(scenario.path("durationSeconds").asLong()).toNanos());double scale=(double)sockets/targets.path("sockets").asLong();long callRate=rate("callAttemptsPerSecond",scale),relayRate=rate("inboundSetupFramesPerSecond",scale),registerRate=rate("registrationsPerSecond",scale);long relayOrdinal=worker,registerOrdinal=worker,connected=0,lastSample=warmup,lastRefreshPoll=warmup,scan=range.start();long connectRate=config.path("connectsPerSecond").asLong();var random=new SplittableRandom(seed^worker);var callSchedule=new CallSchedule(seed,users,range);var refreshExecutor=Executors.newSingleThreadExecutor();CompletableFuture<Map<Long,Token>> refreshing=null;long refreshStamp=0;boolean workloadStarted=false;Instant workloadFinished;long workloadDurationNanos;
        try(var samples=Files.newBufferedWriter(output.resolve("generator.jsonl"),StandardOpenOption.CREATE_NEW)){
            while(System.nanoTime()<end&&!stopped.get()){
                long now=System.nanoTime();if(security.failure()!=null){if(security.generatorLimited())limited.set(true);fail(security.failure());break;}int batch=0;
                while(connected<range.size()&&arrivalNanos(warmup,connected,connectRate)<=now&&batch++<1024){var state=states.get(range.start()+connected);long intended=arrivalNanos(warmup,connected++,connectRate);connect(state,intended);}
                if(batch>=1024&&connected<range.size()&&arrivalNanos(warmup,connected,connectRate)<=now){limited.set(true);fail("CONNECT_SCHEDULER_LIMIT");}
                if(now>=start&&!workloadStarted){if(socketGauge.live()<range.size()){fail("WARMUP_NOT_READY_AT_COMMON_START");break;}workloadStarted=true;}
                batch=0;if(callRate>0){while(batch<1024){var next=callSchedule.nextDue(ordinal->planned(start,ordinal,callRate),now);if(next.isEmpty())break;invite(next.get());batch++;}if(batch>=1024){limited.set(true);fail("CALL_SCHEDULER_LIMIT");}}
                batch=0;while(relayRate>0&&planned(start,relayOrdinal,relayRate)<=now&&batch++<1024){var kind=security.kind(seed,relayOrdinal);long intended=planned(start,relayOrdinal,relayRate);if(kind.isPresent())security(random,kind.get(),intended);else relay(random,relayOrdinal,intended);relayOrdinal+=workers;}
                if(batch>=1024){limited.set(true);fail("RELAY_SCHEDULER_LIMIT");}
                batch=0;while(registerRate>0&&arrivalNanos(start,registerOrdinal,registerRate)<=now&&batch++<1024){var s=choose(random);if(s!=null&&s.client.authenticated())s.client.close().whenComplete((v,e)->{if(e!=null)fail("SOCKET_CLEANUP_UNKNOWN");});else evidence.missed(arrivalNanos(start,registerOrdinal,registerRate),now);registerOrdinal+=workers;}
                int scanCount=Math.max(1,(int)Math.min(2048,(range.size()+99)/100));for(int n=0;n<scanCount&&range.size()>0;n++){if(scan>=range.end())scan=range.start();var s=states.get(scan++);if(s.closed&&now>=s.reconnectAt){s.closed=false;connect(s,now);reconnects.incrementAndGet();}if(s.client.authenticated()){if(s.call==null&&s.invite!=null&&now>=s.nextLookup)lookupInvite(s,now);finishTrace(s,now);if(now>=s.nextHeartbeat){s.client.heartbeat();s.nextHeartbeat=now+Duration.ofSeconds(30).toNanos();}if(now>=s.nextRefresh){s.client.refresh(now);s.nextRefresh=now+Duration.ofSeconds(targets.path("refreshSeconds").asLong()).toNanos();}if(s.outgoing&&s.established&&now>=s.hangupAt){send(s,"HANGUP",null,null,JSON.createObjectNode(),now,EvidenceWriter.Operation.HANGUP);s.hangupAt=Long.MAX_VALUE;}}}
                if(now-lastRefreshPoll>=1_000_000_000L){lastRefreshPoll=now;if(refreshing!=null&&refreshing.isDone()){try{inventory.set(refreshing.join());}catch(CompletionException invalid){fail("REFRESH_INVENTORY_INVALID");}refreshing=null;}var path=Path.of(config.path("refreshIdentitiesFile").asText());long modified=Files.getLastModifiedTime(path).toMillis();if(refreshing==null&&modified!=refreshStamp){refreshStamp=modified;var previous=inventory.get();refreshing=CompletableFuture.supplyAsync(()->{try{return loadInventory(path,previous);}catch(Exception invalid){throw new CompletionException(new IllegalStateException("Approved refresh inventory invalid"));}},refreshExecutor);}}
                if(now-lastSample>=1_000_000_000L){long expected=now;for(var loop:loops)loop.execute(()->eventLoopLag.accumulateAndGet(Math.max(0,System.nanoTime()-expected),Math::max));var sample=resources(Math.max(0,now-start));samples.write(JSON.writeValueAsString(sample));samples.newLine();samples.flush();if(((Map<?,?>)sample.get("headroom")).values().stream().anyMatch(value->!Boolean.TRUE.equals(value))){limited.set(true);fail("GENERATOR_HEADROOM_EXHAUSTED");}lastSample=now;}
                Thread.sleep(10);
            }
        }catch(Exception failure){fail("WORKER_EXECUTION_FAILED");}
        finally{workloadFinished=Instant.now();workloadDurationNanos=Math.max(0,System.nanoTime()-start);stopped.set(true);refreshExecutor.shutdown();try{if(!refreshExecutor.awaitTermination(5,TimeUnit.SECONDS))fail("IDENTITY_SOURCE_CLEANUP_UNKNOWN");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();fail("IDENTITY_SOURCE_CLEANUP_UNKNOWN");}try{CompletableFuture.allOf(states.values().stream().map(s->s.client.drain().toCompletableFuture()).toArray(CompletableFuture[]::new)).get(5,TimeUnit.SECONDS);}catch(Exception unknown){fail("SOCKET_CLEANUP_UNKNOWN");}loops.shutdownGracefully(0,2,TimeUnit.SECONDS).syncUninterruptibly();}
        evidence.export(output.resolve("latency.hdr"));evidence.exportPhases(output.resolve("histograms"));var observed=new LinkedHashMap<String,Object>(evidence.snapshot());observed.put("peakAuthenticatedSockets",socketGauge.peak());observed.put("peakEstablishedCallerCalls",peakEstablished.get());observed.put("callAttempts",callAttempts.get());observed.put("crossCellAttempts",crossAttempts.get());observed.put("relayFrames",relayFrames.get());observed.put("registrations",registrations.get());observed.put("reconnects",reconnects.get());observed.put("durationSeconds",Math.max(0,Duration.between(scheduled,workloadFinished).toSeconds()));observed.put("workloadDurationNanos",workloadDurationNanos);observed.put("scheduledStartAt",scheduled.toString());observed.put("mediaMode",config.path("mediaMode").asText());if(skew!=null)observed.put("skew",skewSnapshot());if(security.enabled())observed.put("security",security.snapshot());
        if(security.enabled()){if(security.generatorLimited())limited.set(true);if(security.failure()!=null)fail(security.failure());if((long)security.snapshot().get("pendingPhysical")!=0)fail("SECURITY_CLEANUP_UNKNOWN");if((long)security.snapshot().get("attempted")==0)fail("SECURITY_TRAFFIC_NOT_OBSERVED");}
        if(socketGauge.peak()<range.size())fail("SOCKET_TARGET_NOT_OBSERVED");if(peakEstablished.get()<Math.floor(targets.path("establishedCalls").asDouble()*scale/workers))fail("ESTABLISHED_TARGET_NOT_OBSERVED");if(evidence.successes()<evidence.attempts()||evidence.attempts()==0)fail("WORKLOAD_FAILURES_OR_EMPTY");
        var summary=JSON.createObjectNode();for(String key:List.of("candidateId","workerHostId","workerIndex","workerCount","gitCommit","imageDigests","configurationFingerprint","compatibilityFingerprint","identityContractFingerprint","topologyFingerprint","hardwareFingerprint","seed","testOnly"))summary.set(key,config.get(key));summary.put("sourceIp",source.getHostAddress());summary.put("scenarioHash",digest(scenarioBytes));summary.put("configHash",digest(configBytes));summary.put("sourceConfig","source-config.json");summary.put("sourceScenario","source-scenario.yaml");summary.put("startedAt",began.toString());summary.put("finishedAt",workloadFinished.toString());summary.put("cleanupFinishedAt",Instant.now().toString());summary.put("status",limited.get()?"GENERATOR_LIMITED":failures.isEmpty()?"PASSED":"FAILED");summary.put("scenario",scenario.path("name").asText());summary.set("requestedTargets",targets);summary.set("observed",JSON.valueToTree(observed));summary.put("rawHistogram","latency.hdr");summary.put("generatorSamples","generator.jsonl");summary.set("failures",JSON.valueToTree(failures));summary.set("socketRange",JSON.valueToTree(Map.of("start",range.start(),"end",range.end())));Files.writeString(output.resolve("summary.json"),JSON.writeValueAsString(summary)+"\n",StandardOpenOption.CREATE_NEW);
        if(!failures.isEmpty())throw new IllegalStateException("Worker stage failed; inspect redacted evidence summary");
    }
    private long rate(String name,double scale){return Math.max(0,(long)Math.ceil(targets.path(name).asDouble()*scale));}
    private void fail(String code){if(!failures.contains(code)&&failures.size()<32)failures.add(code);stopped.set(true);}
    private String user(long index){return config.path("userPrefix").asText()+index;}
    private String home(String user){return buckets[nativeBucket(user)];}
    private static int nativeBucket(String user){try{byte[] hash=MessageDigest.getInstance("SHA-256").digest(user.getBytes(StandardCharsets.UTF_8));return (hash[6]&63)<<8|(hash[7]&255);}catch(NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}}
    private State choose(SplittableRandom random){if(range.size()==0)return null;return states.get(range.start()+random.nextLong(range.size()));}
    private void scheduleReconnect(State state,long generation){
        synchronized(state){if(generation!=state.client.generation()||state.scheduledReconnectGeneration==generation)return;state.scheduledReconnectGeneration=generation;
            long delay=ReconnectBackoff.delayNanos(seed,state.client.index(),state.reconnectAttempt,state.client.retryAfterNanos());if(state.reconnectAttempt<Integer.MAX_VALUE)state.reconnectAttempt++;state.reconnectAt=System.nanoTime()+delay;state.closed=true;}
    }
    private void connect(State state,long intended){var connecting=state.client.connect(intended);long generation=state.client.generation();connecting.whenComplete((value,error)->{if(state.client.generation()!=generation)return;if(error!=null){state.client.close().whenComplete((v,e)->{if(e!=null)fail("SOCKET_CLEANUP_UNKNOWN");else scheduleReconnect(state,generation);});return;}synchronized(state){state.reconnectAttempt=0;}var principal=inventory.get().get(state.client.index()).principal();state.session=new SnapshotOffer.Session(principal.userId().value(),principal.key().issuer(),principal.key().jti(),value.path("sessionIncarnation").asText(),value.path("connectionGeneration").asText());socketGauge.authenticated(state.client.index(),generation);registrations.incrementAndGet();state.nextHeartbeat=System.nanoTime()+30_000_000_000L;state.nextRefresh=System.nanoTime()+Duration.ofSeconds(targets.path("refreshSeconds").asLong()).toNanos();if(state.call!=null){ReconnectFlow.afterResume(send(state,"RESUME",null,null,JSON.createObjectNode(),System.nanoTime(),EvidenceWriter.Operation.RECONNECT),()->state.client.generation()==generation&&state.client.authenticated(),()->sync(state,System.nanoTime())).whenComplete((v,e)->{if(e!=null&&state.client.generation()==generation)fail("RESUME_NOT_COMMITTED");});}else if(state.invite!=null)lookupInvite(state,System.nanoTime());});}
    private void invite(CallSchedule.Assignment assignment){
        var state=states.get(assignment.callerSocket());long intended=assignment.intendedNanos();String target=user(skew==null?assignment.targetUser():skew.target(assignment.ordinal()));
        if(state==null){evidence.missed(EvidenceWriter.Operation.INVITE,intended,System.nanoTime());return;}
        synchronized(state){
            if(!state.client.writable()||state.call!=null||state.outgoing){evidence.missed(EvidenceWriter.Operation.INVITE,intended,System.nanoTime());return;}
            state.outgoing=true;state.invite=DistributedLoadGenerator.operation(seed,state.client.index(),"INVITE",assignment.ordinal());UUID originalInvite=state.invite;
            var frame=JSON.createObjectNode().put("v",1).put("type","INVITE").put("requestId",state.invite.toString());frame.putObject("payload").put("targetUserId",target);
            callAttempts.incrementAndGet();String targetCell=home(target);if(!targetCell.equals(state.client.cell()))crossAttempts.incrementAndGet();if(skew!=null){if(targetCell.equals(skew.destinationCell()))hotDestinationAttempts.incrementAndGet();if(nativeBucket(target)==skew.hotBucket())hotBucketAttempts.incrementAndGet();}
            state.nextLookup=Long.MAX_VALUE;
            state.client.request(frame,intended,EvidenceWriter.Operation.INVITE).whenComplete((reply,error)->resolveInvite(state,originalInvite,error==null?reply:null,false));
        }
    }
    private void lookupInvite(State state,long intended){
        synchronized(state){
            if(state.lookup||state.invite==null||state.call!=null||!state.client.writable())return;
            UUID original=state.invite;long generation=state.client.generation();state.lookup=true;state.nextLookup=Long.MAX_VALUE;
            var request=JSON.createObjectNode().put("v",1).put("type","GET_COMMAND_RESULT").put("requestId",original.toString());request.putObject("payload");
            state.client.request(request,intended,EvidenceWriter.Operation.SYNC).whenComplete((reply,error)->{synchronized(state){
                state.lookup=false;
                if(!Objects.equals(state.invite,original))return;
                if(generation!=state.client.generation()){state.nextLookup=System.nanoTime()+1_000_000_000L;return;}
                resolveInvite(state,original,error==null?reply:null,true);
            }});
        }
    }
    private void resolveInvite(State state,UUID original,JsonNode reply,boolean afterLookup){
        synchronized(state){
            if(!Objects.equals(state.invite,original))return;
            try{switch(InviteOutcome.classify(original,reply)){
                case REJECTED->{if(state.call==null){state.outgoing=false;state.invite=null;}}
                case ENDED->{if(attach(state,reply)){state.cursor.clear(state.call);state.call=null;state.invite=null;state.outgoing=false;}}
                case CREATED->{if(attach(state,reply)&&afterLookup){long generation=state.client.generation();
                    ReconnectFlow.afterResume(send(state,"RESUME",null,null,JSON.createObjectNode(),System.nanoTime(),EvidenceWriter.Operation.RECONNECT),()->state.client.generation()==generation&&state.client.authenticated(),()->sync(state,System.nanoTime()))
                        .whenComplete((v,e)->{if(e!=null&&state.client.generation()==generation)fail("RESUME_NOT_COMMITTED");});}}
                case UNRESOLVED->{state.nextLookup=System.nanoTime()+1_000_000_000L;if(reply!=null&&reply.path("error").path("code").asText().equals("RESULT_EXPIRED"))fail("INVITE_RESULT_UNRESOLVED");}
            }}catch(RuntimeException invalid){fail("INVITE_RESULT_WIRE_CONTRACT_INVALID");}
        }
    }
    private void security(SplittableRandom random,VirtualClient.ProbeKind kind,long intended){
        // Bounded selection does not renew the original scheduled arrival.
        for(int n=0;n<16;n++){
            var state=choose(random);
            if(state!=null&&state.client.probeReady()){security.start(state.client,kind,intended);return;}
        }
        security.unavailable(kind);
    }
    private void relay(SplittableRandom random,long ordinal,long intended){
        var selected=activeTraces.next();if(selected.isEmpty()){evidence.missed(EvidenceWriter.Operation.RELAY,intended,System.nanoTime());return;}
        var entry=selected.get();var state=states.get(entry.key());
        synchronized(state){
            var round=state.round;if(round!=entry.value()||!state.client.writable()){evidence.missed(EvidenceWriter.Operation.RELAY,intended,System.nanoTime());return;}
            var sequence=round.trace().nextCandidate(System.nanoTime());if(sequence.isEmpty()){activeTraces.remove(state.client.index(),round);evidence.missed(EvidenceWriter.Operation.RELAY,intended,System.nanoTime());return;}
            var body=JSON.createObjectNode().put("startSequence",Integer.toString(sequence.getAsInt()));body.putArray("candidates").add(candidates.get((sequence.getAsInt()-1)%candidates.size()));
            send(state,"ICE_CANDIDATES",round.negotiation(),round.ice(),body,intended,EvidenceWriter.Operation.RELAY);relayFrames.incrementAndGet();
        }
    }
    private void finishTrace(State state,long now){
        synchronized(state){var round=state.round;if(round==null)return;var terminal=round.trace().seal(now);if(terminal.isEmpty())return;activeTraces.remove(state.client.index(),round);
            send(state,"END_OF_CANDIDATES",round.negotiation(),round.ice(),JSON.createObjectNode().put("terminalSequence",Integer.toString(terminal.getAsInt())),now,EvidenceWriter.Operation.RELAY)
                .whenComplete((reply,error)->{if(error==null&&!reply.path("type").asText().equals("ERROR")&&state.round==round)
                    send(state,"MEDIA_CONNECTED",round.negotiation(),round.ice(),JSON.createObjectNode().put("senderSequence","1"),System.nanoTime(),EvidenceWriter.Operation.MEDIA);});
        }
    }
    private void frame(VirtualClient client,JsonNode frame){var state=states.get(client.index());if(state==null)return;String type=frame.path("type").asText();long now=System.nanoTime();synchronized(state){
        if(Set.of("RINGING","CALL_READY","ESTABLISHED","TERMINAL","ANSWERED_ELSEWHERE").contains(type)){
            try{
                if(state.call==null&&Set.of("RINGING","CALL_READY").contains(type)&&!attach(state,frame)){ackEvent(state,frame,now);return;}
                if(!state.cursor.accept(frame.path("callId").asText(),nativeVersion(frame))){ackEvent(state,frame,now);return;}
            }catch(RuntimeException invalid){fail("CALL_EVENT_WIRE_CONTRACT_INVALID");return;}
        }else if(Set.of("OFFER","ANSWER","END_OF_CANDIDATES").contains(type)&&!Objects.equals(state.call,frame.path("callId").asText()))return;
        switch(type){
        case "SOCKET_CLOSED"->{if(frame.path("socketGeneration").asLong()!=client.generation())return;socketGauge.closed(client.index(),frame.path("socketGeneration").asLong());activeTraces.remove(client.index(),state.round);state.round=null;client.close().whenComplete((v,e)->{if(e!=null)fail("SOCKET_CLEANUP_UNKNOWN");else scheduleReconnect(state,frame.path("socketGeneration").asLong());});}
        case "RINGING"->{if(!state.outgoing&&!state.accepting){state.accepting=true;send(state,"ACCEPT",null,null,JSON.createObjectNode(),now,EvidenceWriter.Operation.ACCEPT);}}
        case "CALL_READY"->{state.call=frame.path("callId").asText();if(state.outgoing){var body=JSON.createObjectNode().put("iceRestart",false);send(state,"NEGOTIATE_REQUEST",null,null,body,now,EvidenceWriter.Operation.NEGOTIATE).whenComplete((r,e)->{if(e==null)sync(state,System.nanoTime());});}}
        case "OFFER"->{
            final RoundIds ids;
            try{ids=roundIds(frame).orElseThrow();}catch(RuntimeException invalid){fail("NEGOTIATION_WIRE_CONTRACT_MISSING");return;}
            long negotiation=Long.parseLong(ids.negotiation()),ice=Long.parseLong(ids.ice());
            long previous=Math.max(state.lastOfferedRound,state.lastAnsweredRound);
            if(state.round!=null)previous=Math.max(previous,Long.parseLong(state.round.negotiation()));
            long previousIce=state.round==null?state.lastObservedIce:Math.max(state.lastObservedIce,Long.parseLong(state.round.ice()));
            if(negotiation<previous||ice<previousIce||negotiation==previous&&ice!=previousIce)return;
            if(negotiation==state.lastAnsweredRound)return;
            if(state.round==null||!state.round.negotiation().equals(ids.negotiation())||!state.round.ice().equals(ids.ice()))state.round=new Round(state.call,ids.negotiation(),ids.ice());
            state.lastAnsweredRound=negotiation;state.lastObservedIce=ice;
            activeTraces.put(client.index(),state.round);state.round.trace().answerObserved();
            send(state,"ANSWER",ids.negotiation(),ids.ice(),JSON.createObjectNode().put("sdp",answer),now,EvidenceWriter.Operation.RELAY);
        }
        case "ANSWER"->{if(state.round!=null&&state.round.negotiation().equals(frame.path("negotiationId").asText())&&state.round.ice().equals(frame.path("iceGeneration").asText()))state.round.trace().answerObserved();}
        case "END_OF_CANDIDATES"->{} // The peer's terminal sequence never reopens or echoes our local trace.
        case "ESTABLISHED"->{if(state.outgoing&&!state.established){state.established=true;long count=established.incrementAndGet();peakEstablished.accumulateAndGet(count,Math::max);state.hangupAt=now+Duration.ofSeconds(targets.path("meanCallSeconds").asLong()).toNanos();}}
        case "TERMINAL","ANSWERED_ELSEWHERE"->{if(state.established&&state.outgoing)established.decrementAndGet();state.established=false;state.cursor.clear(state.call);state.call=null;state.invite=null;state.lastOfferedRound=0;state.lastAnsweredRound=0;state.lastObservedIce=0;state.recoveryGrantVersion=0;state.accepting=false;state.recovering=false;state.negotiating=false;activeTraces.remove(client.index(),state.round);state.round=null;state.outgoing=false;}
        case "AUTH_EXPIRING"->client.refresh(now);
        case "RECONNECT"->client.close();
        default->{}
    }ackEvent(state,frame,now);}}
    private static long nativeVersion(JsonNode frame){String value=frame.path("callVersion").asText();if(!value.matches("[1-9][0-9]{0,18}"))throw new IllegalArgumentException("Native decimal call version required");return Long.parseLong(value);}
    private boolean attach(State state,JsonNode frame){String call=frame.path("callId").asText();if(!state.cursor.canBind(call))return false;state.cursor.bind(call,nativeVersion(frame));state.call=call;return true;}
    private void ackEvent(State state,JsonNode frame,long intended){if(frame.has("eventId")&&frame.hasNonNull("callId"))sendForCall(state,frame.path("callId").asText(),"EVENT_RECEIVED",null,null,JSON.createObjectNode().put("eventId",frame.path("eventId").asText()),intended,EvidenceWriter.Operation.CONTROL);}
    private void sync(State state,long intended){
        synchronized(state){String requestedCall=state.call;long generation=state.client.generation();
            sendForCall(state,requestedCall,"SYNC_CALL",null,null,JSON.createObjectNode(),intended,EvidenceWriter.Operation.SYNC).whenComplete((reply,error)->{synchronized(state){
                if(error!=null||reply.path("type").asText().equals("ERROR")||!Objects.equals(requestedCall,state.call)||generation!=state.client.generation())return;
                try{
                    long version=nativeVersion(reply);
                    if(state.recoveryGrantVersion>0&&version<state.recoveryGrantVersion){fail("RECOVERY_SNAPSHOT_BEFORE_GRANT");return;}
                    if(!state.cursor.accept(reply.path("callId").asText(),version))return;
                    if(state.recoveryGrantVersion==version&&SnapshotOffer.needsFreshRound(reply,state.session,state.lastOfferedRound,state.recovering)){fail("RECOVERY_SNAPSHOT_NOT_GRANTED");return;}
                    if(SnapshotOffer.needsFreshRound(reply,state.session,state.lastOfferedRound,state.recovering)){requestFreshRound(state,requestedCall,generation);return;}
                    var ids=SnapshotOffer.freshRound(reply,state.session,state.lastOfferedRound);if(ids.isEmpty())return;
                    var round=ids.get();state.recovering=false;state.lastOfferedRound=Long.parseLong(round.negotiation());state.lastObservedIce=Long.parseLong(round.ice());state.round=new Round(requestedCall,round.negotiation(),round.ice());activeTraces.put(state.client.index(),state.round);
                    sendForCall(state,requestedCall,"OFFER",round.negotiation(),round.ice(),JSON.createObjectNode().put("sdp",offer),System.nanoTime(),EvidenceWriter.Operation.RELAY);
                }catch(RuntimeException invalid){fail("SYNC_WIRE_CONTRACT_INVALID");}
            }});
        }
    }
    private void recover(State state,String call,long generation){
        synchronized(state){if(!Objects.equals(call,state.call)||generation!=state.client.generation()||!state.client.authenticated()||state.recovering)return;state.recovering=true;activeTraces.remove(state.client.index(),state.round);state.round=null;sync(state,System.nanoTime());}
    }
    private void requestFreshRound(State state,String call,long generation){
        if(state.negotiating)return;long originalVersion=state.cursor.version();state.recovering=true;state.negotiating=true;activeTraces.remove(state.client.index(),state.round);state.round=null;
        sendForCall(state,call,"NEGOTIATE_REQUEST",null,null,JSON.createObjectNode().put("iceRestart",true),System.nanoTime(),EvidenceWriter.Operation.NEGOTIATE).whenComplete((reply,error)->{synchronized(state){
            if(!Objects.equals(call,state.call)||generation!=state.client.generation())return;state.negotiating=false;
            if(error!=null||!reply.path("type").asText().equals("ACK_COMMITTED")||!reply.path("ackCommitted").asBoolean()||!reply.path("callId").asText().equals(call)||!reply.path("result").path("status").asText().equals("FINAL")||!reply.path("result").path("code").asText().equals("NEGOTIATION_GRANTED")){fail("RECOVERY_NEGOTIATION_NOT_COMMITTED");return;}
            try{long version=nativeVersion(reply);if(version<=originalVersion||!state.cursor.accept(call,version))throw new IllegalArgumentException("Restart did not advance native version");state.recoveryGrantVersion=version;}
            catch(RuntimeException invalid){fail("RECOVERY_GRANT_VERSION_INVALID");return;}
            sync(state,System.nanoTime());
        }});
    }
    private CompletionStage<JsonNode> send(State state,String type,String negotiation,String ice,ObjectNode payload,long intended,EvidenceWriter.Operation operation){return sendForCall(state,state.call,type,negotiation,ice,payload,intended,operation);}
    private CompletionStage<JsonNode> sendForCall(State state,String call,String type,String negotiation,String ice,ObjectNode payload,long intended,EvidenceWriter.Operation operation){var body=JSON.createObjectNode().put("v",1).put("type",type).put("requestId",DistributedLoadGenerator.operation(seed,state.client.index(),type,state.operations.incrementAndGet()).toString());if(call!=null)body.put("callId",call);if(negotiation!=null)body.put("negotiationId",negotiation);if(ice!=null)body.put("iceGeneration",ice);body.set("payload",payload);long generation=state.client.generation();var original=state.client.request(body,intended,operation);if(Set.of("OFFER","ANSWER","ICE_CANDIDATES","END_OF_CANDIDATES").contains(type))original.whenComplete((reply,error)->{if(error==null&&reply.path("type").asText().equals("ERROR")&&reply.path("error").path("code").asText().equals("RESYNC_REQUIRED"))recover(state,call,generation);});return original;}
    private Map<Long,Token> loadInventory(Path file,Map<Long,Token> previous)throws Exception {boundedPath(file.toString(),config.path("maxIdentityBytes").asLong());var values=new HashMap<Long,Token>();var keys=new HashSet<io.webrtc.signaling.protocol.Identity.SessionKey>();try(var reader=Files.newBufferedReader(file)){String line;while((line=boundedLine(reader))!=null){var node=JSON.readTree(line);long index=node.path("socketIndex").asLong(-1);if(index<range.start()||index>=range.end()||values.containsKey(index))throw new IllegalArgumentException("Inventory range duplicate/mismatch");String text=node.path("token").asText();var principal=verifier.validate(text,Instant.now());if(!principal.userId().value().equals(user(DistributedLoadGenerator.userIndex(index,users)))||!keys.add(principal.key())||previous!=null&&(!previous.get(index).principal().key().equals(principal.key())||!previous.get(index).principal().userId().equals(principal.userId())))throw new IllegalArgumentException("Inventory identity mismatch");values.put(index,new Token(text,principal));if(values.size()>config.path("localSocketLimit").asInt())throw new IllegalArgumentException("Inventory exceeds local bounds");}}if(values.size()!=range.size())throw new IllegalArgumentException("Inventory does not cover worker range");return Map.copyOf(values);}
    private static String boundedLine(BufferedReader reader)throws IOException {var result=new StringBuilder();int next;while((next=reader.read())!=-1&&next!='\n'){if(result.length()>=16384)throw new IOException("Inventory line exceeds bound");result.append((char)next);}return next==-1&&result.isEmpty()?null:result.toString();}
    private Map<String,Object> resources(long elapsed)throws IOException {
        var sample=new LinkedHashMap<String,Object>(resourceMonitor.sample(elapsed,eventLoopLag.getAndSet(0),credits.count(),config.path("maxPendingOperations").asInt(),credits.bytes(),config.path("maxPendingBytes").asLong(),config.path("generatorCpuLimit").asDouble()));
        var workload=new LinkedHashMap<String,Object>(Map.of("authenticatedSockets",socketGauge.live(),"establishedCallerCalls",established.get(),"callAttempts",callAttempts.get(),"crossCellAttempts",crossAttempts.get(),"relayFrames",relayFrames.get(),"registrations",registrations.get(),"reconnects",reconnects.get()));if(skew!=null)workload.put("skew",skewSnapshot());sample.put("workload",workload);if(security!=null&&security.enabled())sample.put("security",security.snapshot());return sample;
    }
    private static Path boundedPath(String name,long bound)throws IOException {var path=Path.of(name);if(!Files.isRegularFile(path)||Files.size(path)>bound)throw new IOException("Required input missing or exceeds bound");return path;}
    private static byte[] boundedBytes(Path path,int maximum)throws IOException {boundedPath(path.toString(),maximum);try(var input=Files.newInputStream(path)){byte[] bytes=input.readNBytes(Math.addExact(maximum,1));if(bytes.length>maximum)throw new IOException("Source input exceeds bound");return bytes;}}
    private static JsonNode read(Path path,int maximum,ObjectMapper mapper)throws IOException {return mapper.readTree(boundedBytes(path,maximum));}
    private static String digest(byte[] bytes){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));}catch(NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}}
    private static void validate(JsonNode value,JsonNode schema,String path){String type=schema.path("type").asText();boolean valid=switch(type){case "object"->value.isObject();case "array"->value.isArray();case "string"->value.isTextual();case "integer"->value.isIntegralNumber()&&value.canConvertToLong();case "number"->value.isNumber();case "boolean"->value.isBoolean();default->false;};if(!valid)throw new IllegalArgumentException("Config type mismatch: "+path);if(schema.has("enum")){boolean found=false;for(var allowed:schema.get("enum"))found|=allowed.equals(value);if(!found)throw new IllegalArgumentException("Config enum mismatch: "+path);}if(value.isObject()){for(var required:schema.path("required"))if(!value.has(required.asText()))throw new IllegalArgumentException("Required config missing: "+required.asText());var fields=value.fields();while(fields.hasNext()){var field=fields.next();var child=schema.path("properties").get(field.getKey());if(child==null){var additional=schema.get("additionalProperties");if(additional==null||additional.isBoolean())throw new IllegalArgumentException("Unknown config: "+field.getKey());child=additional;}validate(field.getValue(),child,path+"."+field.getKey());}if(schema.has("minProperties")&&value.size()<schema.path("minProperties").asInt()||schema.has("maxProperties")&&value.size()>schema.path("maxProperties").asInt())throw new IllegalArgumentException("Config object bound: "+path);}if(value.isArray()){if(schema.has("minItems")&&value.size()<schema.path("minItems").asInt()||schema.has("maxItems")&&value.size()>schema.path("maxItems").asInt())throw new IllegalArgumentException("Config array bound: "+path);var distinct=new HashSet<JsonNode>();for(var item:value){validate(item,schema.path("items"),path+"[]");if(schema.path("uniqueItems").asBoolean()&&!distinct.add(item))throw new IllegalArgumentException("Duplicate config item: "+path);}}if(value.isTextual()){String text=value.asText();if(schema.has("pattern")&&!java.util.regex.Pattern.compile(schema.path("pattern").asText()).matcher(text).find()||schema.has("minLength")&&text.length()<schema.path("minLength").asInt()||schema.has("maxLength")&&text.length()>schema.path("maxLength").asInt())throw new IllegalArgumentException("Config text bound: "+path);}if(value.isNumber()){double n=value.asDouble();if(schema.has("minimum")&&n<schema.path("minimum").asDouble()||schema.has("maximum")&&n>schema.path("maximum").asDouble()||schema.has("exclusiveMinimum")&&n<=schema.path("exclusiveMinimum").asDouble())throw new IllegalArgumentException("Config numeric bound: "+path);}}
}
