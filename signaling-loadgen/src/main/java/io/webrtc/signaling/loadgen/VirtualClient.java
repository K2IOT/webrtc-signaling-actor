package io.webrtc.signaling.loadgen;

import com.fasterxml.jackson.databind.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.SslContext;
import java.net.*;
import java.time.Instant;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Genuine hostname-verified WSS with bounded pending writes/replies. No trust-all TLS or synthetic AUTH_OK. */
public final class VirtualClient {
    public static final class Credits {
        private final int maximum;private final long maxBytes;private int count;private long bytes;
        public Credits(int maximum,long maxBytes){if(maximum<1||maxBytes<1)throw new IllegalArgumentException("Invalid generator credit");this.maximum=maximum;this.maxBytes=maxBytes;}
        public synchronized Ticket acquire(long size){if(size<1||size>maxBytes||count>=maximum||bytes>maxBytes-size)return null;count++;bytes+=size;return new Ticket(this,size);}
        public synchronized int count(){return count;}public synchronized long bytes(){return bytes;}
        public static final class Ticket implements AutoCloseable {private final Credits owner;private final long size;private boolean closed;private Ticket(Credits owner,long size){this.owner=owner;this.size=size;}public void close(){synchronized(owner){if(closed)return;closed=true;owner.count--;owner.bytes-=size;}}}
    }
    private static final ObjectMapper JSON=new ObjectMapper();
    private final long index;private final String user,cell;private final URI endpoint;private final InetSocketAddress source;
    private final SslContext tls;private final EventLoopGroup loops;private final Credits credits;private final EvidenceWriter evidence;
    private final BiConsumer<VirtualClient,JsonNode> events;private final Supplier<String> tokens;
    private final ConcurrentHashMap<String,Pending> pending=new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong generations=new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicReference<WireProbe> probeSlot=new java.util.concurrent.atomic.AtomicReference<>();
    private volatile Channel channel;private volatile Pending auth;private volatile boolean authenticated;private volatile long lastPong;
    private volatile boolean draining;private volatile AuthBinding nativeAuth;
    public record AuthBinding(UUID incarnation,long connectionGeneration) {
        static Optional<AuthBinding> read(JsonNode value){
            try {
                if(value==null||!value.path("v").isIntegralNumber()||!value.path("v").canConvertToInt()||value.path("v").intValue()!=1||!value.path("type").asText().equals("AUTH_OK")||!value.path("sessionIncarnation").isTextual()||!value.path("connectionGeneration").isTextual())return Optional.empty();
                String id=value.path("sessionIncarnation").textValue(),generation=value.path("connectionGeneration").textValue();
                UUID uuid=UUID.fromString(id);if(!uuid.toString().equals(id)||!generation.matches("[1-9][0-9]{0,18}"))return Optional.empty();
                return Optional.of(new AuthBinding(uuid,Long.parseLong(generation)));
            }catch(RuntimeException invalid){return Optional.empty();}
        }
    }
    public Optional<AuthBinding> authBinding(){return Optional.ofNullable(nativeAuth);}
    private final class Pending {
        final String id;final long generation;final long intended;final EvidenceWriter.Operation operation;final Credits.Ticket ticket;final CompletableFuture<JsonNode> reply=new CompletableFuture<>();final CompletableFuture<Void> physical=new CompletableFuture<>();
        boolean writeDone,logicalDone,retired;ScheduledFuture<?> timeout;
        Pending(String id,long intended,EvidenceWriter.Operation operation,Credits.Ticket ticket){this.id=id;this.generation=generations.get();this.intended=intended;this.operation=operation;this.ticket=ticket;reply.whenComplete((value,error)->{evidence.record(operation,intended,System.nanoTime(),error==null&&!value.path("type").asText().equals("ERROR"));synchronized(this){logicalDone=true;if(timeout!=null)timeout.cancel(false);}retire();});}
        void written(boolean success){synchronized(this){writeDone=true;}if(!success)reply.completeExceptionally(new IllegalStateException("WSS write failed"));retire();}
        void retire(){synchronized(this){if(retired||!writeDone||!logicalDone)return;retired=true;}pending.remove(id,this);if(auth==this)auth=null;ticket.close();physical.complete(null);}
    }
    public VirtualClient(long index,String user,String cell,URI endpoint,InetSocketAddress source,SslContext tls,EventLoopGroup loops,Credits credits,EvidenceWriter evidence,Supplier<String> tokens,BiConsumer<VirtualClient,JsonNode> events){this.index=index;this.user=Objects.requireNonNull(user);this.cell=Objects.requireNonNull(cell);this.endpoint=Objects.requireNonNull(endpoint);if(!endpoint.getScheme().equals("wss")||endpoint.getHost()==null||endpoint.getUserInfo()!=null)throw new IllegalArgumentException("WSS required");this.source=source;this.tls=Objects.requireNonNull(tls);this.loops=Objects.requireNonNull(loops);this.credits=credits;this.evidence=evidence;this.tokens=tokens;this.events=events;}
    private record RetryHint(long generation,long observedNanos,long delayNanos){}
    private volatile RetryHint retryHint;
    public long retryAfterNanos(){var original=retryHint;return original==null||original.generation()!=generations.get()?0:Math.max(0,original.delayNanos()-(System.nanoTime()-original.observedNanos()));}
    public long generation(){return generations.get();}
    public long index(){return index;}public String user(){return user;}public String cell(){return cell;}public boolean authenticated(){var c=channel;return !draining&&authenticated&&c!=null&&c.isActive();}public boolean writable(){var c=channel;return !draining&&probeSlot.get()==null&&authenticated&&c!=null&&c.isActive()&&c.isWritable()&&pending.size()<8;}
    public synchronized boolean probeReady(){return writable()&&pending.isEmpty();}
    public synchronized CompletionStage<JsonNode> connect(long intended){
        if(draining||probeSlot.get()!=null||channel!=null&&channel.isOpen())return CompletableFuture.failedFuture(new IllegalStateException("Connection already owned"));authenticated=false;nativeAuth=null;long generation=generations.incrementAndGet();
        var ready=new CompletableFuture<JsonNode>();ready.whenComplete((v,e)->evidence.record(EvidenceWriter.Operation.CONNECT,intended,System.nanoTime(),e==null));int port=endpoint.getPort()<0?443:endpoint.getPort();
        var bootstrap=new Bootstrap().group(loops).channel(NioSocketChannel.class).localAddress(source).option(ChannelOption.CONNECT_TIMEOUT_MILLIS,5000).option(ChannelOption.WRITE_BUFFER_WATER_MARK,new WriteBufferWaterMark(65536,131072)).handler(new ChannelInitializer<Channel>(){protected void initChannel(Channel c){
            var ssl=tls.newHandler(c.alloc(),endpoint.getHost(),port);var parameters=ssl.engine().getSSLParameters();parameters.setEndpointIdentificationAlgorithm("HTTPS");ssl.engine().setSSLParameters(parameters);c.pipeline().addLast("tls",ssl).addLast(new HttpClientCodec(),new HttpObjectAggregator(81920),new ChannelInboundHandlerAdapter(){@Override public void channelRead(ChannelHandlerContext ctx,Object message){if(generations.get()==generation&&message instanceof HttpResponse response&&response.status().code()!=101){var hints=response.headers().getAll(HttpHeaderNames.RETRY_AFTER);if(hints.size()==1){long observed=System.nanoTime();ReconnectBackoff.retryAfter(hints.getFirst(),Instant.now()).ifPresent(delay->retryHint=new RetryHint(generation,observed,delay.toNanos()));}}if(message instanceof CloseWebSocketFrame close){var probe=probeSlot.get();if(probe!=null&&probe.generation==generation&&probe.owner==ctx.channel())probe.closedFrame(close.statusCode(),close.reasonText());}ctx.fireChannelRead(message);}},new WebSocketClientProtocolHandler(WebSocketClientProtocolConfig.newBuilder().webSocketUri(endpoint).subprotocol("webrtc-signaling.v1").version(WebSocketVersion.V13).allowExtensions(false).maxFramePayloadLength(81920).dropPongFrames(false).handshakeTimeoutMillis(5000).build()),new WebSocketFrameAggregator(81920),new SimpleChannelInboundHandler<WebSocketFrame>(){
                @Override public void userEventTriggered(ChannelHandlerContext ctx,Object event){if(event==WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE){if(generations.get()!=generation){ctx.close();return;}authenticate(false,intended).whenComplete((r,e)->{if(e==null)ready.complete(r);else ready.completeExceptionally(e);});}else ctx.fireUserEventTriggered(event);}
                @Override protected void channelRead0(ChannelHandlerContext ctx,WebSocketFrame frame)throws Exception {if(generations.get()!=generation)return;if(frame instanceof PongWebSocketFrame){lastPong=System.nanoTime();return;}if(frame instanceof TextWebSocketFrame text){var value=JSON.readTree(text.text());if(value.path("type").asText().equals("AUTH_OK")){retryHint=null;authenticated=true;var a=auth;if(a!=null&&a.generation==generation){nativeAuth=AuthBinding.read(value).orElse(null);a.reply.complete(value);}}else{var p=pending.get(value.path("requestId").asText());if(p!=null&&p.generation==generation)p.reply.complete(value);events.accept(VirtualClient.this,value);}}}
                @Override public void channelInactive(ChannelHandlerContext ctx){if(generations.get()==generation)authenticated=false;var probe=probeSlot.get();if(probe!=null&&probe.generation==generation&&probe.owner==ctx.channel())probe.inactive();for(var p:pending.values())if(p.generation==generation)p.reply.completeExceptionally(new IllegalStateException("WSS closed"));ready.completeExceptionally(new IllegalStateException("WSS closed before authentication"));if(generations.get()==generation)events.accept(VirtualClient.this,JSON.createObjectNode().put("type","SOCKET_CLOSED").put("socketGeneration",generation));}
                @Override public void exceptionCaught(ChannelHandlerContext ctx,Throwable error){var probe=probeSlot.get();if(probe!=null&&probe.generation==generation&&probe.owner==ctx.channel())probe.transportFailed=true;ready.completeExceptionally(new IllegalStateException("WSS transport failed"));ctx.close();}
            });
        }});
        var connect=bootstrap.connect(endpoint.getHost(),port);var ownedChannel=connect.channel();channel=ownedChannel;connect.addListener(done->{if(!done.isSuccess()){ready.completeExceptionally(new IllegalStateException("WSS connect failed"));ownedChannel.close();}});
        ownedChannel.eventLoop().schedule(()->{if(ready.completeExceptionally(new TimeoutException("WSS AUTH deadline")))ownedChannel.close();},5,TimeUnit.SECONDS);
        return ready.minimalCompletionStage();
    }
    public CompletionStage<JsonNode> refresh(long intended){return authenticate(true,intended);}
    private CompletionStage<JsonNode> authenticate(boolean refresh,long intended){if(auth!=null)return CompletableFuture.failedFuture(new IllegalStateException("AUTH already pending"));var body=JSON.createObjectNode().put("v",1).put("type",refresh?"AUTH_REFRESH":"AUTH");body.putObject("payload").put("token",tokens.get());return send(body,intended,refresh?EvidenceWriter.Operation.REFRESH:EvidenceWriter.Operation.AUTH,true);}
    public CompletionStage<JsonNode> request(JsonNode envelope,long intended,EvidenceWriter.Operation operation){return send(envelope,intended,operation,false);}
    private synchronized CompletionStage<JsonNode> send(JsonNode envelope,long intended,EvidenceWriter.Operation operation,boolean authentication){
        String encoded=envelope.toString();int bytes=encoded.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;var c=channel;
        if(draining||probeSlot.get()!=null||bytes>81920||c==null||!c.isActive()||!c.isWritable()||!authentication&&!authenticated||pending.size()>=8){evidence.missed(operation,intended,System.nanoTime());return CompletableFuture.failedFuture(new IllegalStateException("Generator/client admission unavailable"));}
        var ticket=credits.acquire(bytes);if(ticket==null){evidence.missed(operation,intended,System.nanoTime());return CompletableFuture.failedFuture(new IllegalStateException("Generator credit exhausted"));}
        String id=authentication?"AUTH":envelope.path("requestId").asText();if(id.isEmpty()){ticket.close();throw new IllegalArgumentException("Request ID missing");}
        var p=new Pending(id,intended,operation,ticket);if(pending.putIfAbsent(id,p)!=null){ticket.close();throw new IllegalArgumentException("Duplicate in-flight ID");}if(authentication)auth=p;
        p.timeout=c.eventLoop().schedule(()->p.reply.completeExceptionally(new TimeoutException("Original WSS command deadline")),authentication?5:2,TimeUnit.SECONDS);
        evidence.dispatched(intended,System.nanoTime());c.writeAndFlush(new TextWebSocketFrame(encoded)).addListener(write->p.written(write.isSuccess()));return p.reply.minimalCompletionStage();
    }
    public synchronized void heartbeat(){var c=channel;if(draining||c==null||!authenticated||probeSlot.get()!=null)return;long stamp=System.nanoTime();lastPong=stamp;c.writeAndFlush(new PingWebSocketFrame());c.eventLoop().schedule(()->{if(lastPong==stamp)c.close();},10,TimeUnit.SECONDS);}
    public void slowConsumer(boolean slow){var c=channel;if(c!=null)c.config().setAutoRead(!slow);}
    public enum ProbeKind { MALFORMED, OVERSIZED, SECURITY_CLOSURE }
    public enum ProbeOutcome { PROTOCOL_REJECTED, AUTHORIZATION_REJECTED, SOURCE_UNKNOWN, UNCLASSIFIED_CLOSE, ADMISSION_REJECTED, CREDIT_REJECTED, TRANSPORT_FAILED, DEADLINE_UNKNOWN }
    public enum CloseReason { NONE, UNCLASSIFIED, PROTOCOL_REJECTED, PROTOCOL_FRAME_TOO_LARGE, PROTOCOL_INVALID_UTF8, AUTH_REVOKED, AUTH_FRESHNESS_UNKNOWN, AUTH_TOKEN_EXPIRED, AUTHORIZATION_REJECTED, AUTH_REQUIRED, STALE_CONNECTION }
    public record ProbeReceipt(ProbeKind kind,long generation,long intendedNanos,long finishedNanos,ProbeOutcome outcome,int closeCode,CloseReason closeReason) {
        public ProbeReceipt(ProbeKind kind,long generation,long intendedNanos,long finishedNanos,ProbeOutcome outcome,int closeCode){this(kind,generation,intendedNanos,finishedNanos,outcome,closeCode,CloseReason.NONE);}
    }
    public record ProbeOperation(CompletionStage<ProbeReceipt> observed,CompletionStage<Void> physicalCompletion,CompletionStage<Boolean> admission) {
        public ProbeOperation(CompletionStage<ProbeReceipt> observed,CompletionStage<Void> physicalCompletion){this(observed,physicalCompletion,CompletableFuture.completedFuture(false).minimalCompletionStage());}
    }
    /** A receipt is a logical observation; physicalCompletion proves the original write and socket retired. */
    private final class WireProbe {
        final ProbeKind kind;final long generation,intended;final Channel owner;final Credits.Ticket ticket;
        final CompletableFuture<ProbeReceipt> observed=new CompletableFuture<>();final CompletableFuture<Void> physical=new CompletableFuture<>();final CompletableFuture<Boolean> admission=new CompletableFuture<>();
        boolean dispatched,writeDone,writeFailed,transportFailed,closeDone,retired;ScheduledFuture<?> timeout;
        WireProbe(ProbeKind kind,long generation,long intended,Channel owner,Credits.Ticket ticket){this.kind=kind;this.generation=generation;this.intended=intended;this.owner=owner;this.ticket=ticket;}
        ProbeOperation operation(){return new ProbeOperation(observed.minimalCompletionStage(),physical.minimalCompletionStage(),admission.minimalCompletionStage());}
        void finish(ProbeOutcome outcome,int code){finish(outcome,code,CloseReason.NONE);}
        void finish(ProbeOutcome outcome,int code,CloseReason reason){
            if(!dispatched)admission.complete(false);
            if(observed.complete(new ProbeReceipt(kind,generation,intended,System.nanoTime(),outcome,code,reason))&&timeout!=null)timeout.cancel(false);
            retire();
        }
        void inactive(){finish(writeFailed||transportFailed?ProbeOutcome.TRANSPORT_FAILED:ProbeOutcome.UNCLASSIFIED_CLOSE,-1);}
        void closedFrame(int code,String text){
            if(!dispatched)return;
            CloseReason reason;try{reason=CloseReason.valueOf(text);}catch(IllegalArgumentException unknown){reason=CloseReason.UNCLASSIFIED;}
            // I/O can resume before a delayed timer. The original monotonic deadline still applies.
            if(System.nanoTime()-intended>=probeWindow(kind)){finish(ProbeOutcome.DEADLINE_UNKNOWN,code,reason);return;}
            if(kind==ProbeKind.SECURITY_CLOSURE){
                var outcome=code!=1008?ProbeOutcome.UNCLASSIFIED_CLOSE:switch(reason){
                    case AUTH_FRESHNESS_UNKNOWN -> ProbeOutcome.SOURCE_UNKNOWN;
                    case AUTH_REVOKED,AUTH_TOKEN_EXPIRED,AUTHORIZATION_REJECTED,AUTH_REQUIRED,STALE_CONNECTION -> ProbeOutcome.AUTHORIZATION_REJECTED;
                    default -> ProbeOutcome.UNCLASSIFIED_CLOSE;
                };
                finish(outcome,code,reason);
            }else finish(code==(kind==ProbeKind.MALFORMED?1002:1009)?ProbeOutcome.PROTOCOL_REJECTED:ProbeOutcome.UNCLASSIFIED_CLOSE,code,reason);
        }
        void retire(){if(retired||!writeDone||!closeDone||!observed.isDone())return;retired=true;ticket.close();probeSlot.compareAndSet(this,null);physical.complete(null);}
        void start(){
            long remaining=probeWindow(kind)-(System.nanoTime()-intended);
            if(remaining<=0||owner!=channel||generation!=generations.get()||!authenticated||!owner.isActive()||!owner.isWritable()||!pending.isEmpty()){
                writeDone=true;closeDone=true;finish(remaining<=0?ProbeOutcome.DEADLINE_UNKNOWN:ProbeOutcome.ADMISSION_REJECTED,-1);return;
            }
            owner.closeFuture().addListener(done->{closeDone=true;retire();});
            timeout=owner.eventLoop().schedule(()->{finish(ProbeOutcome.DEADLINE_UNKNOWN,-1);owner.close();},remaining,TimeUnit.NANOSECONDS);
            if(kind==ProbeKind.SECURITY_CLOSURE){dispatched=true;writeDone=true;admission.complete(true);return;}
            String raw=kind==ProbeKind.MALFORMED?"{":"x".repeat(81921);
            try {dispatched=true;admission.complete(true);owner.writeAndFlush(new TextWebSocketFrame(raw)).addListener(done->{writeDone=true;writeFailed=!done.isSuccess();retire();});}
            catch(RuntimeException error){writeDone=true;finish(ProbeOutcome.TRANSPORT_FAILED,-1);owner.close();}
        }
    }
    private static long probeWindow(ProbeKind kind){return TimeUnit.SECONDS.toNanos(kind==ProbeKind.SECURITY_CLOSURE?5:2);}
    /** SECURITY_CLOSURE observes an already authenticated socket; it performs no drill or outgoing write. */
    public synchronized ProbeOperation probe(ProbeKind kind,long intended) {
        Objects.requireNonNull(kind);var owner=channel;long generation=generations.get();long elapsed=System.nanoTime()-intended;
        if(draining||elapsed<0||elapsed>=probeWindow(kind)||probeSlot.get()!=null||owner==null||!authenticated||!owner.isActive()||!owner.isWritable())return rejectedProbe(kind,generation,intended,elapsed>=probeWindow(kind)?ProbeOutcome.DEADLINE_UNKNOWN:ProbeOutcome.ADMISSION_REJECTED);
        var ticket=credits.acquire(kind==ProbeKind.OVERSIZED?81921:1);
        if(ticket==null)return rejectedProbe(kind,generation,intended,ProbeOutcome.CREDIT_REJECTED);
        var probe=new WireProbe(kind,generation,intended,owner,ticket);probeSlot.set(probe);
        try {owner.eventLoop().execute(probe::start);}catch(RejectedExecutionException error){probe.writeDone=true;probe.closeDone=true;probe.finish(ProbeOutcome.ADMISSION_REJECTED,-1);}
        return probe.operation();
    }
    private static ProbeOperation rejectedProbe(ProbeKind kind,long generation,long intended,ProbeOutcome outcome){return new ProbeOperation(CompletableFuture.completedFuture(new ProbeReceipt(kind,generation,intended,System.nanoTime(),outcome,-1)).minimalCompletionStage(),CompletableFuture.<Void>completedFuture(null).minimalCompletionStage());}
    public CompletionStage<Void> close(){var result=new CompletableFuture<Void>();var c=channel;if(c==null){result.complete(null);return result.minimalCompletionStage();}c.close().addListener(done->{if(done.isSuccess())result.complete(null);else result.completeExceptionally(new IllegalStateException("Socket cleanup unproven"));});return result.minimalCompletionStage();}
    /** Terminal drain joins original write/logical receipts as well as the actual socket. */
    public synchronized CompletionStage<Void> drain(){
        draining=true;
        var originals=new ArrayList<CompletableFuture<?>>();
        for(var work:pending.values())originals.add(work.physical);
        var probe=probeSlot.get();if(probe!=null)originals.add(probe.physical);
        originals.add(close().toCompletableFuture());
        return CompletableFuture.allOf(originals.toArray(CompletableFuture[]::new)).minimalCompletionStage();
    }
}
