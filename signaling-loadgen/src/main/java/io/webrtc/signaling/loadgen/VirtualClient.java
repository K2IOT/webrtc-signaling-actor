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
    private final class Pending {
        final String id;final long generation;final long intended;final EvidenceWriter.Operation operation;final Credits.Ticket ticket;final CompletableFuture<JsonNode> reply=new CompletableFuture<>();
        boolean writeDone,logicalDone,retired;ScheduledFuture<?> timeout;
        Pending(String id,long intended,EvidenceWriter.Operation operation,Credits.Ticket ticket){this.id=id;this.generation=generations.get();this.intended=intended;this.operation=operation;this.ticket=ticket;reply.whenComplete((value,error)->{evidence.record(operation,intended,System.nanoTime(),error==null&&!value.path("type").asText().equals("ERROR"));synchronized(this){logicalDone=true;if(timeout!=null)timeout.cancel(false);}retire();});}
        void written(boolean success){synchronized(this){writeDone=true;}if(!success)reply.completeExceptionally(new IllegalStateException("WSS write failed"));retire();}
        void retire(){synchronized(this){if(retired||!writeDone||!logicalDone)return;retired=true;}pending.remove(id,this);if(auth==this)auth=null;ticket.close();}
    }
    public VirtualClient(long index,String user,String cell,URI endpoint,InetSocketAddress source,SslContext tls,EventLoopGroup loops,Credits credits,EvidenceWriter evidence,Supplier<String> tokens,BiConsumer<VirtualClient,JsonNode> events){this.index=index;this.user=Objects.requireNonNull(user);this.cell=Objects.requireNonNull(cell);this.endpoint=Objects.requireNonNull(endpoint);if(!endpoint.getScheme().equals("wss")||endpoint.getHost()==null||endpoint.getUserInfo()!=null)throw new IllegalArgumentException("WSS required");this.source=source;this.tls=Objects.requireNonNull(tls);this.loops=Objects.requireNonNull(loops);this.credits=credits;this.evidence=evidence;this.tokens=tokens;this.events=events;}
    private record RetryHint(long generation,long observedNanos,long delayNanos){}
    private volatile RetryHint retryHint;
    public long retryAfterNanos(){var original=retryHint;return original==null||original.generation()!=generations.get()?0:Math.max(0,original.delayNanos()-(System.nanoTime()-original.observedNanos()));}
    public long generation(){return generations.get();}
    public long index(){return index;}public String user(){return user;}public String cell(){return cell;}public boolean authenticated(){return authenticated;}public boolean writable(){var c=channel;return authenticated&&c!=null&&c.isWritable()&&pending.size()<8;}
    public CompletionStage<JsonNode> connect(long intended){
        if(probeSlot.get()!=null||channel!=null&&channel.isOpen())return CompletableFuture.failedFuture(new IllegalStateException("Connection already owned"));authenticated=false;long generation=generations.incrementAndGet();
        var ready=new CompletableFuture<JsonNode>();ready.whenComplete((v,e)->evidence.record(EvidenceWriter.Operation.CONNECT,intended,System.nanoTime(),e==null));int port=endpoint.getPort()<0?443:endpoint.getPort();
        var bootstrap=new Bootstrap().group(loops).channel(NioSocketChannel.class).localAddress(source).option(ChannelOption.CONNECT_TIMEOUT_MILLIS,5000).option(ChannelOption.WRITE_BUFFER_WATER_MARK,new WriteBufferWaterMark(65536,131072)).handler(new ChannelInitializer<Channel>(){protected void initChannel(Channel c){
            var ssl=tls.newHandler(c.alloc(),endpoint.getHost(),port);var parameters=ssl.engine().getSSLParameters();parameters.setEndpointIdentificationAlgorithm("HTTPS");ssl.engine().setSSLParameters(parameters);c.pipeline().addLast("tls",ssl).addLast(new HttpClientCodec(),new HttpObjectAggregator(81920),new ChannelInboundHandlerAdapter(){@Override public void channelRead(ChannelHandlerContext ctx,Object message){if(generations.get()==generation&&message instanceof HttpResponse response&&response.status().code()!=101){var hints=response.headers().getAll(HttpHeaderNames.RETRY_AFTER);if(hints.size()==1){long observed=System.nanoTime();ReconnectBackoff.retryAfter(hints.getFirst(),Instant.now()).ifPresent(delay->retryHint=new RetryHint(generation,observed,delay.toNanos()));}}if(message instanceof CloseWebSocketFrame close){var probe=probeSlot.get();if(probe!=null&&probe.generation==generation&&probe.owner==ctx.channel())probe.closedFrame(close.statusCode());}ctx.fireChannelRead(message);}},new WebSocketClientProtocolHandler(WebSocketClientProtocolConfig.newBuilder().webSocketUri(endpoint).subprotocol("webrtc-signaling.v1").version(WebSocketVersion.V13).allowExtensions(false).maxFramePayloadLength(81920).dropPongFrames(false).handshakeTimeoutMillis(5000).build()),new WebSocketFrameAggregator(81920),new SimpleChannelInboundHandler<WebSocketFrame>(){
                @Override public void userEventTriggered(ChannelHandlerContext ctx,Object event){if(event==WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE){if(generations.get()!=generation){ctx.close();return;}authenticate(false,intended).whenComplete((r,e)->{if(e==null)ready.complete(r);else ready.completeExceptionally(e);});}else ctx.fireUserEventTriggered(event);}
                @Override protected void channelRead0(ChannelHandlerContext ctx,WebSocketFrame frame)throws Exception {if(generations.get()!=generation)return;if(frame instanceof PongWebSocketFrame){lastPong=System.nanoTime();return;}if(frame instanceof TextWebSocketFrame text){var value=JSON.readTree(text.text());if(value.path("type").asText().equals("AUTH_OK")){retryHint=null;authenticated=true;var a=auth;if(a!=null&&a.generation==generation)a.reply.complete(value);}else{var p=pending.get(value.path("requestId").asText());if(p!=null&&p.generation==generation)p.reply.complete(value);events.accept(VirtualClient.this,value);}}}
                @Override public void channelInactive(ChannelHandlerContext ctx){if(generations.get()==generation)authenticated=false;for(var p:pending.values())if(p.generation==generation)p.reply.completeExceptionally(new IllegalStateException("WSS closed"));ready.completeExceptionally(new IllegalStateException("WSS closed before authentication"));if(generations.get()==generation)events.accept(VirtualClient.this,JSON.createObjectNode().put("type","SOCKET_CLOSED").put("socketGeneration",generation));}
                @Override public void exceptionCaught(ChannelHandlerContext ctx,Throwable error){var probe=probeSlot.get();if(probe!=null&&probe.generation==generation&&probe.owner==ctx.channel())probe.finish(ProbeOutcome.TRANSPORT_FAILED,-1);ready.completeExceptionally(new IllegalStateException("WSS transport failed"));ctx.close();}
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
        if(probeSlot.get()!=null||bytes>81920||c==null||!c.isActive()||!c.isWritable()||!authentication&&!authenticated||pending.size()>=8){evidence.missed(operation,intended,System.nanoTime());return CompletableFuture.failedFuture(new IllegalStateException("Generator/client admission unavailable"));}
        var ticket=credits.acquire(bytes);if(ticket==null){evidence.missed(operation,intended,System.nanoTime());return CompletableFuture.failedFuture(new IllegalStateException("Generator credit exhausted"));}
        String id=authentication?"AUTH":envelope.path("requestId").asText();if(id.isEmpty()){ticket.close();throw new IllegalArgumentException("Request ID missing");}
        var p=new Pending(id,intended,operation,ticket);if(pending.putIfAbsent(id,p)!=null){ticket.close();throw new IllegalArgumentException("Duplicate in-flight ID");}if(authentication)auth=p;
        p.timeout=c.eventLoop().schedule(()->p.reply.completeExceptionally(new TimeoutException("Original WSS command deadline")),authentication?5:2,TimeUnit.SECONDS);
        evidence.dispatched(intended,System.nanoTime());c.writeAndFlush(new TextWebSocketFrame(encoded)).addListener(write->p.written(write.isSuccess()));return p.reply.minimalCompletionStage();
    }
    public void heartbeat(){var c=channel;if(c==null||!authenticated||probeSlot.get()!=null)return;long stamp=System.nanoTime();lastPong=stamp;c.writeAndFlush(new PingWebSocketFrame());c.eventLoop().schedule(()->{if(lastPong==stamp)c.close();},10,TimeUnit.SECONDS);}
    public void slowConsumer(boolean slow){var c=channel;if(c!=null)c.config().setAutoRead(!slow);}
    public enum ProbeKind { MALFORMED, OVERSIZED }
    public enum ProbeOutcome { PROTOCOL_REJECTED, UNCLASSIFIED_CLOSE, ADMISSION_REJECTED, CREDIT_REJECTED, TRANSPORT_FAILED, DEADLINE_UNKNOWN }
    public record ProbeReceipt(ProbeKind kind,long generation,long intendedNanos,long finishedNanos,ProbeOutcome outcome,int closeCode) {}
    public record ProbeOperation(CompletionStage<ProbeReceipt> observed,CompletionStage<Void> physicalCompletion) {}
    /** A receipt is a logical observation; physicalCompletion proves the original write and socket retired. */
    private final class WireProbe {
        final ProbeKind kind;final long generation,intended;final Channel owner;final Credits.Ticket ticket;
        final CompletableFuture<ProbeReceipt> observed=new CompletableFuture<>();final CompletableFuture<Void> physical=new CompletableFuture<>();
        boolean dispatched,writeDone,writeFailed,closeDone,retired;ScheduledFuture<?> timeout;
        WireProbe(ProbeKind kind,long generation,long intended,Channel owner,Credits.Ticket ticket){this.kind=kind;this.generation=generation;this.intended=intended;this.owner=owner;this.ticket=ticket;}
        ProbeOperation operation(){return new ProbeOperation(observed.minimalCompletionStage(),physical.minimalCompletionStage());}
        void finish(ProbeOutcome outcome,int code){
            if(observed.complete(new ProbeReceipt(kind,generation,intended,System.nanoTime(),outcome,code))&&timeout!=null)timeout.cancel(false);
            retire();
        }
        void closedFrame(int code){if(!dispatched)return;finish(code==(kind==ProbeKind.MALFORMED?1002:1009)?ProbeOutcome.PROTOCOL_REJECTED:ProbeOutcome.UNCLASSIFIED_CLOSE,code);}
        void retire(){if(retired||!writeDone||!closeDone||!observed.isDone())return;retired=true;ticket.close();probeSlot.compareAndSet(this,null);physical.complete(null);}
        void start(){
            long remaining=TimeUnit.SECONDS.toNanos(2)-(System.nanoTime()-intended);
            if(remaining<=0||owner!=channel||generation!=generations.get()||!authenticated||!owner.isActive()||!owner.isWritable()||!pending.isEmpty()){
                writeDone=true;closeDone=true;finish(remaining<=0?ProbeOutcome.DEADLINE_UNKNOWN:ProbeOutcome.ADMISSION_REJECTED,-1);return;
            }
            owner.closeFuture().addListener(done->{closeDone=true;finish(writeFailed?ProbeOutcome.TRANSPORT_FAILED:ProbeOutcome.UNCLASSIFIED_CLOSE,-1);});
            timeout=owner.eventLoop().schedule(()->{finish(ProbeOutcome.DEADLINE_UNKNOWN,-1);owner.close();},remaining,TimeUnit.NANOSECONDS);
            String raw=kind==ProbeKind.MALFORMED?"{":"x".repeat(81921);
            try {dispatched=true;owner.writeAndFlush(new TextWebSocketFrame(raw)).addListener(done->{writeDone=true;writeFailed=!done.isSuccess();retire();});}
            catch(RuntimeException error){writeDone=true;finish(ProbeOutcome.TRANSPORT_FAILED,-1);owner.close();}
        }
    }
    public synchronized ProbeOperation probe(ProbeKind kind,long intended) {
        Objects.requireNonNull(kind);var owner=channel;long generation=generations.get();long elapsed=System.nanoTime()-intended;
        if(elapsed<0||elapsed>=TimeUnit.SECONDS.toNanos(2)||probeSlot.get()!=null||owner==null||!authenticated||!owner.isActive()||!owner.isWritable())return rejectedProbe(kind,generation,intended,elapsed>=TimeUnit.SECONDS.toNanos(2)?ProbeOutcome.DEADLINE_UNKNOWN:ProbeOutcome.ADMISSION_REJECTED);
        var ticket=credits.acquire(kind==ProbeKind.MALFORMED?1:81921);
        if(ticket==null)return rejectedProbe(kind,generation,intended,ProbeOutcome.CREDIT_REJECTED);
        var probe=new WireProbe(kind,generation,intended,owner,ticket);probeSlot.set(probe);
        try {owner.eventLoop().execute(probe::start);}catch(RejectedExecutionException error){probe.writeDone=true;probe.closeDone=true;probe.finish(ProbeOutcome.ADMISSION_REJECTED,-1);}
        return probe.operation();
    }
    private static ProbeOperation rejectedProbe(ProbeKind kind,long generation,long intended,ProbeOutcome outcome){return new ProbeOperation(CompletableFuture.completedFuture(new ProbeReceipt(kind,generation,intended,System.nanoTime(),outcome,-1)).minimalCompletionStage(),CompletableFuture.<Void>completedFuture(null).minimalCompletionStage());}
    public void malformed(String frame){var c=channel;if(c!=null&&c.isWritable()){var ticket=credits.acquire(frame.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);if(ticket!=null)c.writeAndFlush(new TextWebSocketFrame(frame)).addListener(done->ticket.close());}}
    public CompletionStage<Void> close(){var result=new CompletableFuture<Void>();var c=channel;if(c==null){result.complete(null);return result.minimalCompletionStage();}c.close().addListener(done->{if(done.isSuccess())result.complete(null);else result.completeExceptionally(new IllegalStateException("Socket cleanup unproven"));});return result.minimalCompletionStage();}
}
