package io.webrtc.signaling.gateway;
import io.webrtc.signaling.protocol.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import java.net.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
/** TLS terminates in the gateway. Application services and CPU work are admitted off event loops. */
public final class GatewayServer implements AutoCloseable {
    public record UpgradePolicy(Set<String> origins,Predicate<HttpHeaders> nativePolicy){public UpgradePolicy{origins=Set.copyOf(origins);Objects.requireNonNull(nativePolicy);for(var origin:origins){var uri=URI.create(origin);if(!Set.of("https","http").contains(uri.getScheme())||uri.getHost()==null||uri.getUserInfo()!=null||uri.getQuery()!=null||uri.getFragment()!=null||!uri.getPath().isEmpty())throw new IllegalArgumentException("Exact origin required");}}public boolean accepts(FullHttpRequest request){if(request.method()!=HttpMethod.GET||!request.uri().equals("/ws")||request.headers().getAll(HttpHeaderNames.ORIGIN).size()>1||!supportedProtocol(request.headers()))return false;String origin=request.headers().get(HttpHeaderNames.ORIGIN);return origin==null?nativePolicy.test(request.headers()):origins.contains(origin);}}
    private static boolean supportedProtocol(HttpHeaders headers){
        var values=headers.getAll(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL);if(values.size()!=1)return false;
        String value=values.getFirst();if(value.length()>256)return false;var offered=value.split(",",-1);if(offered.length>8)return false;
        boolean supported=false;for(String item:offered){String protocol=item.trim();if(!protocol.matches("[A-Za-z0-9!#$%&'*+.^_`|~-]{1,128}"))return false;supported|=protocol.equals("webrtc-signaling.v1");}return supported;
    }
    public record Authenticated(ConnectionRegistry.Binding binding) {}
    private final SslContext tls;private final UpgradePolicy policy;private final ConnectionRegistry registry;private final GatewayServices services;private final Clock clock;private final int loops;
    private final ExecutorService cpu=new ThreadPoolExecutor(4,4,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(1024),Thread.ofPlatform().daemon().name("gateway-json-",0).factory(),new ThreadPoolExecutor.AbortPolicy());
    private final EdgeAdmission edge;
    private final io.webrtc.signaling.rpc.DeliveryCreditController outbound=new io.webrtc.signaling.rpc.DeliveryCreditController(16384,268435456,1024,8388608);
    private final GatewayIngressBudget ingress=new GatewayIngressBudget(4096,33554432);
    private EventLoopGroup boss,workers;private HeartbeatWheel heartbeat;private Channel listener;private volatile boolean stopping;
    public GatewayServer(SslContext tls,UpgradePolicy policy,ConnectionRegistry registry,GatewayServices services,Clock clock,int eventLoops){this(tls,policy,registry,services,clock,eventLoops,EdgeAdmission.Limits.candidate());}
    public GatewayServer(SslContext tls,UpgradePolicy policy,ConnectionRegistry registry,GatewayServices services,Clock clock,int eventLoops,EdgeAdmission.Limits edgeLimits){if(tls==null||eventLoops<1||eventLoops>64)throw new IllegalArgumentException("WSS TLS and bounded event loops required");this.tls=tls;this.policy=Objects.requireNonNull(policy);this.registry=Objects.requireNonNull(registry);this.services=Objects.requireNonNull(services);this.clock=Objects.requireNonNull(clock);loops=eventLoops;edge=new EdgeAdmission(edgeLimits,System::nanoTime);}
    public GatewayServer start(InetSocketAddress address)throws InterruptedException{if(listener!=null)throw new IllegalStateException("Already started");boss=new NioEventLoopGroup(1);workers=new NioEventLoopGroup(loops);heartbeat=new HeartbeatWheel(clock,200_000);listener=new ServerBootstrap().group(boss,workers).channel(NioServerSocketChannel.class).childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,new WriteBufferWaterMark(16384,65536)).childOption(ChannelOption.SO_KEEPALIVE,true).childHandler(new ChannelInitializer<SocketChannel>(){@Override public void initChannel(SocketChannel channel){install(channel);}}).bind(address).sync().channel();return this;}
    private void install(SocketChannel channel){var p=channel.pipeline();var ticket=new java.util.concurrent.atomic.AtomicReference<EdgeAdmission.Ticket>();
        p.addLast("edge-admission",new ChannelInboundHandlerAdapter(){@Override public void channelActive(ChannelHandlerContext ctx){if(stopping){ctx.close();return;}try{ticket.set(edge.acquire(((InetSocketAddress)ctx.channel().remoteAddress()).getAddress()));ctx.fireChannelActive();}catch(RuntimeException rejected){ctx.close();}}@Override public void channelInactive(ChannelHandlerContext ctx){var admitted=ticket.getAndSet(null);if(admitted!=null)admitted.close();ctx.fireChannelInactive();}});
        var ssl=tls.newHandler(channel.alloc());ssl.setHandshakeTimeoutMillis(5000);p.addLast("tls",ssl);p.addLast("tls-admission-completion",new ChannelInboundHandlerAdapter(){@Override public void userEventTriggered(ChannelHandlerContext ctx,Object event){if(event instanceof io.netty.handler.ssl.SslHandshakeCompletionEvent){var admitted=ticket.getAndSet(null);if(admitted!=null)admitted.close();}ctx.fireUserEventTriggered(event);}});p.addLast("http",new HttpServerCodec(4096,8192,8192));p.addLast("http-bound",new HttpObjectAggregator(8192));
        p.addLast("origin",new ChannelInboundHandlerAdapter(){@Override public void channelRead(ChannelHandlerContext ctx,Object message){if(message instanceof FullHttpRequest request&&!policy.accepts(request)){io.netty.util.ReferenceCountUtil.release(request);ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,HttpResponseStatus.FORBIDDEN)).addListener(ChannelFutureListener.CLOSE);return;}if(message instanceof PingWebSocketFrame||message instanceof PongWebSocketFrame){var id=ctx.channel().attr(ConnectionRegistry.CONNECTION).get();if(id==null||registry.binding(id)==null){io.netty.util.ReferenceCountUtil.release(message);ctx.close();return;}}ctx.fireChannelRead(message);}});
        p.addLast("websocket",new WebSocketServerProtocolHandler(WebSocketServerProtocolConfig.newBuilder().websocketPath("/ws").subprotocols("webrtc-signaling.v1").checkStartsWith(false).allowExtensions(false).maxFramePayloadLength(81920).closeOnProtocolViolation(false).dropPongFrames(false).handshakeTimeoutMillis(5000).build()));p.addBefore("websocket","decoder-rejection",new DecoderRejectionHandler());p.addLast("outbound-admission",new OutboundAdmissionHandler(outbound));p.addLast("fragment",new FragmentAdmissionHandler(ingress));p.addLast("aggregate",new WebSocketFrameAggregator(81920));p.addLast("frames",new FrameAdmissionHandler(new ProtocolValidator(ProtocolLimits.v1()),cpu,ingress));p.addLast("auth",new AuthHandler(registry,services,clock,cpu));p.addLast("heartbeat",new HeartbeatHandler(registry,services,clock));p.addLast("wheel-registration",new ChannelInboundHandlerAdapter(){@Override public void userEventTriggered(ChannelHandlerContext ctx,Object event){if(event instanceof Authenticated auth)heartbeat.register(auth.binding());else ctx.fireUserEventTriggered(event);}});
    }
    public int port(){if(listener==null)throw new IllegalStateException("Not started");return ((InetSocketAddress)listener.localAddress()).getPort();}
    public boolean accepting(){return !stopping&&listener!=null&&listener.isActive();}
    public void stopAccepting(){stopping=true;if(listener!=null)listener.close();}
    public CompletionStage<Integer> reconnectBatch(int maximum){
        for(var channel:registry.detachBatch(maximum))try{channel.eventLoop().execute(()->{
            if(!channel.isActive())return;
            var deadline=channel.eventLoop().schedule(()->{channel.close();},1,TimeUnit.SECONDS);
            channel.closeFuture().addListener(done->deadline.cancel(false));
            int retryAfter=ThreadLocalRandom.current().nextInt(5001);
            channel.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"RECONNECT\",\"payload\":{\"retryAfterMs\":"+retryAfter+"}}"));
            channel.writeAndFlush(new CloseWebSocketFrame(1001,"RECONNECT")).addListener(ChannelFutureListener.CLOSE);
        });}catch(RejectedExecutionException stopped){channel.close();}
        return CompletableFuture.completedFuture(registry.connectionCount());
    }
    @Override public void close(){stopAccepting();if(listener!=null)listener.close().awaitUninterruptibly();for(var binding:registry.snapshot())binding.channel().close();if(heartbeat!=null)heartbeat.close();if(workers!=null)workers.shutdownGracefully(0,5,TimeUnit.SECONDS).awaitUninterruptibly();if(boss!=null)boss.shutdownGracefully(0,5,TimeUnit.SECONDS).awaitUninterruptibly();cpu.shutdown();try{if(!cpu.awaitTermination(2,TimeUnit.SECONDS))throw new IllegalStateException("Native gateway CPU cleanup unproven");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IllegalStateException("Native gateway CPU cleanup interrupted",interrupted);}}
}
