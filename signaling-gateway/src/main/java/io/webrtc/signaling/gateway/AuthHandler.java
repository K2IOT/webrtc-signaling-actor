package io.webrtc.signaling.gateway;
import io.webrtc.signaling.auth.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.SessionRepository;
import io.netty.channel.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.ReferenceCountUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
/** AUTH_OK is emitted only after committed native registration and a still-current gateway boot. */
public final class AuthHandler extends ChannelInboundHandlerAdapter {
    private final ConnectionRegistry registry;private final GatewayServices services;private final Clock clock;private final ProtocolValidator protocol=new ProtocolValidator(ProtocolLimits.v1());
    private static final Executor DEFAULT_CPU=new ThreadPoolExecutor(2,2,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(1024),Thread.ofPlatform().daemon().name("gateway-binding-",0).factory(),new ThreadPoolExecutor.AbortPolicy());
    private final Executor cpu;
    private UUID connection;private SessionRepository.Route route;private boolean verifying;private io.netty.util.concurrent.ScheduledFuture<?> authTimeout;
    public AuthHandler(ConnectionRegistry registry,GatewayServices services,Clock clock){this(registry,services,clock,DEFAULT_CPU);}
    public AuthHandler(ConnectionRegistry registry,GatewayServices services,Clock clock,Executor cpu){this.cpu=Objects.requireNonNull(cpu);this.registry=Objects.requireNonNull(registry);this.services=Objects.requireNonNull(services);this.clock=Objects.requireNonNull(clock);}
    @Override public void channelActive(ChannelHandlerContext ctx){try{connection=ctx.channel().attr(ConnectionRegistry.CONNECTION).get();if(connection==null)connection=registry.attach(ctx.channel());authTimeout=ctx.executor().schedule(()->{if(route==null)ctx.close();},5,TimeUnit.SECONDS);ctx.fireChannelActive();}catch(RejectedExecutionException full){ctx.close();}}
    @Override public void channelRead(ChannelHandlerContext ctx,Object message){if(!(message instanceof FrameAdmissionHandler.Admitted frame)){if(message instanceof CloseWebSocketFrame){ReferenceCountUtil.release(message);ctx.close();return;}if(route==null){ReferenceCountUtil.release(message);ctx.close();return;}ctx.fireChannelRead(message);return;}
        var envelope=frame.envelope();if(!services.currentBoot()||route!=null&&!registry.current(connection,route)){frame.release().run();ctx.close();return;}
        if(envelope.type()==SignalEnvelope.Type.AUTH||envelope.type()==SignalEnvelope.Type.AUTH_REFRESH){boolean refresh=envelope.type()==SignalEnvelope.Type.AUTH_REFRESH;if(verifying||refresh!=(route!=null)){frame.release().run();ctx.close();return;}verify(ctx,frame,refresh);return;}
        var binding=registry.binding(connection);if(route==null||binding==null||services.cachedSecurity(binding.principal(),clock.instant())!=AuthorizationStatus.ALLOWED){frame.release().run();ctx.close();return;}
        try{var sender=new AuthenticatedSession(route.user(),route.key(),route.incarnation(),route.connectionGeneration(),connection);cpu.execute(()->{try{var command=protocol.bind(envelope,sender);var result=services.command(command,remaining(frame));respond(ctx,frame,result,false);}catch(RuntimeException invalid){ctx.executor().execute(()->{frame.release().run();ctx.close();});}});}catch(RuntimeException invalid){frame.release().run();ctx.close();}
    }
    private Duration remaining(FrameAdmissionHandler.Admitted frame){long left=TimeUnit.SECONDS.toNanos(2)-(System.nanoTime()-frame.submittedNanos());if(left<=0)throw new RejectedExecutionException("INGRESS_EXPIRED");return Duration.ofNanos(left);}
    private void verify(ChannelHandlerContext ctx,FrameAdmissionHandler.Admitted frame,boolean refresh){
        verifying=true;SessionRepository.Route expected=route;
        var failed=new java.util.concurrent.atomic.AtomicBoolean();var nativeClosed=new java.util.concurrent.atomic.AtomicBoolean();
        java.util.function.Consumer<SessionRepository.Route> closeOnce=nativeRoute->{if(nativeClosed.compareAndSet(false,true))services.close(nativeRoute);};
        try{
            String token=new ObjectMapper().readTree(frame.envelope().payloadJson()).path("token").asText();
            var checked=services.verify(token,clock.instant()).thenCompose(principal->{
                if(services.cachedSecurity(principal,clock.instant())!=AuthorizationStatus.ALLOWED||!services.currentBoot())return CompletableFuture.failedFuture(new AuthException());
                if(refresh&&(!principal.userId().equals(expected.user())||!principal.key().equals(expected.key())))return CompletableFuture.failedFuture(new AuthException());
                var original=refresh?services.refresh(expected,principal,remaining(frame)):services.register(principal,connection,remaining(frame));
                // Keep the original completion independent of the logical deadline. A late COMMIT still needs cleanup.
                original.whenComplete((nativeRoute,error)->{if(nativeRoute!=null&&(failed.get()||!ctx.channel().isActive()||System.nanoTime()-frame.submittedNanos()>=TimeUnit.SECONDS.toNanos(2)))closeOnce.accept(nativeRoute);});
                return original.thenApply(committed->new AbstractMap.SimpleImmutableEntry<>(principal,committed));
            });
            checked.toCompletableFuture().orTimeout(remaining(frame).toNanos(),TimeUnit.NANOSECONDS).whenComplete((value,error)->{
                if(error!=null)failed.set(true);
                ctx.executor().execute(()->{verifying=false;try{
                    if(error!=null||!ctx.channel().isActive()||!services.currentBoot()||services.cachedSecurity(value.getKey(),clock.instant())!=AuthorizationStatus.ALLOWED||!registry.bind(connection,value.getValue(),value.getKey())){
                        failed.set(true);if(value!=null)closeOnce.accept(value.getValue());ctx.close();return;
                    }
                    route=value.getValue();authTimeout.cancel(false);ctx.fireUserEventTriggered(new GatewayServer.Authenticated(registry.binding(connection)));
                    ctx.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"AUTH_OK\",\"sessionIncarnation\":\""+route.incarnation().value()+"\",\"connectionGeneration\":\""+route.connectionGeneration()+"\"}"));
                }finally{frame.release().run();}});
            });
        }catch(Exception invalid){failed.set(true);verifying=false;frame.release().run();ctx.close();}
    }
    private void respond(ChannelHandlerContext ctx,FrameAdmissionHandler.Admitted frame,CompletionStage<String> response,boolean auth){response.toCompletableFuture().orTimeout(remaining(frame).toNanos(),TimeUnit.NANOSECONDS).whenComplete((value,error)->ctx.executor().execute(()->{try{if(ctx.channel().isActive()&&services.currentBoot()&&registry.current(connection,route)){String output=error==null?value:"{\"v\":1,\"type\":\"OUTCOME_UNKNOWN\"}";if(output==null||output.getBytes(java.nio.charset.StandardCharsets.UTF_8).length>98304){ctx.close();return;}ctx.writeAndFlush(new TextWebSocketFrame(output));}}finally{frame.release().run();}}));}
    @Override public void channelInactive(ChannelHandlerContext ctx){if(authTimeout!=null)authTimeout.cancel(false);if(connection!=null)registry.remove(connection);if(route!=null)services.close(route);ctx.fireChannelInactive();}
    @Override public void exceptionCaught(ChannelHandlerContext ctx,Throwable error){ctx.close();}
}
