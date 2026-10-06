package io.webrtc.signaling.gateway;
import io.webrtc.signaling.protocol.*;
import io.netty.channel.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ReferenceCountUtil;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
/** Bounded owned copies; large JSON is parsed on the supplied bounded CPU executor. */
public final class FrameAdmissionHandler extends ChannelInboundHandlerAdapter {
    public record Admitted(SignalEnvelope envelope,int bytes,long submittedNanos,Runnable release) {}
    private record Input(byte[] bytes,long submitted,GatewayIngressBudget.Ticket credit) {}
    private static final GatewayIngressBudget SHARED=new GatewayIngressBudget(4096,33554432);
    private final GatewayIngressBudget budget;
    private final ProtocolValidator protocol;private final Executor cpu;private final ArrayDeque<Input> waiting=new ArrayDeque<>();private int retainedBytes,inFlight;private boolean running;private double rateTokens=60;private long rateAt=System.nanoTime();
    public FrameAdmissionHandler(ProtocolValidator protocol,Executor cpu){this(protocol,cpu,SHARED);}
    public FrameAdmissionHandler(ProtocolValidator protocol,Executor cpu,GatewayIngressBudget budget){this.budget=Objects.requireNonNull(budget);this.protocol=Objects.requireNonNull(protocol);this.cpu=Objects.requireNonNull(cpu);}
    @Override public void channelRead(ChannelHandlerContext ctx,Object message){if(GatewayRejection.rejecting(ctx.channel())){ReferenceCountUtil.release(message);return;}if(!(message instanceof TextWebSocketFrame text)){if(message instanceof BinaryWebSocketFrame){ReferenceCountUtil.release(message);ctx.close();}else ctx.fireChannelRead(message);return;}
        try{long now=System.nanoTime();rateTokens=Math.min(60,rateTokens+(now-rateAt)*30.0/TimeUnit.SECONDS.toNanos(1));rateAt=now;if(rateTokens<1){ctx.close();return;}rateTokens--;int bytes=text.content().readableBytes();if(bytes<1||bytes>81920||inFlight>=64||retainedBytes+bytes>262144){ctx.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"OVERLOADED\"}"));return;}
            GatewayIngressBudget.Ticket credit;try{credit=budget.acquire(bytes);}catch(RejectedExecutionException full){ctx.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"OVERLOADED\"}"));return;}byte[] owned;try{owned=ByteBufUtil.getBytes(text.content());}catch(RuntimeException allocation){credit.close();throw allocation;}waiting.add(new Input(owned,System.nanoTime(),credit));retainedBytes+=bytes;inFlight++;start(ctx);
        }finally{text.release();}
    }
    private void start(ChannelHandlerContext ctx){if(running||waiting.isEmpty()||GatewayRejection.rejecting(ctx.channel()))return;Input input=waiting.remove();running=true;try{cpu.execute(()->{SignalEnvelope parsed=null;Throwable failure=null;try{if(System.nanoTime()-input.submitted()>TimeUnit.MILLISECONDS.toNanos(250))throw new RejectedExecutionException("INGRESS_QUEUE_EXPIRED");parsed=protocol.decodePublic(input.bytes());}catch(Throwable invalid){failure=invalid;}var value=parsed;var error=failure;var once=new AtomicBoolean();Runnable release=()->{if(once.compareAndSet(false,true)){retainedBytes-=input.bytes().length;inFlight--;input.credit().close();start(ctx);}};
            try{ctx.executor().execute(()->{running=false;if(!ctx.channel().isActive()){release.run();return;}if(error!=null){if(error instanceof ProtocolException)GatewayRejection.close(ctx.channel(),GatewayRejection.Reason.PROTOCOL_REJECTED);else ctx.close();release.run();return;}ctx.fireChannelRead(new Admitted(value,input.bytes().length,input.submitted(),release));start(ctx);});}catch(RejectedExecutionException closed){input.credit().close();}
        });}catch(RejectedExecutionException overloaded){retainedBytes-=input.bytes().length;inFlight--;input.credit().close();running=false;ctx.writeAndFlush(new TextWebSocketFrame("{\"v\":1,\"type\":\"OVERLOADED\"}"));start(ctx);}}
    @Override public void channelInactive(ChannelHandlerContext ctx){while(!waiting.isEmpty()){var input=waiting.remove();retainedBytes-=input.bytes().length;inFlight--;input.credit().close();}ctx.fireChannelInactive();}
    public int retainedBytes(){return retainedBytes;}
    @Override public void exceptionCaught(ChannelHandlerContext ctx,Throwable cause){ctx.close();}
}
