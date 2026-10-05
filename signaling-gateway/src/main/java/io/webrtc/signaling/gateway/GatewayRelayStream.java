package io.webrtc.signaling.gateway;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import io.netty.util.ReferenceCountUtil;
import io.webrtc.signaling.auth.AuthorizationStatus;
import io.webrtc.signaling.protocol.Identity.AuthenticatedSession;
import io.webrtc.signaling.rpc.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

/** Dedicated volatile lane. Cached local binding checks and original Netty receipts; no SQL or journal. */
public final class GatewayRelayStream {
    private static final ObjectMapper JSON=new ObjectMapper(JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(16).maxStringLength(65536).build()).build()).findAndRegisterModules().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private final ConnectionRegistry registry;private final GatewayServices services;private final Clock clock;private final Executor cpu;private final RpcAdmission admission;private final java.util.function.BooleanSupplier trustedClock;
    public GatewayRelayStream(ConnectionRegistry registry,GatewayServices services,Clock clock,Executor cpu,RpcAdmission admission,java.util.function.BooleanSupplier trustedClock){this.registry=Objects.requireNonNull(registry);this.services=Objects.requireNonNull(services);this.clock=Objects.requireNonNull(clock);this.cpu=Objects.requireNonNull(cpu);this.admission=Objects.requireNonNull(admission);this.trustedClock=Objects.requireNonNull(trustedClock);}
    public RpcOperation<RelayWriteReceipt> send(RelayDelivery delivery,CellRpcServer.Peer peer,Duration budget){
        final RpcAdmission.Ticket credit;final ConnectionRegistry.Binding binding;final long end;
        try{
            Objects.requireNonNull(delivery);Objects.requireNonNull(peer);
            if(budget==null||budget.isNegative()||budget.isZero()||budget.compareTo(Duration.ofSeconds(1))>0)throw new IllegalArgumentException("Invalid relay deadline");
            if(!peer.role().equals("actor")||peer.workloadId().isBlank()||!peer.cell().equals(delivery.command().callId().coordinatorCell()))throw new IllegalArgumentException("UNAUTHORIZED");
            end=System.nanoTime()+budget.toNanos();binding=registry.binding(delivery.recipient().connectionId());
            if(!authorized(delivery)||!current(binding,delivery.recipient())||binding.channel().pipeline().get(OutboundAdmissionHandler.class)==null)throw new IllegalStateException("STALE_BINDING");
            credit=admission.acquire(RpcAdmission.Lane.RELAY,8192+delivery.command().payloadJson().getBytes(StandardCharsets.UTF_8).length);
        }catch(RuntimeException invalid){return rejected(invalid);}
        var result=new CompletableFuture<RelayWriteReceipt>();var physical=new CompletableFuture<Void>();var scope=new ReceiptScope();
        scope.done.whenComplete((v,e)->{credit.close();physical.complete(null);});result.whenComplete((v,e)->scope.release());
        result.orTimeout(Math.max(1,end-System.nanoTime()),TimeUnit.NANOSECONDS);
        try{cpu.execute(()->{
            try{
                if(result.isDone()||System.nanoTime()-end>=0){result.completeExceptionally(new TimeoutException("Original relay deadline"));return;}
                var payload=JSON.readTree(delivery.command().payloadJson());if(payload==null||!payload.isObject())throw new IllegalArgumentException("Invalid relay payload");
                var command=delivery.command();var root=JSON.createObjectNode().put("v",1).put("type",command.type().name()).put("requestId",command.requestId().value().toString()).put("callId",command.callId().value()).put("callVersion",Long.toString(delivery.callVersion())).put("negotiationId",Long.toString(command.negotiationId().value())).put("iceGeneration",Long.toString(command.iceGeneration().value())).put("sessionIncarnation",delivery.recipient().incarnation().value().toString()).put("connectionGeneration",Long.toString(delivery.recipient().connectionGeneration()));
                root.set("sender",JSON.valueToTree(command.sender()));root.set("payload",payload);String encoded=root.toString();if(encoded.getBytes(StandardCharsets.UTF_8).length>81920)throw new IllegalArgumentException("Relay frame exceeds wire bound");
                var frame=new OutboundAdmissionHandler.RelayFrame(encoded);var originalWrite=new CompletableFuture<Void>();scope.track(originalWrite);
                Runnable write=()->{
                    final boolean allowed;
                    try{allowed=!result.isDone()&&System.nanoTime()-end<0&&authorized(delivery)&&current(binding,delivery.recipient());}
                    catch(RuntimeException unavailable){ReferenceCountUtil.release(frame);originalWrite.complete(null);result.completeExceptionally(unavailable);return;}
                    if(!allowed){ReferenceCountUtil.release(frame);originalWrite.complete(null);result.completeExceptionally(new IllegalStateException("RESYNC_REQUIRED"));return;}
                    frame.physicalCompletion().whenComplete((v,e)->{if(e==null)originalWrite.complete(null);});
                    try{binding.channel().writeAndFlush(frame).addListener(done->{if(done.isSuccess())result.complete(new RelayWriteReceipt(command.callId(),command.requestId(),command.type(),delivery.callVersion(),command.negotiationId().value(),command.iceGeneration().value()));else result.completeExceptionally(new IllegalStateException("RESYNC_REQUIRED"));});}
                    catch(Throwable unknown){result.completeExceptionally(unknown);}
                };
                try{if(binding.channel().eventLoop().inEventLoop())write.run();else binding.channel().eventLoop().execute(write);}
                catch(RejectedExecutionException notStarted){ReferenceCountUtil.release(frame);originalWrite.complete(null);result.completeExceptionally(notStarted);}
                catch(Throwable unknown){result.completeExceptionally(unknown);}
            }catch(Throwable failure){result.completeExceptionally(failure);}
            finally{scope.release();}
        });}catch(RejectedExecutionException notStarted){result.completeExceptionally(notStarted);scope.release();}
        catch(Throwable unknown){result.completeExceptionally(unknown);}
        return new RpcOperation<>(result.minimalCompletionStage(),physical.minimalCompletionStage());
    }
    private boolean authorized(RelayDelivery delivery){
        if(!trustedClock.getAsBoolean())return false;var now=clock.instant();
        // Original source expiry, with the enrolled pair uncertainty/drift margin; never a new receiver TTL.
        return delivery.authorizationUntil().isAfter(now.plusMillis(255))&&!delivery.authorizationUntil().isAfter(now.plusMillis(5250));
    }
    private boolean current(ConnectionRegistry.Binding binding,AuthenticatedSession recipient){
        if(binding==null||!services.currentBoot()||!clock.instant().isBefore(binding.principal().expiresAt())||services.cachedSecurity(binding.principal(),clock.instant())!=AuthorizationStatus.ALLOWED)return false;
        var route=binding.route();return registry.current(route.connectionId(),route)&&route.user().equals(recipient.userId())&&route.key().equals(recipient.key())&&route.incarnation().equals(recipient.incarnation())&&route.connectionGeneration()==recipient.connectionGeneration()&&route.connectionId().equals(recipient.connectionId());
    }
    public CompletionStage<Void> drain(){return admission.drain();}
    private static RpcOperation<RelayWriteReceipt> rejected(Throwable error){return new RpcOperation<>(CompletableFuture.failedFuture(error),CompletableFuture.completedFuture(null));}
    private static final class ReceiptScope {
        private int retained=2;final CompletableFuture<Void> done=new CompletableFuture<>();
        synchronized void track(CompletionStage<Void> original){retained++;original.whenComplete((v,e)->{if(e==null)release();});}
        synchronized void release(){if(--retained==0)done.complete(null);}
    }
}
