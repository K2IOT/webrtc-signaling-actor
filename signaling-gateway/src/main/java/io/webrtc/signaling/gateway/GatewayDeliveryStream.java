package io.webrtc.signaling.gateway;
import io.webrtc.signaling.auth.AuthorizationStatus;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.*;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
/** Destination boundary behind authenticated delivery ingress. Write receipt is distinct from application receipt/COMMIT. */
public final class GatewayDeliveryStream {
    private static final ObjectMapper JSON=new ObjectMapper(JsonFactory.builder().streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(16).maxStringLength(8192).build()).enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build());
    private final ConnectionRegistry registry;private final GatewayServices services;private final Clock clock;private final Executor cpu;private final ConcurrentHashMap<UUID,Integer> pending=new ConcurrentHashMap<>();
    public GatewayDeliveryStream(ConnectionRegistry registry,GatewayServices services,Clock clock,Executor cpu){this.registry=Objects.requireNonNull(registry);this.services=Objects.requireNonNull(services);this.clock=Objects.requireNonNull(clock);this.cpu=Objects.requireNonNull(cpu);}
    public CompletionStage<InternalReply> send(ControlEvent event){var result=new CompletableFuture<InternalReply>();long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);try{
        UUID.fromString(event.getEventId());new CallId(event.getCallId());var destination=event.getDestination();new UserId(destination.getUserId());new SessionKey(destination.getIssuer(),destination.getJti());UUID connection=uuid(destination.getConnectionId());uuid(destination.getIncarnation());
        if(event.getCallVersion()<1||event.getAuthorityBucketId()<0||event.getAuthorityBucketId()>16383||!event.getType().matches("[A-Z_]{1,64}")||event.getMetadata().size()>8192||destination.getConnectionGeneration()<1)return CompletableFuture.completedFuture(reply(event,"REJECTED","INVALID_MESSAGE"));
        var binding=registry.binding(connection);if(!current(binding,event))return CompletableFuture.completedFuture(reply(event,"REJECTED","STALE_BINDING"));var admitted=new java.util.concurrent.atomic.AtomicBoolean();pending.compute(connection,(key,count)->{int existing=count==null?0:count;if(existing>=8)return existing;admitted.set(true);return existing+1;});if(!admitted.get())return CompletableFuture.completedFuture(reply(event,"REJECTED","OVERLOADED"));
        result.whenComplete((done,error)->pending.computeIfPresent(connection,(key,count)->count<=1?null:count-1));
        try{cpu.execute(()->{try{if(System.nanoTime()-end>=0){result.complete(reply(event,"REJECTED","OUTCOME_UNKNOWN"));return;}var payload=JSON.readTree(event.getMetadata().toByteArray());String encoded=JSON.writeValueAsString(Map.of("v",1,"type",event.getType(),"eventId",event.getEventId(),"callId",event.getCallId(),"callVersion",Long.toString(event.getCallVersion()),"sessionIncarnation",binding.route().incarnation().value().toString(),"connectionGeneration",Long.toString(binding.route().connectionGeneration()),"payload",payload));
            binding.channel().eventLoop().execute(()->{if(System.nanoTime()-end>=0||!current(binding,event)){result.complete(reply(event,"REJECTED","STALE_BINDING"));return;}binding.channel().writeAndFlush(new TextWebSocketFrame(encoded)).addListener(write->result.complete(write.isSuccess()?reply(event,"WRITE_COMPLETED",""):reply(event,"REJECTED","RESYNC_REQUIRED")));});
        }catch(Exception invalid){result.complete(reply(event,"REJECTED","INVALID_MESSAGE"));}});}catch(RejectedExecutionException full){result.complete(reply(event,"REJECTED","OVERLOADED"));}
    }catch(RuntimeException invalid){result.complete(reply(event,"REJECTED","INVALID_MESSAGE"));}return result.minimalCompletionStage();}
    private boolean current(ConnectionRegistry.Binding binding,ControlEvent event){if(binding==null||!services.currentBoot()||services.cachedSecurity(binding.principal(),clock.instant())!=AuthorizationStatus.ALLOWED)return false;var route=binding.route();var destination=event.getDestination();return registry.current(route.connectionId(),route)&&route.user().value().equals(destination.getUserId())&&route.key().issuer().equals(destination.getIssuer())&&route.key().jti().equals(destination.getJti())&&route.incarnation().value().equals(uuid(destination.getIncarnation()))&&route.connectionGeneration()==destination.getConnectionGeneration()&&route.connectionId().equals(uuid(destination.getConnectionId()));}
    private static UUID uuid(ByteString bytes){if(bytes.size()!=16)throw new IllegalArgumentException("Invalid delivery identity");ByteBuffer buffer=bytes.asReadOnlyByteBuffer();return new UUID(buffer.getLong(),buffer.getLong());}
    private static InternalReply reply(ControlEvent event,String status,String error){return InternalReply.newBuilder().setOperationId(event.getEventId()).setCallId(event.getCallId()).setCallVersion(event.getCallVersion()).setStatus(status).setErrorCode(error).build();}
}
