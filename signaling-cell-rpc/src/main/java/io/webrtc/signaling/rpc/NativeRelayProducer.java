package io.webrtc.signaling.rpc;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import io.webrtc.signaling.actors.admission.ActorOperation;
import io.webrtc.signaling.actors.relay.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.storage.AuthoritySql.GroupToken;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Install only behind RpcBusinessHandler's verified R1/full-envelope ingress. No durable relay journal. */
public final class NativeRelayProducer implements Function<RpcBusinessHandler.RelayRequest,RpcOperation<InternalReply>>,AutoCloseable {
    @FunctionalInterface public interface Network {RpcOperation<RelayWriteReceipt> send(RelayDestination destination,RelayDelivery delivery,Duration remainingBudget);}
    private static final ObjectMapper JSON=new ObjectMapper(JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(16).maxStringLength(81920).build()).build()).findAndRegisterModules().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final ProtocolValidator PROTOCOL=new ProtocolValidator(ProtocolLimits.v1());
    private static final class State {final NegotiationRelay.Grant grant;volatile RelayWriteReceipt offer,answer;State(NegotiationRelay.Grant grant){this.grant=grant;}}
    private final NativeRelayRoundCache rounds;private final Network network;private final int capacity;private final LongSupplier mono;private final NegotiationRelay sdp;private final Map<CallId,State> states=new ConcurrentHashMap<>();private volatile boolean closed;
    public NativeRelayProducer(NativeRelayRoundCache rounds,int capacity,LongSupplier mono,BooleanSupplier trusted,Function<CallId,Optional<GroupToken>> group,RelayBufferBudget memory,Network network){
        if(capacity<1||capacity>4096)throw new IllegalArgumentException("Invalid native relay capacity");this.rounds=Objects.requireNonNull(rounds);this.capacity=capacity;this.mono=Objects.requireNonNull(mono);this.network=Objects.requireNonNull(network);
        var authorization=new RelayAuthorizationCache(capacity,Math.min(capacity,64),mono,trusted,group);
        sdp=new NegotiationRelay(capacity,mono,memory,authorization,new NegotiationRelay.NativeAuthorization(){
            @Override public ActorOperation<RelayAuthorizationCache.Snapshot> load(CallId c,AuthenticatedSession sender,long r,long i,Duration budget){return deniedActor(new IllegalStateException("Original descriptor required"));}
            @Override public ActorOperation<RelayAuthorizationCache.Snapshot> load(NegotiationRelay.Description description,Duration budget){var round=rounds.cached(description.original());return round.isPresent()?new ActorOperation<>(CompletableFuture.completedFuture(round.get().authorization()),CompletableFuture.completedFuture(null)):deniedActor(new IllegalStateException("RESYNC_REQUIRED"));}
        },new NegotiationRelay.Transport(){
            @Override public ActorOperation<Void> send(NegotiationRelay.Description description){return deniedActor(new IllegalStateException("Original budget required"));}
            @Override public ActorOperation<Void> send(NegotiationRelay.Description description,Duration remaining){
                var current=rounds.cached(description.original());if(current.isEmpty())return deniedActor(new IllegalStateException("RESYNC_REQUIRED"));
                final State state=states.get(description.call());if(closed||state==null||!sameScope(state.grant,current.get().grant()))return deniedActor(new IllegalStateException("RESYNC_REQUIRED"));
                var command=description.original();var round=current.get();var delivery=new RelayDelivery(command,round.authorization().recipient(),round.authorization().callVersion(),round.authorizationUntil());
                var original=network.send(round.destination(),delivery,remaining);
                var logical=original.logical().thenApply(receipt->{validate(receipt,delivery);if(description.kind()==NegotiationRelay.Kind.OFFER)state.offer=receipt;else state.answer=receipt;return (Void)null;});
                return new ActorOperation<>(logical,original.physicalCompletion().thenApply(v->(Void)null));
            }
        });
    }
    @Override public RpcOperation<InternalReply> apply(RpcBusinessHandler.RelayRequest request){
        var scope=new PhysicalScope();final long end;final RpcBusinessHandler.CallPayload payload;
        try{
            Objects.requireNonNull(request);var wire=request.command();var peer=request.peer();
            if(request.budget()==null||request.budget().isNegative()||request.budget().isZero()||wire.getRemainingBudgetMs()<=0||wire.getSerializedSize()>98304||wire.getPayload().size()>81920||peer==null||peer.workloadId().isBlank()||!Set.of("actor","gateway").contains(peer.role()))throw new IllegalArgumentException();
            end=System.nanoTime()+Math.min(Duration.ofSeconds(1).toNanos(),Math.min(request.budget().toNanos(),TimeUnit.MILLISECONDS.toNanos(Math.min(1000,wire.getRemainingBudgetMs()))));
            payload=JSON.readValue(wire.getPayload().toByteArray(),RpcBusinessHandler.CallPayload.class);var command=payload.command();
            if(!RelaySessionAuthorizationProof.supports(command)||!wire.getOperationId().equals(command.requestId().value().toString())||!wire.getCallId().equals(command.callId().value())||!wire.getType().equals(command.type().name())||!wire.getDestinationCell().equals(command.callId().coordinatorCell())||!wire.getCommandScope().equals(command.scope().value())||!wire.getPayloadHash().equals(com.google.protobuf.ByteString.copyFrom(HexFormat.of().parseHex(command.intentHash()))))throw new IllegalArgumentException();
            var checked=PROTOCOL.bind(new SignalEnvelope(1,command.type(),command.requestId(),command.callId(),command.negotiationId(),command.iceGeneration(),command.payloadJson()),command.sender());if(!checked.equals(command))throw new IllegalArgumentException();
            synchronized(this){if(closed)throw new IllegalStateException("Relay producer draining");}
        }catch(Exception invalid){return scope.seal(CompletableFuture.completedFuture(error(request,"RESYNC_REQUIRED")));}
        var command=payload.command();
        var logical=invoke(scope,()->rounds.load(command,payload.proof(),remaining(end))).thenCompose(round->{
            if(command.type()==SignalEnvelope.Type.OFFER||command.type()==SignalEnvelope.Type.ANSWER){
                final State state=state(round);final NegotiationRelay.Kind kind=command.type()==SignalEnvelope.Type.OFFER?NegotiationRelay.Kind.OFFER:NegotiationRelay.Kind.ANSWER;
                final String body;try{body=JSON.readTree(command.payloadJson()).path("sdp").textValue();}catch(Exception invalid){return CompletableFuture.failedFuture(invalid);}
                var description=new NegotiationRelay.Description(command.callId(),command.negotiationId().value(),command.iceGeneration().value(),command.sender(),command.requestId().value(),kind,body,command);
                return invoke(scope,()->{var work=sdp.sendTracked(description,remaining(end));return new RpcOperation<>(work.logical(),work.physicalCompletion());}).thenApply(v->{var receipt=kind==NegotiationRelay.Kind.OFFER?state.offer:state.answer;if(receipt==null||!receipt.matches(command))throw new IllegalStateException("RESYNC_REQUIRED");return reply(receipt);});
            }
            var current=rounds.cached(command).orElseThrow(()->new IllegalStateException("RESYNC_REQUIRED"));var delivery=new RelayDelivery(command,current.authorization().recipient(),current.authorization().callVersion(),current.authorizationUntil());
            return invoke(scope,()->network.send(current.destination(),delivery,remaining(end))).thenApply(receipt->{validate(receipt,delivery);return reply(receipt);});
        }).handle((reply,failure)->failure==null?reply:error(request,"RESYNC_REQUIRED"));
        return scope.seal(logical.toCompletableFuture().orTimeout(Math.max(1,end-System.nanoTime()),TimeUnit.NANOSECONDS).exceptionally(e->error(request,"RESYNC_REQUIRED")));
    }
    private synchronized State state(NativeRelayAuthorization.AuthorizedRound round){
        if(closed||mono.getAsLong()-round.grant().untilNanos()>=0)throw new IllegalStateException("RESYNC_REQUIRED");
        for(var entry:states.entrySet().stream().filter(e->mono.getAsLong()-e.getValue().grant.untilNanos()>=0).toList()){states.remove(entry.getKey());sdp.recoverVolatileLoss(entry.getKey());}
        var existing=states.get(round.grant().call());if(existing!=null){if(sameScope(existing.grant,round.grant()))return existing;if(round.grant().negotiationId()<=existing.grant.negotiationId()||round.grant().iceGeneration()<=existing.grant.iceGeneration())throw new IllegalStateException("RESYNC_REQUIRED");sdp.recoverVolatileLoss(round.grant().call());states.remove(round.grant().call());}
        if(states.size()>=capacity||!sdp.install(round.grant()))throw new IllegalStateException("RESYNC_REQUIRED");var state=new State(round.grant());states.put(round.grant().call(),state);return state;
    }
    private static boolean sameScope(NegotiationRelay.Grant a,NegotiationRelay.Grant b){return a.call().equals(b.call())&&a.activationId().equals(b.activationId())&&a.negotiationId()==b.negotiationId()&&a.iceGeneration()==b.iceGeneration()&&a.offerer().equals(b.offerer())&&a.answerer().equals(b.answerer())&&a.group().equals(b.group());}
    private static <T> CompletionStage<T> invoke(PhysicalScope scope,Supplier<RpcOperation<T>> factory){var receipt=new CompletableFuture<Void>();scope.track(new RpcOperation<>(CompletableFuture.completedFuture(null),receipt));try{var original=Objects.requireNonNull(factory.get());original.physicalCompletion().whenComplete((v,e)->{if(e==null)receipt.complete(null);});return original.logical();}catch(Throwable unknown){return CompletableFuture.failedFuture(unknown);}}
    private static void validate(RelayWriteReceipt receipt,RelayDelivery delivery){if(receipt==null||!receipt.matches(delivery.command())||receipt.callVersion()!=delivery.callVersion())throw new IllegalStateException("RESYNC_REQUIRED");}
    private static InternalReply reply(RelayWriteReceipt receipt){return InternalReply.newBuilder().setOperationId(receipt.request().value().toString()).setCallId(receipt.call().value()).setCallVersion(receipt.callVersion()).setStatus("WRITE_COMPLETED").setResult(com.google.protobuf.ByteString.copyFrom(RpcBusinessHandler.encode(receipt))).build();}
    private static InternalReply error(RpcBusinessHandler.RelayRequest request,String code){return InternalReply.newBuilder().setOperationId(request==null?"":request.command().getOperationId()).setCallId(request==null?"":request.command().getCallId()).setStatus("REJECTED").setErrorCode(code).build();}
    private static Duration remaining(long end){long nanos=end-System.nanoTime();if(nanos<=0)throw new CompletionException(new TimeoutException());return Duration.ofNanos(nanos);}
    private static <T> ActorOperation<T> deniedActor(Throwable error){return new ActorOperation<>(CompletableFuture.failedFuture(error),CompletableFuture.completedFuture(null));}
    public void invalidate(CallId call){synchronized(this){states.remove(call);}rounds.invalidate(call);sdp.recoverVolatileLoss(call);}
    @Override public void close(){synchronized(this){closed=true;states.clear();}sdp.close();rounds.drain();}
}
