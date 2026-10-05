package io.webrtc.signaling.actors.relay;

import io.webrtc.signaling.actors.admission.ActorOperation;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.AuthoritySql.GroupToken;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.LongSupplier;

/** Volatile SDP retry lane. Grants must come from committed native negotiation metadata. */
public final class NegotiationRelay implements AutoCloseable {
    public enum Kind {OFFER,ANSWER}
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON=new com.fasterxml.jackson.databind.ObjectMapper(com.fasterxml.jackson.core.JsonFactory.builder().enable(com.fasterxml.jackson.core.StreamReadFeature.STRICT_DUPLICATE_DETECTION).streamReadConstraints(com.fasterxml.jackson.core.StreamReadConstraints.builder().maxNestingDepth(8).maxStringLength(65536).build()).build()).enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    public record Grant(CallId call,UUID activationId,long callVersion,long negotiationId,long iceGeneration,
            AuthenticatedSession offerer,AuthenticatedSession answerer,long untilNanos,GroupToken group) {
        public Grant {Objects.requireNonNull(call);Objects.requireNonNull(activationId);Objects.requireNonNull(offerer);Objects.requireNonNull(answerer);Objects.requireNonNull(group);
            if(callVersion<1||negotiationId<1||iceGeneration<1||offerer.equals(answerer))throw new IllegalArgumentException("Invalid round grant");}
    }
    public record Description(CallId call,long negotiationId,long iceGeneration,AuthenticatedSession sender,
            UUID requestId,Kind kind,String body,io.webrtc.signaling.protocol.CallCommand original) {
        public Description(CallId call,long negotiationId,long iceGeneration,AuthenticatedSession sender,UUID requestId,Kind kind,String body){this(call,negotiationId,iceGeneration,sender,requestId,kind,body,null);}
        public Description {Objects.requireNonNull(call);Objects.requireNonNull(sender);Objects.requireNonNull(requestId);Objects.requireNonNull(kind);Objects.requireNonNull(body);
            if(negotiationId<1||iceGeneration<1||body.isBlank()||body.getBytes(StandardCharsets.UTF_8).length>65536
                    ||body.codePoints().anyMatch(c->c>=0xd800&&c<=0xdfff))throw new IllegalArgumentException("Invalid bounded description");
            if(original!=null){
                if(!call.equals(original.callId())||!sender.equals(original.sender())||!requestId.equals(original.requestId().value())||!kind.name().equals(original.type().name())||!CommandScope.call(call).equals(original.scope())||original.target()!=null||original.negotiationId()==null||original.negotiationId().value()!=negotiationId||original.iceGeneration()==null||original.iceGeneration().value()!=iceGeneration||!original.intentHash().matches("[0-9a-f]{64}")||original.payloadJson().getBytes(StandardCharsets.UTF_8).length>81920)throw new IllegalArgumentException("Native description binding mismatch");
                try{var payload=JSON.readTree(original.payloadJson());if(payload==null||!payload.isObject()||payload.size()!=1||!payload.path("sdp").isTextual()||!body.equals(payload.path("sdp").textValue()))throw new IllegalArgumentException("Native description payload mismatch");}catch(java.io.IOException invalid){throw new IllegalArgumentException("Invalid native description payload",invalid);}
            }
        }
        @Override public String toString(){return "Description[kind="+kind+", negotiationId="+negotiationId+"]";}
    }
    @FunctionalInterface public interface NativeAuthorization {
        ActorOperation<RelayAuthorizationCache.Snapshot> load(CallId call,AuthenticatedSession sender,long round,long ice,Duration budget);
        default ActorOperation<RelayAuthorizationCache.Snapshot> load(Description description,Duration budget){return load(description.call(),description.sender(),description.negotiationId(),description.iceGeneration(),budget);}
    }
    @FunctionalInterface public interface Transport {ActorOperation<Void> send(Description message);default ActorOperation<Void> send(Description message,Duration remainingBudget){return send(message);}}
    private static final class Retained {
        final Description message;final RelayBufferBudget.Ticket credit;
        CompletableFuture<Void> result;CompletionStage<Void> physical;boolean physicalPending,closed;
        Retained(Description message,RelayBufferBudget.Ticket credit){this.message=message;this.credit=credit;}
        void close(){closed=true;if(!physicalPending)credit.close();}
    }
    private static final class Lane {
        final Grant grant;final long created;Retained offer,answer;boolean closed;ScheduledFuture<?> expiry;
        Lane(Grant grant,long created){this.grant=grant;this.created=created;}
        void close(){closed=true;if(expiry!=null)expiry.cancel(false);if(offer!=null)offer.close();if(answer!=null)answer.close();}
    }
    private final int capacity;private final LongSupplier clock;private final RelayBufferBudget memory;
    private final RelayAuthorizationCache cache;private final NativeAuthorization authority;private final Transport transport;
    private final Map<CallId,Lane> lanes=new HashMap<>();private boolean closed;
    public NegotiationRelay(int capacity,LongSupplier clock,RelayBufferBudget memory,
            RelayAuthorizationCache cache,NativeAuthorization authority,Transport transport){
        if(capacity<1)throw new IllegalArgumentException("Invalid relay capacity");this.capacity=capacity;
        this.clock=Objects.requireNonNull(clock);this.memory=Objects.requireNonNull(memory);this.cache=Objects.requireNonNull(cache);
        this.authority=Objects.requireNonNull(authority);this.transport=Objects.requireNonNull(transport);
    }
    public synchronized boolean install(Grant grant){
        if(closed||clock.getAsLong()-grant.untilNanos()>=0)return false;
        expire();var previous=lanes.get(grant.call());
        if(previous!=null){if(previous.grant.equals(grant))return true;
            if(grant.negotiationId()<=previous.grant.negotiationId()||grant.iceGeneration()<=previous.grant.iceGeneration()
                    ||previous.answer==null||previous.answer.result==null||!successful(previous.answer.result))return false;
            previous.close();cache.invalidate(grant.call());
        }else if(lanes.size()>=capacity)return false;
        lanes.put(grant.call(),new Lane(grant,clock.getAsLong()));return true;
    }
    public CompletionStage<Void> send(Description message,Duration budget){return sendTracked(message,budget).logical();}
    public synchronized ActorOperation<Void> sendTracked(Description message,Duration budget){
        if(budget.isZero()||budget.isNegative()||budget.compareTo(Duration.ofSeconds(2))>0)
            return rejected(new IllegalArgumentException("Invalid relay budget"));
        expire();var lane=lanes.get(message.call());
        if(closed||lane==null||lane.grant.negotiationId()!=message.negotiationId()
                ||lane.grant.iceGeneration()!=message.iceGeneration()
                ||!message.sender().equals(message.kind()==Kind.OFFER?lane.grant.offerer():lane.grant.answerer())
                ||message.kind()==Kind.ANSWER&&(lane.offer==null||lane.offer.result==null||!successful(lane.offer.result)))
            return rejected(new IllegalStateException("RESYNC_REQUIRED"));
        Retained retained=message.kind()==Kind.OFFER?lane.offer:lane.answer;
        if(retained!=null){if(!retained.message.equals(message))return rejected(new IllegalArgumentException("SDP retry identity conflict"));
            if(retained.physicalPending||successful(retained.result))return view(retained,budget);
        }else{
            try{retained=new Retained(message,memory.acquire(256+message.body().getBytes(StandardCharsets.UTF_8).length+(message.original()==null?0:message.original().payloadJson().getBytes(StandardCharsets.UTF_8).length+64)));}
            catch(RuntimeException overloaded){return rejected(overloaded);}
            if(message.kind()==Kind.OFFER)lane.offer=retained;else lane.answer=retained;
            if(lane.expiry==null){long delay=Math.min(lane.grant.untilNanos()-clock.getAsLong(),Duration.ofSeconds(30).toNanos()-(clock.getAsLong()-lane.created));
                lane.expiry=RelayExpiry.schedule(()->expireLane(lane),delay);}

        }
        final Retained attempt=retained;var result=new CompletableFuture<Void>();attempt.result=result;
        var scope=new ReceiptScope();attempt.physical=scope.completion.minimalCompletionStage();attempt.physicalPending=true;
        scope.completion.whenComplete((v,e)->{if(e==null)synchronized(this){attempt.physicalPending=false;if(attempt.closed)attempt.credit.close();}});
        result.whenComplete((v,e)->{synchronized(this){scope.release();}});
        long started=clock.getAsLong();
        var authorization=cache.refreshTracked(message.call(),message.sender(),message.negotiationId(),message.iceGeneration(),budget,
            ()->authority.load(message,budget));
        scope.track(authorization.physicalCompletion());
        authorization.logical().whenComplete((snapshot,error)->{
                synchronized(this){
                    if(error!=null){result.completeExceptionally(error);return;}
                    long remaining=budget.toNanos()-(clock.getAsLong()-started);
                    if(result.isDone()||lane.closed||attempt.closed||expired(lane)||remaining<=0||!snapshot.group().equals(lane.grant.group())||!snapshot.activationId().equals(lane.grant.activationId())||snapshot.callVersion()!=lane.grant.callVersion()
                            ||!snapshot.recipient().equals(message.kind()==Kind.OFFER?lane.grant.answerer():lane.grant.offerer())){
                        result.completeExceptionally(new IllegalStateException("RESYNC_REQUIRED"));return;
                    }
                    var originalWriteReceipt=new CompletableFuture<Void>();scope.track(originalWriteReceipt);
                    final ActorOperation<Void> operation;
                    try{operation=Objects.requireNonNull(transport.send(message,Duration.ofNanos(remaining)));}
                    catch(Throwable failure){result.completeExceptionally(failure);return;}
                    result.orTimeout(remaining,TimeUnit.NANOSECONDS);
                    operation.logical().whenComplete((ignored,failure)->{if(failure==null)result.complete(null);else result.completeExceptionally(failure);});
                    operation.physicalCompletion().whenComplete((ignored,failure)->{if(failure==null)originalWriteReceipt.complete(null);});
                }
            });
        return view(attempt,budget);
    }
    private static final class ReceiptScope {
        private int retained=1;final CompletableFuture<Void> completion=new CompletableFuture<>();
        synchronized void track(CompletionStage<Void> original){if(completion.isDone())throw new IllegalStateException("Relay receipt scope already closed");retained++;original.whenComplete((v,e)->{if(e==null)release();});}
        synchronized void release(){if(--retained==0)completion.complete(null);}
    }
    private static ActorOperation<Void> rejected(Throwable error){return new ActorOperation<>(CompletableFuture.failedFuture(error),CompletableFuture.completedFuture(null));}
    private static ActorOperation<Void> view(Retained attempt,Duration budget){return new ActorOperation<>(attempt.result.thenApply(v->v).orTimeout(budget.toNanos(),TimeUnit.NANOSECONDS).minimalCompletionStage(),attempt.physical);}
    private static boolean successful(CompletableFuture<?> stage){return stage!=null&&stage.isDone()&&!stage.isCompletedExceptionally()&&!stage.isCancelled();}
    private boolean expired(Lane lane){long now=clock.getAsLong();return now-lane.grant.untilNanos()>=0||now-lane.created>=Duration.ofSeconds(30).toNanos();}
    private synchronized void expireLane(Lane lane){if(lanes.get(lane.grant.call())==lane&&expired(lane)){lanes.remove(lane.grant.call());lane.close();cache.invalidate(lane.grant.call());}}
    public synchronized void expire(){var iterator=lanes.values().iterator();while(iterator.hasNext()){var lane=iterator.next();if(expired(lane)){lane.close();cache.invalidate(lane.grant.call());iterator.remove();}}}
    public synchronized void recoverVolatileLoss(CallId call){var lane=lanes.remove(call);if(lane!=null)lane.close();cache.invalidate(call);}
    @Override public synchronized void close(){closed=true;lanes.values().forEach(Lane::close);lanes.clear();}
}
