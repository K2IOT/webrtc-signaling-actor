package io.webrtc.signaling.rpc;

import io.webrtc.signaling.actors.admission.ActorOperation;
import io.webrtc.signaling.actors.relay.RelayAuthorizationCache;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.AuthoritySql.GroupToken;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Bounded native round metadata coupled to the original authorization cache, never a new authority TTL. */
public final class NativeRelayRoundCache implements AutoCloseable {
    @FunctionalInterface interface Loader {RpcOperation<NativeRelayAuthorization.AuthorizedRound> load(CallCommand command,String proof,Duration budget);}
    private record Key(CallId call,AuthenticatedSession sender,long round,long ice){}
    private static final class Flight {boolean invalidated;}
    private final Object lock=new Object();private final int capacity;private final LongSupplier mono;private final Loader loader;private final RelayAuthorizationCache authorization;
    private final LinkedHashMap<Key,NativeRelayAuthorization.AuthorizedRound> rounds=new LinkedHashMap<>(16,.75f,true);
    private final Map<Key,Flight> pending=new HashMap<>();private boolean closed;private final CompletableFuture<Void> drained=new CompletableFuture<>();
    public NativeRelayRoundCache(int capacity,int maximumPending,NativeRelayAuthorization source,LongSupplier mono,BooleanSupplier trusted,Function<CallId,Optional<GroupToken>> group){this(capacity,maximumPending,Objects.requireNonNull(source)::loadRound,mono,trusted,group);}
    NativeRelayRoundCache(int capacity,int maximumPending,Loader loader,LongSupplier mono,BooleanSupplier trusted,Function<CallId,Optional<GroupToken>> group){
        if(capacity<1||capacity>4096)throw new IllegalArgumentException("Invalid native round capacity");this.capacity=capacity;this.mono=Objects.requireNonNull(mono);this.loader=Objects.requireNonNull(loader);authorization=new RelayAuthorizationCache(capacity,maximumPending,mono,trusted,group);
    }
    /** Caller must first verify the incoming R1 proof and exact envelope; cache hits do not reverify signatures. */
    public RpcOperation<NativeRelayAuthorization.AuthorizedRound> load(CallCommand command,String proof,Duration budget){
        final Key key;
        try{if(!RelaySessionAuthorizationProof.supports(command)||proof==null||proof.length()>8192)throw new IllegalArgumentException("Invalid relay cache input");key=new Key(command.callId(),command.sender(),command.negotiationId().value(),command.iceGeneration().value());synchronized(lock){if(closed)throw new IllegalStateException("Native relay cache draining");}}
        catch(RuntimeException invalid){return rejected(invalid);}
        synchronized(lock){var previous=rounds.get(key);if(previous!=null&&mono.getAsLong()-previous.grant().untilNanos()>=0)return rejected(new IllegalStateException("RESYNC_REQUIRED"));}
        // A metadata eviction cannot authorize reuse of the remaining bare snapshot.
        var hit=authorization.get(key.call(),key.sender(),key.round(),key.ice());
        if(hit.isPresent()){boolean missing;synchronized(lock){var original=rounds.get(key);missing=original==null||!original.authorization().equals(hit.get());}if(missing)invalidate(key.call());}
        var own=new java.util.concurrent.atomic.AtomicReference<Flight>();
        var original=authorization.refreshTracked(key.call(),key.sender(),key.round(),key.ice(),budget,()->{
            final Flight flight=new Flight();synchronized(lock){if(closed)return new ActorOperation<>(CompletableFuture.failedFuture(new IllegalStateException("Native relay cache draining")),CompletableFuture.completedFuture(null));pending.put(key,flight);own.set(flight);}
            var source=Objects.requireNonNull(loader.load(command,proof,budget));
            var logical=source.logical().thenApply(round->{
                synchronized(lock){if(closed||flight.invalidated)throw new IllegalStateException("Invalidated original native round");rounds.put(key,Objects.requireNonNull(round));while(rounds.size()>capacity)rounds.remove(rounds.keySet().iterator().next());}
                return round.authorization();
            });
            return new ActorOperation<>(logical,source.physicalCompletion().thenApply(v->(Void)null));
        });
        var flight=own.get();if(flight!=null)original.physicalCompletion().whenComplete((v,e)->{if(e==null)synchronized(lock){pending.remove(key,flight);if(closed&&pending.isEmpty())drained.complete(null);}});
        var logical=original.logical().thenApply(snapshot->{synchronized(lock){var round=rounds.get(key);if(closed||round==null||mono.getAsLong()-round.grant().untilNanos()>=0||!round.authorization().equals(snapshot))throw new IllegalStateException("Native relay metadata unavailable");if(command.type()==SignalEnvelope.Type.OFFER&&!command.sender().equals(round.grant().offerer())||command.type()==SignalEnvelope.Type.ANSWER&&!command.sender().equals(round.grant().answerer()))throw new IllegalStateException("RESYNC_REQUIRED");return round;}});
        return new RpcOperation<>(logical,original.physicalCompletion().thenCombine(logical.handle((v,e)->null),(a,b)->null));
    }
    public void invalidate(CallId call){synchronized(lock){rounds.keySet().removeIf(k->k.call().equals(call));pending.forEach((key,flight)->{if(key.call().equals(call))flight.invalidated=true;});}authorization.invalidate(call);}
    public int size(){synchronized(lock){return rounds.size();}}
    public int pending(){synchronized(lock){return pending.size();}}
    public CompletionStage<Void> drain(){
        final Set<CallId> calls=new HashSet<>();synchronized(lock){closed=true;rounds.keySet().forEach(k->calls.add(k.call()));pending.forEach((key,flight)->{calls.add(key.call());flight.invalidated=true;});rounds.clear();}
        calls.forEach(authorization::invalidate);synchronized(lock){if(pending.isEmpty())drained.complete(null);}return drained.minimalCompletionStage();
    }
    private static <T> RpcOperation<T> rejected(Throwable cause){return new RpcOperation<>(CompletableFuture.failedFuture(cause),CompletableFuture.completedFuture(null));}
    @Override public void close(){drain();}
}
