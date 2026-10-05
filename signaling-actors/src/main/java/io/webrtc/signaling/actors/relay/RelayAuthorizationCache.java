package io.webrtc.signaling.actors.relay;

import io.webrtc.signaling.actors.admission.ActorOperation;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.AuthoritySql.GroupToken;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Traffic-driven, bounded native authorization snapshots; no idle refresh timers. */
public final class RelayAuthorizationCache {
    public record Snapshot(CallId callId, UUID activationId, long callVersion,
            long negotiationId, long iceGeneration, String state,
            AuthenticatedSession sender, AuthenticatedSession recipient, GroupToken group,
            long checkedAtNanos, long tokenUntilNanos, long reservationUntilNanos,
            long groupUntilNanos, long securityUntilNanos) {
        public Snapshot {
            Objects.requireNonNull(callId); Objects.requireNonNull(activationId);
            Objects.requireNonNull(sender); Objects.requireNonNull(recipient); Objects.requireNonNull(group);
            if (callVersion < 1 || negotiationId < 1 || iceGeneration < 1
                    || !("CONNECTING".equals(state) || "ESTABLISHED".equals(state)))
                throw new IllegalArgumentException("invalid relay snapshot");
        }
        boolean live(long now) {
            long age=now-checkedAtNanos;
            return age>=0 && age<Duration.ofSeconds(5).toNanos()
                && now-tokenUntilNanos<0 && now-reservationUntilNanos<0
                && now-groupUntilNanos<0 && now-securityUntilNanos<0;
        }
    }
    private record ParticipantKey(CallId call,AuthenticatedSession sender) {}
    private record Key(CallId call, AuthenticatedSession sender, long negotiation, long ice) {}
    private final int capacity, maxPending;
    private final LongSupplier clock;
    private final BooleanSupplier trusted;
    private final Function<CallId,Optional<GroupToken>> currentGroup;
    private final LinkedHashMap<ParticipantKey,Snapshot> snapshots=new LinkedHashMap<>(16,.75f,true);
    private static final class Flight {
        final CompletableFuture<Snapshot> logical=new CompletableFuture<>();
        final CompletableFuture<Void> physical=new CompletableFuture<>();boolean cleaned,invalidated;
    }
    private final Map<Key,Flight> pending=new HashMap<>();
    public RelayAuthorizationCache(int capacity,int maxPending,LongSupplier clock,
            BooleanSupplier trusted,Function<CallId,Optional<GroupToken>> currentGroup) {
        if(capacity<1 || maxPending<1 || maxPending>capacity) throw new IllegalArgumentException("invalid capacity");
        this.capacity=capacity;this.maxPending=maxPending;this.clock=Objects.requireNonNull(clock);
        this.trusted=Objects.requireNonNull(trusted);this.currentGroup=Objects.requireNonNull(currentGroup);
    }
    public synchronized void put(Snapshot value) {
        if(!valid(value)) return;
        snapshots.put(new ParticipantKey(value.callId(),value.sender()),value);
        while(snapshots.size()>capacity) snapshots.remove(snapshots.keySet().iterator().next());
    }
    private boolean valid(Snapshot value) {
        return trusted.getAsBoolean() && value.live(clock.getAsLong())
            && currentGroup.apply(value.callId()).filter(value.group()::equals).isPresent();
    }
    public synchronized Optional<Snapshot> get(CallId call,AuthenticatedSession sender,long negotiation,long ice) {
        if(!trusted.getAsBoolean()){invalidate(call);return Optional.empty();}
        var key=new ParticipantKey(call,sender);
        Snapshot value=snapshots.get(key);
        if(value==null) return Optional.empty();
        if(!valid(value)) {snapshots.remove(key);return Optional.empty();}
        return value.sender().equals(sender) && value.negotiationId()==negotiation && value.iceGeneration()==ice
            ? Optional.of(value):Optional.empty();
    }
    public CompletionStage<Snapshot> refresh(CallId call,AuthenticatedSession sender,long negotiation,long ice,Duration budget,Supplier<ActorOperation<Snapshot>> loader){
        return refreshTracked(call,sender,negotiation,ice,budget,loader).logical();
    }
    public synchronized ActorOperation<Snapshot> refreshTracked(CallId call,AuthenticatedSession sender,
            long negotiation,long ice,Duration budget,Supplier<ActorOperation<Snapshot>> loader) {
        if(budget==null||budget.isNegative()||budget.isZero()||budget.compareTo(Duration.ofSeconds(2))>0)
            return denied(new IllegalArgumentException("invalid budget"));
        if(!trusted.getAsBoolean()){
            invalidate(call);var existing=pending.get(new Key(call,sender,negotiation,ice));
            return existing==null?denied(new RejectedExecutionException("relay refresh unavailable")):consumer(existing,budget);
        }
        var hit=get(call,sender,negotiation,ice);
        if(hit.isPresent())return new ActorOperation<>(CompletableFuture.completedFuture(hit.get()),CompletableFuture.completedFuture(null));
        var key=new Key(call,sender,negotiation,ice);
        var existing=pending.get(key);if(existing!=null)return consumer(existing,budget);
        if(!trusted.getAsBoolean()||pending.size()>=maxPending)return denied(new RejectedExecutionException("relay refresh unavailable"));
        var flight=new Flight();pending.put(key,flight);var result=flight.logical;
        result.whenComplete((v,e)->{synchronized(this){finish(key,flight);}});
        result.orTimeout(budget.toNanos(),TimeUnit.NANOSECONDS);
        final ActorOperation<Snapshot> operation;
        try {operation=Objects.requireNonNull(loader.get());}
        catch(Throwable failure){result.completeExceptionally(failure);return consumer(flight,budget);}
        operation.logical().whenComplete((value,failure)->{
            synchronized(this){
                if(failure!=null)result.completeExceptionally(failure);
                else if(value==null||!value.callId().equals(call)||!value.sender().equals(sender)
                        ||value.negotiationId()!=negotiation||value.iceGeneration()!=ice||!valid(value))
                    result.completeExceptionally(new IllegalStateException("stale relay authorization"));
                else if(!result.isDone()){put(value);result.complete(value);}
            }
        });
        operation.physicalCompletion().whenComplete((v,e)->{if(e==null)synchronized(this){flight.cleaned=true;finish(key,flight);}});
        return consumer(flight,budget);
    }
    private static ActorOperation<Snapshot> denied(Throwable cause){return new ActorOperation<>(CompletableFuture.failedFuture(cause),CompletableFuture.completedFuture(null));}
    private static ActorOperation<Snapshot> consumer(Flight flight,Duration budget){
        CompletionStage<Snapshot> logical=flight.invalidated?CompletableFuture.failedFuture(new IllegalStateException("stale relay authorization")):flight.logical.thenApply(value->value).orTimeout(budget.toNanos(),TimeUnit.NANOSECONDS).minimalCompletionStage();
        return new ActorOperation<>(logical,flight.physical.minimalCompletionStage());
    }
    private void finish(Key key,Flight flight){if(flight.cleaned&&flight.logical.isDone()){pending.remove(key,flight);flight.physical.complete(null);}}
    public synchronized void invalidate(CallId call) {
        snapshots.keySet().removeIf(key->key.call().equals(call));
        for(var entry:pending.entrySet().stream().filter(e->e.getKey().call().equals(call)).toList()){
            entry.getValue().invalidated=true;entry.getValue().logical.completeExceptionally(new IllegalStateException("stale relay authorization"));
        }
    }
    public synchronized int pending(){return pending.size();}
    public synchronized int size(){return snapshots.size();}
}
