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
    private final Map<Key,CompletableFuture<Snapshot>> pending=new HashMap<>();
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
        var key=new ParticipantKey(call,sender);
        Snapshot value=snapshots.get(key);
        if(value==null) return Optional.empty();
        if(!valid(value)) {snapshots.remove(key);return Optional.empty();}
        return value.sender().equals(sender) && value.negotiationId()==negotiation && value.iceGeneration()==ice
            ? Optional.of(value):Optional.empty();
    }
    public synchronized CompletionStage<Snapshot> refresh(CallId call,AuthenticatedSession sender,
            long negotiation,long ice,Duration budget,Supplier<ActorOperation<Snapshot>> loader) {
        if(budget.isNegative() || budget.isZero() || budget.compareTo(Duration.ofSeconds(2))>0)
            return CompletableFuture.failedFuture(new IllegalArgumentException("invalid budget"));
        var hit=get(call,sender,negotiation,ice);
        if(hit.isPresent()) return CompletableFuture.completedFuture(hit.get());
        var key=new Key(call,sender,negotiation,ice);
        if(pending.containsKey(key)) return pending.get(key);
        if(!trusted.getAsBoolean() || pending.size()>=maxPending)
            return CompletableFuture.failedFuture(new RejectedExecutionException("relay refresh unavailable"));
        var result=new CompletableFuture<Snapshot>();pending.put(key,result);
        final ActorOperation<Snapshot> operation;
        try {operation=Objects.requireNonNull(loader.get());}
        catch(Throwable failure) {pending.remove(key);result.completeExceptionally(failure);return result;}
        result.orTimeout(budget.toNanos(),TimeUnit.NANOSECONDS);
        operation.logical().whenComplete((value,failure)->{
            synchronized(this) {
                if(failure!=null) result.completeExceptionally(failure);
                else if(value==null || !value.callId().equals(call) || !value.sender().equals(sender)
                        || value.negotiationId()!=negotiation || value.iceGeneration()!=ice || !valid(value))
                    result.completeExceptionally(new IllegalStateException("stale relay authorization"));
                else if(!result.isDone()) {put(value);result.complete(value);}
            }
        });
        operation.physicalCompletion().whenComplete((ignored,failure)->{
            synchronized(this) {pending.remove(key,result);}
        });
        return result;
    }
    public synchronized void invalidate(CallId call) {snapshots.keySet().removeIf(key->key.call().equals(call));}
    public synchronized int pending(){return pending.size();}
    public synchronized int size(){return snapshots.size();}
}
