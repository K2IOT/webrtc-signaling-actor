package io.webrtc.signaling.actors.lease;

import io.webrtc.signaling.storage.*;
import org.apache.pekko.actor.ExtendedActorSystem;
import org.apache.pekko.coordination.lease.LeaseSettings;
import org.apache.pekko.coordination.lease.javadsl.Lease;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

/** Public Lease adapter. Its local snapshot never substitutes for a primary SQL fence. */
public final class PostgresShardLease extends Lease {
    public record Namespace(String systemName,String cell,long storageEpoch,String memberHostPort,String ownerNode) {
        public Namespace {if(systemName==null||cell==null||storageEpoch<1||memberHostPort==null||ownerNode==null||ownerNode.getBytes(java.nio.charset.StandardCharsets.UTF_8).length>128)throw new IllegalArgumentException("Invalid lease namespace");}
        int group(LeaseSettings settings){
            String prefix=systemName+"-shard-SignalingCallV1-",name=settings.leaseName();
            if(!name.startsWith(prefix)||!settings.ownerName().equals(memberHostPort))throw new IllegalArgumentException("Lease type/member namespace mismatch");
            String suffix=name.substring(prefix.length());if(!suffix.matches("0|[1-9][0-9]{0,3}"))throw new IllegalArgumentException("Noncanonical lease shard");
            int group=Integer.parseInt(suffix);if(group>1023)throw new IllegalArgumentException("Lease shard out of range");return group;
        }
    }
    private final LeaseSettings settings;private final Engine engine;
    /** Constructor loaded by Pekko's public LeaseProvider. */
    public PostgresShardLease(LeaseSettings settings,ExtendedActorSystem system){this.settings=settings;this.engine=PostgresShardLeaseProvider.resolve(settings,system);}
    public PostgresShardLease(LeaseSettings settings,Namespace namespace,GroupOwnership repository,ScheduledExecutorService timers,BooleanSupplier clockHealthy,LongSupplier nanos){this.settings=settings;engine=new Engine(settings,namespace,repository,timers,clockHealthy,nanos);}
    @Override public LeaseSettings getSettings(){return settings;}
    @Override public CompletionStage<Boolean> acquire(){return engine.acquire(error->{});}
    @Override public CompletionStage<Boolean> acquire(Consumer<Optional<Throwable>> callback){return engine.acquire(callback);}
    @Override public CompletionStage<Boolean> release(){return engine.release();}
    @Override public boolean checkLease(){return engine.check();}
    public Optional<GroupOwnerRepository.Grant> currentGrant(){var held=engine.held.get();return engine.check()&&held!=null?Optional.of(held.grant()):Optional.empty();}
    public void invalidate(Throwable reason){engine.invalidate(reason);}

    static final class Engine {
        private enum Kind {ACQUIRE,PULSE,RELEASE}
        private static final class Tenure {
            final UUID incarnation;final AtomicBoolean notified=new AtomicBoolean();Consumer<Optional<Throwable>> callback;
            Tenure(UUID incarnation,Consumer<Optional<Throwable>> callback){this.incarnation=incarnation;this.callback=callback;}
        }
        private record Held(GroupOwnerRepository.Grant grant,long until,Tenure tenure) {}
        private static final class Pending {
            final Kind kind;final Tenure tenure;final UUID operation;final long start;final CompletableFuture<Boolean> result=new CompletableFuture<>();
            boolean unknown,physicalDone,cleaning;CompletableFuture<Boolean> releaseResult;ScheduledFuture<?> deadline,retry;
            Pending(Kind kind,Tenure tenure,UUID operation,long start){this.kind=kind;this.tenure=tenure;this.operation=operation;this.start=start;}
        }
        private final LeaseSettings settings;private final Namespace namespace;private final int group;
        private final GroupOwnership repository;private final ScheduledExecutorService timers;private final BooleanSupplier clock;private final LongSupplier nanos;
        final AtomicReference<Held> held=new AtomicReference<>();private Held retired;private Pending pending;private ScheduledFuture<?> pulse,expiry;
        Engine(LeaseSettings settings,Namespace namespace,GroupOwnership repository,ScheduledExecutorService timers,BooleanSupplier clock,LongSupplier nanos){
            this.settings=settings;this.namespace=namespace;group=namespace.group(settings);this.repository=Objects.requireNonNull(repository);this.timers=Objects.requireNonNull(timers);this.clock=Objects.requireNonNull(clock);this.nanos=Objects.requireNonNull(nanos);
            var timeouts=settings.timeoutSettings();if(!timeouts.getHeartbeatInterval().equals(Duration.ofSeconds(5))||!timeouts.getHeartbeatTimeout().equals(Duration.ofSeconds(15))||timeouts.getOperationTimeout().isZero()||timeouts.getOperationTimeout().isNegative()||timeouts.getOperationTimeout().compareTo(Duration.ofSeconds(2))>0)throw new IllegalArgumentException("Unqualified lease timeout settings");
        }
        boolean check(){Held current=held.get();return current!=null&&clock.getAsBoolean()&&nanos.getAsLong()-current.until()<0;}
        synchronized CompletionStage<Boolean> acquire(Consumer<Optional<Throwable>> callback){
            Objects.requireNonNull(callback);if(check()){held.get().tenure().callback=callback;return done(true);}
            if(held.get()!=null)lose(new AuthoritySql.FencedException(),true);
            if(pending!=null)return pending.kind==Kind.ACQUIRE&&!pending.unknown?pending.result.minimalCompletionStage():failed(new DbOutcomeUnknownException());
            if(retired!=null){release();return done(false);}
            if(!clock.getAsBoolean())return failed(new AuthoritySql.FencedException());
            var tenure=new Tenure(UUID.randomUUID(),callback);var p=new Pending(Kind.ACQUIRE,tenure,UUID.randomUUID(),nanos.getAsLong());pending=p;
            try{var attempt=repository.acquireTracked(group,namespace.ownerNode(),tenure.incarnation,p.operation);track(p,attempt);attempt.logical().whenComplete((value,error)->acquired(p,value,error));}
            catch(RuntimeException error){p.physicalDone=true;unknown(p,error,false);}
            return p.result.minimalCompletionStage();
        }
        private synchronized void acquired(Pending p,Optional<GroupOwnerRepository.Grant> result,Throwable error){
            if(pending!=p||p.unknown)return;if(error!=null){unknown(p,error,false);return;}
            if(result.isEmpty()){p.result.complete(false);cancel(p.deadline);finishKnown(p);return;}
            var grant=result.get();if(!valid(grant,p.tenure)||!p.operation.equals(grant.acquireOperation())||!publish(grant,p)){unknown(p,new AuthoritySql.FencedException(),false);return;}
            p.result.complete(true);cancel(p.deadline);finishKnown(p);
        }
        private boolean valid(GroupOwnerRepository.Grant grant,Tenure tenure){var token=grant.token();return token.cell().equals(namespace.cell())&&token.storageEpoch()==namespace.storageEpoch()&&token.hashVersion()==1&&token.group()==group&&token.node().equals(namespace.ownerNode())&&token.incarnation().equals(tenure.incarnation);}
        private boolean publish(GroupOwnerRepository.Grant grant,Pending p){
            long remaining=Duration.between(grant.databaseTime(),grant.leaseUntil()).toNanos();long until=p.start+Math.min(remaining,Duration.ofSeconds(15).toNanos())-Duration.ofSeconds(5).toNanos();
            if(!clock.getAsBoolean()||nanos.getAsLong()-until>=0)return false;held.set(new Held(grant,until,p.tenure));retired=null;cancel(expiry);
            expiry=timers.schedule(()->expire(p.tenure),Math.max(0,until-nanos.getAsLong()),TimeUnit.NANOSECONDS);return true;
        }
        private void track(Pending p,DbOperation<?> attempt){
            p.deadline=timers.schedule(()->timeout(p),settings.timeoutSettings().getOperationTimeout().toNanos(),TimeUnit.NANOSECONDS);
            attempt.physicalCompletion().whenComplete((completion,error)->physical(p));
        }
        private synchronized void physical(Pending p){if(pending!=p)return;p.physicalDone=true;if(p.unknown)cleanup(p);else finishKnown(p);}
        private void finishKnown(Pending p){
            if(pending!=p||!p.physicalDone||!p.result.isDone()||p.unknown)return;pending=null;if(p.kind==Kind.RELEASE&&retired!=null&&retired.tenure()==p.tenure)retired=null;Held current=held.get();
            if(current!=null&&current.tenure()==p.tenure){cancel(pulse);long next=p.start+Duration.ofSeconds(5).toNanos();pulse=timers.schedule(this::pulse,Math.max(0,next-nanos.getAsLong()),TimeUnit.NANOSECONDS);}
        }
        private synchronized void timeout(Pending p){if(pending==p&&!p.result.isDone())unknown(p,new DbOutcomeUnknownException(),p.kind==Kind.PULSE);}
        private synchronized void pulse(){
            Held current=held.get();if(current==null)return;if(!check()){lose(new AuthoritySql.FencedException(),true);return;}
            if(pending!=null)return;var p=new Pending(Kind.PULSE,current.tenure(),UUID.randomUUID(),nanos.getAsLong());pending=p;
            try{long sequence=Math.addExact(current.grant().sequence(),1);var attempt=repository.pulseTracked(current.grant(),sequence,p.operation);track(p,attempt);attempt.logical().whenComplete((value,error)->pulsed(p,current.grant(),sequence,value,error));}
            catch(RuntimeException error){p.physicalDone=true;unknown(p,error,true);}
        }
        private synchronized void pulsed(Pending p,GroupOwnerRepository.Grant previous,long sequence,GroupOwnerRepository.Grant grant,Throwable error){
            if(pending!=p||p.unknown)return;if(error!=null||!valid(grant,p.tenure)||!grant.token().equals(previous.token())||grant.sequence()!=sequence||!grant.operation().equals(p.operation)||!publish(grant,p)){unknown(p,error==null?new AuthoritySql.FencedException():error,true);return;}
            p.result.complete(true);cancel(p.deadline);finishKnown(p);
        }
        private synchronized void expire(Tenure tenure){Held current=held.get();if(current!=null&&current.tenure()==tenure&&!check())lose(new AuthoritySql.FencedException(),true);}
        synchronized void invalidate(Throwable error){lose(error,true);if(pending!=null&&!pending.unknown)unknown(pending,error,true);}
        private void lose(Throwable error,boolean notify){
            Held previous=held.getAndSet(null);if(previous!=null)retired=previous;cancel(pulse);cancel(expiry);if(previous!=null&&notify&&previous.tenure().notified.compareAndSet(false,true))notifyLost(previous.tenure(),error);
        }
        private static void notifyLost(Tenure tenure,Throwable error){try{tenure.callback.accept(Optional.ofNullable(error));}catch(RuntimeException callbackFailure){org.slf4j.LoggerFactory.getLogger(PostgresShardLease.class).warn("Lease-loss callback failed");}}
        private void unknown(Pending p,Throwable error,boolean notify){
            if(pending!=p)return;p.unknown=true;cancel(p.deadline);lose(error,notify);p.result.completeExceptionally(error);if(p.physicalDone)cleanup(p);
        }
        synchronized CompletionStage<Boolean> release(){
            Held current=held.getAndSet(null);if(current!=null)retired=current;else current=retired;cancel(pulse);cancel(expiry);
            if(pending!=null){var p=pending;if(p.releaseResult!=null)return p.releaseResult.minimalCompletionStage();p.releaseResult=new CompletableFuture<>();p.unknown=true;cancel(p.deadline);p.result.completeExceptionally(new DbOutcomeUnknownException());releaseDeadline(p);if(p.physicalDone)cleanup(p);return p.releaseResult.minimalCompletionStage();}
            if(current==null)return done(true);var p=new Pending(Kind.RELEASE,current.tenure(),UUID.randomUUID(),nanos.getAsLong());pending=p;
            try{var attempt=repository.releaseTracked(current.grant().token());track(p,attempt);attempt.logical().whenComplete((value,error)->released(p,value,error));}
            catch(RuntimeException error){p.physicalDone=true;unknown(p,error,false);}return p.result.minimalCompletionStage();
        }
        private synchronized void released(Pending p,Boolean value,Throwable error){if(pending!=p||p.unknown)return;if(error!=null){unknown(p,error,false);return;}p.result.complete(value);cancel(p.deadline);if(Boolean.TRUE.equals(value))finishKnown(p);else {p.unknown=true;if(p.physicalDone)cleanup(p);}}
        private void releaseDeadline(Pending p){p.deadline=timers.schedule(()->{synchronized(this){if(pending==p&&p.releaseResult!=null)p.releaseResult.completeExceptionally(new DbOutcomeUnknownException());}},settings.timeoutSettings().getOperationTimeout().toNanos(),TimeUnit.NANOSECONDS);}
        private void cleanup(Pending p){
            if(pending!=p||!p.physicalDone||p.cleaning)return;p.cleaning=true;
            repository.reconcile(group,namespace.ownerNode(),p.tenure.incarnation).whenComplete((value,error)->reconciled(p,value,error));
        }
        private synchronized void reconciled(Pending p,Optional<GroupOwnerRepository.Grant> result,Throwable error){
            if(pending!=p)return;if(error!=null){p.cleaning=false;retryCleanup(p);return;}
            if(result.isEmpty()){clean(p);return;}var grant=result.get();if(!valid(grant,p.tenure)){p.cleaning=false;retryCleanup(p);return;}
            var release=repository.releaseTracked(grant.token());var known=new AtomicBoolean();
            release.logical().whenComplete((value,failure)->{if(failure==null&&Boolean.TRUE.equals(value))known.set(true);});
            release.physicalCompletion().whenComplete((completion,failure)->{synchronized(this){if(pending!=p)return;if(known.get())clean(p);else {p.cleaning=false;retryCleanup(p);}}});
        }
        private void retryCleanup(Pending p){cancel(p.retry);p.retry=timers.schedule(()->{synchronized(this){cleanup(p);}},Duration.ofSeconds(1).toNanos(),TimeUnit.NANOSECONDS);}
        private void clean(Pending p){if(pending!=p)return;pending=null;if(retired!=null&&retired.tenure()==p.tenure)retired=null;cancel(p.deadline);cancel(p.retry);if(p.releaseResult!=null)p.releaseResult.complete(true);}
        private static void cancel(ScheduledFuture<?> future){if(future!=null)future.cancel(false);}
        private static CompletionStage<Boolean> done(boolean value){return CompletableFuture.completedFuture(value).minimalCompletionStage();}
        private static CompletionStage<Boolean> failed(Throwable error){return CompletableFuture.<Boolean>failedFuture(error).minimalCompletionStage();}
    }
}
