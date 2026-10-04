package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.rpc.RpcOperation;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Fixed process jobs, one physically retained invocation each. No catch-up burst after a JVM pause. */
public final class NativeWorkerScheduler implements AutoCloseable {
    public enum Priority { SAFETY, MAINTENANCE }
    public enum Status { COMPLETED, UNKNOWN, OVERLOADED }
    public record Event(String name,Status status,long elapsedNanos) {}
    public record Job(String name,Priority priority,Duration period,Function<Duration,RpcOperation<?>> run) {
        public Job {
            if(name==null||!name.matches("[a-z][a-z0-9_-]{0,63}"))throw new IllegalArgumentException("Invalid worker name");
            Objects.requireNonNull(priority);Objects.requireNonNull(period);Objects.requireNonNull(run);
            if(period.compareTo(Duration.ofMillis(100))<0||period.compareTo(Duration.ofSeconds(30))>0)throw new IllegalArgumentException("Worker period outside bound");
        }
    }
    private static final long BUDGET_NANOS=Duration.ofSeconds(2).toNanos();
    private static final class Invocation {
        final long start=System.nanoTime();
        final CompletableFuture<Void> physical=new CompletableFuture<>();
        final CompletableFuture<Status> logical=new CompletableFuture<>();
    }
    private static final class Slot {
        final Job job;ScheduledFuture<?> periodic;Invocation active;
        Slot(Job job){this.job=job;}
    }
    private final List<Slot> slots;
    private final Consumer<Event> events;
    private final ScheduledThreadPoolExecutor timers=new ScheduledThreadPoolExecutor(1,Thread.ofPlatform().daemon().name("native-worker-timer").factory());
    private final ExecutorService tasks=Executors.newVirtualThreadPerTaskExecutor();
    private boolean started,shed,draining;
    private final CompletableFuture<Void> drained=new CompletableFuture<>();
    public NativeWorkerScheduler(List<Job> jobs,Consumer<Event> events){
        if(jobs==null||jobs.isEmpty()||jobs.size()>16||jobs.stream().map(Job::name).distinct().count()!=jobs.size())throw new IllegalArgumentException("Workers require 1..16 distinct fixed jobs");
        this.events=Objects.requireNonNull(events);slots=List.copyOf(jobs).stream().map(Slot::new).toList();
        timers.setRemoveOnCancelPolicy(true);
    }
    public synchronized void start(){
        if(started||draining)throw new IllegalStateException("Workers already started or drained");started=true;
        for(var slot:slots)slot.periodic=timers.scheduleWithFixedDelay(()->tick(slot),0,slot.job.period().toNanos(),TimeUnit.NANOSECONDS);
    }
    private void tick(Slot slot){
        Invocation invocation;
        synchronized(this){if(draining||shed&&slot.job.priority()!=Priority.SAFETY||slot.active!=null)return;invocation=new Invocation();slot.active=invocation;}
        invocation.logical.whenComplete((status,error)->{
            try{events.accept(new Event(slot.job.name(),status,Math.max(0,System.nanoTime()-invocation.start)));}catch(RuntimeException ignored){/* Metrics callback cannot replace native completion authority. */}
        });
        ScheduledFuture<?> expiry;
        try{expiry=timers.schedule(()->invocation.logical.complete(Status.UNKNOWN),BUDGET_NANOS,TimeUnit.NANOSECONDS);}
        catch(RejectedExecutionException notStarted){invocation.logical.complete(Status.OVERLOADED);retire(slot,invocation);return;}
        try{tasks.execute(()->{
            try{
                long remaining=BUDGET_NANOS-(System.nanoTime()-invocation.start);
                if(remaining<=0){invocation.logical.complete(Status.OVERLOADED);expiry.cancel(false);retire(slot,invocation);return;}
                var operation=Objects.requireNonNull(slot.job.run().apply(Duration.ofNanos(remaining)));
                operation.logical().whenComplete((value,error)->{invocation.logical.complete(error==null?Status.COMPLETED:Status.UNKNOWN);expiry.cancel(false);});
                operation.physicalCompletion().thenCombine(operation.logical().handle((v,e)->null),(a,b)->null)
                    .whenComplete((v,error)->{if(error==null)retire(slot,invocation);else invocation.logical.complete(Status.UNKNOWN);});
            }catch(Throwable unknown){invocation.logical.complete(Status.UNKNOWN);expiry.cancel(false);/* A throwing factory supplies no proof that its effects never started. */}
        });}catch(RejectedExecutionException notStarted){expiry.cancel(false);invocation.logical.complete(Status.OVERLOADED);retire(slot,invocation);}
    }
    private void retire(Slot slot,Invocation invocation){
        boolean complete;
        synchronized(this){if(slot.active!=invocation)return;slot.active=null;complete=draining&&slots.stream().allMatch(s->s.active==null);}
        invocation.physical.complete(null);
        if(complete){timers.shutdown();drained.complete(null);}
    }
    public synchronized void shedNormal(){shed=true;for(var slot:slots)if(slot.job.priority()!=Priority.SAFETY&&slot.periodic!=null)slot.periodic.cancel(false);}
    public synchronized CompletionStage<Void> settleAdmitted(){return CompletableFuture.allOf(slots.stream().filter(s->s.active!=null).map(s->s.active.physical).toArray(CompletableFuture[]::new)).minimalCompletionStage();}
    public CompletionStage<Void> drain(){
        boolean complete;
        synchronized(this){draining=true;for(var slot:slots)if(slot.periodic!=null)slot.periodic.cancel(false);tasks.shutdown();complete=slots.stream().allMatch(s->s.active==null);}
        if(complete){timers.shutdown();drained.complete(null);}return drained.minimalCompletionStage();
    }
    @Override public void close(){drain();}
}
