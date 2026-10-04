package io.webrtc.signaling.storage;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.concurrent.*;
/** Logical expiry never cancels physical work or releases its credit. */
public final class DbBoundary implements AutoCloseable {
    private final DbAdmission admission;
    private final ExecutorService tasks=Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledThreadPoolExecutor deadlines=new ScheduledThreadPoolExecutor(1,Thread.ofPlatform().daemon().name("db-deadlines").factory());
    private final CompletableFuture<Void> drained=new CompletableFuture<>();
    private boolean accepting=true;
    private int physicalTasks;
    public DbBoundary(DbAdmission admission) {this.admission=admission;deadlines.setRemoveOnCancelPolicy(true);}
    public <T> CompletionStage<T> submit(DbClass clazz,Duration budget,Callable<T> tx) {
        return submitTracked(clazz,budget,tx).logical();
    }
    public <T> DbOperation<T> submitTracked(DbClass clazz,Duration budget,Callable<T> tx) {
        if(budget==null||budget.isNegative()||budget.isZero())return rejected();
        synchronized(this){if(!accepting||!admission.acquire(clazz))return rejected();physicalTasks++;}
        var result=new CompletableFuture<T>();
        var physical=new CompletableFuture<DbOperation.PhysicalCompletion>();
        ScheduledFuture<?> expiry;
        try{expiry=deadlines.schedule(()->result.completeExceptionally(new DbOutcomeUnknownException()),budget.toNanos(),TimeUnit.NANOSECONDS);}
        catch(RuntimeException e){admission.release(clazz);retired();return rejected();}
        try{tasks.execute(()->{try{if(!result.isDone()){T value=tx.call();validate(value,0);result.complete(value);}}catch(Throwable e){result.completeExceptionally(e);}finally{expiry.cancel(false);admission.release(clazz);physical.complete(DbOperation.PhysicalCompletion.FINISHED);retired();}});}
        catch(RejectedExecutionException e){expiry.cancel(false);admission.release(clazz);result.completeExceptionally(new DbOverloadedException());physical.complete(DbOperation.PhysicalCompletion.NOT_STARTED);retired();}
        return new DbOperation<>(result.minimalCompletionStage(),physical.minimalCompletionStage());
    }
    private static <T> DbOperation<T> rejected(){return new DbOperation<>(CompletableFuture.<T>failedFuture(new DbOverloadedException()).minimalCompletionStage(),CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.NOT_STARTED).minimalCompletionStage());}
    static void validate(Object value,int depth) throws ReflectiveOperationException {
        if(value==null)return;
        if(depth>16)throw new IllegalArgumentException("DTO nesting exceeds bound");
        Class<?> c=value.getClass();
        if(c==String.class||c==Boolean.class||c==Byte.class||c==Short.class||c==Integer.class||c==Long.class||c==Float.class||c==Double.class||c==java.util.UUID.class||c==java.time.Instant.class||c==Duration.class||c.isEnum())return;
        if(c.isRecord()&&Modifier.isFinal(c.getModifiers())){for(var part:c.getRecordComponents()){var accessor=part.getAccessor();if(!accessor.trySetAccessible())throw new IllegalArgumentException("Inaccessible DTO");validate(accessor.invoke(value),depth+1);}return;}
        if(c.getName().startsWith("java.util.ImmutableCollections$")&&value instanceof java.util.Collection<?> list){for(Object item:list)validate(item,depth+1);return;}
        if(value instanceof java.util.Optional<?> optional){validate(optional.orElse(null),depth+1);return;}
        throw new IllegalArgumentException("Only immutable native DTOs may leave database boundary");
    }
    private synchronized void retired(){physicalTasks--;if(!accepting&&physicalTasks==0){deadlines.shutdown();drained.complete(null);}}
    /** Call after framework handoff and native root release, before closing either database pool. */
    public synchronized CompletionStage<Void> drain(){
        if(accepting){accepting=false;tasks.shutdown();if(physicalTasks==0){deadlines.shutdown();drained.complete(null);}}
        return drained.minimalCompletionStage();
    }
    @Override public void close(){drain();}
}
