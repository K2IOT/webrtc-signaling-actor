package io.webrtc.signaling.storage;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.concurrent.*;
/** Logical expiry never cancels physical work or releases its credit. */
public final class DbBoundary implements AutoCloseable {
    private final DbAdmission admission;
    private final ExecutorService tasks=Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledThreadPoolExecutor deadlines=new ScheduledThreadPoolExecutor(1,Thread.ofPlatform().daemon().name("db-deadlines").factory());
    public DbBoundary(DbAdmission admission) {this.admission=admission;deadlines.setRemoveOnCancelPolicy(true);}
    public <T> CompletionStage<T> submit(DbClass clazz,Duration budget,Callable<T> tx) {
        if(budget==null||budget.isNegative()||budget.isZero()||!admission.acquire(clazz))return CompletableFuture.<T>failedFuture(new DbOverloadedException()).minimalCompletionStage();
        var result=new CompletableFuture<T>();
        ScheduledFuture<?> expiry;
        try{expiry=deadlines.schedule(()->result.completeExceptionally(new DbOutcomeUnknownException()),budget.toNanos(),TimeUnit.NANOSECONDS);}
        catch(RuntimeException e){admission.release(clazz);return CompletableFuture.<T>failedFuture(new DbOverloadedException()).minimalCompletionStage();}
        try{tasks.execute(()->{try{if(!result.isDone()){T value=tx.call();validate(value,0);result.complete(value);}}catch(Throwable e){result.completeExceptionally(e);}finally{expiry.cancel(false);admission.release(clazz);}});}
        catch(RejectedExecutionException e){expiry.cancel(false);admission.release(clazz);result.completeExceptionally(new DbOverloadedException());}
        return result.minimalCompletionStage();
    }
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
    @Override public void close(){tasks.shutdown();deadlines.shutdown();}
}
