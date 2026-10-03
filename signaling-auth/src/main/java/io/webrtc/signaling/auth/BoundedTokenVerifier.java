package io.webrtc.signaling.auth;
import java.time.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
public final class BoundedTokenVerifier implements AutoCloseable {
    private final TokenVerifier verifier;private final ThreadPoolExecutor executor;private final Semaphore admission;
    private final AtomicInteger pending=new AtomicInteger();private final Duration maxQueueAge;
    public BoundedTokenVerifier(TokenVerifier verifier,int workers,int queueCapacity,Duration maxQueueAge){
        if(workers<=0||queueCapacity<=0||maxQueueAge==null||maxQueueAge.isNegative()||maxQueueAge.isZero())throw new IllegalArgumentException("bounded verification configuration");
        this.verifier=verifier;this.maxQueueAge=maxQueueAge;admission=new Semaphore(workers+queueCapacity);
        executor=new ThreadPoolExecutor(workers,workers,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(queueCapacity),Thread.ofPlatform().name("signal-auth-",0).factory(),new ThreadPoolExecutor.AbortPolicy());
    }
    public CompletionStage<AuthPrincipal> verify(String token,Instant now){
        if(!admission.tryAcquire())return CompletableFuture.failedFuture(new RejectedExecutionException("AUTH_OVERLOADED"));
        pending.incrementAndGet();long submitted=System.nanoTime();var result=new CompletableFuture<AuthPrincipal>();
        try{executor.execute(()->{try{if(System.nanoTime()-submitted>maxQueueAge.toNanos())throw new RejectedExecutionException("AUTH_QUEUE_EXPIRED");result.complete(verifier.validate(token,now));}catch(Throwable e){result.completeExceptionally(e);}finally{pending.decrementAndGet();admission.release();}});}
        catch(RejectedExecutionException e){pending.decrementAndGet();admission.release();result.completeExceptionally(e);}
        return result.minimalCompletionStage();
    }
    public int pending(){return pending.get();}
    public void close(){executor.shutdown();}
}
