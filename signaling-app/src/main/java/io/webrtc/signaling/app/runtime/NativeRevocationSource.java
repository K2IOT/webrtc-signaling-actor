package io.webrtc.signaling.app.runtime;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import io.webrtc.signaling.rpc.RpcOperation;
import io.webrtc.signaling.storage.DbOperation;
import io.webrtc.signaling.storage.worker.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import javax.net.ssl.SSLContext;

/** One source page per original bounded poll. SQL commit remains the security authority. */
public final class NativeRevocationSource implements AutoCloseable {
    private static final ObjectMapper JSON=new ObjectMapper(JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(8).maxStringLength(8192).maxNumberLength(20).build()).build())
        .findAndRegisterModules().disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
        .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES);
    private final NativeSourceHttp http;private final RevocationSourceVerifier verifier;private final RevocationReconciler reconciler;
    private boolean active,draining;private long checkedNanos;private Instant checkedAt;
    private CompletableFuture<Void> admitted=CompletableFuture.completedFuture(null);
    private final CompletableFuture<Void> drained=new CompletableFuture<>();
    public NativeRevocationSource(URI endpoint,SSLContext tls,RevocationSourceVerifier verifier,RevocationReconciler reconciler,UUID podUid,UUID processBoot){
        http=new NativeSourceHttp(endpoint,tls,podUid,processBoot);this.verifier=Objects.requireNonNull(verifier);this.reconciler=Objects.requireNonNull(reconciler);
    }
    public synchronized boolean usable(){
        long age=System.nanoTime()-checkedNanos;Instant now=Instant.now();
        return !draining&&checkedAt!=null&&age>=0&&age<reconciler.freshness().toNanos()&&!checkedAt.isAfter(now)&&now.isBefore(checkedAt.plus(reconciler.freshness()));
    }
    /** Blocking HTTP and bounded native waits run only on an admitted worker, never event loops. */
    public RpcOperation<Long> poll(Duration budget){
        if(budget==null||budget.isNegative()||budget.isZero()||budget.compareTo(Duration.ofSeconds(2))>0)throw new IllegalArgumentException("Source poll budget outside bound");
        final CompletableFuture<Void> physical;
        synchronized(this){
            if(active||draining)return new RpcOperation<>(CompletableFuture.failedFuture(new RejectedExecutionException("Revocation source unavailable")),CompletableFuture.completedFuture(null));
            active=true;admitted=physical=new CompletableFuture<>();
        }
        long start=System.nanoTime(),end=start+budget.toNanos();var receipts=new ArrayList<CompletableFuture<?>>(2);
        CompletionStage<Long> logical;
        try{
            var cursor=tracked(reconciler.progress(remaining(end)),end,receipts);
            var bytes=http.fetch(end,cursor.offset());
            var decoder=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
            var batch=JSON.readValue(decoder.decode(ByteBuffer.wrap(bytes)).toString(),RevocationReconciler.Batch.class);
            if(batch.fromOffset()!=cursor.offset()||!verifier.test(batch))throw new IllegalStateException("Invalid revocation page");
            long offset=tracked(reconciler.apply(batch,remaining(end)),end,receipts);
            if(offset!=batch.highWater())throw new IllegalStateException("Revocation cursor mismatch");
            synchronized(this){checkedAt=!draining&&batch.caughtUp()?batch.checkedAt():null;checkedNanos=start;}
            logical=CompletableFuture.completedFuture(offset);
        }catch(Throwable invalid){synchronized(this){checkedAt=null;}logical=CompletableFuture.failedFuture(invalid);}
        if(!http.physicallySettled())receipts.add(new CompletableFuture<Void>());
        CompletableFuture.allOf(receipts.toArray(CompletableFuture[]::new)).whenComplete((v,error)->{
            synchronized(this){
                if(error!=null){checkedAt=null;return;}
                active=false;physical.complete(null);if(draining)drained.complete(null);
            }
        });
        return new RpcOperation<>(logical,physical);
    }
    private static Duration remaining(long end)throws TimeoutException {long nanos=end-System.nanoTime();if(nanos<=0)throw new TimeoutException("Original revocation source deadline");return Duration.ofNanos(nanos);}
    private static <T> T tracked(DbOperation<T> work,long end,List<CompletableFuture<?>> receipts)throws Exception {
        receipts.add(work.physicalCompletion().toCompletableFuture());
        return work.logical().toCompletableFuture().get(remaining(end).toNanos(),TimeUnit.NANOSECONDS);
    }
    public NativeWorkerScheduler.Job job(Duration period){
        if(period==null||period.compareTo(Duration.ofSeconds(1))>0||period.compareTo(reconciler.freshness().dividedBy(2))>0)throw new IllegalArgumentException("Source poll period exceeds freshness margin");
        return new NativeWorkerScheduler.Job("revocation_source",NativeWorkerScheduler.Priority.SAFETY,period,this::poll);
    }
    public synchronized CompletionStage<Void> settleAdmitted(){return admitted.minimalCompletionStage();}
    public synchronized CompletionStage<Void> drain(){draining=true;checkedAt=null;if(!active)drained.complete(null);return drained.minimalCompletionStage();}
    @Override public void close(){drain();}
}
