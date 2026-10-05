package io.webrtc.signaling.app.runtime;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import io.webrtc.signaling.auth.ClockSafetyMonitor;
import io.webrtc.signaling.rpc.RpcOperation;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import javax.net.ssl.*;

/** One enrolled HTTPS clock source. No connection pool, redirects, worker threads or implicit trust. */
public final class NativeClockSource implements AutoCloseable {
    private static final ObjectMapper JSON=new ObjectMapper(JsonFactory.builder()
        .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(8).maxStringLength(4096).maxNumberLength(20).build()).build())
        .findAndRegisterModules().disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
        .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private final NativeSourceHttp http;private final ClockSafetyMonitor monitor;
    private final CompletableFuture<Void> drained=new CompletableFuture<>();
    private boolean active,draining;
    private CompletableFuture<Void> admittedPhysical=CompletableFuture.completedFuture(null);
    public NativeClockSource(URI endpoint,SSLContext tls,ClockSafetyMonitor monitor,UUID podUid,UUID processBoot){
        http=new NativeSourceHttp(endpoint,tls,podUid,processBoot);this.monitor=Objects.requireNonNull(monitor);
    }
    /** Called only off event loops. Its one socket is synchronously closed before physical retirement. */
    public boolean poll(Duration budget){
        synchronized(this){if(draining)return false;if(active)return false;active=true;admittedPhysical=new CompletableFuture<>();}
        long started=System.nanoTime();boolean accepted=false;
        try{
            if(budget==null||budget.isZero()||budget.isNegative())throw new IllegalArgumentException("Positive source budget required");
            long end=started+Math.min(budget.toNanos(),Duration.ofSeconds(1).toNanos());
            var payload=http.fetch(end,null);
            var decoder=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
            var value=JSON.readTree(decoder.decode(ByteBuffer.wrap(payload)).toString());
            if(!value.isObject()||value.size()!=2||!value.has("report")||!value.path("signature").isTextual())throw new IOException("Invalid clock response");
            var report=JSON.treeToValue(value.get("report"),ClockSafetyMonitor.Report.class);
            if(System.nanoTime()-end>=0)throw new SocketTimeoutException("Clock source deadline");
            accepted=monitor.observe(report,value.get("signature").asText(),started);
        }catch(Exception invalid){accepted=false;}
        finally{
            synchronized(this){if(!accepted||draining)monitor.invalidate();if(http.physicallySettled()){active=false;admittedPhysical.complete(null);if(draining)drained.complete(null);}}
        }
        synchronized(this){return accepted&&!draining;}
    }
    public NativeWorkerScheduler.Job job(Duration period){
        if(period==null||period.compareTo(Duration.ofSeconds(1))>0)throw new IllegalArgumentException("Clock source poll must run at least once per second");
        return new NativeWorkerScheduler.Job("clock_source",NativeWorkerScheduler.Priority.SAFETY,period,budget->{boolean valid=poll(budget);return new RpcOperation<>(valid?CompletableFuture.completedFuture(true):CompletableFuture.failedFuture(new IllegalStateException("Clock source unavailable")),settleAdmitted());});
    }
    public synchronized CompletionStage<Void> settleAdmitted(){return admittedPhysical.minimalCompletionStage();}
    public synchronized CompletionStage<Void> drain(){draining=true;monitor.invalidate();if(!active)drained.complete(null);return drained.minimalCompletionStage();}
    @Override public void close(){drain();}
}
