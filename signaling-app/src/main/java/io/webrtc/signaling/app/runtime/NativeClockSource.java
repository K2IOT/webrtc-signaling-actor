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
    private static final int BODY_LIMIT=32768,HEADER_LIMIT=8192;
    private static final ObjectMapper JSON=new ObjectMapper(JsonFactory.builder()
        .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(8).maxStringLength(4096).maxNumberLength(20).build()).build())
        .findAndRegisterModules().disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
        .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);
    private final URI endpoint;private final SSLContext tls;private final ClockSafetyMonitor monitor;
    private final UUID podUid,processBoot;
    private final CompletableFuture<Void> drained=new CompletableFuture<>();
    private boolean active,draining;
    public NativeClockSource(URI endpoint,SSLContext tls,ClockSafetyMonitor monitor,UUID podUid,UUID processBoot){
        if(endpoint==null||!"https".equals(endpoint.getScheme())||endpoint.getHost()==null||endpoint.getUserInfo()!=null||endpoint.getQuery()!=null||endpoint.getFragment()!=null
            ||endpoint.getPort()==0||endpoint.getPort()<-1||endpoint.getPort()>65535||endpoint.getRawPath()==null
            ||!endpoint.getRawPath().matches("/[A-Za-z0-9/_%.~-]{0,2047}")||!endpoint.normalize().equals(endpoint))
            throw new IllegalArgumentException("Pinned canonical HTTPS clock endpoint required");
        this.endpoint=endpoint;this.tls=Objects.requireNonNull(tls);this.monitor=Objects.requireNonNull(monitor);
        this.podUid=Objects.requireNonNull(podUid);this.processBoot=Objects.requireNonNull(processBoot);
    }
    /** Called only off event loops. Its one socket is synchronously closed before physical retirement. */
    public boolean poll(Duration budget){
        synchronized(this){if(draining)return false;if(active)return false;active=true;}
        long started=System.nanoTime();boolean accepted=false;
        try{
            if(budget==null||budget.isZero()||budget.isNegative())throw new IllegalArgumentException("Positive source budget required");
            long end=started+Math.min(budget.toNanos(),Duration.ofSeconds(1).toNanos());
            var payload=fetch(end);
            var decoder=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
            var value=JSON.readTree(decoder.decode(ByteBuffer.wrap(payload)).toString());
            if(!value.isObject()||value.size()!=2||!value.has("report")||!value.path("signature").isTextual())throw new IOException("Invalid clock response");
            var report=JSON.treeToValue(value.get("report"),ClockSafetyMonitor.Report.class);
            if(System.nanoTime()-end>=0)throw new SocketTimeoutException("Clock source deadline");
            accepted=monitor.observe(report,value.get("signature").asText(),started);
        }catch(Exception invalid){accepted=false;}
        finally{
            synchronized(this){active=false;if(!accepted||draining)monitor.invalidate();if(draining)drained.complete(null);}
        }
        synchronized(this){return accepted&&!draining;}
    }
    private byte[] fetch(long end)throws IOException {
        int port=endpoint.getPort()<0?443:endpoint.getPort();
        try(var socket=(SSLSocket)tls.getSocketFactory().createSocket()){
            socket.setEnabledProtocols(new String[]{"TLSv1.3"});var parameters=socket.getSSLParameters();parameters.setEndpointIdentificationAlgorithm("HTTPS");socket.setSSLParameters(parameters);
            socket.connect(new InetSocketAddress(endpoint.getHost(),port),remainingMillis(end));socket.setSoTimeout(remainingMillis(end));socket.startHandshake();
            String host=endpoint.getHost().contains(":")?"["+endpoint.getHost()+"]":endpoint.getHost();if(port!=443)host+=":"+port;
            String request="GET "+endpoint.getRawPath()+" HTTP/1.1\r\nHost: "+host+"\r\nAccept: application/json\r\nConnection: close\r\nX-Signaling-Pod-Uid: "+podUid+"\r\nX-Signaling-Process-Boot: "+processBoot+"\r\n\r\n";
            socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));socket.getOutputStream().flush();
            var input=new BufferedInputStream(socket.getInputStream(),4096);String status=line(input,socket,end);
            if(!status.matches("HTTP/1\\.[01] 200(?: [\\x20-\\x7e]*)?"))throw new IOException("Clock source status rejected");
            int bytes=status.length()+2;var headers=new HashMap<String,String>();String header;
            while(!(header=line(input,socket,end)).isEmpty()){
                bytes+=header.length()+2;if(bytes>HEADER_LIMIT)throw new IOException("Clock headers exceed bound");int colon=header.indexOf(':');if(colon<1)throw new IOException("Malformed clock header");
                String name=header.substring(0,colon).toLowerCase(Locale.ROOT);if(!name.matches("[a-z0-9!#$%&'*+.^_`|~-]+")||headers.putIfAbsent(name,header.substring(colon+1).trim())!=null)throw new IOException("Duplicate clock header");
            }
            String length=headers.get("content-length"),type=headers.get("content-type");
            if(length==null||!length.matches("[0-9]{1,5}")||headers.containsKey("transfer-encoding")||headers.containsKey("content-encoding")||type==null||!type.toLowerCase(Locale.ROOT).matches("application/json(?:;\\s*charset=utf-8)?"))throw new IOException("Bounded JSON response required");
            int size=Integer.parseInt(length);if(size<1||size>BODY_LIMIT)throw new IOException("Clock response exceeds bound");var body=new byte[size];int offset=0;
            while(offset<size){socket.setSoTimeout(remainingMillis(end));int read=input.read(body,offset,size-offset);if(read<0)throw new EOFException("Truncated clock response");offset+=read;}
            socket.setSoTimeout(remainingMillis(end));if(input.read()!=-1)throw new IOException("Trailing clock response bytes");return body;
        }
    }
    private static String line(InputStream input,SSLSocket socket,long end)throws IOException {
        var result=new StringBuilder();while(true){socket.setSoTimeout(remainingMillis(end));int next=input.read();if(next<0)throw new EOFException("Truncated clock headers");if(next=='\r'){if(input.read()!='\n')throw new IOException("Clock header line ending");return result.toString();}if(next<32&&next!='\t'||next>126||result.length()>=4096)throw new IOException("Clock header bounds");result.append((char)next);}
    }
    private static int remainingMillis(long end)throws SocketTimeoutException {long remaining=end-System.nanoTime();if(remaining<=0)throw new SocketTimeoutException("Clock source deadline");return (int)Math.max(1,Math.min(1000,TimeUnit.NANOSECONDS.toMillis(remaining)));}
    public NativeWorkerScheduler.Job job(Duration period){
        if(period==null||period.compareTo(Duration.ofSeconds(1))>0)throw new IllegalArgumentException("Clock source poll must run at least once per second");
        return new NativeWorkerScheduler.Job("clock_source",NativeWorkerScheduler.Priority.SAFETY,period,budget->{boolean valid=poll(budget);return new RpcOperation<>(valid?CompletableFuture.completedFuture(true):CompletableFuture.failedFuture(new IllegalStateException("Clock source unavailable")),CompletableFuture.completedFuture(null));});
    }
    public synchronized CompletionStage<Void> drain(){draining=true;monitor.invalidate();if(!active)drained.complete(null);return drained.minimalCompletionStage();}
    @Override public void close(){drain();}
}
