package io.webrtc.signaling.app.runtime;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.*;

/** Single enrolled source connection with bounded strict framing and explicit physical cleanup. */
final class NativeSourceHttp {
    private static final int BODY_LIMIT=32768,HEADER_LIMIT=8192;
    private final URI endpoint;private final SSLContext tls;private final UUID podUid,processBoot;
    private volatile boolean physicallySettled=true;
    NativeSourceHttp(URI endpoint,SSLContext tls,UUID podUid,UUID processBoot){
        if(endpoint==null||!"https".equals(endpoint.getScheme())||endpoint.getHost()==null||endpoint.getUserInfo()!=null||endpoint.getQuery()!=null||endpoint.getFragment()!=null
            ||endpoint.getPort()==0||endpoint.getPort()<-1||endpoint.getPort()>65535||endpoint.getRawPath()==null
            ||!endpoint.getRawPath().matches("/[A-Za-z0-9/_%.~-]{0,2047}")||!endpoint.normalize().equals(endpoint))
            throw new IllegalArgumentException("Pinned canonical HTTPS source endpoint required");
        this.endpoint=endpoint;this.tls=Objects.requireNonNull(tls);this.podUid=Objects.requireNonNull(podUid);this.processBoot=Objects.requireNonNull(processBoot);
    }
    boolean physicallySettled(){return physicallySettled;}
    byte[] fetch(long end,Long cursor)throws IOException {
        int port=endpoint.getPort()<0?443:endpoint.getPort();
        physicallySettled=false;
        var socket=(SSLSocket)tls.getSocketFactory().createSocket();
        try{
            socket.setEnabledProtocols(new String[]{"TLSv1.3"});var parameters=socket.getSSLParameters();parameters.setEndpointIdentificationAlgorithm("HTTPS");socket.setSSLParameters(parameters);
            socket.connect(new InetSocketAddress(endpoint.getHost(),port),remainingMillis(end));socket.setSoTimeout(remainingMillis(end));socket.startHandshake();
            String host=endpoint.getHost().contains(":")?"["+endpoint.getHost()+"]":endpoint.getHost();if(port!=443)host+=":"+port;
            String request="GET "+endpoint.getRawPath()+" HTTP/1.1\r\nHost: "+host+"\r\nAccept: application/json\r\nConnection: close\r\nX-Signaling-Pod-Uid: "+podUid+"\r\nX-Signaling-Process-Boot: "+processBoot+"\r\n"+(cursor==null?"":"X-Signaling-Source-Offset: "+cursor+"\r\n")+"\r\n";
            socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));socket.getOutputStream().flush();
            var input=new BufferedInputStream(socket.getInputStream(),4096);String status=line(input,socket,end);
            if(!status.matches("HTTP/1\\.[01] 200(?: [\\x20-\\x7e]*)?"))throw new IOException("Enrolled source status rejected");
            int bytes=status.length()+2;var headers=new HashMap<String,String>();String header;
            while(!(header=line(input,socket,end)).isEmpty()){
                bytes+=header.length()+2;if(bytes>HEADER_LIMIT)throw new IOException("Enrolled headers exceed bound");int colon=header.indexOf(':');if(colon<1)throw new IOException("Malformed source header");
                String name=header.substring(0,colon).toLowerCase(Locale.ROOT);if(!name.matches("[a-z0-9!#$%&'*+.^_`|~-]+")||headers.putIfAbsent(name,header.substring(colon+1).trim())!=null)throw new IOException("Duplicate source header");
            }
            String length=headers.get("content-length"),type=headers.get("content-type");
            if(length==null||!length.matches("[0-9]{1,5}")||headers.containsKey("transfer-encoding")||headers.containsKey("content-encoding")||type==null||!type.toLowerCase(Locale.ROOT).matches("application/json(?:;\\s*charset=utf-8)?"))throw new IOException("Bounded JSON response required");
            int size=Integer.parseInt(length);if(size<1||size>BODY_LIMIT)throw new IOException("Enrolled response exceeds bound");var body=new byte[size];int offset=0;
            while(offset<size){socket.setSoTimeout(remainingMillis(end));int read=input.read(body,offset,size-offset);if(read<0)throw new EOFException("Truncated source response");offset+=read;}
            socket.setSoTimeout(remainingMillis(end));if(input.read()!=-1)throw new IOException("Trailing source response bytes");return body;
        }finally{socket.close();physicallySettled=true;}
    }
    private static String line(InputStream input,SSLSocket socket,long end)throws IOException {
        var result=new StringBuilder();while(true){socket.setSoTimeout(remainingMillis(end));int next=input.read();if(next<0)throw new EOFException("Truncated source headers");if(next=='\r'){if(input.read()!='\n')throw new IOException("Enrolled header line ending");return result.toString();}if(next<32&&next!='\t'||next>126||result.length()>=4096)throw new IOException("Enrolled header bounds");result.append((char)next);}
    }
    private static int remainingMillis(long end)throws SocketTimeoutException {long remaining=end-System.nanoTime();if(remaining<=0)throw new SocketTimeoutException("Enrolled source deadline");return (int)Math.max(1,Math.min(1000,TimeUnit.NANOSECONDS.toMillis(remaining)));}
}
