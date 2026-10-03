package io.webrtc.signaling.rpc;
import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.*;
import java.io.*;
import java.net.Socket;
import java.security.*;
import java.security.cert.*;
import javax.net.ssl.*;
import java.util.concurrent.ConcurrentHashMap;
/** Bound server identity is verified during handshake, before sending application payloads. */
public final class RpcTlsContexts {
    @FunctionalInterface public interface ClientTls {SslContext context(String destinationCell);}
    private RpcTlsContexts(){}
    public static SslContext server(String environment,String cell,File ca,File certificate,File key)throws Exception {try(var in=new FileInputStream(certificate)){var peer=RpcTlsIdentity.extract((X509Certificate)CertificateFactory.getInstance("X.509").generateCertificate(in),environment);if(peer==null||!peer.cell().equals(cell)||!peer.role().equals("actor"))throw new IllegalArgumentException("RPC server certificate identity mismatch");}return GrpcSslContexts.forServer(certificate,key).sslProvider(SslProvider.JDK).trustManager(ca).clientAuth(ClientAuth.REQUIRE).protocols("TLSv1.3").build();}
    public static ClientTls clients(String environment,File ca,File certificate,File key)throws Exception {
        var store=KeyStore.getInstance(KeyStore.getDefaultType());store.load(null,null);try(var in=new FileInputStream(ca)){int i=0;for(var c:CertificateFactory.getInstance("X.509").generateCertificates(in))store.setCertificateEntry("ca-"+i++,c);}var factory=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());factory.init(store);X509ExtendedTrustManager chain=null;for(var manager:factory.getTrustManagers())if(manager instanceof X509ExtendedTrustManager x)chain=x;if(chain==null)throw new IllegalArgumentException("Extended X509 trust manager required");var trustedChain=chain;var contexts=new ConcurrentHashMap<String,SslContext>();
        return destination->{if(!destination.matches("[a-z][a-z0-9-]{0,23}"))throw new IllegalArgumentException("Invalid destination cell");return contexts.computeIfAbsent(destination,cell->{try{return GrpcSslContexts.forClient().sslProvider(SslProvider.JDK).trustManager(new X509ExtendedTrustManager(){
            private void identity(X509Certificate[] certificates)throws CertificateException {var peer=certificates.length==0?null:RpcTlsIdentity.extract(certificates[0],environment);if(peer==null||!peer.cell().equals(cell)||!peer.role().equals("actor"))throw new CertificateException("RPC destination workload identity mismatch");}
            @Override public X509Certificate[] getAcceptedIssuers(){return trustedChain.getAcceptedIssuers();}
            @Override public void checkServerTrusted(X509Certificate[] certificates,String auth)throws CertificateException{trustedChain.checkServerTrusted(certificates,auth);identity(certificates);}
            @Override public void checkServerTrusted(X509Certificate[] certificates,String auth,Socket socket)throws CertificateException{trustedChain.checkServerTrusted(certificates,auth,socket);identity(certificates);}
            @Override public void checkServerTrusted(X509Certificate[] certificates,String auth,SSLEngine engine)throws CertificateException{trustedChain.checkServerTrusted(certificates,auth,engine);identity(certificates);}
            @Override public void checkClientTrusted(X509Certificate[] certificates,String auth)throws CertificateException{trustedChain.checkClientTrusted(certificates,auth);}
            @Override public void checkClientTrusted(X509Certificate[] certificates,String auth,Socket socket)throws CertificateException{trustedChain.checkClientTrusted(certificates,auth,socket);}
            @Override public void checkClientTrusted(X509Certificate[] certificates,String auth,SSLEngine engine)throws CertificateException{trustedChain.checkClientTrusted(certificates,auth,engine);}
        }).keyManager(certificate,key).protocols("TLSv1.3").build();}catch(Exception invalid){throw new IllegalArgumentException("Invalid RPC client TLS configuration",invalid);}});};
    }
}
