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
    @FunctionalInterface public interface GatewayClientTls {SslContext context(String destinationCell,String gatewayId);}
    private RpcTlsContexts(){}
    public static SslContext server(String environment,String cell,File ca,File certificate,File key)throws Exception {
        return server(environment,cell,"actor",null,ca,certificate,key);
    }
    public static SslContext gatewayServer(String environment,String cell,String gatewayId,File ca,File certificate,File key)throws Exception {
        workload(gatewayId);return server(environment,cell,"gateway",gatewayId,ca,certificate,key);
    }
    private static SslContext server(String environment,String cell,String role,String workload,File ca,File certificate,File key)throws Exception {
        identityInputs(environment,cell);var peer=leaf(certificate,environment);
        if(peer==null||!peer.cell().equals(cell)||!peer.role().equals(role)||workload!=null&&!peer.workloadId().equals(workload))throw new IllegalArgumentException("RPC server certificate identity mismatch");
        return GrpcSslContexts.forServer(certificate,key).sslProvider(SslProvider.JDK).trustManager(ca).clientAuth(ClientAuth.REQUIRE).protocols("TLSv1.3").build();
    }
    public static ClientTls clients(String environment,File ca,File certificate,File key)throws Exception {
        identityInputs(environment,"c001");var trusted=chain(ca);var contexts=new ConcurrentHashMap<String,SslContext>();
        return destination->{identityInputs(environment,destination);return contexts.computeIfAbsent(destination,cell->client(environment,cell,"actor",null,trusted,certificate,key));};
    }
    /** Channels own their contexts; no unbounded cache of gateway processes or boot generations. */
    public static GatewayClientTls gatewayClients(String environment,File ca,File certificate,File key)throws Exception {
        identityInputs(environment,"c001");var own=leaf(certificate,environment);
        if(own==null||!own.role().equals("actor"))throw new IllegalArgumentException("Gateway relay client must be an actor workload");
        var trusted=chain(ca);return (cell,gateway)->{identityInputs(environment,cell);workload(gateway);return client(environment,cell,"gateway",gateway,trusted,certificate,key);};
    }
    private static SslContext client(String environment,String cell,String role,String workload,X509ExtendedTrustManager trusted,File certificate,File key){
        try{return GrpcSslContexts.forClient().sslProvider(SslProvider.JDK).trustManager(new X509ExtendedTrustManager(){
            private void identity(X509Certificate[] certificates)throws CertificateException {
                var peer=certificates.length==0?null:RpcTlsIdentity.extract(certificates[0],environment);
                if(peer==null||!peer.cell().equals(cell)||!peer.role().equals(role)||workload!=null&&!peer.workloadId().equals(workload))throw new CertificateException("RPC destination workload identity mismatch");
            }
            @Override public X509Certificate[] getAcceptedIssuers(){return trusted.getAcceptedIssuers();}
            @Override public void checkServerTrusted(X509Certificate[] certificates,String auth)throws CertificateException{trusted.checkServerTrusted(certificates,auth);identity(certificates);}
            @Override public void checkServerTrusted(X509Certificate[] certificates,String auth,Socket socket)throws CertificateException{trusted.checkServerTrusted(certificates,auth,socket);identity(certificates);}
            @Override public void checkServerTrusted(X509Certificate[] certificates,String auth,SSLEngine engine)throws CertificateException{trusted.checkServerTrusted(certificates,auth,engine);identity(certificates);}
            @Override public void checkClientTrusted(X509Certificate[] certificates,String auth)throws CertificateException{trusted.checkClientTrusted(certificates,auth);}
            @Override public void checkClientTrusted(X509Certificate[] certificates,String auth,Socket socket)throws CertificateException{trusted.checkClientTrusted(certificates,auth,socket);}
            @Override public void checkClientTrusted(X509Certificate[] certificates,String auth,SSLEngine engine)throws CertificateException{trusted.checkClientTrusted(certificates,auth,engine);}
        }).keyManager(certificate,key).protocols("TLSv1.3").build();}catch(Exception invalid){throw new IllegalArgumentException("Invalid RPC client TLS configuration",invalid);}
    }
    private static X509ExtendedTrustManager chain(File ca)throws Exception {
        var store=KeyStore.getInstance(KeyStore.getDefaultType());store.load(null,null);
        try(var in=new FileInputStream(ca)){int i=0;for(var c:CertificateFactory.getInstance("X.509").generateCertificates(in))store.setCertificateEntry("ca-"+i++,c);}
        var factory=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());factory.init(store);
        for(var manager:factory.getTrustManagers())if(manager instanceof X509ExtendedTrustManager x)return x;
        throw new IllegalArgumentException("Extended X509 trust manager required");
    }
    private static CellRpcServer.Peer leaf(File certificate,String environment)throws Exception {
        try(var in=new FileInputStream(certificate)){return RpcTlsIdentity.extract((X509Certificate)CertificateFactory.getInstance("X.509").generateCertificate(in),environment);}
    }
    private static void identityInputs(String environment,String cell){if(environment==null||!environment.matches("[a-z0-9-]{1,32}")||cell==null||!cell.matches("[a-z][a-z0-9-]{0,23}"))throw new IllegalArgumentException("Invalid RPC TLS identity");}
    private static void workload(String id){if(id==null||!id.matches("[A-Za-z0-9_.-]{1,128}"))throw new IllegalArgumentException("Invalid gateway workload identity");}
}
