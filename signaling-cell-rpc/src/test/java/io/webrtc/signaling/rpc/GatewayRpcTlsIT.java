package io.webrtc.signaling.rpc;
import static org.assertj.core.api.Assertions.*;
import io.grpc.*;
import io.grpc.netty.*;
import io.grpc.stub.StreamObserver;
import io.webrtc.signaling.protocol.internal.*;
import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
class GatewayRpcTlsIT {
    File cert(String name){return new File(Objects.requireNonNull(getClass().getResource("/test-only-pki/session/"+name)).getFile());}
    @Test void gatewayListenerRequiresExactLocalRoleCellAndWorkload()throws Exception {
        assertThat(RpcTlsContexts.gatewayServer("test","c001","gw-1",cert("ca.crt"),cert("gateway.crt"),cert("gateway.key"))).isNotNull();
        assertThatThrownBy(()->RpcTlsContexts.gatewayServer("test","c001","other",cert("ca.crt"),cert("gateway.crt"),cert("gateway.key"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->RpcTlsContexts.gatewayServer("test","c002","gw-1",cert("ca.crt"),cert("gateway.crt"),cert("gateway.key"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->RpcTlsContexts.gatewayServer("test","c001","gw-1",cert("ca.crt"),cert("server.crt"),cert("server.key"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->RpcTlsContexts.server("test","c001",cert("ca.crt"),cert("gateway.crt"),cert("gateway.key"))).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void actualTlsHandshakePinsRemoteGatewayRoleCellAndWorkload()throws Exception {
        var tls=RpcTlsContexts.gatewayClients("test",cert("ca.crt"),cert("server.crt"),cert("server.key"));
        var service=new SessionIngressGrpc.SessionIngressImplBase(){@Override public void mutateSession(SessionCommand c,StreamObserver<SessionReply> o){o.onNext(SessionReply.newBuilder().setOperationId(c.getOperationId()).setStatus("TEST_ONLY_TLS_RECEIPT").build());o.onCompleted();}};
        var server=NettyServerBuilder.forPort(0).sslContext(RpcTlsContexts.gatewayServer("test","c001","gw-1",cert("ca.crt"),cert("gateway.crt"),cert("gateway.key"))).addService(service).build().start();
        try{
            for(String[] dest:List.of(new String[]{"c001","gw-1"},new String[]{"c001","other"},new String[]{"c002","gw-1"})){
                var channel=NettyChannelBuilder.forAddress("localhost",server.getPort()).overrideAuthority("localhost").sslContext(tls.context(dest[0],dest[1])).disableRetry().build();
                try{var stub=SessionIngressGrpc.newBlockingStub(channel).withDeadlineAfter(3,TimeUnit.SECONDS);var request=SessionCommand.newBuilder().setOperationId(UUID.randomUUID().toString()).build();
                    if(dest[0].equals("c001")&&dest[1].equals("gw-1"))assertThat(stub.mutateSession(request).getStatus()).isEqualTo("TEST_ONLY_TLS_RECEIPT");
                    else assertThatThrownBy(()->stub.mutateSession(request)).isInstanceOf(StatusRuntimeException.class);
                }finally{channel.shutdownNow().awaitTermination(3,TimeUnit.SECONDS);}
            }
        }finally{server.shutdownNow().awaitTermination(3,TimeUnit.SECONDS);}
        var actor=NettyServerBuilder.forPort(0).sslContext(RpcTlsContexts.server("test","c001",cert("ca.crt"),cert("server.crt"),cert("server.key"))).addService(service).build().start();
        var channel=NettyChannelBuilder.forAddress("localhost",actor.getPort()).overrideAuthority("localhost").sslContext(tls.context("c001","gw-1")).disableRetry().build();
        try{assertThatThrownBy(()->SessionIngressGrpc.newBlockingStub(channel).withDeadlineAfter(3,TimeUnit.SECONDS).mutateSession(SessionCommand.getDefaultInstance())).isInstanceOf(StatusRuntimeException.class);}
        finally{channel.shutdownNow().awaitTermination(3,TimeUnit.SECONDS);actor.shutdownNow().awaitTermination(3,TimeUnit.SECONDS);}
    }
}
