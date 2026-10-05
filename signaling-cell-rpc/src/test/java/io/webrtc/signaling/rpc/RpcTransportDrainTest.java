package io.webrtc.signaling.rpc;

import static org.assertj.core.api.Assertions.*;
import io.grpc.*;
import java.util.Map;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/** TEST_ONLY original channel-termination barrier; never a network delivery fixture. */
class RpcTransportDrainTest {
    @Test void gatewayDrainWaitsForOriginalManagedChannelTermination()throws Exception {check(true,false);}
    @Test void controlDrainWaitsForOriginalManagedChannelTermination()throws Exception {check(false,false);}
    @Test void gatewayDrainCannotAttestUnprovenTransportTermination()throws Exception {check(true,true);}
    @Test void controlDrainCannotAttestUnprovenTransportTermination()throws Exception {check(false,true);}
    private void check(boolean gateway,boolean unknown)throws Exception {
        var transport=new Transport(unknown);Object owner=gateway
            ?new GatewayRelayRpcClient("test",1,Map.of(),(cell,id)->{throw new AssertionError("No TLS work");},new RpcAdmission(1,98304,1,98304))
            :new CellRpcClient("test",Map.of("c001",new CellRpcClient.Endpoint("localhost",443,"localhost")),cell->{throw new AssertionError("No TLS work");},new RpcAdmission(1,98304,1,98304));
        var field=owner.getClass().getDeclaredField("channels");field.setAccessible(true);
        @SuppressWarnings("unchecked") var channels=(Map<Object,ManagedChannel>)field.get(owner);channels.put(new Object(),transport);
        try {
            var draining=(gateway?((GatewayRelayRpcClient)owner).drain():((CellRpcClient)owner).drain()).toCompletableFuture();
            assertThat(transport.shutdown).isTrue();
            if(unknown){assertThatThrownBy(()->draining.get(3,TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);return;}
            assertThat(draining).isNotDone();assertThat(transport.awaited.await(1,TimeUnit.SECONDS)).isTrue();assertThat(draining).isNotDone();
            transport.terminated.countDown();draining.get(3,TimeUnit.SECONDS);assertThat(transport.isTerminated()).isTrue();
        }finally{transport.terminated.countDown();}
    }
    private static final class Transport extends ManagedChannel {
        final CountDownLatch terminated=new CountDownLatch(1),awaited=new CountDownLatch(1);final boolean unknown;volatile boolean shutdown;
        Transport(boolean unknown){this.unknown=unknown;}
        public ManagedChannel shutdown(){shutdown=true;return this;}
        public ManagedChannel shutdownNow(){return shutdown();}
        public boolean isShutdown(){return shutdown;}
        public boolean isTerminated(){return terminated.getCount()==0;}
        public boolean awaitTermination(long timeout,TimeUnit unit)throws InterruptedException {awaited.countDown();return !unknown&&terminated.await(timeout,unit);}
        public <Q,A> ClientCall<Q,A> newCall(MethodDescriptor<Q,A> method,CallOptions options){throw new AssertionError("No replacement stream");}
        public String authority(){return "TEST_ONLY";}
    }
}
