package io.webrtc.signaling.rpc;

import static org.assertj.core.api.Assertions.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCalls;
import io.webrtc.signaling.protocol.internal.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Actual mTLS transport; the source factory pause is an explicit TEST_ONLY fault. */
class CellRpcConstructionDeadlineIT {
    @ParameterizedTest @ValueSource(strings={"EXECUTE","RELAY","SESSION","DELIVER"})
    void expiredOriginalBudgetCannotDispatchAfterColdChannelConstruction(String kind)throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var delivered=new AtomicInteger();
        var tls=RpcTlsContexts.clients("test",CellRpcPhysicalCompletionIT.cert("ca.crt"),CellRpcPhysicalCompletionIT.cert("actor.crt"),CellRpcPhysicalCompletionIT.cert("actor.key")).context("c002");
        var service=ServerServiceDefinition.builder(CellIngressGrpc.SERVICE_NAME)
            .addMethod(CellIngressGrpc.getExecuteCallCommandMethod(),ServerCalls.asyncUnaryCall((InternalCommand c,io.grpc.stub.StreamObserver<InternalReply> reply)->{delivered.incrementAndGet();reply.onNext(reply(c));reply.onCompleted();}))
            .addMethod(CellIngressGrpc.getRelayNegotiationMethod(),ServerCalls.asyncUnaryCall((InternalCommand c,io.grpc.stub.StreamObserver<InternalReply> reply)->{delivered.incrementAndGet();reply.onNext(reply(c));reply.onCompleted();}))
            .addMethod(CellIngressGrpc.getDeliverControlEventMethod(),ServerCalls.asyncUnaryCall((ControlEvent c,io.grpc.stub.StreamObserver<InternalReply> reply)->{delivered.incrementAndGet();reply.onNext(InternalReply.newBuilder().setOperationId(c.getEventId()).setCallId(c.getCallId()).setStatus("WRITE_COMPLETED").build());reply.onCompleted();})).build();
        var sessions=ServerServiceDefinition.builder(SessionIngressGrpc.SERVICE_NAME)
            .addMethod(SessionIngressGrpc.getMutateSessionMethod(),ServerCalls.asyncUnaryCall((SessionCommand c,io.grpc.stub.StreamObserver<SessionReply> reply)->{delivered.incrementAndGet();reply.onNext(SessionReply.newBuilder().setOperationId(c.getOperationId()).setStatus("COMMITTED").build());reply.onCompleted();})).build();
        var server=NettyServerBuilder.forPort(0).sslContext(RpcTlsContexts.server("test","c002",CellRpcPhysicalCompletionIT.cert("ca.crt"),CellRpcPhysicalCompletionIT.cert("server.crt"),CellRpcPhysicalCompletionIT.cert("server.key"))).addService(service).addService(sessions).build().start();
        var admission=new RpcAdmission(1,98304,1,98304);
        try(var tasks=Executors.newVirtualThreadPerTaskExecutor();var client=new CellRpcClient("test",Map.of("c002",new CellRpcClient.Endpoint("localhost",server.getPort(),"localhost")),cell->{
            entered.countDown();try{if(!release.await(3,TimeUnit.SECONDS))throw new IllegalStateException("TEST_ONLY_FACTORY_RELEASE_MISSING");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IllegalStateException(interrupted);}return tls;
        },admission)){
            String operation=UUID.randomUUID().toString(),call="c002.e1."+UUID.randomUUID();var budget=Duration.ofSeconds(1);
            var pending=tasks.submit(()->{
                if(kind.equals("SESSION")){var work=client.sessionTracked(SessionCommand.newBuilder().setDestinationCell("c002").setOperationId(operation).setRemainingBudgetMs(1000).build(),budget);var code=work.logical().toCompletableFuture().get(3,TimeUnit.SECONDS).getErrorCode();work.physicalCompletion().toCompletableFuture().get(3,TimeUnit.SECONDS);return code;}
                RpcOperation<InternalReply> work;
                if(kind.equals("DELIVER"))work=client.deliverTracked("c002",ControlEvent.newBuilder().setEventId(operation).setCallId(call).build(),budget);
                else work=client.callTracked(CellRpcServer.Operation.valueOf(kind),InternalCommand.newBuilder().setDestinationCell("c002").setOperationId(operation).setCallId(call).setRemainingBudgetMs(1000).build(),budget);
                var code=work.logical().toCompletableFuture().get(3,TimeUnit.SECONDS).getErrorCode();work.physicalCompletion().toCompletableFuture().get(3,TimeUnit.SECONDS);return code;
            });
            assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
            assertThat(admission.inFlight(kind.equals("RELAY")?RpcAdmission.Lane.RELAY:RpcAdmission.Lane.CONTROL)).isEqualTo(1);
            java.util.concurrent.locks.LockSupport.parkNanos(Duration.ofMillis(1200).toNanos());release.countDown();
            assertThat(pending.get(4,TimeUnit.SECONDS)).isEqualTo("OUTCOME_UNKNOWN");
            assertThat(delivered).hasValue(0);
            assertThat(admission.inFlight(RpcAdmission.Lane.CONTROL)+admission.inFlight(RpcAdmission.Lane.RELAY)).isZero();
        }finally{release.countDown();server.shutdownNow().awaitTermination(3,TimeUnit.SECONDS);}
    }
    private static InternalReply reply(InternalCommand c){return InternalReply.newBuilder().setOperationId(c.getOperationId()).setCallId(c.getCallId()).setStatus("WRITE_COMPLETED").build();}
}
