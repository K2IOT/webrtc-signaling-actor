package io.webrtc.signaling.rpc;

import static org.assertj.core.api.Assertions.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.*;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import io.webrtc.signaling.protocol.internal.*;
import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

class CellRpcPhysicalCompletionIT {
    static File cert(String name) { return new File(Objects.requireNonNull(CellRpcPhysicalCompletionIT.class.getResource("/test-only-pki/" + name)).getFile()); }
    @Test void actualRpcListenerDrainWaitsForNativeCleanupAfterUnknownResponse() throws Exception {
        var physical=new CompletableFuture<Void>();var admission=new RpcAdmission(1,98304,1,98304);
        var backend=new CellRpcServer.Backend(){
            public CompletionStage<InternalReply> execute(CellRpcServer.Operation op,InternalCommand c,CellRpcServer.Peer p,Duration b){return executeTracked(op,c,p,b).logical();}
            public RpcOperation<InternalReply> executeTracked(CellRpcServer.Operation op,InternalCommand c,CellRpcServer.Peer p,Duration b){
                return new RpcOperation<>(CompletableFuture.completedFuture(InternalReply.newBuilder().setOperationId(c.getOperationId()).setCallId(c.getCallId()).setErrorCode("OUTCOME_UNKNOWN").build()),physical);
            }
        };
        try(var server=new CellRpcServer("c002","test",0,RpcTlsContexts.server("test","c002",cert("ca.crt"),cert("server.crt"),cert("server.key")),admission,backend,event->CompletableFuture.failedFuture(new AssertionError())).start();
            var client=new CellRpcClient("test",Map.of("c002",new CellRpcClient.Endpoint("localhost",server.port(),"localhost")),RpcTlsContexts.clients("test",cert("ca.crt"),cert("actor.crt"),cert("actor.key")),new RpcAdmission(1,98304,1,98304))){
            var c=InternalCommand.newBuilder().setSchemaMajor(1).setDestinationCell("c002").setOperationId(UUID.randomUUID().toString()).setCallId("c001.e1."+UUID.randomUUID()).setRemainingBudgetMs(2000).setPayloadHash(com.google.protobuf.ByteString.copyFrom(new byte[32])).build();
            assertThat(client.call(CellRpcServer.Operation.RESERVE,c,Duration.ofSeconds(2)).toCompletableFuture().get(2,TimeUnit.SECONDS).getErrorCode()).isEqualTo("OUTCOME_UNKNOWN");
            assertThat(admission.inFlight(RpcAdmission.Lane.CONTROL)).isEqualTo(1);
            var snapshot=server.settleAdmitted();assertThat(snapshot.toCompletableFuture()).isNotDone();
            var drain=server.drain();assertThat(drain.toCompletableFuture()).isNotDone();
            physical.complete(null);snapshot.toCompletableFuture().get(3,TimeUnit.SECONDS);drain.toCompletableFuture().get(3,TimeUnit.SECONDS);
            assertThat(admission.inFlight(RpcAdmission.Lane.CONTROL)).isZero();
        }finally{physical.complete(null);}
    }
    @Test void actualTlsUnaryReplyKeepsTransportCreditUntilStreamEndsAndDrainRejectsNewWork() throws Exception {
        var held = new AtomicReference<StreamObserver<InternalReply>>();
        var entered = new CountDownLatch(1);
        var server = NettyServerBuilder.forPort(0)
            .sslContext(RpcTlsContexts.server("test", "c002", cert("ca.crt"), cert("server.crt"), cert("server.key")))
            .addService(ServerServiceDefinition.builder(CellIngressGrpc.SERVICE_NAME).addMethod(
                CellIngressGrpc.getExecuteCallCommandMethod().toBuilder().setType(MethodDescriptor.MethodType.SERVER_STREAMING).build(),
                ServerCalls.asyncServerStreamingCall((InternalCommand c, StreamObserver<InternalReply> reply) -> {
                    held.set(reply); reply.onNext(InternalReply.newBuilder().setOperationId(c.getOperationId()).setCallId(c.getCallId()).setStatus("READ").build()); entered.countDown();
                })).build()).build().start();
        var admission = new RpcAdmission(1, 98304, 1, 98304);
        try (var client = new CellRpcClient("test", Map.of("c002", new CellRpcClient.Endpoint("localhost", server.getPort(), "localhost")),
            RpcTlsContexts.clients("test", cert("ca.crt"), cert("actor.crt"), cert("actor.key")), admission)) {
            var c = InternalCommand.newBuilder().setDestinationCell("c002").setOperationId(UUID.randomUUID().toString())
                .setCallId("c002.e1." + UUID.randomUUID()).setRemainingBudgetMs(2000).build();
            var pending = client.callTracked(CellRpcServer.Operation.EXECUTE, c, Duration.ofSeconds(2));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(pending.logical().toCompletableFuture().get(1, TimeUnit.SECONDS).getStatus()).isEqualTo("READ");
            assertThat(admission.inFlight(RpcAdmission.Lane.CONTROL)).isEqualTo(1);
            assertThat(pending.physicalCompletion().toCompletableFuture()).isNotDone();
            assertThat(client.call(CellRpcServer.Operation.EXECUTE, c, Duration.ofSeconds(1)).toCompletableFuture().join().getErrorCode()).isEqualTo("OVERLOADED");
            var snapshot = client.settleAdmitted();assertThat(snapshot.toCompletableFuture()).isNotDone();
            var drain = client.drain();
            assertThat(drain.toCompletableFuture()).isNotDone();
            assertThat(client.call(CellRpcServer.Operation.EXECUTE, c, Duration.ofSeconds(1)).toCompletableFuture().join().getErrorCode()).isEqualTo("OUTCOME_UNKNOWN");
            held.get().onCompleted();
            pending.physicalCompletion().toCompletableFuture().get(2, TimeUnit.SECONDS);
            drain.toCompletableFuture().get(2, TimeUnit.SECONDS);snapshot.toCompletableFuture().get(2,TimeUnit.SECONDS);
            assertThat(admission.inFlight(RpcAdmission.Lane.CONTROL)).isZero();
        } finally { server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS); }
    }
    @ParameterizedTest @ValueSource(strings = {"SESSION", "DELIVER"})
    void sessionAndEventStreamsAlsoKeepCreditAfterReplyUntilTerminalCallback(String kind) throws Exception {
        var held = new AtomicReference<StreamObserver<?>>();
        var service = kind.equals("SESSION")
            ? ServerServiceDefinition.builder(SessionIngressGrpc.SERVICE_NAME).addMethod(
                SessionIngressGrpc.getMutateSessionMethod().toBuilder().setType(MethodDescriptor.MethodType.SERVER_STREAMING).build(),
                ServerCalls.asyncServerStreamingCall((SessionCommand c, StreamObserver<SessionReply> reply) -> {
                    held.set(reply); reply.onNext(SessionReply.newBuilder().setOperationId(c.getOperationId()).setStatus("READ").build());
                })).build()
            : ServerServiceDefinition.builder(CellIngressGrpc.SERVICE_NAME).addMethod(
                CellIngressGrpc.getDeliverControlEventMethod().toBuilder().setType(MethodDescriptor.MethodType.SERVER_STREAMING).build(),
                ServerCalls.asyncServerStreamingCall((ControlEvent c, StreamObserver<InternalReply> reply) -> {
                    held.set(reply); reply.onNext(InternalReply.newBuilder().setOperationId(c.getEventId()).setCallId(c.getCallId()).setStatus("WRITE_COMPLETED").build());
                })).build();
        var server = NettyServerBuilder.forPort(0)
            .sslContext(RpcTlsContexts.server("test", "c002", cert("ca.crt"), cert("server.crt"), cert("server.key")))
            .addService(service).build().start();
        var admission = new RpcAdmission(1, 98304, 1, 98304);
        try (var client = new CellRpcClient("test", Map.of("c002", new CellRpcClient.Endpoint("localhost", server.getPort(), "localhost")),
            RpcTlsContexts.clients("test", cert("ca.crt"), cert("actor.crt"), cert("actor.key")), admission)) {
            String operation = UUID.randomUUID().toString();
            RpcOperation<?> pending = kind.equals("SESSION")
                ? client.sessionTracked(SessionCommand.newBuilder().setDestinationCell("c002").setOperationId(operation).setType("READ_PROOF").setRemainingBudgetMs(2000).build(), Duration.ofSeconds(2))
                : client.deliverTracked("c002", ControlEvent.newBuilder().setEventId(operation).setCallId("c002.e1." + UUID.randomUUID()).setType("READY").build(), Duration.ofSeconds(2));
            pending.logical().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertThat(admission.inFlight(RpcAdmission.Lane.CONTROL)).isEqualTo(1);
            assertThat(pending.physicalCompletion().toCompletableFuture()).isNotDone();
            // An error after an already delivered message retires transport without changing that logical reply.
            held.get().onError(Status.UNAVAILABLE.asRuntimeException());
            pending.physicalCompletion().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertThat(admission.inFlight(RpcAdmission.Lane.CONTROL)).isZero();
            client.drain().toCompletableFuture().get(2, TimeUnit.SECONDS);
        } finally { server.shutdownNow().awaitTermination(3, TimeUnit.SECONDS); }
    }
}
