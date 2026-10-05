package io.webrtc.signaling.rpc;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.internal.*;
import com.google.protobuf.ByteString;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class NativeSessionHandlerTest {
    record PreviousRequest(String type,NativeSessionHandler.GatewayIdentity gateway,String token,io.webrtc.signaling.storage.SessionRepository.Route route,UUID connection,long directoryEpoch,long renewalSequence,UUID operation) {}
    @Test void ordinarySessionRequestRemainsReadableByTheStrictPreviousMinorDecoder()throws Exception{
        var request=new NativeSessionHandler.Request("BOOT_START",new NativeSessionHandler.GatewayIdentity("gw-1",UUID.randomUUID(),"c002",1,"TEST_ONLY_REGION"),null,null,null,1,1,UUID.randomUUID());
        var previous=new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules().enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        assertThat(previous.readValue(RpcBusinessHandler.encode(request),PreviousRequest.class).operation()).isEqualTo(request.operation());
    }

    @Test void nativeSessionIngressRejectsWorkloadMismatchAndCrossCellBootBeforeTokenOrActorWork(){
        var calls=new AtomicInteger();NativeSessionHandler.Operations operations=(request,budget)->{calls.incrementAndGet();throw new AssertionError();};
        var handler=new NativeSessionHandler("c002",1,(peer,gateway)->peer.workloadId().equals(gateway.gatewayId()),operations);
        var gateway=new NativeSessionHandler.GatewayIdentity("gw-1",UUID.randomUUID(),"c002",1,"TEST_ONLY_REGION");var request=new NativeSessionHandler.Request("BOOT_START",gateway,null,null,null,1,1,UUID.randomUUID());var command=SessionCommand.newBuilder().setSchemaMajor(1).setOperationId(request.operation().toString()).setDestinationCell("c002").setType("BOOT_START").setRemainingBudgetMs(2000).setPayload(ByteString.copyFrom(RpcBusinessHandler.encode(request))).build();
        assertThat(handler.execute(command,new CellRpcServer.Peer("c002","gateway","gw-other"),Duration.ofSeconds(2)).logical().toCompletableFuture().join().getAckCommitted()).isFalse();
        assertThat(handler.execute(command,new CellRpcServer.Peer("c001","gateway","gw-1"),Duration.ofSeconds(2)).logical().toCompletableFuture().join().getAckCommitted()).isFalse();
        assertThat(handler.execute(command,new CellRpcServer.Peer("c002","actor","gw-1"),Duration.ofSeconds(2)).logical().toCompletableFuture().join().getAckCommitted()).isFalse();assertThat(calls).hasValue(0);
    }
    @Test void reusableRelayPurposeRequiresMetadataOnlyAndKeepsCriticalReadPurposeClosed(){
        var count=new AtomicInteger();var cleanup=new CompletableFuture<Void>();
        var handler=new NativeSessionHandler("c001",1,(p,g)->true,(request,budget)->{count.incrementAndGet();return new RpcOperation<>(CompletableFuture.completedFuture(SessionReply.newBuilder().setStatus("READ").build()),cleanup);});
        var fixture=new RelaySessionAuthorizationProofTest();var r=fixture.route;
        var g=new NativeSessionHandler.GatewayIdentity(r.gatewayId(),r.bootId(),"c001",1,"TEST_ONLY");
        var relay=fixture.command(io.webrtc.signaling.protocol.SignalEnvelope.Type.OFFER,fixture.sender,fixture.call,1,1,"{}");
        java.util.function.BiFunction<String,io.webrtc.signaling.protocol.CallCommand,RpcOperation<SessionReply>> invoke=(type,c)->{
            var request=new NativeSessionHandler.Request(type,g,"TEST_ONLY_TOKEN",r,null,1,0,c.requestId().value(),c);
            var wire=SessionCommand.newBuilder().setSchemaMajor(1).setDestinationCell("c001").setOperationId(request.operation().toString()).setType(type).setRemainingBudgetMs(2000).setPayload(ByteString.copyFrom(RpcBusinessHandler.encode(request))).build();
            return handler.execute(wire,new CellRpcServer.Peer("c001","gateway",g.gatewayId()),Duration.ofSeconds(2));
        };
        assertThat(invoke.apply("READ_PROOF",relay).logical().toCompletableFuture().join().getErrorCode()).isEqualTo("UNAUTHORIZED");
        assertThat(invoke.apply("READ_RELAY_PROOF",fixture.command(io.webrtc.signaling.protocol.SignalEnvelope.Type.HANGUP,fixture.sender,fixture.call,1,1,"{}")).logical().toCompletableFuture().join().getErrorCode()).isEqualTo("UNAUTHORIZED");
        assertThat(invoke.apply("READ_RELAY_PROOF",fixture.command(io.webrtc.signaling.protocol.SignalEnvelope.Type.OFFER,fixture.sender,fixture.call,1,1,"{\"sdp\":true}")).logical().toCompletableFuture().join().getErrorCode()).isEqualTo("UNAUTHORIZED");
        var admitted=invoke.apply("READ_RELAY_PROOF",relay);assertThat(admitted.logical().toCompletableFuture().join().getStatus()).isEqualTo("READ");
        assertThat(admitted.physicalCompletion().toCompletableFuture()).isNotDone();assertThat(count).hasValue(1);cleanup.complete(null);
        assertThat(admitted.physicalCompletion().toCompletableFuture()).isDone();
    }
    @Test void secondJsonRootAfterRelayMetadataIsRejectedBeforeProducerAdmission(){
        var count=new AtomicInteger();var fixture=new RelaySessionAuthorizationProofTest();var route=fixture.route;
        var g=new NativeSessionHandler.GatewayIdentity(route.gatewayId(),route.bootId(),"c001",1,"TEST_ONLY");
        var command=fixture.command(io.webrtc.signaling.protocol.SignalEnvelope.Type.OFFER,fixture.sender,fixture.call,1,1,"{}");
        var request=new NativeSessionHandler.Request("READ_RELAY_PROOF",g,"TEST_ONLY_TOKEN",route,null,1,0,command.requestId().value(),command);
        var wire=SessionCommand.newBuilder().setSchemaMajor(1).setDestinationCell("c001").setType(request.type()).setOperationId(request.operation().toString()).setRemainingBudgetMs(1000).setPayload(ByteString.copyFromUtf8(new String(RpcBusinessHandler.encode(request),java.nio.charset.StandardCharsets.UTF_8)+"{}")).build();
        var handler=new NativeSessionHandler("c001",1,(p,gateway)->true,(r,b)->{count.incrementAndGet();return new RpcOperation<>(CompletableFuture.completedFuture(SessionReply.newBuilder().setStatus("READ").build()),CompletableFuture.completedFuture(null));});
        assertThat(handler.execute(wire,new CellRpcServer.Peer("c001","gateway",g.gatewayId()),Duration.ofSeconds(1)).logical().toCompletableFuture().join().getErrorCode()).isEqualTo("UNAUTHORIZED");assertThat(count).hasValue(0);
    }
    @Test void independentPhysicalCompletionIsPreservedForAnAdmittedSessionOperation(){
        var logical=new CompletableFuture<SessionReply>();var physical=new CompletableFuture<Void>();
        var handler=new NativeSessionHandler("c002",1,(peer,gateway)->peer.workloadId().equals(gateway.gatewayId()),(request,budget)->new RpcOperation<>(logical,physical));
        var request=new NativeSessionHandler.Request("BOOT_START",new NativeSessionHandler.GatewayIdentity("gw-1",UUID.randomUUID(),"c002",1,"TEST_ONLY_REGION"),null,null,null,1,1,UUID.randomUUID());
        var command=SessionCommand.newBuilder().setSchemaMajor(1).setOperationId(request.operation().toString()).setDestinationCell("c002").setType(request.type()).setRemainingBudgetMs(2000).setPayload(ByteString.copyFrom(RpcBusinessHandler.encode(request))).build();
        var result=handler.execute(command,new CellRpcServer.Peer("c002","gateway","gw-1"),Duration.ofSeconds(2));logical.complete(SessionReply.newBuilder().setOperationId(command.getOperationId()).setStatus("UNKNOWN").build());
        assertThat(result.logical().toCompletableFuture()).isDone();assertThat(result.physicalCompletion().toCompletableFuture()).isNotDone();physical.complete(null);assertThat(result.physicalCompletion().toCompletableFuture()).isDone();
    }
}
