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
    @Test void independentPhysicalCompletionIsPreservedForAnAdmittedSessionOperation(){
        var logical=new CompletableFuture<SessionReply>();var physical=new CompletableFuture<Void>();
        var handler=new NativeSessionHandler("c002",1,(peer,gateway)->peer.workloadId().equals(gateway.gatewayId()),(request,budget)->new RpcOperation<>(logical,physical));
        var request=new NativeSessionHandler.Request("BOOT_START",new NativeSessionHandler.GatewayIdentity("gw-1",UUID.randomUUID(),"c002",1,"TEST_ONLY_REGION"),null,null,null,1,1,UUID.randomUUID());
        var command=SessionCommand.newBuilder().setSchemaMajor(1).setOperationId(request.operation().toString()).setDestinationCell("c002").setType(request.type()).setRemainingBudgetMs(2000).setPayload(ByteString.copyFrom(RpcBusinessHandler.encode(request))).build();
        var result=handler.execute(command,new CellRpcServer.Peer("c002","gateway","gw-1"),Duration.ofSeconds(2));logical.complete(SessionReply.newBuilder().setOperationId(command.getOperationId()).setStatus("UNKNOWN").build());
        assertThat(result.logical().toCompletableFuture()).isDone();assertThat(result.physicalCompletion().toCompletableFuture()).isNotDone();physical.complete(null);assertThat(result.physicalCompletion().toCompletableFuture()).isDone();
    }
}
