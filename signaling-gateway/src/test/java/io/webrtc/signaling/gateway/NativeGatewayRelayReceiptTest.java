package io.webrtc.signaling.gateway;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.rpc.*;
import io.webrtc.signaling.storage.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/** Explicit TEST_ONLY transport replies; no native delivery or peer application attestation. */
class NativeGatewayRelayReceiptTest {
    @Test void originalVolatileWriteReceiptProjectsEachRelayKind()throws Exception {
        for(var type:List.of(SignalEnvelope.Type.OFFER,SignalEnvelope.Type.ANSWER,SignalEnvelope.Type.ICE_CANDIDATES,SignalEnvelope.Type.END_OF_CANDIDATES)){
            var reply=run(type,0);assertThat(reply.path("type").asText()).isEqualTo("COMMAND_RESULT");assertThat(reply.path("ackCommitted").asBoolean(true)).isFalse();
            assertThat(reply.path("result").path("status").asText()).isEqualTo("VOLATILE");assertThat(reply.path("result").path("code").asText()).isEqualTo("WRITE_COMPLETED");
            assertThat(reply.path("negotiationId").asText()).isEqualTo("3");assertThat(reply.path("iceGeneration").asText()).isEqualTo("2");assertThat(reply.path("result").path("frameType").asText()).isEqualTo(type.name());
        }
    }
    @Test void reboundOrDurableShapedReceiptCannotProjectSuccess()throws Exception {
        for(int defect=1;defect<=10;defect++){var reply=run(SignalEnvelope.Type.OFFER,defect);assertThat(reply.path("type").asText()).isEqualTo("ERROR");assertThat(reply.path("error").path("code").asText()).isEqualTo("OUTCOME_UNKNOWN");assertThat(reply.path("ackCommitted").asBoolean(true)).isFalse();}
    }
    private static ByteString encoded(RelayWriteReceipt receipt,int defect){
        String body=new String(RpcBusinessHandler.encode(receipt),java.nio.charset.StandardCharsets.UTF_8);
        if(defect==8)body+=" {}";
        if(defect==9)body=body.replace("\"callVersion\":7","\"callVersion\":7,\"callVersion\":7");
        return ByteString.copyFromUtf8(body);
    }
    private static com.fasterxml.jackson.databind.JsonNode run(SignalEnvelope.Type type,int defect)throws Exception {
        var sender=new AuthenticatedSession(new UserId("TEST_ONLY_USER"),new SessionKey("TEST_ONLY_ISSUER","TEST_ONLY_JTI"),new SessionIncarnation(UUID.randomUUID()),1,UUID.randomUUID());var boot=UUID.randomUUID();
        var route=new SessionRepository.Route(sender.userId(),sender.key(),sender.incarnation(),1,"TEST_ONLY_GW",boot,sender.connectionId(),Instant.now().plusSeconds(60),"TEST_ONLY_KEY",1);
        var gateway=new NativeSessionHandler.GatewayIdentity(route.gatewayId(),boot,"c001",1,"TEST_ONLY");var call=CallId.create("c002",1);var request=new RequestId(UUID.randomUUID());
        var command=new CallCommand(type,sender,request,call,CommandScope.call(call),null,new NegotiationId(3),new IceGeneration(2),"{}","a".repeat(64));
        var receipt=new RelayWriteReceipt(defect==1?CallId.create("c002",1):call,defect==2?new RequestId(UUID.randomUUID()):request,defect==3?SignalEnvelope.Type.ANSWER:type,7,defect==4?4:3,defect==5?3:2);
        var network=new NativeGatewayCommands.Network(){
            public CompletionStage<SessionReply> session(SessionCommand wire,Duration budget){return CompletableFuture.completedFuture(SessionReply.newBuilder().setOperationId(wire.getOperationId()).setStatus("READ").setResult(ByteString.copyFrom(RpcBusinessHandler.encode("TEST_ONLY_PROOF"))).build());}
            public CompletionStage<InternalReply> call(CellRpcServer.Operation operation,InternalCommand wire,Duration budget){assertThat(operation).isEqualTo(CellRpcServer.Operation.RELAY);return CompletableFuture.completedFuture(InternalReply.newBuilder().setOperationId(wire.getOperationId()).setCallId(wire.getCallId()).setCallVersion(defect==6?8:7).setAckCommitted(defect==7).setStatus(defect==10?"COMMITTED":"WRITE_COMPLETED").setResult(encoded(receipt,defect)).build());}
        };
        return new ObjectMapper().readTree(new NativeGatewayCommands(gateway,u->new ProofBindings.TrustedHome("c001",1,1),network,Clock.systemUTC()).execute(command,route,"TEST_ONLY_TOKEN",Duration.ofSeconds(2)).toCompletableFuture().join());
    }
}
