package io.webrtc.signaling.rpc;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.actors.user.*;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;
import java.security.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class RpcActorBridgeTest {
    @Test void bridgeValidatesSignedActionAndFullRpcIdentityBeforeSubmittingTypedActorWork()throws Exception {
        var key=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var proofs=new HomeAuthorizationProof("c001","test",key.getPrivate(),Map.of("c001/test",key.getPublic()));Instant now=Instant.now();UUID operation=UUID.randomUUID(),owner=UUID.randomUUID();var call=new CallId(CrossCellSagaIT.CALL);
        var unsigned=new HomeParticipationService.Request(new UserId("bob"),call,UUID.randomUUID(),"a".repeat(64),1,HomeParticipationService.Phase.RINGING,new HomeParticipationService.Grant("c001",1,1,685,2,1,operation,now,now.plusSeconds(5),"UNSIGNED"));String hash=ProofBindings.homeIntent(unsigned);
        String signed=proofs.issue(new HomeAuthorizationProof.Claims(1,"COORDINATOR_GRANT","c001","c002",operation,call,unsigned.user(),null,null,0,1,1,1,685,2,1,owner,null,0,null,1,0,hash,now,now.plusSeconds(5),1,null));
        var request=new HomeParticipationService.Request(unsigned.user(),call,unsigned.acquireOperation(),unsigned.payloadHash(),1,unsigned.phase(),new HomeParticipationService.Grant("c001",1,1,685,2,1,operation,now,now.plusSeconds(5),signed,1));
        var submitted=new AtomicInteger();RpcBusinessHandler.ActorIngress ingress=new RpcBusinessHandler.ActorIngress(){
            public CompletionStage<UserCommand.Result> user(UserCommand.Operation op,Instant deadline,int bytes){assertThat(op).isEqualTo(new UserCommand.Reserve(request));submitted.incrementAndGet();return CompletableFuture.completedFuture(new UserCommand.Result(UserCommand.Code.RESERVED,null,null,null));}
            public CompletionStage<CallCommandService.Outcome> call(CallCommand c,String p,Instant d,int b){throw new AssertionError();}
            public CompletionStage<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition t,Instant d,int b){throw new AssertionError();}
        };
        var bridge=new RpcBusinessHandler("c002",ingress,new ProofBindings(proofs,Clock.systemUTC()),proofs,user->new ProofBindings.TrustedHome("c002",1,1),read->CompletableFuture.failedFuture(new UnsupportedOperationException()),relay->CompletableFuture.failedFuture(new UnsupportedOperationException()),Clock.systemUTC());
        byte[] payload=RpcBusinessHandler.encode(new RpcBusinessHandler.UserPayload(new UserCommand.Reserve(request),null));
        var command=InternalCommand.newBuilder().setSchemaMajor(1).setType("ReserveUser").setOperationId(operation.toString()).setCallId(call.value()).setCommandScope(CommandScope.call(call).value()).setDestinationCell("c002").setRemainingBudgetMs(2000).setPayloadHash(ByteString.copyFrom(HexFormat.of().parseHex(hash))).setPayload(ByteString.copyFrom(payload)).setAuthority(GroupAuthority.newBuilder().setCellId("c001").setStorageEpoch(1).setOwnershipHashVersion(1).setGroupId(685).setGroupEpoch(2).setLeaseSequence(1).setOwnerIncarnation(ByteString.copyFrom(ByteBuffer.allocate(16).putLong(owner.getMostSignificantBits()).putLong(owner.getLeastSignificantBits()).array()))).build();
        assertThat(bridge.execute(CellRpcServer.Operation.RESERVE,command,new CellRpcServer.Peer("c001","actor"),Duration.ofSeconds(2)).toCompletableFuture().join().getAckCommitted()).isTrue();assertThat(submitted).hasValue(1);
        assertThat(bridge.execute(CellRpcServer.Operation.RELEASE,command.toBuilder().setType("ReleaseIfCallVersion").build(),new CellRpcServer.Peer("c001","actor"),Duration.ofSeconds(2)).toCompletableFuture().join().getAckCommitted()).isFalse();
        assertThat(bridge.execute(CellRpcServer.Operation.RESERVE,command,new CellRpcServer.Peer("c003","actor"),Duration.ofSeconds(2)).toCompletableFuture().join().getAckCommitted()).isFalse();
        assertThat(bridge.execute(CellRpcServer.Operation.RESERVE,command.toBuilder().setAuthority(command.getAuthority().toBuilder().setOwnerIncarnation(ByteString.copyFrom(new byte[16]))).build(),new CellRpcServer.Peer("c001","actor"),Duration.ofSeconds(2)).toCompletableFuture().join().getAckCommitted()).isFalse();assertThat(submitted).hasValue(1);
    }
}
