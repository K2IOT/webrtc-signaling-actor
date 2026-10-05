package io.webrtc.signaling.rpc;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.actors.user.UserCommand;
import com.google.protobuf.ByteString;
import java.time.*;
import java.util.*;
import java.security.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/** TEST_ONLY producer lifecycle signals; these are not native authorization or delivery evidence. */
class NativeRelayPhysicalTest {
    @Test void logicalOnlyRelayFactoryCannotInventIndependentPhysicalRetirement()throws Exception {
        var started=new AtomicInteger();var handler=handler(request->{started.incrementAndGet();return CompletableFuture.completedFuture(delivered(request.command()));});
        var operation=run(handler);
        assertThat(started).hasValue(0);assertThat(operation.logical().toCompletableFuture().join().getErrorCode()).isEqualTo("UNSUPPORTED_OPERATION");
        operation.physicalCompletion().toCompletableFuture().join();
    }
    @Test void nativeRelayRetainsItsOriginalPhysicalReceiptAfterLogicalReply()throws Exception {
        var physical=new CompletableFuture<Void>();var handler=handler(request->{throw new AssertionError();});
        handler.nativeRelay(request->new RpcOperation<>(CompletableFuture.completedFuture(delivered(request.command())),physical));
        var operation=run(handler);assertThat(operation.logical().toCompletableFuture().join().getAckCommitted()).isFalse();assertThat(operation.physicalCompletion().toCompletableFuture()).isNotDone();
        physical.complete(null);operation.physicalCompletion().toCompletableFuture().join();
        assertThatThrownBy(()->handler.nativeRelay(request->new RpcOperation<>(CompletableFuture.completedFuture(delivered(request.command())),physical))).isInstanceOf(IllegalStateException.class);
    }
    @Test void nativeRelayCannotTurnCleanupErrorIntoPhysicalSuccess()throws Exception {
        var physical=new CompletableFuture<Void>();var handler=handler(request->{throw new AssertionError();});
        handler.nativeRelay(request->new RpcOperation<>(CompletableFuture.completedFuture(delivered(request.command())),physical));
        var operation=run(handler);physical.completeExceptionally(new IllegalStateException("TEST_ONLY_UNKNOWN_PHYSICAL_CLEANUP"));
        assertThat(operation.physicalCompletion().toCompletableFuture()).isCompletedExceptionally();
    }
    @Test void throwingNativeRelayFactoryKeepsItsUnprovedPhysicalEffectsUnknown()throws Exception {
        var started=new AtomicInteger();var handler=handler(request->{throw new AssertionError();});
        handler.nativeRelay(request->{started.incrementAndGet();throw new IllegalStateException("TEST_ONLY_FACTORY_EFFECTS_UNKNOWN");});
        var operation=run(handler);assertThat(started).hasValue(1);assertThat(operation.logical().toCompletableFuture().join().getAckCommitted()).isFalse();
        assertThat(operation.physicalCompletion().toCompletableFuture()).isNotDone();
    }
    private static RpcOperation<InternalReply> run(RpcBusinessHandler handler){
        var call=CallId.create("c001",1);var command=InternalCommand.newBuilder().setSchemaMajor(1).setType("OFFER").setCallId(call.value()).setOperationId(UUID.randomUUID().toString()).setCommandScope(CommandScope.call(call).value()).setDestinationCell("c001").setPayloadHash(ByteString.copyFrom(new byte[32])).build();
        return handler.executeTracked(CellRpcServer.Operation.RELAY,command,new CellRpcServer.Peer("c001","gateway","TEST_ONLY"),Duration.ofSeconds(1));
    }
    private static InternalReply delivered(InternalCommand command){return InternalReply.newBuilder().setOperationId(command.getOperationId()).setCallId(command.getCallId()).setStatus("TEST_ONLY_VOLATILE_REPLY").build();}
    private static RpcBusinessHandler handler(Function<RpcBusinessHandler.RelayRequest,CompletionStage<InternalReply>> legacy)throws Exception {
        var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var proofs=new HomeAuthorizationProof("c001","test",keys.getPrivate(),Map.of("c001/test",keys.getPublic()));
        var actors=new RpcBusinessHandler.ActorIngress(){
            public CompletionStage<UserCommand.Result> user(UserCommand.Operation operation,Instant deadline,int bytes){throw new AssertionError();}
            public CompletionStage<CallCommandService.Outcome> call(CallCommand command,String proof,Instant deadline,int bytes){throw new AssertionError();}
            public CompletionStage<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition transition,Instant deadline,int bytes){throw new AssertionError();}
        };
        return new RpcBusinessHandler("c001",actors,new ProofBindings(proofs,Clock.systemUTC()),proofs,user->new ProofBindings.TrustedHome("c001",1,1),read->{throw new AssertionError();},legacy,Clock.systemUTC());
    }
}
