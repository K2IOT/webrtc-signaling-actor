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
        handler.backend().nativeRelay(request->new RpcOperation<>(CompletableFuture.completedFuture(delivered(request.command())),physical));
        var operation=run(handler);assertThat(operation.logical().toCompletableFuture().join().getAckCommitted()).isFalse();assertThat(operation.physicalCompletion().toCompletableFuture()).isNotDone();
        physical.complete(null);operation.physicalCompletion().toCompletableFuture().join();
        assertThatThrownBy(()->handler.backend().nativeRelay(request->new RpcOperation<>(CompletableFuture.completedFuture(delivered(request.command())),physical))).isInstanceOf(IllegalStateException.class);
    }
    @Test void nativeRelayCannotTurnCleanupErrorIntoPhysicalSuccess()throws Exception {
        var physical=new CompletableFuture<Void>();var handler=handler(request->{throw new AssertionError();});
        handler.backend().nativeRelay(request->new RpcOperation<>(CompletableFuture.completedFuture(delivered(request.command())),physical));
        var operation=run(handler);physical.completeExceptionally(new IllegalStateException("TEST_ONLY_UNKNOWN_PHYSICAL_CLEANUP"));
        assertThat(operation.physicalCompletion().toCompletableFuture()).isCompletedExceptionally();
    }
    @Test void throwingNativeRelayFactoryKeepsItsUnprovedPhysicalEffectsUnknown()throws Exception {
        var started=new AtomicInteger();var handler=handler(request->{throw new AssertionError();});
        handler.backend().nativeRelay(request->{started.incrementAndGet();throw new IllegalStateException("TEST_ONLY_FACTORY_EFFECTS_UNKNOWN");});
        var operation=run(handler);assertThat(started).hasValue(1);assertThat(operation.logical().toCompletableFuture().join().getAckCommitted()).isFalse();
        assertThat(operation.physicalCompletion().toCompletableFuture()).isNotDone();
    }
    private static RpcOperation<InternalReply> run(Fixture handler){
        return handler.backend().executeTracked(CellRpcServer.Operation.RELAY,wire(handler),new CellRpcServer.Peer("c001","gateway","TEST_ONLY"),Duration.ofSeconds(1));
    }
    private record Fixture(RpcBusinessHandler backend,HomeAuthorizationProof proofs,AtomicInteger controls){}
    private static ByteString uuid(UUID id){return ByteString.copyFrom(java.nio.ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array());}
    private static InternalCommand wire(Fixture f){
        var now=Instant.now();var call=CallId.create("c001",1);var route=new SessionRepository.Route(new UserId("TEST_ONLY_USER"),new SessionKey("TEST_ONLY","TEST_ONLY_JTI"),new SessionIncarnation(UUID.randomUUID()),1,"TEST_ONLY",UUID.randomUUID(),UUID.randomUUID(),now.plusSeconds(60),"TEST_ONLY_KEY",1);
        var sender=new AuthenticatedSession(route.user(),route.key(),route.incarnation(),1,route.connectionId());
        var command=new CallCommand(SignalEnvelope.Type.OFFER,sender,new RequestId(UUID.randomUUID()),call,CommandScope.call(call),null,new NegotiationId(1),new IceGeneration(1),"{}","a".repeat(64));
        String proof=f.proofs().relaySessionProofs().issue(new SessionRegistryService.SessionProofView(route,now,now.plusSeconds(4),"c001",1,1),command);
        var identity=SessionIdentity.newBuilder().setIssuer(sender.key().issuer()).setJti(sender.key().jti()).setUserId(sender.userId().value()).setIncarnation(uuid(sender.incarnation().value())).setConnectionGeneration(1).setConnectionId(uuid(sender.connectionId())).build();
        return InternalCommand.newBuilder().setSchemaMajor(1).setType("OFFER").setCallId(call.value()).setOperationId(command.requestId().value().toString()).setCommandScope(command.scope().value()).setDestinationCell("c001").setSender(identity).setPayloadHash(ByteString.copyFrom(HexFormat.of().parseHex(command.intentHash()))).setPayload(ByteString.copyFrom(RpcBusinessHandler.encode(new RpcBusinessHandler.CallPayload(command,proof)))).build();
    }
    @Test void malformedOrReboundRelayNeverStartsTheNativeProducer()throws Exception{
        var starts=new AtomicInteger();var fixture=handler(r->{throw new AssertionError();});fixture.backend().nativeRelay(r->{starts.incrementAndGet();return new RpcOperation<>(CompletableFuture.completedFuture(delivered(r.command())),CompletableFuture.completedFuture(null));});
        var valid=wire(fixture);
        for(var bad:List.of(valid.toBuilder().setPayload(ByteString.copyFromUtf8("{}")).build(),valid.toBuilder().setOperationId(UUID.randomUUID().toString()).build(),valid.toBuilder().setType("HANGUP").build(),valid.toBuilder().setPayloadHash(ByteString.copyFrom(new byte[32])).build(),valid.toBuilder().setCommandScope("INVITE").build(),valid.toBuilder().setSender(valid.getSender().toBuilder().setConnectionGeneration(2)).build())){
            var work=fixture.backend().executeTracked(CellRpcServer.Operation.RELAY,bad,new CellRpcServer.Peer("c001","gateway","TEST_ONLY"),Duration.ofSeconds(1));
            assertThat(work.logical().toCompletableFuture().join().getErrorCode()).isEqualTo("UNAUTHORIZED");assertThat(work.physicalCompletion().toCompletableFuture()).isDone();
        }
        assertThat(starts).hasValue(0);
    }
    @Test void relaySourceMustBeTheIndependentlyBoundHomeWorkloadCell()throws Exception{
        var starts=new AtomicInteger();var fixture=handler(r->{throw new AssertionError();});fixture.backend().nativeRelay(r->{starts.incrementAndGet();return new RpcOperation<>(CompletableFuture.completedFuture(delivered(r.command())),CompletableFuture.completedFuture(null));});
        for(var peer:List.of(new CellRpcServer.Peer("c002","gateway","TEST_ONLY"),new CellRpcServer.Peer("c001","unrecognized","TEST_ONLY"))){
            var work=fixture.backend().executeTracked(CellRpcServer.Operation.RELAY,wire(fixture),peer,Duration.ofSeconds(1));assertThat(work.logical().toCompletableFuture().join().getErrorCode()).isEqualTo("UNAUTHORIZED");
        }
        assertThat(starts).hasValue(0);
    }
    @Test void secondJsonRootCannotHideBehindAnOtherwiseBoundRelayPayload()throws Exception{
        var fixture=handler(r->{throw new AssertionError();});var starts=new AtomicInteger();fixture.backend().nativeRelay(r->{starts.incrementAndGet();return new RpcOperation<>(CompletableFuture.completedFuture(delivered(r.command())),CompletableFuture.completedFuture(null));});
        var original=wire(fixture);var extra=original.toBuilder().setPayload(ByteString.copyFromUtf8(original.getPayload().toStringUtf8()+"{}")).build();
        var work=fixture.backend().executeTracked(CellRpcServer.Operation.RELAY,extra,new CellRpcServer.Peer("c001","gateway","TEST_ONLY"),Duration.ofSeconds(1));
        assertThat(work.logical().toCompletableFuture().join().getErrorCode()).isEqualTo("UNAUTHORIZED");assertThat(starts).hasValue(0);
    }
    @Test void volatilePurposeCannotEnterTheDurableExecuteLane()throws Exception{
        var fixture=handler(r->{throw new AssertionError();});
        var work=fixture.backend().executeTracked(CellRpcServer.Operation.EXECUTE,wire(fixture),new CellRpcServer.Peer("c001","gateway","TEST_ONLY"),Duration.ofSeconds(1));
        assertThat(work.logical().toCompletableFuture().join().getErrorCode()).isEqualTo("UNAUTHORIZED");assertThat(fixture.controls()).hasValue(0);
    }
    private static InternalReply delivered(InternalCommand command){return InternalReply.newBuilder().setOperationId(command.getOperationId()).setCallId(command.getCallId()).setStatus("TEST_ONLY_VOLATILE_REPLY").build();}
    private static Fixture handler(Function<RpcBusinessHandler.RelayRequest,CompletionStage<InternalReply>> legacy)throws Exception {
        var keys=KeyPairGenerator.getInstance("Ed25519").generateKeyPair();var proofs=new HomeAuthorizationProof("c001","test",keys.getPrivate(),Map.of("c001/test",keys.getPublic()));
        var controls=new AtomicInteger();
        var actors=new RpcBusinessHandler.ActorIngress(){
            public CompletionStage<UserCommand.Result> user(UserCommand.Operation operation,Instant deadline,int bytes){throw new AssertionError();}
            public CompletionStage<CallCommandService.Outcome> call(CallCommand command,String proof,Instant deadline,int bytes){controls.incrementAndGet();return CompletableFuture.completedFuture(new CallCommandService.Outcome("FINAL","TEST_ONLY_UNAUTHORIZED_DISPATCH",command.callId(),1,"CONNECTING",List.of()));}
            public CompletionStage<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition transition,Instant deadline,int bytes){throw new AssertionError();}
        };
        return new Fixture(new RpcBusinessHandler("c001",actors,new ProofBindings(proofs,Clock.systemUTC()),proofs,user->new ProofBindings.TrustedHome("c001",1,1),read->{throw new AssertionError();},legacy,Clock.systemUTC()),proofs,controls);
    }
}
