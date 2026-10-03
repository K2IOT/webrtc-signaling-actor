package io.webrtc.signaling.rpc;
import io.webrtc.signaling.actors.user.UserCommand;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.HomeParticipationService.*;
import com.google.protobuf.ByteString;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
/** Transport orchestration holds no SQL locks. Each effect asks the current entity for a native sealed grant. */
public final class NativeSagaEffects implements CrossCellSaga.Effects {
    public sealed interface Step permits HomeStep,CoordinatorStep {}
    public record HomeStep(String destination,UserCommand.Operation operation,String sessionProof) implements Step {public HomeStep{Objects.requireNonNull(destination);Objects.requireNonNull(operation);}}
    public record CoordinatorStep(CallWorkflowService.Transition transition) implements Step {public CoordinatorStep{Objects.requireNonNull(transition);}}
    @FunctionalInterface public interface Planner {Step next(CrossCellSaga.Phase phase);default void completed(CrossCellSaga.Phase phase,Object committedDto){}}
    @FunctionalInterface public interface Network {CompletionStage<InternalReply> call(CellRpcServer.Operation operation,InternalCommand command,Duration budget);}
    private static final ObjectMapper JSON=new ObjectMapper().findAndRegisterModules();
    private final RpcBusinessHandler.ActorIngress actors;private final Network network;private final Planner planner;private final Clock clock;
    public NativeSagaEffects(RpcBusinessHandler.ActorIngress actors,Network network,Planner planner,Clock clock){this.actors=Objects.requireNonNull(actors);this.network=Objects.requireNonNull(network);this.planner=Objects.requireNonNull(planner);this.clock=Objects.requireNonNull(clock);}
    @Override public CompletionStage<String> apply(CrossCellSaga.Phase phase,UUID originalOperation,Duration budget){
        if(budget==null||budget.isNegative()||budget.isZero())return CompletableFuture.completedFuture("OUTCOME_UNKNOWN");long end=System.nanoTime()+Math.min(budget.toNanos(),Duration.ofSeconds(2).toNanos());var step=planner.next(phase);
        if(step instanceof CoordinatorStep local){var t=local.transition();if(!t.operation().equals(originalOperation))throw new IllegalArgumentException("Saga operation changed");return actors.progress(t,clock.instant().plus(remaining(end)),RpcBusinessHandler.encode(new RpcBusinessHandler.WorkflowPayload(t)).length).thenApply(value->{planner.completed(phase,value);return value.code();});}
        var home=(HomeStep)step;var request=RpcBusinessHandler.request(home.operation());if(!request.grant().operation().equals(originalOperation))throw new IllegalArgumentException("Saga operation changed");var action=RpcBusinessHandler.action(home.operation());
        return actors.grant(request,action,home.destination(),clock.instant().plus(remaining(end)),RpcBusinessHandler.encode(home.operation()).length).thenCompose(granted->{
            if(!granted.code().equals("GRANTED")||granted.signedRequest()==null||granted.issued()==null)return CompletableFuture.completedFuture(granted.code());
            var signed=granted.signedRequest();if(!sameIntent(request,signed)||!signed.grant().operation().equals(originalOperation))return CompletableFuture.completedFuture("UNKNOWN");var operation=replace(home.operation(),signed);var token=granted.issued().token();var g=signed.grant();
            var authority=GroupAuthority.newBuilder().setCellId(g.cell()).setStorageEpoch(g.storageEpoch()).setOwnershipHashVersion(Math.toIntExact(g.hashVersion())).setGroupId(g.group()).setGroupEpoch(g.groupEpoch()).setLeaseSequence(g.sequence()).setOwnerIncarnation(ByteString.copyFrom(ByteBuffer.allocate(16).putLong(token.incarnation().getMostSignificantBits()).putLong(token.incarnation().getLeastSignificantBits()).array()));
            var requestType=type(operation);var command=InternalCommand.newBuilder().setSchemaMajor(1).setSchemaMinor(0).setDestinationCell(home.destination()).setOperationId(originalOperation.toString()).setCallId(signed.call().value()).setCommandScope(CommandScope.call(signed.call()).value()).setType(requestType).setAuthority(authority).setExpectedCallVersion(g.authorizedCallVersion()).setPayloadHash(ByteString.copyFrom(HexFormat.of().parseHex(HomeParticipationService.authorizationHash(signed,action)))).setRemainingBudgetMs(Math.max(1,remaining(end).toMillis())).setPayload(ByteString.copyFrom(RpcBusinessHandler.encode(new RpcBusinessHandler.UserPayload(operation,home.sessionProof())))).build();
            CellRpcServer.Operation rpc=operation instanceof UserCommand.Accept?CellRpcServer.Operation.CLAIM:operation instanceof UserCommand.Release?CellRpcServer.Operation.RELEASE:CellRpcServer.Operation.RESERVE;
            return network.call(rpc,command,remaining(end)).thenApply(reply->{if(!reply.getOperationId().equals(originalOperation.toString())||!reply.getCallId().equals(signed.call().value()))return "OUTCOME_UNKNOWN";if(!reply.getAckCommitted())return reply.getErrorCode().isEmpty()?"OUTCOME_UNKNOWN":reply.getErrorCode();try{var result=JSON.readValue(reply.getResult().toByteArray(),UserCommand.Result.class);planner.completed(phase,result);return result.code().name();}catch(Exception invalid){return "OUTCOME_UNKNOWN";}});
        });
    }
    private static Duration remaining(long end){long left=end-System.nanoTime();if(left<=0)throw new CompletionException(new TimeoutException());return Duration.ofNanos(left);}
    private static boolean sameIntent(Request a,Request b){return a.user().equals(b.user())&&a.call().equals(b.call())&&a.acquireOperation().equals(b.acquireOperation())&&a.payloadHash().equals(b.payloadHash())&&a.directoryEpoch()==b.directoryEpoch()&&a.phase()==b.phase();}
    private static UserCommand.Operation replace(UserCommand.Operation op,Request r){return switch(op){case UserCommand.Reserve v->new UserCommand.Reserve(r);case UserCommand.Renew v->new UserCommand.Renew(r,v.reservation(),v.version());case UserCommand.Release v->new UserCommand.Release(r,v.reservation(),v.version());case UserCommand.Accept v->new UserCommand.Accept(r,v.reservation(),v.route());case UserCommand.Activate v->new UserCommand.Activate(r,v.reservation(),v.version(),v.activation(),v.callVersion(),v.winner(),v.operation());case UserCommand.QueryProof v->new UserCommand.QueryProof(r,v.sender());default->throw new IllegalArgumentException("Home effect required");};}
    private static String type(UserCommand.Operation op){return switch(op){case UserCommand.Reserve r->"ReserveUser";case UserCommand.Renew r->"RenewReservation";case UserCommand.Release r->"ReleaseIfCallVersion";case UserCommand.Accept r->"ClaimAccept";case UserCommand.Activate r->"ConfirmActivation";case UserCommand.QueryProof r->"QueryParticipation";default->throw new IllegalArgumentException("Home effect required");};}
}
