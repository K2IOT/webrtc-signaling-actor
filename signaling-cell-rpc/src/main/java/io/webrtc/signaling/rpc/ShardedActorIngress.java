package io.webrtc.signaling.rpc;
import io.webrtc.signaling.actors.cluster.*;
import io.webrtc.signaling.actors.user.*;
import io.webrtc.signaling.actors.call.*;
import io.webrtc.signaling.protocol.CallCommand;
import io.webrtc.signaling.storage.*;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletionStage;
/** Stable public EntityRefs are routing handles; owner tokens are supplied only on the hosting shard. */
public final class ShardedActorIngress implements RpcBusinessHandler.ActorIngress {
    private final ClusterSharding sharding;private final Clock clock;
    public ShardedActorIngress(ActorSystem<?> system,Clock clock){sharding=ClusterSharding.get(system);this.clock=Objects.requireNonNull(clock);}
    private Duration remaining(Instant deadline){var d=Duration.between(clock.instant(),deadline);if(d.isNegative()||d.isZero())throw new DbOverloadedException();return d;}
    public CompletionStage<CallActor.GrantReply> grant(HomeParticipationService.Request r,HomeParticipationService.AuthorizationIntent action,String destination,Instant deadline,int bytes){return sharding.entityRefFor(ShardingBootstrap.CALL_TYPE,r.call().value()).ask(reply->new CallActor.GrantToHome(r,action,destination,reply,deadline,bytes),remaining(deadline));}
    public CompletionStage<UserCommand.Result> user(UserCommand.Operation operation,Instant deadline,int bytes){return sharding.entityRefFor(ShardingBootstrap.USER_TYPE,operation.user().value()).ask(reply->new UserCommand.Mutate(operation,reply,deadline,bytes),remaining(deadline));}
    public CompletionStage<CallCommandService.Outcome> call(CallCommand command,String proof,Instant deadline,int bytes){if(command.callId()==null)throw new IllegalArgumentException("Candidate call must be selected before shard routing");return sharding.entityRefFor(ShardingBootstrap.CALL_TYPE,command.callId().value()).ask(reply->new CallActor.Execute(command,proof,reply,deadline,bytes),remaining(deadline));}
    public CompletionStage<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition transition,Instant deadline,int bytes){return sharding.entityRefFor(ShardingBootstrap.CALL_TYPE,transition.call().value()).ask(reply->new CallActor.Progress(transition,reply,deadline,bytes),remaining(deadline));}
}
