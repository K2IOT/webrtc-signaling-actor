package io.webrtc.signaling.actors.call;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.Snapshot;
import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;
/** Local shard grant is checked before admission and again against primary authority in SQL. */
public final class CallCommandHandler implements CallActor.Backend {
    private final CallWorkflowService workflow;private final CallCommandService commands;private final Supplier<Optional<AuthoritySql.GroupToken>> local;private final long directoryEpoch;
    public CallCommandHandler(CallWorkflowService workflow,CallCommandService commands,Supplier<Optional<AuthoritySql.GroupToken>> local,long directoryEpoch){this.workflow=Objects.requireNonNull(workflow);this.commands=Objects.requireNonNull(commands);this.local=Objects.requireNonNull(local);if(directoryEpoch<1)throw new IllegalArgumentException("Missing directory epoch");this.directoryEpoch=directoryEpoch;}
    private void check(AuthoritySql.GroupToken token){if(local.get().filter(token::equals).isEmpty())throw new AuthoritySql.FencedException();}
    public DbOperation<Optional<Snapshot>> load(CallId call,AuthoritySql.GroupToken token,Duration budget){check(token);return workflow.load(call,token,budget);}
    public DbOperation<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition transition,Duration budget){check(transition.group());return workflow.apply(transition,budget);}
    public DbOperation<CallCommandService.Outcome> command(CallCommand command,AuthoritySql.GroupToken token,long version,String proof,Duration budget){check(token);if(command.callId()==null)throw new IllegalArgumentException("INVITE must select a candidate call before shard routing");return commands.executeUnderAuthorityTracked(command,new CallCommandService.Authority(command.callId(),token,directoryEpoch,proof,version),budget);}
    public DbOperation<CallWorkflowService.Outcome> expire(CallId call,AuthoritySql.GroupToken token,long version,Duration budget){check(token);return workflow.expire(call,token,version,budget);}
}
