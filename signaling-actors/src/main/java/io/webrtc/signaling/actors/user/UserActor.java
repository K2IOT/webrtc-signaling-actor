package io.webrtc.signaling.actors.user;
import io.webrtc.signaling.actors.cluster.UserMessage;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletionException;
/** One bounded user workflow at a time. Physical DB cleanup outlives logical replies and actor residency. */
public final class UserActor extends AbstractBehavior<UserMessage> {
    public interface Backend {DbOperation<UserSnapshotService.Snapshot> load(UserId user,long directoryEpoch,Duration budget);DbOperation<UserCommand.Result> execute(UserCommand.Operation operation,Duration budget);}
    private record Completed(UUID actor,UUID operation,Object value,Throwable error) implements UserMessage {}
    private record Cleaned(UUID actor,UUID operation) implements UserMessage {}
    private enum Idle implements UserMessage {INSTANCE}
    public static final Duration IDLE_TIMEOUT=Duration.ofSeconds(60);
    public static Behavior<UserMessage> create(UserId user,long directoryEpoch,ActorRef<ClusterSharding.ShardCommand> shard,Backend backend,Clock clock){return Behaviors.withTimers(timers->Behaviors.setup(ctx->new UserActor(ctx,timers,user,directoryEpoch,shard,backend,clock)));}
    private static final class Pending {final UUID id=UUID.randomUUID();final UserCommand.Mutate request;boolean logical,physical;Pending(UserCommand.Mutate request){this.request=request;}}
    private final UUID incarnation=UUID.randomUUID();private final UserId user;private final long directoryEpoch;private final Backend backend;private final Clock clock;private final TimerScheduler<UserMessage> timers;private final ActorRef<ClusterSharding.ShardCommand> shard;
    private final ArrayDeque<UserCommand.Mutate> queued=new ArrayDeque<>();private final List<ActorRef<UserState>> observers=new ArrayList<>();
    private UserState state;private Pending pending;private int retainedBytes;private boolean ready,unknown,draining,idleRequested,passivating,stopRequested;
    private UserActor(ActorContext<UserMessage> ctx,TimerScheduler<UserMessage> timers,UserId user,long epoch,ActorRef<ClusterSharding.ShardCommand> shard,Backend backend,Clock clock){super(ctx);this.user=Objects.requireNonNull(user);if(epoch<1)throw new IllegalArgumentException("Invalid directory epoch");directoryEpoch=epoch;this.shard=Objects.requireNonNull(shard);this.backend=Objects.requireNonNull(backend);this.clock=Objects.requireNonNull(clock);this.timers=timers;state=UserState.empty(user);activity();pending=new Pending(null);
        try{track(pending,backend.load(user,epoch,Duration.ofSeconds(2)));}catch(RuntimeException error){pending.logical=pending.physical=true;unknown=true;finish();}
    }
    @Override public Receive<UserMessage> createReceive(){return newReceiveBuilder().onMessage(UserCommand.Mutate.class,this::mutate).onMessage(UserCommand.GetState.class,this::observe).onMessage(Completed.class,this::completed).onMessage(Cleaned.class,this::cleaned).onMessage(Idle.class,this::idle).onMessage(UserCommand.Stop.class,this::stop).build();}
    private void activity(){idleRequested=false;timers.startSingleTimer(Idle.INSTANCE,IDLE_TIMEOUT);}
    private Behavior<UserMessage> observe(UserCommand.GetState request){if(ready||unknown)request.replyTo().tell(state);else if(observers.size()<16)observers.add(request.replyTo());else request.replyTo().tell(state);return this;}
    private Behavior<UserMessage> mutate(UserCommand.Mutate request){
        if(unknown||draining||passivating){reply(request,UserCommand.Code.UNAVAILABLE);return this;}
        if(!user.equals(request.operation().user())||request.operation().directoryEpoch()!=directoryEpoch){reply(request,UserCommand.Code.INVALID);return this;}
        if(!request.deadline().isAfter(clock.instant())){reply(request,UserCommand.Code.EXPIRED);return this;}
        if(queued.size()+(pending==null?0:1)>=64||retainedBytes+request.encodedBytes()>256*1024){reply(request,UserCommand.Code.OVERLOADED);return this;}
        activity();queued.add(request);retainedBytes+=request.encodedBytes();startNext();return this;
    }
    private void startNext(){
        while(ready&&pending==null&&!queued.isEmpty()&&!unknown&&!draining){var request=queued.remove();if(!request.deadline().isAfter(clock.instant())){retainedBytes-=request.encodedBytes();reply(request,UserCommand.Code.EXPIRED);continue;}
            var route=switch(request.operation()){case UserCommand.Refresh r->r.route();case UserCommand.Close r->r.route();case UserCommand.Accept r->r.route();default->null;};
            if(route!=null&&!state.current(route)){retainedBytes-=request.encodedBytes();reply(request,UserCommand.Code.STALE_BINDING);continue;}
            pending=new Pending(request);Duration remaining=Duration.between(clock.instant(),request.deadline());Duration budget=remaining.compareTo(Duration.ofSeconds(2))>0?Duration.ofSeconds(2):remaining;
            try{track(pending,backend.execute(request.operation(),budget));}catch(RuntimeException error){getContext().getSelf().tell(new Completed(incarnation,pending.id,null,error));getContext().getSelf().tell(new Cleaned(incarnation,pending.id));}
        }
    }
    private <T> void track(Pending operation,DbOperation<T> handle){UUID id=operation.id;getContext().pipeToSelf(handle.logical(),(value,error)->new Completed(incarnation,id,value,error));getContext().pipeToSelf(handle.physicalCompletion(),(value,error)->new Cleaned(incarnation,id));}
    private Behavior<UserMessage> completed(Completed event){
        if(!incarnation.equals(event.actor())||pending==null||!pending.id.equals(event.operation())||pending.logical)return this;pending.logical=true;
        if(event.error()!=null){var code=classify(event.error());unknown=code==UserCommand.Code.UNKNOWN;if(pending.request!=null)reply(pending.request,code);else unknown=true;}
        else try{if(pending.request==null){var loaded=(UserSnapshotService.Snapshot)event.value();if(!user.equals(loaded.user()))throw new IllegalArgumentException("Cross-user hydration");state=UserState.from(loaded);ready=true;}
            else {var result=(UserCommand.Result)event.value();state=state.apply(result);pending.request.replyTo().tell(result);}}
        catch(RuntimeException invalid){unknown=true;if(pending.request!=null)reply(pending.request,UserCommand.Code.UNKNOWN);}
        if(ready||unknown){observers.forEach(ref->ref.tell(state));observers.clear();}
        finish();return nextBehavior();
    }
    private Behavior<UserMessage> cleaned(Cleaned event){if(incarnation.equals(event.actor())&&pending!=null&&pending.id.equals(event.operation())){pending.physical=true;finish();}return nextBehavior();}
    private void finish(){if(pending==null||!pending.logical||!pending.physical)return;if(pending.request!=null)retainedBytes-=pending.request.encodedBytes();pending=null;
        if(unknown||draining){failQueued();passivate();}else {startNext();if(pending==null&&queued.isEmpty()&&idleRequested)passivate();}}
    private void failQueued(){while(!queued.isEmpty()){var request=queued.remove();retainedBytes-=request.encodedBytes();reply(request,UserCommand.Code.UNAVAILABLE);}}
    private Behavior<UserMessage> idle(Idle ignored){idleRequested=true;if(pending==null&&queued.isEmpty())passivate();return this;}
    private void passivate(){if(!passivating){passivating=true;shard.tell(new ClusterSharding.Passivate<>(getContext().getSelf()));}}
    private Behavior<UserMessage> nextBehavior(){return stopRequested&&pending==null?Behaviors.stopped():this;}
    private Behavior<UserMessage> stop(UserCommand.Stop ignored){stopRequested=true;draining=true;failQueued();return pending==null?Behaviors.stopped():this;}
    private static void reply(UserCommand.Mutate request,UserCommand.Code code){request.replyTo().tell(UserCommand.Result.error(code));}
    private static UserCommand.Code classify(Throwable error){while(error instanceof CompletionException&&error.getCause()!=null)error=error.getCause();if(error instanceof DbOverloadedException||error instanceof AuthoritySql.RetryableConflict)return UserCommand.Code.OVERLOADED;if(error instanceof AuthoritySql.FencedException)return UserCommand.Code.STALE_BINDING;if(error instanceof SessionRepository.BindingRejected)return UserCommand.Code.BINDING_REJECTED;if(error instanceof SessionRepository.SessionLimit)return UserCommand.Code.OVERLOADED;if(error instanceof UserReservationService.UserBusy)return UserCommand.Code.USER_BUSY;if(error instanceof HomeParticipationService.IntentConflict)return UserCommand.Code.INTENT_CONFLICT;if(error instanceof IllegalArgumentException)return UserCommand.Code.INVALID;return UserCommand.Code.UNKNOWN;}
}
