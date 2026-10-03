package io.webrtc.signaling.actors.call;
import io.webrtc.signaling.actors.cluster.CallMessage;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.Snapshot;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.*;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
/** One admitted transaction per incarnation, including its physical cleanup after a logical timeout. */
public final class CallActor extends AbstractBehavior<CallMessage> {
    public interface Backend {
        DbOperation<Optional<Snapshot>> load(CallId call,AuthoritySql.GroupToken token,Duration budget);
        DbOperation<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition transition,Duration budget);
        DbOperation<CallCommandService.Outcome> command(CallCommand command,AuthoritySql.GroupToken token,long version,String proof,Duration budget);
        DbOperation<CallWorkflowService.Outcome> expire(CallId call,AuthoritySql.GroupToken token,long version,Duration budget);
    }
    public record Progress(CallWorkflowService.Transition transition,ActorRef<CallWorkflowService.Outcome> replyTo,Instant deadline,int encodedBytes) implements CallMessage {public Progress{Objects.requireNonNull(transition);Objects.requireNonNull(replyTo);check(deadline,encodedBytes);}}
    public record Execute(CallCommand command,String proof,ActorRef<CallCommandService.Outcome> replyTo,Instant deadline,int encodedBytes) implements CallMessage {public Execute{Objects.requireNonNull(command);Objects.requireNonNull(replyTo);check(deadline,encodedBytes);}}
    public record WakeCall(CallId call,ActorRef<Boolean> replyTo,Instant deadline) implements CallMessage {public WakeCall{Objects.requireNonNull(call);Objects.requireNonNull(replyTo);Objects.requireNonNull(deadline);}}
    public record GetSnapshot(ActorRef<Optional<Snapshot>> replyTo) implements CallMessage {}
    public enum Stop implements CallMessage {INSTANCE}
    private record Completed(UUID actor,UUID operation,AuthoritySql.GroupToken token,Object value,Throwable error) implements CallMessage {}
    private record Cleaned(UUID actor,UUID operation) implements CallMessage {}
    private record Expire(UUID actor,AuthoritySql.GroupToken token,long version) implements CallMessage {}
    private enum Retire implements CallMessage {INSTANCE}
    private static void check(Instant deadline,int bytes){Objects.requireNonNull(deadline);if(bytes<1||bytes>96*1024)throw new IllegalArgumentException("Invalid command size");}
    public static Behavior<CallMessage> create(CallId call,Backend backend,Supplier<Optional<AuthoritySql.GroupToken>> gate,Clock clock){return create(call,backend,gate,clock,null);}
    public static Behavior<CallMessage> create(CallId call,Backend backend,Supplier<Optional<AuthoritySql.GroupToken>> gate,Clock clock,ActorRef<ClusterSharding.ShardCommand> shard){return Behaviors.withTimers(t->Behaviors.setup(c->new CallActor(c,t,call,backend,gate,clock,shard)));}
    private static final class Pending {final UUID id=UUID.randomUUID();final CallMessage request;final AuthoritySql.GroupToken token;boolean logical,physical;Pending(CallMessage request,AuthoritySql.GroupToken token){this.request=request;this.token=token;}}
    private final UUID incarnation=UUID.randomUUID();private final CallId call;private final Backend backend;private final Supplier<Optional<AuthoritySql.GroupToken>> gate;private final Clock clock;private final TimerScheduler<CallMessage> timers;private final ActorRef<ClusterSharding.ShardCommand> shard;
    private final ArrayDeque<CallMessage> queue=new ArrayDeque<>();private final List<ActorRef<Optional<Snapshot>>> observers=new ArrayList<>();private Optional<Snapshot> snapshot=Optional.empty();private Pending pending;private int bytes;private boolean ready,unknown,stopping,passivating;
    private CallActor(ActorContext<CallMessage> c,TimerScheduler<CallMessage> t,CallId call,Backend backend,Supplier<Optional<AuthoritySql.GroupToken>> gate,Clock clock,ActorRef<ClusterSharding.ShardCommand> shard){super(c);this.call=Objects.requireNonNull(call);this.backend=Objects.requireNonNull(backend);this.gate=Objects.requireNonNull(gate);this.clock=Objects.requireNonNull(clock);timers=t;this.shard=shard;
        var token=current();if(token.isEmpty()){unknown=true;ready=true;return;}pending=new Pending(null,token.get());submit(()->backend.load(call,pending.token,Duration.ofSeconds(2)));
    }
    @Override public Receive<CallMessage> createReceive(){return newReceiveBuilder().onMessage(Progress.class,this::enqueue).onMessage(Execute.class,this::enqueue).onMessage(WakeCall.class,this::enqueue).onMessage(GetSnapshot.class,this::observe).onMessage(Completed.class,this::completed).onMessage(Cleaned.class,this::cleaned).onMessage(Expire.class,this::expire).onMessage(Retire.class,r->retire()).onMessage(Stop.class,r->stop()).build();}
    private Optional<AuthoritySql.GroupToken> current(){return gate.get().filter(t->call.coordinatorCell().equals(t.cell())&&call.routingEpoch()==t.storageEpoch()&&t.hashVersion()==1&&t.group()==HomeParticipationService.group(call));}
    private Behavior<CallMessage> enqueue(CallMessage request){
        if(current().isEmpty()){reply(request,"FENCED");return this;}if(stopping||passivating||unknown){reply(request,"UNAVAILABLE");return this;}
        if(!deadline(request).isAfter(clock.instant())){reply(request,"EXPIRED");return this;}
        if(request instanceof Progress p&&(!call.equals(p.transition().call())||!current().get().equals(p.transition().group()))||request instanceof Execute e&&e.command().callId()!=null&&!call.equals(e.command().callId())||request instanceof WakeCall w&&!call.equals(w.call())){reply(request,"FENCED");return this;}
        if(queue.size()+(pending==null?0:1)>=64||bytes+size(request)>256*1024){reply(request,"OVERLOADED");return this;}
        queue.add(request);bytes+=size(request);start();return this;
    }
    private void start(){while(ready&&pending==null&&!queue.isEmpty()&&!unknown&&!stopping){var r=queue.remove();var token=current();if(token.isEmpty()||!deadline(r).isAfter(clock.instant())){bytes-=size(r);reply(r,token.isEmpty()?"FENCED":"EXPIRED");continue;}
        if(r instanceof Progress p&&!p.transition().group().equals(token.get())){bytes-=size(r);reply(r,"FENCED");continue;}
        pending=new Pending(r,token.get());Duration remaining=Duration.between(clock.instant(),deadline(r));Duration budget=remaining.compareTo(Duration.ofSeconds(2))>0?Duration.ofSeconds(2):remaining;
        submit(()->switch(r){case Progress p->backend.progress(p.transition(),budget);case Execute e->backend.command(e.command(),token.get(),snapshot.map(Snapshot::version).orElse(0L),e.proof(),budget);case WakeCall w->backend.load(call,token.get(),budget);default->throw new IllegalArgumentException("Unknown call work");});
    }}
    private void submit(Supplier<DbOperation<?>> action){UUID operation=pending.id;var token=pending.token;try{var handle=action.get();getContext().pipeToSelf(handle.logical(),(v,e)->new Completed(incarnation,operation,token,v,e));getContext().pipeToSelf(handle.physicalCompletion(),(v,e)->new Cleaned(incarnation,operation));}catch(RuntimeException e){getContext().getSelf().tell(new Completed(incarnation,operation,token,null,e));getContext().getSelf().tell(new Cleaned(incarnation,operation));}}
    private Behavior<CallMessage> completed(Completed e){
        if(!incarnation.equals(e.actor())||pending==null||!pending.id.equals(e.operation())||!pending.token.equals(e.token())||pending.logical)return this;pending.logical=true;
        if(e.error()!=null){String code=classify(e.error());unknown=code.equals("UNKNOWN")||pending.request==null;if(pending.request!=null)reply(pending.request,code);}
        else if(!current().filter(e.token()::equals).isPresent()){unknown=true;if(pending.request!=null)reply(pending.request,"UNKNOWN");}
        else try{
            if(e.value() instanceof Optional<?> loaded){setSnapshot(loaded.map(v->(Snapshot)v));ready=true;if(pending.request instanceof WakeCall w)w.replyTo().tell(snapshot.isPresent());}
            else if(e.value() instanceof CallWorkflowService.Outcome value){setSnapshot(Optional.of(value.snapshot()));if(pending.request instanceof Progress p)p.replyTo().tell(value);}
            else if(e.value() instanceof CallCommandService.Outcome value){if(pending.request instanceof Execute r)r.replyTo().tell(value);/* Re-read primary after the command before serving another mutation. */ready=false;}
            else throw new IllegalArgumentException("Invalid committed DTO");
        }catch(RuntimeException invalid){unknown=true;if(pending.request!=null)reply(pending.request,"UNKNOWN");}
        if(ready||unknown){observers.forEach(ref->ref.tell(snapshot));observers.clear();}finish();return next();
    }
    private void setSnapshot(Optional<Snapshot> value){if(value.isPresent()){Snapshot s=value.get();if(!call.equals(s.callId())||snapshot.isPresent()&&s.version()<snapshot.get().version())throw new IllegalArgumentException("Stale hydration");}snapshot=value;timers.cancel("deadline");timers.cancel(Retire.INSTANCE);if(snapshot.isPresent()){var s=snapshot.get();if(s.terminalAt()!=null)timers.startSingleTimer(Retire.INSTANCE,Duration.ofMinutes(2));else {Duration d=DurableDeadlineScheduler.delay(s,clock);if(d!=null)timers.startSingleTimer("deadline",new Expire(incarnation,pending.token,s.version()),d);}}}
    private Behavior<CallMessage> cleaned(Cleaned e){if(incarnation.equals(e.actor())&&pending!=null&&pending.id.equals(e.operation())){pending.physical=true;finish();}return next();}
    private void finish(){if(pending==null||!pending.logical||!pending.physical)return;if(pending.request!=null)bytes-=size(pending.request);pending=null;
        if(unknown||stopping){while(!queue.isEmpty()){var r=queue.remove();bytes-=size(r);reply(r,"UNAVAILABLE");}}
        else if(!ready){var token=current();if(token.isEmpty()){unknown=true;finishUnavailable();return;}pending=new Pending(null,token.get());submit(()->backend.load(call,pending.token,Duration.ofSeconds(2)));}
        else start();
    }
    private void finishUnavailable(){while(!queue.isEmpty()){var r=queue.remove();bytes-=size(r);reply(r,"UNAVAILABLE");}}
    private Behavior<CallMessage> expire(Expire e){if(!incarnation.equals(e.actor())||unknown||stopping||snapshot.isEmpty()||snapshot.get().version()!=e.version()||!current().filter(e.token()::equals).isPresent())return this;
        if(pending!=null||!queue.isEmpty()){timers.startSingleTimer("deadline",e,Duration.ofMillis(50));return this;}
        pending=new Pending(null,e.token());submit(()->backend.expire(call,e.token(),e.version(),Duration.ofSeconds(2)));return this;
    }
    private Behavior<CallMessage> observe(GetSnapshot r){if(ready||unknown)r.replyTo().tell(snapshot);else if(observers.size()<16)observers.add(r.replyTo());else r.replyTo().tell(Optional.empty());return this;}
    private Behavior<CallMessage> retire(){if(pending!=null||!queue.isEmpty()){timers.startSingleTimer(Retire.INSTANCE,Duration.ofSeconds(1));return this;}if(shard==null)return Behaviors.stopped();passivating=true;shard.tell(new ClusterSharding.Passivate<>(getContext().getSelf()));return this;}
    private Behavior<CallMessage> stop(){stopping=true;finishUnavailable();return next();}
    private Behavior<CallMessage> next(){return stopping&&pending==null?Behaviors.stopped():this;}
    private static int size(CallMessage r){return switch(r){case Progress p->p.encodedBytes();case Execute e->e.encodedBytes();default->128;};}
    private static Instant deadline(CallMessage r){return switch(r){case Progress p->p.deadline();case Execute e->e.deadline();case WakeCall w->w.deadline();default->throw new IllegalArgumentException("Not queued work");};}
    private void reply(CallMessage r,String code){switch(r){case Progress p->p.replyTo().tell(new CallWorkflowService.Outcome(code,snapshot.orElse(null),List.of()));case Execute e->e.replyTo().tell(new CallCommandService.Outcome(code,code,call,snapshot.map(Snapshot::version).orElse(0L),snapshot.map(Snapshot::state).orElse("NONE"),List.of()));case WakeCall w->w.replyTo().tell(false);default->{}}}
    private static String classify(Throwable e){while(e instanceof CompletionException&&e.getCause()!=null)e=e.getCause();if(e instanceof AuthoritySql.FencedException||e instanceof CallCommandService.AuthorizationRejected)return "FENCED";if(e instanceof AuthoritySql.RetryableConflict||e instanceof DbOverloadedException)return "OVERLOADED";if(e instanceof IllegalArgumentException)return "INVALID";return "UNKNOWN";}
}
