package io.webrtc.signaling.actors.call;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.actors.cluster.CallMessage;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.*;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.testkit.typed.javadsl.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
class CallActorStateMachineTest {
    static final ActorTestKit kit=ActorTestKit.create(ManualTime.config());static final ManualTime time=ManualTime.get(kit.system());
    static final CallId CALL=new CallId("c001.e1.00000000-0000-0000-0000-000000000001");static final Instant NOW=Instant.parse("2026-10-03T00:00:00Z");
    static final AuthoritySql.GroupToken TOKEN=new AuthoritySql.GroupToken("c001",1,1,HomeParticipationService.group(CALL),2,"test-node",new UUID(0,1));
    static Snapshot snapshot(String state,long version){return new Snapshot(CALL,175,1,TOKEN.group(),state,version,0,new Participant(new UserId("alice"),new SessionKey("TEST_ONLY","caller"),new SessionIncarnation(new UUID(0,2)),1),new UserId("bob"),null,null,"RESERVE_REMOTE","{\"prepareUntil\":\"2026-10-03T00:00:15Z\"}","[]","[]",2,null,null,null,new RequestId(new UUID(0,3)));}
    static final class Pending<T>{final CompletableFuture<T> logical=new CompletableFuture<>();final CompletableFuture<DbOperation.PhysicalCompletion> physical=new CompletableFuture<>();DbOperation<T> handle(){return new DbOperation<>(logical.minimalCompletionStage(),physical.minimalCompletionStage());}void done(T value){logical.complete(value);physical.complete(DbOperation.PhysicalCompletion.FINISHED);}}
    static final class Backend implements CallActor.Backend {
        final Pending<Optional<Snapshot>> hydration=new Pending<>();final BlockingQueue<Pending<CallWorkflowService.Outcome>> mutations=new LinkedBlockingQueue<>();final AtomicInteger started=new AtomicInteger();
        public DbOperation<Optional<Snapshot>> load(CallId call,AuthoritySql.GroupToken token,Duration budget){return hydration.handle();}
        public DbOperation<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition transition,Duration budget){started.incrementAndGet();var p=new Pending<CallWorkflowService.Outcome>();mutations.add(p);return p.handle();}
        public DbOperation<CallCommandService.Outcome> command(io.webrtc.signaling.protocol.CallCommand command,AuthoritySql.GroupToken token,long version,String proof,Duration budget){throw new UnsupportedOperationException("not used in this fixture");}
        public DbOperation<CallWorkflowService.Outcome> expire(CallId call,AuthoritySql.GroupToken token,long version,Duration budget){started.incrementAndGet();var p=new Pending<CallWorkflowService.Outcome>();mutations.add(p);return p.handle();}
        Pending<CallWorkflowService.Outcome> next()throws Exception{return Objects.requireNonNull(mutations.poll(2,TimeUnit.SECONDS));}
    }
    final Backend backend=new Backend();final AtomicBoolean held=new AtomicBoolean(true);final TestProbe<CallWorkflowService.Outcome> replies=kit.createTestProbe();
    ActorRef<CallMessage> spawn(){var actor=kit.spawn(CallActor.create(CALL,backend,()->held.get()?Optional.of(TOKEN):Optional.empty(),Clock.fixed(NOW,ZoneOffset.UTC)));backend.hydration.done(Optional.of(snapshot("PREPARING",1)));return actor;}
    static CallWorkflowService.Transition ring(long version){return new CallWorkflowService.Transition(CALL,TOKEN,1,version,new UUID(0,9),CallWorkflowService.Step.RING,null,List.of(new Participant(new UserId("bob"),new SessionKey("TEST_ONLY","callee"),new SessionIncarnation(new UUID(0,4)),1)),null,NOW.plusSeconds(5),NOW.plusSeconds(30),"TEST_ONLY_VERIFIED",null);}
    void progress(ActorRef<CallMessage> actor){actor.tell(new CallActor.Progress(ring(1),replies.ref(),NOW.plusSeconds(2),1024));}
    @AfterAll static void close(){kit.shutdownTestKit();}
    @Test void legalLifecycleAndTerminalAbsorptionFollowSpec(){
        assertThat(CallWorkflowService.nextState("PREPARING",CallWorkflowService.Step.RING)).isEqualTo("RINGING");
        assertThat(CallWorkflowService.nextState("RINGING",CallWorkflowService.Step.ACCEPT)).isEqualTo("ACCEPTED");
        assertThat(CallWorkflowService.nextState("ACCEPTED",CallWorkflowService.Step.ACTIVATE)).isEqualTo("ACTIVATING");
        assertThat(CallWorkflowService.nextState("ACTIVATING",CallWorkflowService.Step.READY)).isEqualTo("CONNECTING");
        assertThat(CallWorkflowService.nextState("CONNECTING",CallWorkflowService.Step.ESTABLISH)).isEqualTo("ESTABLISHED");
        for(String state:List.of("PREPARING","RINGING","ACCEPTED","ACTIVATING","CONNECTING","ESTABLISHED"))assertThat(CallWorkflowService.nextState(state,CallWorkflowService.Step.TERMINATE)).isEqualTo("TERMINAL");
        for(var step:CallWorkflowService.Step.values())assertThat(CallWorkflowService.nextState("TERMINAL",step)).isEqualTo("TERMINAL");
        assertThatThrownBy(()->CallWorkflowService.nextState("RINGING",CallWorkflowService.Step.READY)).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void actorCannotAcknowledgeFromLocalStateBeforeCommittedProofAndPhysicalWorkIsSerialized()throws Exception {
        var actor=spawn();progress(actor);var first=backend.next();replies.expectNoMessage(Duration.ofMillis(50));progress(actor);assertThat(backend.started).hasValue(1);
        first.logical.complete(new CallWorkflowService.Outcome("RINGING",snapshot("RINGING",2),List.of()));assertThat(replies.receiveMessage().snapshot().version()).isEqualTo(2);assertThat(backend.started).hasValue(1);
        first.physical.complete(DbOperation.PhysicalCompletion.FINISHED);var replay=backend.next();replies.expectNoMessage(Duration.ofMillis(50));replay.done(new CallWorkflowService.Outcome("RINGING",snapshot("RINGING",2),List.of()));assertThat(replies.receiveMessage().code()).isEqualTo("RINGING");assertThat(backend.started).hasValue(2);
    }
    @Test void closedGroupGatePreventsAnyNewMutationEvenWithPreviouslyHydratedState(){var actor=spawn();var state=kit.<Optional<Snapshot>>createTestProbe();actor.tell(new CallActor.GetSnapshot(state.ref()));state.receiveMessage();held.set(false);progress(actor);assertThat(replies.receiveMessage().code()).isEqualTo("FENCED");assertThat(backend.started).hasValue(0);}
    @Test void unknownCompletionAndLateOldIncarnationCannotEnableReplacement()throws Exception {
        var old=spawn();progress(old);var p=backend.next();p.logical.completeExceptionally(new DbOutcomeUnknownException());assertThat(replies.receiveMessage().code()).isEqualTo("UNKNOWN");progress(old);assertThat(replies.receiveMessage().code()).isEqualTo("UNAVAILABLE");assertThat(backend.started).hasValue(1);
        kit.stop(old);var freshBackend=new Backend();var replacement=kit.spawn(CallActor.create(CALL,freshBackend,()->Optional.of(TOKEN),Clock.fixed(NOW,ZoneOffset.UTC)));freshBackend.hydration.done(Optional.of(snapshot("RINGING",2)));p.physical.complete(DbOperation.PhysicalCompletion.FINISHED);
        var state=kit.<Optional<Snapshot>>createTestProbe();replacement.tell(new CallActor.GetSnapshot(state.ref()));assertThat(state.receiveMessage()).contains(snapshot("RINGING",2));
    }
    @Test void restoresDurableTimerWithoutClientTraffic()throws Exception {var actor=spawn();time.timePasses(Duration.ofSeconds(15));var timeout=backend.next();timeout.done(new CallWorkflowService.Outcome("TIMEOUT",snapshot("TERMINAL",2),List.of()));assertThat(backend.started).hasValue(1);kit.stop(actor);}
    @Test void unknownEntityRetiresOnlyAfterPhysicalCleanupSoRecoveryCanRecreateIt()throws Exception {
        var actor=spawn();progress(actor);var work=backend.next();work.logical.completeExceptionally(new DbOutcomeUnknownException());assertThat(replies.receiveMessage().code()).isEqualTo("UNKNOWN");progress(actor);assertThat(replies.receiveMessage().code()).isEqualTo("UNAVAILABLE");assertThat(backend.started).hasValue(1);
        work.physical.complete(DbOperation.PhysicalCompletion.FINISHED);replies.expectTerminated(actor,Duration.ofSeconds(2));
    }
    @Test void shardStopDrainsPhysicalWorkThenTerminatesWithoutRequestingAnotherPassivation()throws Exception {
        var shard=kit.<org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding.ShardCommand>createTestProbe();
        var actor=kit.spawn(CallActor.create(CALL,backend,()->Optional.of(TOKEN),Clock.fixed(NOW,ZoneOffset.UTC),shard.ref()));
        backend.hydration.done(Optional.of(snapshot("PREPARING",1)));progress(actor);var work=backend.next();actor.tell(CallActor.Stop.INSTANCE);
        work.logical.complete(new CallWorkflowService.Outcome("RINGING",snapshot("RINGING",2),List.of()));replies.receiveMessage();
        work.physical.complete(DbOperation.PhysicalCompletion.FINISHED);replies.expectTerminated(actor,Duration.ofSeconds(2));shard.expectNoMessage(Duration.ofMillis(50));
    }

    @Test void homeGrantIsQueuedThroughActorAndOldOwnershipCannotProduceSuccessfulProof()throws Exception {
        var grantWork=new Pending<CoordinatorGrantService.Issued>();var delegate=new Backend();
        CallActor.Backend grantBackend=new CallActor.Backend(){
            public DbOperation<Optional<Snapshot>> load(CallId c,AuthoritySql.GroupToken t,Duration b){return delegate.load(c,t,b);}
            public DbOperation<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition t,Duration b){return delegate.progress(t,b);}
            public DbOperation<CallCommandService.Outcome> command(io.webrtc.signaling.protocol.CallCommand c,AuthoritySql.GroupToken t,long v,String p,Duration b){return delegate.command(c,t,v,p,b);}
            public DbOperation<CallWorkflowService.Outcome> expire(CallId c,AuthoritySql.GroupToken t,long v,Duration b){return delegate.expire(c,t,v,b);}
            public DbOperation<CoordinatorGrantService.Issued> grant(HomeParticipationService.Request r,HomeParticipationService.AuthorizationIntent a,AuthoritySql.GroupToken t,long v,Duration b){assertThat(t).isEqualTo(TOKEN);assertThat(v).isEqualTo(1);return grantWork.handle();}
        };
        var actor=kit.spawn(CallActor.create(CALL,grantBackend,()->held.get()?Optional.of(TOKEN):Optional.empty(),Clock.fixed(NOW,ZoneOffset.UTC)));delegate.hydration.done(Optional.of(snapshot("PREPARING",1)));
        var request=new HomeParticipationService.Request(new UserId("bob"),CALL,UUID.randomUUID(),"a".repeat(64),1,HomeParticipationService.Phase.PREPARING,new HomeParticipationService.Grant("c001",1,1,TOKEN.group(),2,1,UUID.randomUUID(),NOW,NOW.plusSeconds(5),"UNSIGNED"));
        var results=kit.<CallActor.GrantReply>createTestProbe();actor.tell(new CallActor.GrantToHome(request,HomeParticipationService.AuthorizationIntent.reserve(),results.ref(),NOW.plusSeconds(2),1024));
        var observer=kit.<Optional<Snapshot>>createTestProbe();actor.tell(new CallActor.GetSnapshot(observer.ref()));observer.receiveMessage();held.set(false);
        grantWork.done(new CoordinatorGrantService.Issued(snapshot("PREPARING",1),TOKEN,1,NOW,NOW.plusSeconds(5),"a".repeat(64)));
        var result=results.receiveMessage();assertThat(result.code()).isEqualTo("UNKNOWN");assertThat(result.issued()).isNull();
    }

    @Test void knownWarmHydrationOverloadRetriesAfterCleanupWithoutDiscardingTheActor()throws Exception {
        var loads=new ArrayBlockingQueue<Pending<Optional<Snapshot>>>(4);var cold=new AtomicInteger();var commandWork=new Pending<CallCommandService.Outcome>();
        CallActor.Backend nativeFault=new CallActor.Backend(){
            public DbOperation<Optional<Snapshot>> loadCold(CallId c,AuthoritySql.GroupToken t,Duration b){cold.incrementAndGet();return new DbOperation<>(CompletableFuture.completedFuture(Optional.of(snapshot("RINGING",1))),CompletableFuture.completedFuture(DbOperation.PhysicalCompletion.FINISHED));}
            public DbOperation<Optional<Snapshot>> load(CallId c,AuthoritySql.GroupToken t,Duration b){assertThat(b).isLessThanOrEqualTo(Duration.ofSeconds(2));var work=new Pending<Optional<Snapshot>>();assertThat(loads.offer(work)).isTrue();return work.handle();}
            public DbOperation<CallCommandService.Outcome> command(io.webrtc.signaling.protocol.CallCommand c,AuthoritySql.GroupToken t,long v,String p,Duration b){return commandWork.handle();}
            public DbOperation<CallWorkflowService.Outcome> progress(CallWorkflowService.Transition t,Duration b){throw new AssertionError();}
            public DbOperation<CallWorkflowService.Outcome> expire(CallId c,AuthoritySql.GroupToken t,long v,Duration b){throw new AssertionError();}
        };
        var actor=kit.spawn(CallActor.create(CALL,nativeFault,()->Optional.of(TOKEN),Clock.fixed(NOW,ZoneOffset.UTC)));
        var observer=kit.<Optional<Snapshot>>createTestProbe();actor.tell(new CallActor.GetSnapshot(observer.ref()));observer.receiveMessage();
        var command=new io.webrtc.signaling.protocol.CallCommand(io.webrtc.signaling.protocol.SignalEnvelope.Type.NEGOTIATE_REQUEST,new AuthenticatedSession(new UserId("alice"),new SessionKey("TEST_ONLY","caller"),new SessionIncarnation(new UUID(0,2)),1,UUID.randomUUID()),new RequestId(UUID.randomUUID()),CALL,CommandScope.call(CALL),null,null,null,"{}","a".repeat(64));
        var result=kit.<CallCommandService.Outcome>createTestProbe();actor.tell(new CallActor.Execute(command,"TEST_ONLY",result.ref(),NOW.plusSeconds(2),1024));
        commandWork.done(new CallCommandService.Outcome("FINAL","NEGOTIATION_GRANTED",CALL,2,"CONNECTING",List.of()));result.receiveMessage();
        var first=Objects.requireNonNull(loads.poll(1,TimeUnit.SECONDS));first.logical.completeExceptionally(new DbOverloadedException());
        assertThat(loads.poll(50,TimeUnit.MILLISECONDS)).isNull();first.physical.complete(DbOperation.PhysicalCompletion.FINISHED);
        // Observe processing of cleanup before advancing the actor's retry timer.
        actor.tell(new CallActor.GetSnapshot(observer.ref()));time.timePasses(Duration.ofMillis(50));
        var retry=loads.poll(1,TimeUnit.SECONDS);assertThat(retry).describedAs("Known NOT_STARTED hydration must retry on the same actor").isNotNull();
        retry.done(Optional.of(snapshot("CONNECTING",2)));assertThat(observer.receiveMessage()).contains(snapshot("CONNECTING",2));assertThat(cold).hasValue(1);kit.stop(actor);
    }

    @Test void promotedNativeRootHydratesOldRoutingIdentityAndProcessesExistingCall()throws Exception {
        var promoted=new AuthoritySql.GroupToken(TOKEN.cell(),2,TOKEN.hashVersion(),TOKEN.group(),3,TOKEN.node(),TOKEN.incarnation());
        var actor=kit.spawn(CallActor.create(CALL,backend,()->Optional.of(promoted),Clock.fixed(NOW,ZoneOffset.UTC)));
        backend.hydration.done(Optional.of(snapshot("PREPARING",1)));
        var observer=kit.<Optional<Snapshot>>createTestProbe();actor.tell(new CallActor.GetSnapshot(observer.ref()));assertThat(observer.receiveMessage()).contains(snapshot("PREPARING",1));
        var original=ring(1);var transition=new CallWorkflowService.Transition(CALL,promoted,1,1,original.operation(),original.step(),original.winner(),original.offered(),original.activationId(),original.proofExpiresAt(),original.participantUntil(),original.proof(),original.reason());
        actor.tell(new CallActor.Progress(transition,replies.ref(),NOW.plusSeconds(2),1024));var work=backend.next();work.done(new CallWorkflowService.Outcome("RINGING",snapshot("RINGING",2),List.of()));assertThat(replies.receiveMessage().code()).isEqualTo("RINGING");kit.stop(actor);
    }

}
