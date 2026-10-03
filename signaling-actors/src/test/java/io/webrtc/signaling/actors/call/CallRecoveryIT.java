package io.webrtc.signaling.actors.call;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.storage.CallSnapshotRepository.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import io.webrtc.signaling.actors.cluster.CallMessage;
class CallRecoveryIT {
    static <T>T finish(DbOperation<T> operation){try{return operation.logical().toCompletableFuture().join();}finally{operation.physicalCompletion().toCompletableFuture().join();}}
    static <T>T finishRetry(DbOperation<T> first,java.util.function.Supplier<DbOperation<T>> sameIntent){try{return finish(first);}catch(CompletionException e){if(e.getCause() instanceof AuthoritySql.RetryableConflict)return finish(sameIntent.get());throw e;}}
    static final class Fixture implements AutoCloseable {
        final DbTestRuntime runtime;final String url;final Map<CallId,AuthoritySql.GroupToken> tokens=new HashMap<>();final SessionRegistryService sessions;final GatewayLeaseRepository.Boot boot;final CallWorkflowService workflow;final CallCommandService commands;final Map<CallId,AuthenticatedSession> callers=new HashMap<>();final Map<CallId,Participant> callees=new HashMap<>();
        Fixture()throws Exception {
            String schema="call_recovery_"+UUID.randomUUID().toString().replace("-","");url=PgFixture.PG.getJdbcUrl()+"&currentSchema="+schema;Flyway.configure().dataSource(url,PgFixture.PG.getUsername(),PgFixture.PG.getPassword()).schemas(schema).defaultSchema(schema).locations("classpath:db/migration").load().migrate();runtime=new DbTestRuntime(url,PgFixture.PG.getUsername(),PgFixture.PG.getPassword());
            try(var c=connection();var s=c.createStatement()){s.execute("INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,lease_sequence,status) SELECT 'c001',1,n,1,1,0,'IDLE' FROM generate_series(0,1023)n");}
            sessions=new SessionRegistryService(runtime.sql,"c001",1);boot=sessions.startGatewayBoot("call-recovery-test",UUID.randomUUID(),"TEST_ONLY",UUID.randomUUID()).toCompletableFuture().join();
            workflow=new CallWorkflowService(runtime.sql,"c001",1,"TEST_ONLY_OWNER",(transition,snapshot)->transition.proof().equals("TEST_ONLY_VERIFIED"));
            commands=new CallCommandService(runtime.sql,"c001",1,c->{var call=c.callId();return CompletableFuture.completedFuture(new CallCommandService.Authority(call,tokens.get(call),1,"TEST_ONLY_VERIFIED"));},(c,s,p)->p.equals("TEST_ONLY_VERIFIED"));
        }
        Connection connection()throws Exception{return DriverManager.getConnection(url,PgFixture.PG.getUsername(),PgFixture.PG.getPassword());}
        AuthenticatedSession sender(String prefix){var p=new AuthPrincipal(new UserId(prefix+UUID.randomUUID()),new SessionKey("TEST_ONLY",UUID.randomUUID().toString()),Instant.now().plusSeconds(600),Instant.now(),"test-key",1);var r=sessions.registerSession(p,boot,UUID.randomUUID(),1).toCompletableFuture().join();return new AuthenticatedSession(r.user(),r.key(),r.incarnation(),r.connectionGeneration(),r.connectionId());}
        CallId create(){var call=CallId.create("c001",1);int group=HomeParticipationService.group(call);var ownership=new GroupOwnerRepository(runtime.sql,"c001",1);AuthoritySql.GroupToken token=tokens.values().stream().filter(t->t.group()==group).findFirst().orElseGet(()->finish(ownership.acquireTracked(group,"TEST_ONLY_OWNER",UUID.randomUUID(),UUID.randomUUID())).orElseThrow().token());tokens.put(call,token);var caller=sender("caller-");callers.put(call,caller);var callee=sender("callee-");callees.put(call,new Participant(callee.userId(),callee.key(),callee.incarnation(),callee.connectionGeneration()));var command=new CallCommand(SignalEnvelope.Type.INVITE,caller,new RequestId(UUID.randomUUID()),call,CommandScope.invite(),callee.userId(),null,null,"{}","a".repeat(64));assertThat(commands.executeCallCommand(command).toCompletableFuture().join().status()).isEqualTo("PENDING");return call;}
        CallWorkflowService.Transition transition(CallId call,long version,CallWorkflowService.Step step,UUID operation,Participant winner,UUID activation){Instant now=Instant.now();Participant offered=callees.get(call);return new CallWorkflowService.Transition(call,tokens.get(call),1,version,operation,step,winner,List.of(offered),activation,now.plusSeconds(5),now.plusSeconds(30),"TEST_ONLY_VERIFIED",step==CallWorkflowService.Step.TERMINATE?"TIMEOUT":null);}
        public void close(){runtime.close();}
    }
    @Test void durableTransitionsReplayOriginalOutcomeRejectStaleVersionAndPreserveWinnerInTerminalHistory()throws Exception {
        try(var f=new Fixture()){
            var call=f.create();var ring=f.transition(call,1,CallWorkflowService.Step.RING,UUID.randomUUID(),null,null);var first=finish(f.workflow.apply(ring,Duration.ofSeconds(2)));assertThat(first.snapshot().state()).isEqualTo("RINGING");assertThat(finish(f.workflow.apply(ring,Duration.ofSeconds(2)))).isEqualTo(first);
            var winner=f.callees.get(call);
            assertThat(finish(f.workflow.apply(f.transition(call,1,CallWorkflowService.Step.ACCEPT,UUID.randomUUID(),winner,null),Duration.ofSeconds(2))).code()).isEqualTo("STALE_VERSION");
            var accepted=finish(f.workflow.apply(f.transition(call,2,CallWorkflowService.Step.ACCEPT,UUID.randomUUID(),winner,null),Duration.ofSeconds(2)));assertThat(accepted.code()).isEqualTo("ACCEPTED_PENDING_ACTIVATION");assertThat(accepted.snapshot().winner()).isEqualTo(winner);
            UUID activation=UUID.randomUUID();var activating=finish(f.workflow.apply(f.transition(call,3,CallWorkflowService.Step.ACTIVATE,UUID.randomUUID(),winner,activation),Duration.ofSeconds(2)));assertThat(activating.snapshot().state()).isEqualTo("ACTIVATING");
            var connecting=finish(f.workflow.apply(f.transition(call,4,CallWorkflowService.Step.READY,UUID.randomUUID(),winner,activation),Duration.ofSeconds(2)));assertThat(connecting.code()).isEqualTo("CALL_READY");
            // Seed the committed negotiation round produced by Task 16; media cannot establish round zero.
            try(var c=f.connection();var q=c.prepareStatement("UPDATE call_state SET negotiation_id=1,version=version+1 WHERE call_id=? AND version=5")){q.setString(1,call.value());assertThat(q.executeUpdate()).isEqualTo(1);}
            var established=finish(f.workflow.apply(f.transition(call,6,CallWorkflowService.Step.ESTABLISH,UUID.randomUUID(),winner,activation),Duration.ofSeconds(2)));assertThat(established.snapshot().state()).isEqualTo("ESTABLISHED");
            var terminal=finish(f.workflow.apply(f.transition(call,7,CallWorkflowService.Step.TERMINATE,UUID.randomUUID(),winner,activation),Duration.ofSeconds(2)));assertThat(terminal.snapshot().winner()).isEqualTo(winner);assertThat(terminal.snapshot().terminalAt()).isNotNull();assertThat(finish(f.workflow.apply(f.transition(call,8,CallWorkflowService.Step.READY,UUID.randomUUID(),winner,activation),Duration.ofSeconds(2))).snapshot().state()).isEqualTo("TERMINAL");
        }
    }
    @Test void proofExpiryAndPrimaryGroupTakeoverFenceCachedActors()throws Exception {
        try(var f=new Fixture()){var call=f.create();var valid=f.transition(call,1,CallWorkflowService.Step.RING,UUID.randomUUID(),null,null);var bad=new CallWorkflowService.Transition(call,valid.group(),1,1,valid.operation(),valid.step(),null,valid.offered(),null,Instant.now().minusSeconds(1),Instant.now().plusSeconds(30),"TEST_ONLY_VERIFIED",null);assertThatThrownBy(()->finish(f.workflow.apply(bad,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThat(finish(new GroupOwnerRepository(f.runtime.sql,"c001",1).releaseTracked(valid.group()))).isTrue();var newer=finish(new GroupOwnerRepository(f.runtime.sql,"c001",1).acquireTracked(valid.group().group(),"TEST_ONLY_NEW_OWNER",UUID.randomUUID(),UUID.randomUUID())).orElseThrow();assertThat(newer.token().epoch()).isGreaterThan(valid.group().epoch());assertThatThrownBy(()->finish(f.workflow.apply(valid,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);}
    }
    @Test void lostWakeHintIsRepairedAndCompleteHealthyIndexCycleHasMeasuredFiveSecondBound()throws Exception {
        try(var f=new Fixture()){var calls=new ArrayList<CallId>();for(int i=0;i<12;i++)calls.add(f.create());var observed=ConcurrentHashMap.<CallId>newKeySet();var dropFirst=new AtomicBoolean(true);
            try(var recovery=new CallRecoveryService(f.runtime.sql,"c001",1,(call,budget)->{if(dropFirst.getAndSet(false))return CompletableFuture.completedFuture(false);observed.add(call);return f.workflow.load(call,f.tokens.get(call),budget).logical().thenApply(snapshot->snapshot.isPresent());})){
                long started=System.nanoTime();var first=recovery.sweep().toCompletableFuture().join();assertThat(first.examined()).isEqualTo(12);assertThat(observed).hasSize(11);var repaired=recovery.sweep().toCompletableFuture().join();assertThat(repaired.cycleComplete()).isTrue();assertThat(observed).containsAll(calls);assertThat(Duration.ofNanos(System.nanoTime()-started)).isLessThanOrEqualTo(Duration.ofSeconds(5));
            }
        }
    }
    @Test void expiredCallerReservationSafelyTerminalizesSilentWorkflowOnHydration()throws Exception {
        try(var f=new Fixture()){var call=f.create();try(var c=f.connection();var s=c.prepareStatement("UPDATE user_reservation SET lease_until=clock_timestamp()-interval '1 second' WHERE call_id=?")){s.setString(1,call.value());s.executeUpdate();}
            var loaded=finish(f.workflow.load(call,f.tokens.get(call),Duration.ofSeconds(2))).orElseThrow();assertThat(loaded.state()).isEqualTo("TERMINAL");assertThat(loaded.terminalReason()).isEqualTo("PARTICIPATION_EXPIRED");assertThat(loaded.terminalAt()).isNotNull();
        }
    }
    @Test void cancelAndAcceptRaceUsesPrimaryVersionAndWinnerNeverChangesAfterTerminalization()throws Exception {
        try(var f=new Fixture()){
            var call=f.create();finish(f.workflow.apply(f.transition(call,1,CallWorkflowService.Step.RING,UUID.randomUUID(),null,null),Duration.ofSeconds(2)));
            var cancel=new CallCommand(SignalEnvelope.Type.CANCEL,f.callers.get(call),new RequestId(UUID.randomUUID()),call,CommandScope.call(call),null,null,null,"{}","b".repeat(64));
            var cancelHandle=f.commands.executeUnderAuthorityTracked(cancel,new CallCommandService.Authority(call,f.tokens.get(call),1,"TEST_ONLY_VERIFIED",2),Duration.ofSeconds(2));
            var accept=f.transition(call,2,CallWorkflowService.Step.ACCEPT,UUID.randomUUID(),f.callees.get(call),null);var acceptHandle=f.workflow.apply(accept,Duration.ofSeconds(2));
            var cancellation=finishRetry(cancelHandle,()->f.commands.executeUnderAuthorityTracked(cancel,new CallCommandService.Authority(call,f.tokens.get(call),1,"TEST_ONLY_VERIFIED",2),Duration.ofSeconds(2)));var acceptance=finishRetry(acceptHandle,()->f.workflow.apply(accept,Duration.ofSeconds(2)));
            assertThat(cancellation.code()).isIn("CANCEL","STALE_VERSION");assertThat(acceptance.code()).isIn("ACCEPTED_PENDING_ACTIVATION","ALREADY_TERMINAL");
            if(cancellation.code().equals("STALE_VERSION")){var retry=new CallCommand(SignalEnvelope.Type.CANCEL,f.callers.get(call),new RequestId(UUID.randomUUID()),call,CommandScope.call(call),null,null,null,"{}","c".repeat(64));finish(f.commands.executeUnderAuthorityTracked(retry,new CallCommandService.Authority(call,f.tokens.get(call),1,"TEST_ONLY_VERIFIED",3),Duration.ofSeconds(2)));}
            var current=finish(f.workflow.load(call,f.tokens.get(call),Duration.ofSeconds(2))).orElseThrow();assertThat(current.state()).isEqualTo("TERMINAL");
            var delayed=finish(f.workflow.apply(f.transition(call,current.version(),CallWorkflowService.Step.ACTIVATE,UUID.randomUUID(),f.callees.get(call),UUID.randomUUID()),Duration.ofSeconds(2)));assertThat(delayed.snapshot().winner()).isEqualTo(current.winner());assertThat(delayed.snapshot().state()).isEqualTo("TERMINAL");
        }
    }
    @Test void primaryRejectsShortRootEvenWhenProcessGateAndProofStillLookLive()throws Exception {
        try(var f=new Fixture()){var call=f.create();try(var c=f.connection();var q=c.prepareStatement("UPDATE group_owner SET lease_until=clock_timestamp()+interval '2 seconds' WHERE group_id=?")){q.setInt(1,f.tokens.get(call).group());q.executeUpdate();}
            assertThatThrownBy(()->finish(f.workflow.apply(f.transition(call,1,CallWorkflowService.Step.RING,UUID.randomUUID(),null,null),Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        }
    }
    @Test void entityExceptionThenActorHostTerminationRecoversPrimaryStateWithoutClientTraffic()throws Exception {
        try(var f=new Fixture()){
            var call=f.create();var token=f.tokens.get(call);var backend=new CallCommandHandler(f.workflow,f.commands,()->Optional.of(token),1);
            ActorTestKit first=ActorTestKit.create();try{
                var crashed=first.spawn(CallActor.create(call,backend,()->Optional.of(token),Clock.systemUTC()));var observer=first.<Optional<Snapshot>>createTestProbe();crashed.tell(new CallActor.GetSnapshot(observer.ref()));assertThat(observer.receiveMessage().orElseThrow().version()).isEqualTo(1);
                Adapter.toClassic(crashed).tell(org.apache.pekko.actor.Kill.getInstance(),org.apache.pekko.actor.ActorRef.noSender());observer.expectTerminated(crashed);
                var recreated=first.spawn(CallActor.create(call,backend,()->Optional.of(token),Clock.systemUTC()));recreated.tell(new CallActor.GetSnapshot(observer.ref()));assertThat(observer.receiveMessage().orElseThrow().state()).isEqualTo("PREPARING");
            }finally{first.shutdownTestKit();}
            ActorTestKit replacement=ActorTestKit.create();try{
                var entities=new ConcurrentHashMap<CallId,org.apache.pekko.actor.typed.ActorRef<CallMessage>>();
                try(var recovery=new CallRecoveryService(f.runtime.sql,"c001",1,(hint,budget)->{
                    var entity=entities.computeIfAbsent(hint,id->replacement.spawn(CallActor.create(id,backend,()->Optional.of(token),Clock.systemUTC())));
                    return AskPattern.ask(entity,reply->new CallActor.WakeCall(hint,reply,Instant.now().plus(budget)),budget,replacement.scheduler());
                })){var report=recovery.sweep().toCompletableFuture().join();assertThat(report.hydrated()).isEqualTo(1);assertThat(report.cycleComplete()).isTrue();assertThat(finish(f.workflow.load(call,token,Duration.ofSeconds(2))).orElseThrow().version()).isEqualTo(1);}
            }finally{replacement.shutdownTestKit();}
        }
    }
}
