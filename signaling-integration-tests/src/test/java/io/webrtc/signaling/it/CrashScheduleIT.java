package io.webrtc.signaling.storage;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.actors.call.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
class CrashScheduleIT {
    static int count(LocalInviteAtomicIT.Fixture f,String table)throws Exception{try(var c=f.connection();var q=c.createStatement();var r=q.executeQuery("SELECT count(*) FROM "+table)){r.next();return r.getInt(1);}}
    @Test void failureBeforeCommitRollsBackCallResultsBothHomesAndOutboxBeforeSameIdentityRetry()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var caller=f.sender("crash-caller");f.sender("crash-callee");var command=f.invite(caller,new UserId("crash-callee"));
            try(var c=f.connection();var q=c.createStatement()){
                q.execute("CREATE FUNCTION test_only_abort_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'TEST_ONLY_ABORT_BEFORE_COMMIT'; END $$");
                q.execute("CREATE TRIGGER test_only_abort_outbox BEFORE INSERT ON control_outbox FOR EACH ROW EXECUTE FUNCTION test_only_abort_outbox()");
            }
            assertThatThrownBy(()->f.service().executeCallCommand(command).toCompletableFuture().join()).isInstanceOf(CompletionException.class);
            for(String table:List.of("call_state","command_result","home_participation","user_reservation","control_outbox"))assertThat(count(f,table)).as(table).isZero();
            try(var c=f.connection();var q=c.createStatement()){q.execute("DROP TRIGGER test_only_abort_outbox ON control_outbox");}
            assertThat(f.service().executeCallCommand(command).toCompletableFuture().join().code()).isEqualTo("RINGING");
            assertThat(count(f,"call_state")).isEqualTo(1);assertThat(count(f,"home_participation")).isEqualTo(2);
        }
    }
    @Test void lostResponseAfterActualCommitReplaysOriginalCallAndImmutableResultWithoutDuplicateOutbox()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var caller=f.sender("ack-caller");f.sender("ack-callee");var command=f.invite(caller,new UserId("ack-callee"));
            assertThatThrownBy(()->f.service().executeCallCommand(command).thenApply(value->{throw new DbOutcomeUnknownException();}).toCompletableFuture().join()).hasCauseInstanceOf(DbOutcomeUnknownException.class);
            var result=f.service().executeCallCommand(command).toCompletableFuture().join();
            assertThat(result.code()).isEqualTo("RINGING");assertThat(f.service().executeCallCommand(command).toCompletableFuture().join()).isEqualTo(result);
            assertThat(count(f,"call_state")).isEqualTo(1);assertThat(count(f,"command_result")).isEqualTo(1);assertThat(count(f,"home_participation")).isEqualTo(2);assertThat(count(f,"user_reservation")).isEqualTo(2);assertThat(count(f,"control_outbox")).isEqualTo(2);
        }
    }
    @Test void productionBackendColdHydrationInvalidatesLostVolatileRoundWithoutResettingDeadline()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var caller=f.sender("cold-caller");var callee=f.sender("cold-callee");var call=NegotiationGrantIT.ready(f,caller,callee);var commands=NegotiationGrantIT.commands(f,callee);
            NegotiationGrantIT.done(commands.executeUnderAuthorityTracked(NegotiationGrantIT.request(caller,call),new CallCommandService.Authority(call,f.token(call),1,"TEST_ONLY",4),Duration.ofSeconds(2)));
            var before=commands.loadCallSnapshot(caller,call).toCompletableFuture().join();
            var workflow=new CallWorkflowService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER",(t,s)->false);
            var backend=new CallCommandHandler(workflow,commands,()->Optional.of(f.token(call)),1);
            var after=NegotiationGrantIT.done(backend.loadCold(call,f.token(call),Duration.ofSeconds(2))).orElseThrow();
            assertThat(after.deadlines()).contains("INVALIDATED");assertThat(after.version()).isEqualTo(before.version()+1);assertThat(after.negotiationId()).isEqualTo(before.negotiationId());
            assertThat(DurableDeadlines.due(after)).isEqualTo(DurableDeadlines.due(before));
        }
    }
}
