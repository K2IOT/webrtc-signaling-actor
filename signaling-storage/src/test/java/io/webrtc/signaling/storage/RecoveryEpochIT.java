package io.webrtc.signaling.storage;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
class RecoveryEpochIT {
    static <T>T done(DbOperation<T> op)throws Exception{try{return op.logical().toCompletableFuture().get(3,TimeUnit.SECONDS);}finally{op.physicalCompletion().toCompletableFuture().get(3,TimeUnit.SECONDS);}}
    static RecoveryEpochService.Completion completion(UUID op,String evidence){return new RecoveryEpochService.Completion("c001",11,op,0,0,Instant.now().plusSeconds(5),evidence);}
    static RecoveryEpochService.Permit permit(long epoch,UUID op){return new RecoveryEpochService.Permit("c001",1,epoch,10,op,Instant.now().plusSeconds(5),"TEST_ONLY_PHYSICAL_FENCE_AND_EXTERNAL_HIGH_WATER");}
    @Test void restoredEpochMustExceedExternalPreDisasterHighWaterAndInvalidatesEveryOldAuthority()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var caller=f.sender("restore-caller");var target=new UserId("restore-callee");f.sender(target.value());var outcome=f.service().executeCallCommand(f.invite(caller,target)).toCompletableFuture().join();var token=f.tokens.get(HomeParticipationService.group(outcome.callId()));
            var recovery=new RecoveryEpochService(f.runtime.sql,"c001",p->p.signedEvidence().equals("TEST_ONLY_PHYSICAL_FENCE_AND_EXTERNAL_HIGH_WATER"),r->r.signedEvidence().equals("TEST_ONLY_SECURITY_AND_PRIVACY_REPLAY_COMPLETE"));
            UUID op=UUID.randomUUID();assertThatThrownBy(()->done(recovery.begin(permit(2,op)))).hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThat(done(recovery.begin(permit(11,op)))).isEqualTo(11);
            assertThat(done(recovery.begin(permit(11,op)))).isEqualTo(11);
            assertThatThrownBy(()->done(f.groups.pulseTracked(new GroupOwnerRepository.Grant(token,1,UUID.randomUUID(),UUID.randomUUID(),Instant.now().plusSeconds(15),Instant.now()),2,UUID.randomUUID()))).hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThatThrownBy(()->done(f.sessions.renewGatewayBootTracked(f.boot.gatewayId(),f.boot.bootId(),2,UUID.randomUUID(),Duration.ofSeconds(2)))).hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThatThrownBy(()->done(recovery.activate(permit(11,op),completion(op,"TEST_ONLY_SECURITY_AND_PRIVACY_REPLAY_COMPLETE")))).hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
            for(int i=0;i<8;i++){var batch=done(recovery.abortBatch(permit(11,op),1));if(batch.remaining()==0)break;}
            try(var c=f.connection();var q=c.createStatement()){
                try(var r=q.executeQuery("SELECT state,terminal_reason FROM call_state")){assertThat(r.next()).isTrue();assertThat(r.getString(1)).isEqualTo("TERMINAL");assertThat(r.getString(2)).isEqualTo("DISASTER_RESTORE");}
                try(var r=q.executeQuery("SELECT count(*) FROM home_participation WHERE terminal_at IS NULL")){r.next();assertThat(r.getInt(1)).isZero();}
                try(var r=q.executeQuery("SELECT count(*) FROM user_reservation")){r.next();assertThat(r.getInt(1)).isZero();}
                try(var r=q.executeQuery("SELECT count(*) FROM session_registry WHERE closed_at IS NULL")){r.next();assertThat(r.getInt(1)).isZero();}
                try(var r=q.executeQuery("SELECT count(*) FROM control_outbox WHERE delivery_state='PENDING' AND quarantined=false")){r.next();assertThat(r.getInt(1)).isZero();}
                try(var r=q.executeQuery("SELECT count(*) FROM command_result WHERE status='FINAL'")){r.next();assertThat(r.getInt(1)).isEqualTo(1);}
            }
            assertThat(done(recovery.abortBatch(permit(11,op),1)).remaining()).isZero();
            assertThatThrownBy(()->done(recovery.activate(permit(11,op),completion(op,"UNTRUSTED_COMPLETION")))).isInstanceOf(AuthoritySql.FencedException.class);
            try(var c=f.connection();var q=c.createStatement()){q.execute("INSERT INTO security_progress VALUES(1,0,clock_timestamp(),clock_timestamp())");}
            assertThat(done(recovery.activate(permit(11,op),completion(op,"TEST_ONLY_SECURITY_AND_PRIVACY_REPLAY_COMPLETE")))).isEqualTo(11);
            assertThat(done(recovery.begin(permit(11,op)))).isEqualTo(11);
            assertThat(done(recovery.activate(permit(11,op),completion(op,"TEST_ONLY_SECURITY_AND_PRIVACY_REPLAY_COMPLETE")))).isEqualTo(11);
            var newGroup=new GroupOwnerRepository(f.runtime.sql,"c001",11);assertThat(done(newGroup.acquireTracked(token.group(),"TEST_ONLY_NEW_OWNER",UUID.randomUUID(),UUID.randomUUID())).orElseThrow().token().storageEpoch()).isEqualTo(11);
        }
    }
    @Test void missingUntrustedOrExpiredRecoveryPermitCannotAdvanceNativeEpoch()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var recovery=new RecoveryEpochService(f.runtime.sql,"c001",p->false);
            assertThatThrownBy(()->done(recovery.begin(permit(11,UUID.randomUUID())))).isInstanceOf(AuthoritySql.FencedException.class);
            var expired=new RecoveryEpochService.Permit("c001",1,11,10,UUID.randomUUID(),Instant.now().minusSeconds(1),"TEST_ONLY");
            assertThatThrownBy(()->done(new RecoveryEpochService(f.runtime.sql,"c001",p->true).begin(expired))).hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
            try(var c=f.connection();var q=c.createStatement();var r=q.executeQuery("SELECT storage_epoch,status FROM cell_authority")){r.next();assertThat(r.getLong(1)).isEqualTo(1);assertThat(r.getString(2)).isEqualTo("ACTIVE");}
        }
    }
    @Test void replayCompletionNeedsExactNativeSecurityCursorAndCompletedPrivacyRequests()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var recovery=new RecoveryEpochService(f.runtime.sql,"c001",p->true,r->true);
            UUID op=UUID.randomUUID();done(recovery.begin(permit(11,op)));done(recovery.abortBatch(permit(11,op),128));
            try(var c=f.connection();var q=c.createStatement()){
                q.execute("INSERT INTO security_progress VALUES(1,1,clock_timestamp(),clock_timestamp())");
                q.execute("INSERT INTO privacy_deletion_request(opaque_subject,authority_bucket_id,request_id,requested_at) VALUES(decode(repeat('aa',32),'hex'),1,'00000000-0000-0000-0000-000000000001',clock_timestamp())");
            }
            assertThatThrownBy(()->done(recovery.activate(permit(11,op),completion(op,"TEST_ONLY")))).hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
            var current=new RecoveryEpochService.Completion("c001",11,op,1,1,Instant.now().plusSeconds(5),"TEST_ONLY");
            assertThatThrownBy(()->done(recovery.activate(permit(11,op),current))).hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
            try(var c=f.connection();var q=c.createStatement()){q.execute("UPDATE privacy_deletion_request SET completed_at=clock_timestamp()");}
            assertThat(done(recovery.activate(permit(11,op),new RecoveryEpochService.Completion("c001",11,op,1,1,Instant.now().plusSeconds(5),"TEST_ONLY")))).isEqualTo(11);
        }
    }

}
