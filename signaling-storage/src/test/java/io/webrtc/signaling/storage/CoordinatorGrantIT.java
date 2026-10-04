package io.webrtc.signaling.storage;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
class CoordinatorGrantIT {
    static <T>T done(DbOperation<T> op){try{return op.logical().toCompletableFuture().join();}finally{op.physicalCompletion().toCompletableFuture().join();}}
    @Test void readOnlyGrantUsesFreshPrimaryCallGroupSequenceAndExactLocalOwner()throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()){var caller=f.sender("grant-caller");var callee=f.sender("grant-callee");var command=f.invite(caller,callee.userId());var call=f.service().executeCallCommand(command).toCompletableFuture().join().callId();var token=f.token(call);var accept=AcceptCompletionIT.accept(callee,call);done(f.service().executeUnderAuthorityTracked(accept,new CallCommandService.Authority(call,token,1,"TEST_ONLY_VERIFIED",1),Duration.ofSeconds(2)));Instant now=Instant.now();UUID operation=accept.requestId().value();var route=done(f.runtime.sql.submitTracked(DbClass.CRITICAL,Duration.ofSeconds(2),c->new SessionRepository().find(c,callee.key())));
            var request=new HomeParticipationService.Request(callee.userId(),call,command.requestId().value(),command.intentHash(),1,HomeParticipationService.Phase.RINGING,new HomeParticipationService.Grant("c001",1,1,token.group(),token.epoch(),1,operation,now,now.plusSeconds(5),"UNSIGNED"));var action=new HomeParticipationService.AuthorizationIntent("CLAIM",null,0,null,0,null,route);var grants=new CoordinatorGrantService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER");
            var issued=done(grants.issue(request,action,token,1,1,Duration.ofSeconds(2)));assertThat(issued.snapshot().state()).isEqualTo("RINGING");assertThat(issued.snapshot().version()).isEqualTo(1);assertThat(issued.sequence()).isEqualTo(1);assertThat(Duration.between(issued.checkedAt(),issued.expiresAt())).isLessThanOrEqualTo(Duration.ofSeconds(5));
            assertThatThrownBy(()->done(new CoordinatorGrantService(f.runtime.sql,"c001",1,"BORROWED_OWNER").issue(request,action,token,1,1,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThatThrownBy(()->done(grants.issue(request,action,token,1,2,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            try(var c=f.connection();var q=c.prepareStatement("UPDATE bucket_authority SET status='FROZEN' WHERE bucket_id=?")){q.setInt(1,SessionRegistryService.bucket(caller.userId()));q.executeUpdate();}assertThatThrownBy(()->done(grants.issue(request,action,token,1,1,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        }
    }
    @Test void renewalGrantRetainsDedicatedSqlFloorWhenNewCallCreditsAreFull()throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var caller=f.sender("grant-floor-caller");var callee=f.sender("grant-floor-callee");var invite=f.invite(caller,callee.userId());var call=f.service().executeCallCommand(invite).toCompletableFuture().join().callId();var token=f.token(call);var now=Instant.now();
            var request=new HomeParticipationService.Request(caller.userId(),call,invite.requestId().value(),invite.intentHash(),1,HomeParticipationService.Phase.RINGING,new HomeParticipationService.Grant("c001",1,1,token.group(),token.epoch(),1,UUID.randomUUID(),now,now.plusSeconds(5),"UNSIGNED"));
            var release=new java.util.concurrent.CountDownLatch(1);var entered=new java.util.concurrent.CountDownLatch(8);var admitted=new ArrayList<DbOperation<Boolean>>();
            try{
                for(int n=0;n<8;n++)admitted.add(f.runtime.sql.submitTracked(DbClass.CRITICAL,Duration.ofSeconds(2),c->{entered.countDown();release.await();return true;}));
                assertThat(entered.await(1,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                var issued=done(new CoordinatorGrantService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER").issue(request,new HomeParticipationService.AuthorizationIntent("RENEW",null,0,null,0,null,null),token,1,1,Duration.ofSeconds(1)));
                assertThat(issued.snapshot().callId()).isEqualTo(call);
            }finally{release.countDown();for(var work:admitted)work.physicalCompletion().toCompletableFuture().get(3,java.util.concurrent.TimeUnit.SECONDS);}
        }
    }
    @Test void grantCannotAuthorizeReserveAfterRingingOrAnotherAcquisitionIntent()throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()){var caller=f.sender("grant-state-caller");var callee=f.sender("grant-state-callee");var command=f.invite(caller,callee.userId());var call=f.service().executeCallCommand(command).toCompletableFuture().join().callId();var token=f.token(call);Instant now=Instant.now();UUID operation=UUID.randomUUID();var request=new HomeParticipationService.Request(callee.userId(),call,command.requestId().value(),command.intentHash(),1,HomeParticipationService.Phase.RINGING,new HomeParticipationService.Grant("c001",1,1,token.group(),token.epoch(),1,operation,now,now.plusSeconds(5),"UNSIGNED"));var grants=new CoordinatorGrantService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER");assertThatThrownBy(()->done(grants.issue(request,HomeParticipationService.AuthorizationIntent.reserve(),token,1,1,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);var changed=new HomeParticipationService.Request(request.user(),call,UUID.randomUUID(),request.payloadHash(),1,request.phase(),request.grant());assertThatThrownBy(()->done(grants.issue(changed,new HomeParticipationService.AuthorizationIntent("QUERY",null,0,null,0,null,null),token,1,1,Duration.ofSeconds(2)))).hasCauseInstanceOf(HomeParticipationService.IntentConflict.class);}
    }
    @Test void activationConfirmationAcceptsHistoricalWinnerOnlyForTheSameReboundPrincipal()throws Exception{
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var caller=f.sender("confirm-caller");var callee=f.sender("confirm-callee");var invite=f.invite(caller,callee.userId());var call=f.service().executeCallCommand(invite).toCompletableFuture().join().callId();var token=f.token(call);var activation=UUID.randomUUID();
            try(var c=f.connection();var q=c.prepareStatement("UPDATE call_state SET state='ACTIVATING',version=3,activation_id=?,winner_issuer=?,winner_jti=?,winner_incarnation=?,winner_generation=2,deadlines=jsonb_build_object('activationUntil',?::text) WHERE call_id=?")){
                q.setObject(1,activation);q.setString(2,callee.key().issuer());q.setString(3,callee.key().jti());q.setObject(4,callee.incarnation().value());q.setString(5,Instant.now().plusSeconds(10).toString());q.setString(6,call.value());q.executeUpdate();
            }
            var now=Instant.now();var request=new HomeParticipationService.Request(callee.userId(),call,invite.requestId().value(),invite.intentHash(),1,HomeParticipationService.Phase.RINGING,new HomeParticipationService.Grant("c001",1,1,token.group(),token.epoch(),1,UUID.randomUUID(),now,now.plusSeconds(5),"UNSIGNED"));
            var winner=new HomeParticipationService.Winner(callee.key(),callee.incarnation(),1);var action=new HomeParticipationService.AuthorizationIntent("CONFIRM",UUID.randomUUID(),1,activation,3,winner,null);var grants=new CoordinatorGrantService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER");
            assertThat(done(grants.issue(request,action,token,1,3,Duration.ofSeconds(2))).snapshot().winner().generation()).isEqualTo(2);
            var ahead=new HomeParticipationService.AuthorizationIntent("CONFIRM",action.reservation(),1,activation,3,new HomeParticipationService.Winner(callee.key(),callee.incarnation(),3),null);
            assertThatThrownBy(()->done(grants.issue(request,ahead,token,1,3,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        }
    }

}
