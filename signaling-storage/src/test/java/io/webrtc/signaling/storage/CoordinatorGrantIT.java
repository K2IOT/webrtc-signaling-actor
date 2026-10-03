package io.webrtc.signaling.storage;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
class CoordinatorGrantIT {
    static <T>T done(DbOperation<T> op){try{return op.logical().toCompletableFuture().join();}finally{op.physicalCompletion().toCompletableFuture().join();}}
    @Test void readOnlyGrantUsesFreshPrimaryCallGroupSequenceAndExactLocalOwner()throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()){var caller=f.sender("grant-caller");var callee=f.sender("grant-callee");var command=f.invite(caller,callee.userId());var call=f.service().executeCallCommand(command).toCompletableFuture().join().callId();var token=f.token(call);Instant now=Instant.now();UUID operation=UUID.randomUUID();var route=done(f.runtime.sql.submitTracked(DbClass.CRITICAL,Duration.ofSeconds(2),c->new SessionRepository().find(c,callee.key())));
            var request=new HomeParticipationService.Request(callee.userId(),call,command.requestId().value(),command.intentHash(),1,HomeParticipationService.Phase.RINGING,new HomeParticipationService.Grant("c001",1,1,token.group(),token.epoch(),1,operation,now,now.plusSeconds(5),"UNSIGNED"));var action=new HomeParticipationService.AuthorizationIntent("CLAIM",null,0,null,0,null,route);var grants=new CoordinatorGrantService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER");
            var issued=done(grants.issue(request,action,token,1,1,Duration.ofSeconds(2)));assertThat(issued.snapshot().state()).isEqualTo("RINGING");assertThat(issued.snapshot().version()).isEqualTo(1);assertThat(issued.sequence()).isEqualTo(1);assertThat(Duration.between(issued.checkedAt(),issued.expiresAt())).isLessThanOrEqualTo(Duration.ofSeconds(5));
            assertThatThrownBy(()->done(new CoordinatorGrantService(f.runtime.sql,"c001",1,"BORROWED_OWNER").issue(request,action,token,1,1,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThatThrownBy(()->done(grants.issue(request,action,token,1,2,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            try(var c=f.connection();var q=c.prepareStatement("UPDATE bucket_authority SET status='FROZEN' WHERE bucket_id=?")){q.setInt(1,SessionRegistryService.bucket(caller.userId()));q.executeUpdate();}assertThatThrownBy(()->done(grants.issue(request,action,token,1,1,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        }
    }
    @Test void grantCannotAuthorizeReserveAfterRingingOrAnotherAcquisitionIntent()throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()){var caller=f.sender("grant-state-caller");var callee=f.sender("grant-state-callee");var command=f.invite(caller,callee.userId());var call=f.service().executeCallCommand(command).toCompletableFuture().join().callId();var token=f.token(call);Instant now=Instant.now();UUID operation=UUID.randomUUID();var request=new HomeParticipationService.Request(callee.userId(),call,command.requestId().value(),command.intentHash(),1,HomeParticipationService.Phase.RINGING,new HomeParticipationService.Grant("c001",1,1,token.group(),token.epoch(),1,operation,now,now.plusSeconds(5),"UNSIGNED"));var grants=new CoordinatorGrantService(f.runtime.sql,"c001",1,"TEST_ONLY_LOCAL_OWNER");assertThatThrownBy(()->done(grants.issue(request,HomeParticipationService.AuthorizationIntent.reserve(),token,1,1,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);var changed=new HomeParticipationService.Request(request.user(),call,UUID.randomUUID(),request.payloadHash(),1,request.phase(),request.grant());assertThatThrownBy(()->done(grants.issue(changed,new HomeParticipationService.AuthorizationIntent("QUERY",null,0,null,0,null,null),token,1,1,Duration.ofSeconds(2)))).hasCauseInstanceOf(HomeParticipationService.IntentConflict.class);}
    }
}
