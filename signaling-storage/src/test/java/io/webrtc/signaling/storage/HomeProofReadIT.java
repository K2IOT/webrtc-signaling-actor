package io.webrtc.signaling.storage;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.auth.AuthPrincipal;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
class HomeProofReadIT {
    @Test void primaryHomeReadBindsCurrentGenerationAndReservationAndRejectsExpiredOrStaleSecurity()throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()){var caller=f.sender("home-proof-caller");var callee=f.sender("home-proof-callee");var command=f.invite(caller,callee.userId());var call=f.service().executeCallCommand(command).toCompletableFuture().join().callId();var token=f.token(call);Instant now=Instant.now();UUID op=UUID.randomUUID();var request=new HomeParticipationService.Request(callee.userId(),call,command.requestId().value(),command.intentHash(),1,HomeParticipationService.Phase.RINGING,new HomeParticipationService.Grant("c001",1,1,token.group(),token.epoch(),1,op,now,now.plusSeconds(5),"TEST_ONLY_QUERY_PROOF"));var home=new HomeParticipationService(f.runtime.sql,"c001",1,r->r.grant().proof().equals("TEST_ONLY_QUERY_PROOF"));var allowed=new java.util.concurrent.atomic.AtomicBoolean(true);var reads=new HomeProofReadService(home,(c,user)->true,(c,r)->allowed.get());
            var view=CoordinatorGrantIT.done(reads.observe(request,callee,Duration.ofSeconds(2)));assertThat(view.currentRoutes()).hasSize(1);assertThat(view.participation().call()).isEqualTo(call);assertThat(view.participation().leaseUntil()).isAfter(view.checkedAt().plusSeconds(5));assertThat(view.sourceCell()).isEqualTo("c001");assertThat(view.sourceStorageEpoch()).isEqualTo(1);
            allowed.set(false);assertThatThrownBy(()->CoordinatorGrantIT.done(reads.observe(request,callee,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);assertThat(CoordinatorGrantIT.done(reads.observe(request,null,Duration.ofSeconds(2))).currentRoutes()).isEmpty();allowed.set(true);
            assertThatThrownBy(()->CoordinatorGrantIT.done(new HomeProofReadService(home,(c,user)->false).observe(request,callee,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            var principal=new AuthPrincipal(callee.userId(),callee.key(),Instant.now().plusSeconds(600),Instant.now(),"TEST_ONLY",1);var route=f.sessions.registerSession(principal,f.boot,UUID.randomUUID(),1).toCompletableFuture().join();assertThat(route.connectionGeneration()).isEqualTo(2);assertThatThrownBy(()->CoordinatorGrantIT.done(reads.observe(request,callee,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            var current=new AuthenticatedSession(route.user(),route.key(),route.incarnation(),route.connectionGeneration(),route.connectionId());assertThat(CoordinatorGrantIT.done(reads.observe(request,current,Duration.ofSeconds(2))).currentRoutes().getFirst().connectionGeneration()).isEqualTo(2);
            try(var c=f.connection();var q=c.prepareStatement("UPDATE user_reservation SET lease_until=clock_timestamp()-interval '1 second' WHERE user_id=?")){q.setString(1,callee.userId().value());q.executeUpdate();}assertThatThrownBy(()->CoordinatorGrantIT.done(reads.observe(request,current,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        }
    }
}
