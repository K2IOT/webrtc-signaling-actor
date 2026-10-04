package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class PortableHomeGrantIT {
    HomeParticipationService.Request request(CallId call,UserId user,Instant issued,Instant until){return new HomeParticipationService.Request(user,call,UUID.randomUUID(),"a".repeat(64),1,HomeParticipationService.Phase.PREPARING,new HomeParticipationService.Grant("c002",1,1,HomeParticipationService.group(call),1,1,UUID.randomUUID(),issued,until,"TEST_ONLY_SIGNATURE_ALREADY_VALIDATED"));}
    @Test void freshPrimaryHomeGuardRejectsFutureIssuedProofOutsidePairBound()throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var user=f.sender("TEST_ONLY_future_home_grant").userId();var call=CallId.create("c002",1);
            var home=new HomeParticipationService(f.runtime.sql,"c001",1,r->true); // TEST_ONLY isolates native primary timing from codec rejection.
            Instant primary;try(var c=f.connection();var q=c.createStatement();var r=q.executeQuery("SELECT clock_timestamp()")){r.next();primary=r.getTimestamp(1).toInstant();}
            var future=request(call,user,primary.plusMillis(750),primary.plusSeconds(4));
            assertThatThrownBy(()->home.queryParticipation(future).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        }
    }
    @Test void freshPrimaryHomeGuardReservesPairUncertaintyBeforeApplyingGrant()throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()){
            var user=f.sender("TEST_ONLY_short_home_grant").userId();var call=CallId.create("c002",1);var home=new HomeParticipationService(f.runtime.sql,"c001",1,r->true);
            home.queryParticipation(request(call,user,Instant.now(),Instant.now().plusSeconds(4))).toCompletableFuture().join();
            Instant primary;try(var c=f.connection();var q=c.createStatement();var r=q.executeQuery("SELECT clock_timestamp()")){r.next();primary=r.getTimestamp(1).toInstant();}
            var shortGrant=request(call,user,primary,primary.plusMillis(200));
            assertThatThrownBy(()->home.queryParticipation(shortGrant).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        }
    }
}
