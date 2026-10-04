package io.webrtc.signaling.rpc;
import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.Identity.*;
import io.webrtc.signaling.storage.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
class HomeActivationIT {
    static <T>T done(DbOperation<T> op){try{return op.logical().toCompletableFuture().join();}finally{op.physicalCompletion().toCompletableFuture().join();}}
    @Test void nativeHomeRejectsSameCellGrantStorageEpochDifferentFromItsNativeAuthority()throws Exception {
        try(var runtime=new DbTestRuntime()){Instant now=Instant.now();UUID operation=UUID.randomUUID();var home=new HomeParticipationService(runtime.sql,"c001",1,r->true);var request=new HomeParticipationService.Request(new UserId("old-epoch-user"),new CallId(CrossCellSagaIT.CALL),operation,"a".repeat(64),1,HomeParticipationService.Phase.RINGING,new HomeParticipationService.Grant("c001",2,1,685,2,1,operation,now,now.plusSeconds(5),"TEST_ONLY_VERIFIED"));assertThatThrownBy(()->done(new UserReservationService(home).reserveUserTracked(request,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);}
    }
    @Test void committedConfirmationBindsActivationAndVersionWithoutRenewingDuplicateOrExpiredReservations()throws Exception {
        try(var runtime=new DbTestRuntime()){
            var home=new HomeParticipationService(runtime.sql,"c001",1,r->r.grant().proof().equals("TEST_ONLY_VERIFIED"));var reservations=new UserReservationService(home);var activation=new HomeActivationService(home);Instant now=Instant.now();UUID operation=UUID.randomUUID();var call=new CallId(CrossCellSagaIT.CALL);
            var request=new HomeParticipationService.Request(new UserId("activation-caller"),call,operation,"a".repeat(64),1,HomeParticipationService.Phase.RINGING,new HomeParticipationService.Grant("c001",1,1,685,2,1,operation,now,now.plusSeconds(5),"TEST_ONLY_VERIFIED"));
            var p=done(reservations.reserveUserTracked(request,Duration.ofSeconds(2)));UUID id=UUID.randomUUID();var confirmation=done(activation.confirmTracked(request,p.reservationId(),p.version(),id,4,null,operation,Duration.ofSeconds(2)));assertThat(confirmation.activationId()).isEqualTo(id);assertThat(confirmation.callVersion()).isEqualTo(4);assertThat(confirmation.validUntil()).isEqualTo(p.leaseUntil());
            assertThat(done(activation.confirmTracked(request,p.reservationId(),p.version(),id,4,null,operation,Duration.ofSeconds(2)))).isEqualTo(confirmation);
            assertThatThrownBy(()->done(activation.confirmTracked(request,p.reservationId(),confirmation.version(),UUID.randomUUID(),5,null,UUID.randomUUID(),Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            try(var c=PgFixture.connection();var q=c.prepareStatement("UPDATE user_reservation SET lease_until=clock_timestamp()-interval '1 second' WHERE user_id=?")){q.setString(1,request.user().value());q.executeUpdate();}
            assertThatThrownBy(()->done(activation.confirmTracked(request,p.reservationId(),p.version(),id,4,null,operation,Duration.ofSeconds(2)))).hasCauseInstanceOf(AuthoritySql.FencedException.class);
        }
    }
}
