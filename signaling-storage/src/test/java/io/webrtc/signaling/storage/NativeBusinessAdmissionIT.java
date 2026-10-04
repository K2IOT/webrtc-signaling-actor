package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class NativeBusinessAdmissionIT {
    @Test void lossOfBusinessCapacityInsideAuthorizationCannotCreateCallButReplayAndCancelRemainAvailable() throws Exception {
        try(var f=new LocalInviteAtomicIT.Fixture()) {
            var caller=f.sender("capacity-caller");var callee=f.sender("capacity-callee");var invite=f.invite(caller,callee.userId());
            var call=CallId.create("c001",1);var group=f.token(call);var capacity=new AtomicBoolean(true);var loseDuringAuthorization=new AtomicBoolean(true);
            var service=new CallCommandService(f.runtime.sql,"c001",1,c->{throw new AssertionError();},(c,s,p)->{if(loseDuringAuthorization.getAndSet(false))capacity.set(false);return p.equals("TEST_ONLY_VERIFIED");}).businessAdmission(capacity::get);
            var authority=new CallCommandService.Authority(call,group,1,"TEST_ONLY_VERIFIED",0,new CallCommandService.TargetHome("c001",1));
            assertThatThrownBy(()->SessionAuthReadIT.done(service.executeUnderAuthorityTracked(invite,authority,Duration.ofSeconds(2)))).hasRootCauseInstanceOf(DbOverloadedException.class);
            try(var c=f.connection();var q=c.createStatement();var r=q.executeQuery("SELECT count(*) FROM call_state")){r.next();assertThat(r.getInt(1)).isZero();}
            capacity.set(true);var accepted=SessionAuthReadIT.done(service.executeUnderAuthorityTracked(invite,authority,Duration.ofSeconds(2)));assertThat(accepted.state()).isEqualTo("RINGING");
            capacity.set(false);assertThat(SessionAuthReadIT.done(service.executeUnderAuthorityTracked(invite,authority,Duration.ofSeconds(2)))).isEqualTo(accepted);
            var cancel=new CallCommand(SignalEnvelope.Type.CANCEL,caller,new RequestId(java.util.UUID.randomUUID()),call,CommandScope.call(call),null,null,null,"{}","c".repeat(64));
            assertThat(SessionAuthReadIT.done(service.executeUnderAuthorityTracked(cancel,new CallCommandService.Authority(call,group,1,"TEST_ONLY_VERIFIED",1),Duration.ofSeconds(2))).state()).isEqualTo("TERMINAL");
        }
    }
}
