package io.webrtc.signaling.rpc;
import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
class RpcAdmissionDrainTest {
    @Test void permanentDrainRetainsBothIsolatedLanesUntilTheirFinalPhysicalTicketRetires() {
        var admission=new RpcAdmission(2,98304,2,98304);
        var control=admission.acquire(RpcAdmission.Lane.CONTROL,100);var relay=admission.acquire(RpcAdmission.Lane.RELAY,200);
        var drain=admission.drain();assertThat(drain.toCompletableFuture()).isNotDone();
        assertThatThrownBy(()->admission.acquire(RpcAdmission.Lane.CONTROL,1)).isInstanceOf(RpcAdmission.Overloaded.class);
        control.close();control.close();assertThat(drain.toCompletableFuture()).isNotDone();
        assertThat(admission.inFlight(RpcAdmission.Lane.RELAY)).isEqualTo(1);
        relay.close();drain.toCompletableFuture().join();assertThat(admission.inFlight(RpcAdmission.Lane.CONTROL)).isZero();
        assertThatThrownBy(()->admission.acquire(RpcAdmission.Lane.RELAY,1)).isInstanceOf(RpcAdmission.Overloaded.class);
    }
}
