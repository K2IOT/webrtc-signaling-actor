package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.util.List;

class GeneratorResourcesTest {
    @Test void sourceInterfaceIsMeasuredWithoutMixingOtherHostTraffic(){
        var measured=GeneratorResources.counters(List.of("Inter-| Receive | Transmit"," eth0: 100 0 0 0 0 0 0 0 200 0 0 0 0 0 0 0"," lo: 9999 0 0 0 0 0 0 0 8888 0 0 0 0 0 0 0"),"eth0");
        assertThat(measured.received()).isEqualTo(100);assertThat(measured.transmitted()).isEqualTo(200);
        assertThatThrownBy(()->GeneratorResources.counters(List.of(" lo: 1"),"eth0")).isInstanceOf(IllegalStateException.class);
    }
    @Test void nicAndFdHeadroomUseActualRatesAndSoftLimit(){
        var healthy=GeneratorResources.headroom(.1,100,100,1000,100,1000,1_000_000,1,100,1024,10000);
        assertThat(healthy.values()).containsOnly(true);
        assertThat(GeneratorResources.headroom(.1,801,100,1000,801,1000,1_000_000,1,100,1024,10000)).containsEntry("nic",false).containsEntry("fd",false);
    }
    @Test void unknownOrNonFiniteMeasurementsCannotProveHeadroom(){
        assertThat(GeneratorResources.headroom(Double.NaN,0,0,1000,1,100,0,0,10,0,100)).containsEntry("cpu",false);
        assertThat(GeneratorResources.headroom(.1,Double.NaN,0,1000,1,100,0,0,10,0,100)).containsEntry("nic",false);
        assertThat(GeneratorResources.headroom(.1,0,0,1000,1,0,0,0,10,0,100)).containsEntry("fd",false);
    }
}
