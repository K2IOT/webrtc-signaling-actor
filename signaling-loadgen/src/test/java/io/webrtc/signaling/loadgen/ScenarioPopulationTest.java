package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;

class ScenarioPopulationTest {
    @Test void stagedPopulationUsesExactIntegerCeilingForOriginalSocketShare(){
        assertThat(ScenarioRunner.effectiveUsers(25,50,14)).isEqualTo(7);
        assertThat(ScenarioRunner.effectiveUsers(26,50,14)).isEqualTo(8);
        assertThat(ScenarioRunner.effectiveUsers(100000,10000000,8000000)).isEqualTo(80000);
    }
    @Test void impossibleOrUnboundedOriginalPopulationFailsBeforeInventoryAdmission(){
        for(var shape:new long[][]{{0,50,14},{51,50,14},{25,50,51},{25,0,14},{25,50,0},{10000001,10000001,10000001}})
            assertThatThrownBy(()->ScenarioRunner.effectiveUsers(shape[0],shape[1],shape[2])).isInstanceOf(IllegalArgumentException.class);
    }
}
