package io.webrtc.signaling.actors.cluster;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;

class ClusterReadinessDrainTest {
    @Test void freshFactsCannotUndoProcessDrainWhileHeldSafetyWorkRemainsReady(){
        var readiness=new ClusterReadiness();readiness.update(new ClusterReadiness.Snapshot(true,true,true,true,true,true,4,3,false));
        assertThat(readiness.businessReady()).isTrue();assertThat(readiness.safetyReady()).isTrue();
        readiness.beginDrain();assertThat(readiness.businessReady()).isFalse();assertThat(readiness.safetyReady()).isTrue();
        readiness.update(new ClusterReadiness.Snapshot(true,true,true,true,true,true,6,3,false));
        assertThat(readiness.snapshot().draining()).isTrue();assertThat(readiness.businessReady()).isFalse();assertThat(readiness.safetyReady()).isTrue();
    }
}
