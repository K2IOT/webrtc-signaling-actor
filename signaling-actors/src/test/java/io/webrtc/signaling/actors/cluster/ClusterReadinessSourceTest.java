package io.webrtc.signaling.actors.cluster;
import static org.assertj.core.api.Assertions.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ClusterReadinessSourceTest {
    @Test void unavailableNativeSourceCannotEscapeAsReady(){
        var readiness=new ClusterReadiness();readiness.installSafetyGate(()->{throw new IllegalStateException("TEST_ONLY_SOURCE_UNAVAILABLE");});
        readiness.update(new ClusterReadiness.Snapshot(true,true,true,true,true,true,4,3,false));
        assertThat(readiness.businessReady()).isFalse();assertThat(readiness.safetyReady()).isFalse();
    }
    @Test void safetyRefreshPreservesNativeMembershipAndRegisteredRegions(){
        var readiness=new ClusterReadiness();readiness.update(new ClusterReadiness.Snapshot(true,true,true,true,true,true,4,3,false));
        readiness.beginDrain();readiness.updateSafety(false,false,false,false);
        assertThat(readiness.snapshot().localUp()).isTrue();assertThat(readiness.snapshot().regionsRegistered()).isTrue();
        assertThat(readiness.snapshot().upActors()).isEqualTo(4);assertThat(readiness.snapshot().reachableAzCount()).isEqualTo(3);
        assertThat(readiness.snapshot().draining()).isTrue();assertThat(readiness.businessReady()).isFalse();
    }
    @Test void originalLiveSourceExpirationClosesAdmissionWithoutWaitingForTheNextPoll(){
        var readiness=new ClusterReadiness();var source=new AtomicBoolean(true);
        readiness.installSafetyGate(source::get);
        readiness.update(new ClusterReadiness.Snapshot(true,true,true,true,true,true,4,3,false));
        assertThat(readiness.businessReady()).isTrue();source.set(false);
        assertThat(readiness.snapshot().clockBoundValid()).isTrue();
        assertThat(readiness.safetyReady()).isFalse();assertThat(readiness.businessReady()).isFalse();
        assertThatThrownBy(()->readiness.installSafetyGate(()->true)).isInstanceOf(IllegalStateException.class);
        source.set(true);assertThat(readiness.businessReady()).isTrue();readiness.beginDrain();
        assertThat(readiness.businessReady()).isFalse();
    }
}
