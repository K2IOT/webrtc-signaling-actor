package io.webrtc.signaling.app;
import static org.assertj.core.api.Assertions.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
class ShutdownCoordinatorTest {
    static final class Hooks implements ShutdownCoordinator.Hooks {
        final List<String> events=new CopyOnWriteArrayList<>();
        final CompletableFuture<Void> settled=new CompletableFuture<>(),released=new CompletableFuture<>(),left=new CompletableFuture<>();
        final AtomicInteger connections=new AtomicInteger(257),maxBatch=new AtomicInteger();
        final AtomicBoolean dbClosed=new AtomicBoolean();
        public void readinessOff(){events.add("ready-off");}
        public void shedIngress(){events.add("shed");}
        public CompletionStage<Void> settleAdmitted(){events.add("settle");return settled;}
        public CompletionStage<Void> handoffAndRelease(){events.add("release");return released;}
        public CompletionStage<Void> leaveCluster(){events.add("leave");return left;}
        public CompletionStage<Void> closeDatabase(){events.add("db-close");dbClosed.set(true);return CompletableFuture.completedFuture(null);}
        public CompletionStage<Integer> reconnectBatch(int maximum){maxBatch.accumulateAndGet(maximum,Math::max);events.add("reconnect");return CompletableFuture.completedFuture(connections.updateAndGet(n->Math.max(0,n-maximum)));}
    }
    @Test void databaseClosesOnlyAfterPhysicalSettlementAndLeaseReleaseAndClusterLeave()throws Exception{
        var hooks=new Hooks();try(var coordinator=new ShutdownCoordinator(hooks)){
            var result=coordinator.shutdown(SignalingApplication.Plane.ACTOR,Duration.ofSeconds(2));
            assertThat(hooks.events).containsExactly("ready-off","shed","settle");assertThat(hooks.dbClosed).isFalse();
            assertThat((Object)coordinator.shutdown(SignalingApplication.Plane.ACTOR,Duration.ofSeconds(2))).isSameAs(result);
            hooks.settled.complete(null);assertThat(hooks.events).containsExactly("ready-off","shed","settle","release");assertThat(hooks.dbClosed).isFalse();
            hooks.released.complete(null);assertThat(hooks.events).endsWith("leave");assertThat(hooks.dbClosed).isFalse();
            hooks.left.complete(null);assertThat(result.toCompletableFuture().get(2,TimeUnit.SECONDS)).isEqualTo(ShutdownCoordinator.Status.COMPLETE);assertThat(hooks.events).endsWith("leave","db-close");
        }
    }
    @Test void timeoutNeverTreatsUnknownCleanupAsFinishedOrClosesPoolAfterLateRelease()throws Exception{
        var hooks=new Hooks();try(var coordinator=new ShutdownCoordinator(hooks)){
            var result=coordinator.shutdown(SignalingApplication.Plane.ACTOR,Duration.ofMillis(80));
            hooks.settled.complete(null);
            assertThat(result.toCompletableFuture().get(2,TimeUnit.SECONDS)).isEqualTo(ShutdownCoordinator.Status.TIMED_OUT);
            hooks.released.complete(null);hooks.left.complete(null);
            assertThat(hooks.events).doesNotContain("leave","db-close");assertThat(hooks.dbClosed).isFalse();
        }
    }
    @Test void gatewayTurnsReadinessOffFirstAndReconnectsInBoundedBatchesBeforeSettlement()throws Exception{
        var hooks=new Hooks();hooks.settled.complete(null);hooks.released.complete(null);hooks.left.complete(null);
        try(var coordinator=new ShutdownCoordinator(hooks)){
            assertThat(coordinator.shutdown(SignalingApplication.Plane.GATEWAY,Duration.ofSeconds(2)).toCompletableFuture().get(3,TimeUnit.SECONDS)).isEqualTo(ShutdownCoordinator.Status.COMPLETE);
            assertThat(hooks.maxBatch.get()).isEqualTo(128);assertThat(hooks.events.subList(0,2)).containsExactly("ready-off","shed");
            assertThat(Collections.frequency(hooks.events,"reconnect")).isEqualTo(3);
            assertThat(hooks.events).endsWith("settle","release","leave","db-close");
        }
    }
    @Test void releaseFailureKeepsDatabaseOpenAndReportsFailure()throws Exception{
        var hooks=new Hooks();hooks.settled.complete(null);hooks.released.completeExceptionally(new IllegalStateException("TEST_ONLY_RELEASE_UNKNOWN"));
        try(var coordinator=new ShutdownCoordinator(hooks)){
            assertThat(coordinator.shutdown(SignalingApplication.Plane.ACTOR,Duration.ofSeconds(1)).toCompletableFuture().get()).isEqualTo(ShutdownCoordinator.Status.FAILED);
            assertThat(hooks.events).doesNotContain("leave","db-close");
        }
    }
    @Test void shutdownBudgetCannotConsumeActorSbrHeadroomOrExceedGatewayFiveMinutes(){
        try(var coordinator=new ShutdownCoordinator(new Hooks())){
            assertThatThrownBy(()->coordinator.shutdown(SignalingApplication.Plane.ACTOR,Duration.ofSeconds(66))).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(()->coordinator.shutdown(SignalingApplication.Plane.GATEWAY,Duration.ofSeconds(301))).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
