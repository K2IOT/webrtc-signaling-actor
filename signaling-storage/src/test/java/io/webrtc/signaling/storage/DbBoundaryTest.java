package io.webrtc.signaling.storage;
import static org.assertj.core.api.Assertions.*;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
class DbBoundaryTest {
    @Test void drainRejectsNewWorkAndKeepsResourcesOpenThroughLogicalTimeoutUntilPhysicalSettlement() throws Exception {
        var admission=new DbAdmission(Map.of(DbClass.CRITICAL,1,DbClass.RENEWAL,1));
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var db=new DbBoundary(admission)) {
            var pending=db.submitTracked(DbClass.CRITICAL,Duration.ofMillis(50),()->{entered.countDown();release.await();return "late-native-completion";});
            assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(()->pending.logical().toCompletableFuture().join()).hasCauseInstanceOf(DbOutcomeUnknownException.class);
            var drain=db.drain();assertThat(drain.toCompletableFuture()).isNotDone();
            assertThat(admission.running(DbClass.CRITICAL)).isEqualTo(1);
            var rejected=db.submitTracked(DbClass.RENEWAL,Duration.ofSeconds(1),()->{throw new AssertionError("No SQL may start after drain");});
            assertThatThrownBy(()->rejected.logical().toCompletableFuture().join()).hasCauseInstanceOf(DbOverloadedException.class);
            assertThat(rejected.physicalCompletion().toCompletableFuture().join()).isEqualTo(DbOperation.PhysicalCompletion.NOT_STARTED);
            release.countDown();pending.physicalCompletion().toCompletableFuture().get(1,TimeUnit.SECONDS);
            drain.toCompletableFuture().get(1,TimeUnit.SECONDS);assertThat(admission.running(DbClass.CRITICAL)).isZero();
            assertThatThrownBy(()->db.submit(DbClass.CRITICAL,Duration.ofSeconds(1),()->"never").toCompletableFuture().join()).hasCauseInstanceOf(DbOverloadedException.class);
        } finally {release.countDown();}
    }
    @Test void trackedPhysicalCompletionSurvivesTimeoutAndClientCancellation()throws Exception {
        var admission=new DbAdmission(Map.of(DbClass.RECOVERY,1));var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var db=new DbBoundary(admission)){
            var operation=db.submitTracked(DbClass.RECOVERY,Duration.ofMillis(50),()->{entered.countDown();release.await();return "done";});
            assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();operation.logical().toCompletableFuture().cancel(true);
            assertThat(operation.physicalCompletion().toCompletableFuture().isDone()).isFalse();assertThat(admission.running(DbClass.RECOVERY)).isEqualTo(1);
            release.countDown();operation.physicalCompletion().toCompletableFuture().get(1,TimeUnit.SECONDS);assertThat(admission.running(DbClass.RECOVERY)).isZero();
        }finally{release.countDown();}
    }
    @Test void admissionPrecedesVirtualThreadAndOverloadCannotConsumeRenewalFloor() throws Exception {
        var admission=new DbAdmission(Map.of(DbClass.CRITICAL,1,DbClass.RENEWAL,1));var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var count=new AtomicInteger();
        try(var db=new DbBoundary(admission)) {
            var first=db.submit(DbClass.CRITICAL,Duration.ofSeconds(2),()->{count.incrementAndGet();assertThat(Thread.currentThread().isVirtual()).isTrue();entered.countDown();release.await();return "committed";});
            assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(()->db.submit(DbClass.CRITICAL,Duration.ofSeconds(1),()->{count.incrementAndGet();return "impossible";}).toCompletableFuture().join()).hasCauseInstanceOf(DbOverloadedException.class);
            assertThat(db.submit(DbClass.RENEWAL,Duration.ofSeconds(1),()->"renewed").toCompletableFuture().join()).isEqualTo("renewed");
            assertThat(count.get()).isEqualTo(1);release.countDown();assertThat(first.toCompletableFuture().join()).isEqualTo("committed");
        }finally{release.countDown();}
    }
    @Test void logicalTimeoutRetainsPhysicalCreditUntilWorkFinishes() throws Exception {
        var admission=new DbAdmission(Map.of(DbClass.CRITICAL,1));var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var db=new DbBoundary(admission)){
            var task=db.submit(DbClass.CRITICAL,Duration.ofMillis(50),()->{entered.countDown();release.await();return "physical-finish";});
            assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();assertThatThrownBy(()->task.toCompletableFuture().join()).hasCauseInstanceOf(DbOutcomeUnknownException.class);
            assertThat(admission.running(DbClass.CRITICAL)).isEqualTo(1);release.countDown();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(()->assertThat(admission.running(DbClass.CRITICAL)).isZero());
        }finally{release.countDown();}
    }
    @Test void rejectsManagedOrMutableResultsAndExpiredAdmission(){try(var db=new DbBoundary(new DbAdmission(Map.of(DbClass.NORMAL,1)))){
        assertThatThrownBy(()->db.submit(DbClass.NORMAL,Duration.ofSeconds(1),()->new MutableResult()).toCompletableFuture().join()).hasCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->db.submit(DbClass.NORMAL,Duration.ZERO,()->"not-created").toCompletableFuture().join()).hasCauseInstanceOf(DbOverloadedException.class);
    }}
    static class MutableResult {int field;}
}
