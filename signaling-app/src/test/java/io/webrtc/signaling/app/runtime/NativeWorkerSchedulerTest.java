package io.webrtc.signaling.app.runtime;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.rpc.RpcOperation;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class NativeWorkerSchedulerTest {
    @Test void logicalUnknownKeepsOneJobSlotUntilPhysicalCompletionAndDrainRejectsNewTicks()throws Exception {
        var physical=new CompletableFuture<Void>();var calls=new AtomicInteger();var unknown=new CountDownLatch(1);
        try(var workers=new NativeWorkerScheduler(List.of(new NativeWorkerScheduler.Job("renewal",NativeWorkerScheduler.Priority.SAFETY,Duration.ofMillis(100),budget->{calls.incrementAndGet();return new RpcOperation<>(CompletableFuture.failedFuture(new IllegalStateException("TEST_ONLY_UNKNOWN")),physical);})),event->{if(event.status()==NativeWorkerScheduler.Status.UNKNOWN)unknown.countDown();})){
            workers.start();assertThat(unknown.await(1,TimeUnit.SECONDS)).isTrue();
            org.awaitility.Awaitility.await().during(Duration.ofMillis(350)).atMost(Duration.ofSeconds(1)).until(()->calls.get()==1);
            var drained=workers.drain();assertThat(drained.toCompletableFuture()).isNotDone();physical.complete(null);drained.toCompletableFuture().get(1,TimeUnit.SECONDS);
            assertThatThrownBy(workers::start).isInstanceOf(IllegalStateException.class);
            assertThat(calls).hasValue(1);
        }finally{physical.complete(null);}
    }
    @Test void shedStopsMaintenanceButSafetyContinuesUntilFinalDrain()throws Exception {
        var normal=new AtomicInteger();var safety=new AtomicInteger();
        try(var workers=new NativeWorkerScheduler(List.of(
            new NativeWorkerScheduler.Job("outbox",NativeWorkerScheduler.Priority.MAINTENANCE,Duration.ofMillis(100),b->{normal.incrementAndGet();return complete();}),
            new NativeWorkerScheduler.Job("clock",NativeWorkerScheduler.Priority.SAFETY,Duration.ofMillis(100),b->{safety.incrementAndGet();return complete();})),event->{})){
            workers.start();org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(1)).until(()->normal.get()>0&&safety.get()>0);
            workers.shedNormal();workers.settleAdmitted().toCompletableFuture().get(1,TimeUnit.SECONDS);int normalAt=normal.get(),safetyAt=safety.get();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(1)).until(()->safety.get()>=safetyAt+3);assertThat(normal).hasValue(normalAt);
            workers.drain().toCompletableFuture().get(1,TimeUnit.SECONDS);
        }
    }
    @Test void factoryExceptionCannotInventPhysicalCleanup()throws Exception {
        var entered=new CountDownLatch(1);
        try(var workers=new NativeWorkerScheduler(List.of(new NativeWorkerScheduler.Job("native_source",NativeWorkerScheduler.Priority.SAFETY,Duration.ofMillis(100),b->{entered.countDown();throw new IllegalStateException("TEST_ONLY_UNKNOWN_FACTORY");})),event->{})){
            workers.start();assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();assertThatThrownBy(()->workers.drain().toCompletableFuture().get(250,TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
        }
    }
    static RpcOperation<String> complete(){return new RpcOperation<>(CompletableFuture.completedFuture("TEST_ONLY"),CompletableFuture.completedFuture(null));}
}
