package io.webrtc.signaling.rpc;

import static org.assertj.core.api.Assertions.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

class RpcOperationIsolationTest {
    @Test void cancellingOrCompletingAnObserverCannotForgePhysicalCleanup() {
        var nativeCleanup=new CompletableFuture<Void>();var operation=new RpcOperation<>(CompletableFuture.completedFuture("UNKNOWN"),nativeCleanup);
        operation.physicalCompletion().toCompletableFuture().complete(null);
        assertThat(operation.physicalCompletion().toCompletableFuture()).isNotDone();
        operation.physicalCompletion().toCompletableFuture().cancel(true);
        assertThat(operation.physicalCompletion().toCompletableFuture()).isNotDone();
        nativeCleanup.complete(null);assertThat(operation.physicalCompletion().toCompletableFuture()).isCompleted();
    }
    @Test void timingOutOrCompletingALogicalObserverCannotReplaceTheOriginalNativeResult() {
        var nativeResult=new CompletableFuture<String>();var operation=new RpcOperation<>(nativeResult,CompletableFuture.completedFuture(null));
        operation.logical().toCompletableFuture().complete("FAKE_COMMITTED");
        assertThat(operation.logical().toCompletableFuture()).isNotDone();
        nativeResult.complete("COMMITTED");assertThat(operation.logical().toCompletableFuture().join()).isEqualTo("COMMITTED");
    }
}
