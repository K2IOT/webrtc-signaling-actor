package io.webrtc.signaling.auth;
import static org.assertj.core.api.Assertions.*;
import java.time.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
class BoundedTokenVerifierTest {
    @Test void admissionIsBoundedAndTimeoutDoesNotCreateReplacementWork() throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var boundary=new BoundedTokenVerifier((t,n)->{entered.countDown();try{release.await();}catch(InterruptedException e){Thread.currentThread().interrupt();throw new AuthException();}throw new AuthException();},1,1,Duration.ofMillis(250))) {
            var first=boundary.verify("token",Instant.now());assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();
            var second=boundary.verify("queued",Instant.now());var third=boundary.verify("overload",Instant.now());
            assertThatThrownBy(()->third.toCompletableFuture().join()).hasCauseInstanceOf(RejectedExecutionException.class);
            assertThat(boundary.pending()).isEqualTo(2);release.countDown();
            assertThatThrownBy(()->first.toCompletableFuture().join()).hasCauseInstanceOf(AuthException.class);
            assertThatThrownBy(()->second.toCompletableFuture().join()).hasCauseInstanceOf(AuthException.class);
        }finally{release.countDown();}
    }
    @Test void closeRetainsOriginalVerificationUntilWorkerActuallyExits()throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        var boundary=new BoundedTokenVerifier((t,n)->{entered.countDown();try{release.await();}catch(InterruptedException e){Thread.currentThread().interrupt();throw new AuthException();}throw new AuthException();},1,1,Duration.ofSeconds(1));
        try{
            var original=boundary.verify("TEST_ONLY",Instant.now());assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();
            var closed=CompletableFuture.runAsync(boundary::close);
            try{assertThatThrownBy(()->closed.get(150,TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);assertThat(boundary.pending()).isEqualTo(1);}
            finally{release.countDown();}
            closed.get(3,TimeUnit.SECONDS);assertThat(boundary.pending()).isZero();assertThatThrownBy(()->original.toCompletableFuture().join()).hasCauseInstanceOf(AuthException.class);
            assertThatThrownBy(()->boundary.verify("TEST_ONLY_LATE",Instant.now()).toCompletableFuture().join()).hasCauseInstanceOf(RejectedExecutionException.class);
            assertThat(boundary.pending()).isZero();
        }finally{release.countDown();boundary.close();}
    }
}
