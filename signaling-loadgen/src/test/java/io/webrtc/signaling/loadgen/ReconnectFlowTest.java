package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ReconnectFlowTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void syncWaitsForTheCommittedResumeOfTheCurrentSocket() {
    var resume = new CompletableFuture<com.fasterxml.jackson.databind.JsonNode>();
    var sync = new AtomicInteger();
    var flow =
        ReconnectFlow.afterResume(
                resume.minimalCompletionStage(), () -> true, sync::incrementAndGet)
            .toCompletableFuture();
    assertThat(sync).hasValue(0);
    assertThat(flow).isNotDone();
    var reply = JSON.createObjectNode().put("type", "ACK_COMMITTED").put("ackCommitted", true);
    reply.putObject("result").put("status", "FINAL").put("code", "RESUMED");
    resume.complete(reply);
    assertThat(flow).isCompleted();
    assertThat(sync).hasValue(1);
  }

  @Test
  void errorUnknownOrSupersededResumeNeverStartsSync() {
    for (String code : new String[] {"OUTCOME_UNKNOWN", "NOT_AUTHORIZED", "RESUMED"}) {
      var sync = new AtomicInteger();
      var reply =
          JSON.createObjectNode()
              .put("type", code.equals("RESUMED") ? "ACK_COMMITTED" : "ERROR")
              .put("ackCommitted", code.equals("RESUMED"));
      reply.putObject("result").put("status", "FINAL").put("code", code);
      assertThat(
              ReconnectFlow.afterResume(
                      CompletableFuture.completedFuture(reply),
                      () -> !code.equals("RESUMED"),
                      sync::incrementAndGet)
                  .toCompletableFuture())
          .isCompletedExceptionally();
      assertThat(sync).hasValue(0);
    }
    var sync = new AtomicInteger();
    assertThat(
            ReconnectFlow.afterResume(
                    CompletableFuture.failedFuture(new TimeoutException()),
                    () -> true,
                    sync::incrementAndGet)
                .toCompletableFuture())
        .isCompletedExceptionally();
    assertThat(sync).hasValue(0);
  }
}
