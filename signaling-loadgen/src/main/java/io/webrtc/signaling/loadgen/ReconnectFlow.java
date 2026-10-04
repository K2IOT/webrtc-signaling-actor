package io.webrtc.signaling.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/** A snapshot may follow only a committed rebind for this still-current connection. */
final class ReconnectFlow {
    static CompletionStage<Void> afterResume(CompletionStage<JsonNode> resume,BooleanSupplier current,Runnable sync){
        return resume.thenAccept(reply->{
            if(!current.getAsBoolean()||!reply.path("type").asText().equals("ACK_COMMITTED")||!reply.path("ackCommitted").asBoolean()||!reply.path("result").path("status").asText().equals("FINAL")||!reply.path("result").path("code").asText().equals("RESUMED"))
                throw new IllegalStateException("Committed current-socket RESUME required");
            sync.run();
        });
    }
}
