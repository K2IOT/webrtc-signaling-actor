package io.webrtc.signaling.storage;
import java.util.concurrent.CompletionStage;
/** Read-only lifecycle; FINISHED means physical cleanup, never a claimed COMMIT result. */
public record DbOperation<T>(CompletionStage<T> logical,CompletionStage<PhysicalCompletion> physicalCompletion) {
    public enum PhysicalCompletion {NOT_STARTED,FINISHED}
}
