package io.webrtc.signaling.rpc;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
/** Logical outcome and independent physical cleanup; timeout never returns physical credit. */
public record RpcOperation<T>(CompletionStage<T> logical,CompletionStage<?> physicalCompletion) {
    public RpcOperation {Objects.requireNonNull(logical);Objects.requireNonNull(physicalCompletion);}
}
