package io.webrtc.signaling.actors.admission;

import java.util.concurrent.CompletionStage;

public record ActorOperation<T>(
    CompletionStage<T> logical, CompletionStage<Void> physicalCompletion) {}
