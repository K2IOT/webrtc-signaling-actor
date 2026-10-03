package io.webrtc.signaling.auth;
import java.util.function.Consumer;
public final class RevocationConsumer {
    private final RevocationState state;private final Consumer<RevocationState.Event> close;
    public RevocationConsumer(RevocationState state,Consumer<RevocationState.Event> close){this.state=state;this.close=close;}
    public void apply(RevocationState.Event event){if(state.apply(event))close.accept(event);}
}
