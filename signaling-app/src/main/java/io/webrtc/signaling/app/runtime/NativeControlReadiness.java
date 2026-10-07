package io.webrtc.signaling.app.runtime;

import org.springframework.context.SmartLifecycle;

/** Starts after the native HTTP server; readiness falls before Boot's graceful HTTP drain. */
public final class NativeControlReadiness implements SmartLifecycle {
    private final NativeControlBusinessEnrollment inputs;
    private volatile boolean running;
    NativeControlReadiness(NativeControlBusinessEnrollment inputs){this.inputs=inputs;}
    public boolean safe(){try{return inputs.clock().valid()&&inputs.securityFresh().getAsBoolean()&&inputs.clock().valid();}catch(RuntimeException unavailable){return false;}}
    public boolean ready(){return running&&safe();}
    @Override public void start(){running=true;}
    @Override public boolean isRunning(){return running;}
    @Override public int getPhase(){return Integer.MAX_VALUE;}
    @Override public void stop(){running=false;}
}
