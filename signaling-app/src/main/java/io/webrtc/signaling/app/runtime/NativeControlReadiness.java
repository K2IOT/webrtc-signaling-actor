package io.webrtc.signaling.app.runtime;

import org.springframework.context.SmartLifecycle;

/** Starts after the native HTTP server; readiness falls before Boot's graceful HTTP drain. */
public final class NativeControlReadiness implements SmartLifecycle {
    private final NativeControlBusinessEnrollment inputs;
    private volatile boolean running,draining;
    NativeControlReadiness(NativeControlBusinessEnrollment inputs){this.inputs=inputs;}
    public boolean safe(){try{return !draining&&inputs.clock().valid()&&inputs.securityFresh().getAsBoolean()&&inputs.clock().valid()&&!draining;}catch(RuntimeException unavailable){return false;}}
    public boolean ready(){return running&&safe();}
    @Override public void start(){if(draining)throw new IllegalStateException("Control readiness already drained");running=true;}
    @Override public boolean isRunning(){return running;}
    @Override public int getPhase(){return Integer.MAX_VALUE;}
    @Override public void stop(){draining=true;running=false;}
}
