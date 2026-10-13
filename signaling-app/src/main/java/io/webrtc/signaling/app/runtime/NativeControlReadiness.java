package io.webrtc.signaling.app.runtime;

import org.springframework.context.SmartLifecycle;

/**
 * Starts after the native HTTP server; readiness falls before Boot's graceful HTTP drain.
 */
public final class NativeControlReadiness implements SmartLifecycle {
    private final NativeControlBusinessEnrollment inputs;
    private final boolean transferInstalled;
    private volatile boolean running, draining;
    private java.util.function.BooleanSupplier compatibility = () -> false;

    NativeControlReadiness(NativeControlBusinessEnrollment inputs) {
        this(inputs, false);
    }

    NativeControlReadiness(NativeControlBusinessEnrollment inputs, boolean transferInstalled) {
        this.inputs = inputs;
        this.transferInstalled = transferInstalled;
    }

    public void compatibility(java.util.function.BooleanSupplier compatibility) {
        this.compatibility = java.util.Objects.requireNonNull(compatibility);
    }

    public boolean safe() {
        try {
            return compatibility.getAsBoolean()
                    && transferInstalled
                    && !draining
                    && inputs.clock().valid()
                    && inputs.securityFresh().getAsBoolean()
                    && inputs.clock().valid()
                    && !draining;
        } catch (RuntimeException unavailable) {
            return false;
        }
    }

    public boolean ready() {
        return running && safe();
    }

    @Override
    public void start() {
        if (draining) throw new IllegalStateException("Control readiness already drained");
        running = true;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void stop() {
        draining = true;
        running = false;
    }
}
