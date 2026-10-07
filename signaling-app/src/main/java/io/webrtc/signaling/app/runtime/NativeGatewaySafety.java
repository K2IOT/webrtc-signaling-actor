package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.ClockSafetyMonitor;
import java.util.function.BooleanSupplier;

/** Cached source facts only; unavailable or stale security cannot borrow a valid clock. */
public final class NativeGatewaySafety {
    private final ClockSafetyMonitor clock;
    private final BooleanSupplier securityFresh;
    NativeGatewaySafety(ClockSafetyMonitor clock,BooleanSupplier securityFresh){this.clock=clock;this.securityFresh=securityFresh;}
    public boolean valid(){try{return clock.valid()&&securityFresh.getAsBoolean()&&clock.valid();}catch(RuntimeException unavailable){return false;}}
}
