package io.webrtc.signaling.app.runtime;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/** Private probe inputs; business readiness is derived from native ownership and safety. */
public record NativeGatewayLifecycleEnrollment(InetSocketAddress healthAddress,BooleanSupplier live,Supplier<String> metrics) {
    public NativeGatewayLifecycleEnrollment {
        Objects.requireNonNull(healthAddress); Objects.requireNonNull(live); Objects.requireNonNull(metrics);
        if(healthAddress.isUnresolved())throw new IllegalArgumentException("Resolved private health address required");
    }
}
