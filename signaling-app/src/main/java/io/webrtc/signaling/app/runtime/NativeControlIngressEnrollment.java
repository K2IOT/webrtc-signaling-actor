package io.webrtc.signaling.app.runtime;

import io.netty.handler.ssl.SslContext;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.function.*;

/** Explicit HTTPS and private probe inputs; no plaintext fallback or healthy source default. */
public record NativeControlIngressEnrollment(InetSocketAddress address,SslContext tls,
        InetSocketAddress healthAddress,BooleanSupplier live,Supplier<String> metrics) {
    public NativeControlIngressEnrollment {
        Objects.requireNonNull(address);Objects.requireNonNull(tls);Objects.requireNonNull(healthAddress);Objects.requireNonNull(live);Objects.requireNonNull(metrics);
        if(address.isUnresolved()||healthAddress.isUnresolved()||healthAddress.getAddress().isAnyLocalAddress()||tls.isClient())
            throw new IllegalArgumentException("Control requires resolved HTTPS and private probe addresses");
    }
}
