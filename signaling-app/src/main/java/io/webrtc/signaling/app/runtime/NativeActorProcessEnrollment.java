package io.webrtc.signaling.app.runtime;

import com.typesafe.config.Config;
import io.webrtc.signaling.actors.cluster.ClusterShape;
import io.webrtc.signaling.actors.cluster.ShardingBootstrap;

import javax.net.ssl.SSLContext;
import java.util.Objects;

/**
 * Explicit cell configuration and PKI; no default identity or trust is synthesized.
 */
public record NativeActorProcessEnrollment(
        String systemName,
        Config config,
        SSLContext managementServerTls,
        SSLContext managementClientTls,
        ClusterShape shape) {
    /** A production cell: six members, quorum four. */
    public NativeActorProcessEnrollment(
            String systemName,
            Config config,
            SSLContext managementServerTls,
            SSLContext managementClientTls) {
        this(systemName, config, managementServerTls, managementClientTls, ClusterShape.PRODUCTION);
    }

    public NativeActorProcessEnrollment {
        if (systemName == null || !systemName.matches("[A-Za-z][A-Za-z0-9_-]{0,127}"))
            throw new IllegalArgumentException("Invalid native actor process name");
        Objects.requireNonNull(config);
        Objects.requireNonNull(managementServerTls);
        Objects.requireNonNull(managementClientTls);
        Objects.requireNonNull(shape);
        config = config.resolve();
        ShardingBootstrap.validateProduction(config, shape);
    }

    /** The config holds PKCS12 passwords and the TLS contexts hold key material: never rendered. */
    @Override
    public String toString() {
        return "NativeActorProcessEnrollment[systemName=" + systemName + "]";
    }
}
