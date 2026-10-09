package io.webrtc.signaling.app.runtime;

import com.typesafe.config.Config;
import io.webrtc.signaling.actors.cluster.ShardingBootstrap;
import java.util.Objects;
import javax.net.ssl.SSLContext;

/** Explicit cell configuration and PKI; no default identity or trust is synthesized. */
public record NativeActorProcessEnrollment(
    String systemName,
    Config config,
    SSLContext managementServerTls,
    SSLContext managementClientTls) {
  public NativeActorProcessEnrollment {
    if (systemName == null || !systemName.matches("[A-Za-z][A-Za-z0-9_-]{0,127}"))
      throw new IllegalArgumentException("Invalid native actor process name");
    Objects.requireNonNull(config);
    Objects.requireNonNull(managementServerTls);
    Objects.requireNonNull(managementClientTls);
    config = config.resolve();
    ShardingBootstrap.validateProduction(config);
  }
}
