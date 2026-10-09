package io.webrtc.signaling.actors.cluster;

import com.typesafe.config.Config;
import java.util.List;
import org.apache.pekko.cluster.*;

public final class FingerprintCompatibilityChecker extends JoinConfigCompatChecker {
  @Override
  public scala.collection.immutable.Seq<String> requiredKeys() {
    return scala.jdk.javaapi.CollectionConverters.asScala(
            List.of(
                "signaling.cluster-fingerprint",
                "signaling.cell-id",
                "signaling.user-hash-version",
                "signaling.ownership-hash-version",
                "pekko.cluster.sharding.number-of-shards",
                "pekko.cluster.sharding.state-store-mode",
                "pekko.actor.allow-java-serialization"))
        .toSeq();
  }

  @Override
  public ConfigValidation check(Config joining, Config actual) {
    return JoinConfigCompatChecker.fullMatch(requiredKeys(), joining, actual);
  }
}
