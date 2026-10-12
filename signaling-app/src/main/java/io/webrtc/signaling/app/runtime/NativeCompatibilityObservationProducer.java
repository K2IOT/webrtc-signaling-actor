package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.actors.cluster.*;
import io.webrtc.signaling.app.CompatibilityWindow;
import io.webrtc.signaling.protocol.internal.*;
import io.webrtc.signaling.rpc.*;
import java.time.*;
import java.util.*;
import java.util.function.Supplier;

/**
 * Produces descriptors directly from authenticated current Pekko roles and local writer history.
 */
public final class NativeCompatibilityObservationProducer {
  private final NativeCompatibilityObservationEnrollment inputs;
  private final Supplier<NativeActorSchedulingEnrollment> actors;
  private final NativeCompatibility compatibility;
  private volatile CompatibilityObservationReply actorFacts;

  public NativeCompatibilityObservationProducer(
      NativeCompatibilityObservationEnrollment inputs,
      Supplier<NativeActorSchedulingEnrollment> actors,
      NativeCompatibility compatibility) {
    this.inputs = inputs;
    this.actors = actors;
    this.compatibility = compatibility;
  }

  public NativeWorkerScheduler.Job membershipJob() {
    return new NativeWorkerScheduler.Job(
        "compatibility_membership",
        NativeWorkerScheduler.Priority.SAFETY,
        Duration.ofSeconds(1),
        budget -> {
          try {
            refreshMembership();
          } catch (RuntimeException unavailable) {
            actorFacts = null;
          }
          return new RpcOperation<>(
              java.util.concurrent.CompletableFuture.completedFuture(null),
              java.util.concurrent.CompletableFuture.completedFuture(null));
        });
  }

  private void refreshMembership() {
    var enrolled = Objects.requireNonNull(actors.get());
    if (!enrolled
        .system()
        .settings()
        .config()
        .getBoolean("pekko.remote.artery.ssl.config.require-mutual-authentication"))
      throw new IllegalStateException("Unauthenticated membership");
    var peers =
        new NativeClusterMembership(enrolled.system(), new ClusterReadiness(), enrolled.azRoles())
            .authenticatedUpPeers();
    if (peers.isEmpty() || peers.size() > 7)
      throw new IllegalStateException("Incomplete membership");
    long now = Instant.now().toEpochMilli();
    var result =
        CompatibilityObservationReply.newBuilder()
            .setSourceCell(inputs.cell())
            .setObservedAtMs(now)
            .setValidUntilMs(now + 5000);
    for (var peer : peers) {
      if (peer.zone().equals("INVALID")) throw new IllegalStateException("Invalid membership");
      var roles = peer.roles();
      var build =
          new NativeCompatibilityEnrollment.BuildIdentity(
              Integer.parseInt(single(roles, "compat-major-")),
              Integer.parseInt(single(roles, "compat-minor-")),
              single(roles, "compat-executable-"),
              single(roles, "compat-fingerprint-"),
              features(roles));
      if (!inputs.cell().equals(single(roles, "compat-cell-")))
        throw new IllegalStateException("Wrong membership cell");
      result.addActors(
          descriptor(build).setMemberId(peer.memberId().toString()).setZone(peer.zone()));
    }
    actorFacts = result.build();
  }

  public CompatibilityObservationReply actorFacts() {
    var snapshot = actorFacts;
    if (snapshot == null) return null;
    var schema = compatibility.localSchemaObservation();
    if (schema == null) return snapshot;
    return snapshot.toBuilder()
        .setSchemaKnown(true)
        .setExactMigration16Applied(schema.exactMigration16Applied())
        .setSchemaValidUntilMs(schema.validUntil().toEpochMilli())
        .build();
  }

  /**
   * The server overwrites these bindings with its immutable native gateway/boot constructor
   * identity.
   */
  public CompatibilityObservationReply gatewayFacts() {
    long now = Instant.now().toEpochMilli();
    return CompatibilityObservationReply.newBuilder()
        .setSourceCell(inputs.cell())
        .setObservedAtMs(now)
        .setValidUntilMs(now + 5000)
        .setGateway(descriptor(inputs.installedBuild()))
        .build();
  }

  private CompatibilityActorDescriptor.Builder descriptor(
      NativeCompatibilityEnrollment.BuildIdentity build) {
    var result =
        CompatibilityActorDescriptor.newBuilder()
            .setCell(inputs.cell())
            .setMajor(build.major())
            .setMinor(build.minor())
            .setExecutableId(build.executableId())
            .setFingerprint(build.fingerprint());
    build.implementedFeatures().forEach(feature -> result.addFeatures(feature.name()));
    return result;
  }

  private static Set<CompatibilityWindow.Feature> features(Set<String> roles) {
    var result = EnumSet.noneOf(CompatibilityWindow.Feature.class);
    for (String role : roles)
      if (role.startsWith("compat-feature-"))
        result.add(
            CompatibilityWindow.Feature.valueOf(
                role.substring(15).replace('-', '_').toUpperCase(Locale.ROOT)));
    if (roles.contains("compat-observation-native-actor-trace-v1"))
      result.add(CompatibilityWindow.Feature.NATIVE_ACTOR_TRACE_V1);
    return result;
  }

  private static String single(Set<String> roles, String prefix) {
    var matching = roles.stream().filter(r -> r.startsWith(prefix)).toList();
    if (matching.size() != 1) throw new IllegalStateException("Missing membership descriptor");
    return matching.getFirst().substring(prefix.length());
  }
}
