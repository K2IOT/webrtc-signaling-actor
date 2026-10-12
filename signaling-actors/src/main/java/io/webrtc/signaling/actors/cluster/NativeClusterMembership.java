package io.webrtc.signaling.actors.cluster;

import java.util.*;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.cluster.Cluster;
import org.apache.pekko.cluster.MemberStatus;

/**
 * Non-authorizing local Pekko facts. Only reachable Up actor members in exactly one enrolled AZ
 * count.
 */
public final class NativeClusterMembership {
  private final Cluster cluster;
  private final ClusterReadiness readiness;
  private final Set<String> enrolledAzRoles;

  public NativeClusterMembership(
      ActorSystem<?> system, ClusterReadiness readiness, Set<String> enrolledAzRoles) {
    this.cluster = Cluster.get(Adapter.toClassic(Objects.requireNonNull(system)));
    this.readiness = Objects.requireNonNull(readiness);
    if (enrolledAzRoles == null
        || enrolledAzRoles.isEmpty()
        || enrolledAzRoles.size() > 3
        || enrolledAzRoles.stream()
            .anyMatch(role -> role == null || !role.matches("az-[a-z0-9][a-z0-9-]{0,62}")))
      throw new IllegalArgumentException("Enrolled AZ roles required");
    this.enrolledAzRoles = Set.copyOf(enrolledAzRoles);
  }

  public record AuthenticatedPeer(java.util.UUID memberId, String zone, Set<String> roles) {
    public AuthenticatedPeer {
      roles = Set.copyOf(roles);
    }
  }

  /** Includes invalid descriptors so a missing capability cannot disappear from the gate. */
  public List<AuthenticatedPeer> authenticatedUpPeers() {
    var state = cluster.state();
    var unreachable = new HashSet<>(state.getUnreachable());
    var peers = new ArrayList<AuthenticatedPeer>();
    for (var member : state.getMembers()) {
      if (!member.status().equals(MemberStatus.up())
          || unreachable.contains(member)
          || !member.hasRole("signaling-actor")) continue;
      var zones = member.getRoles().stream().filter(role -> role.startsWith("az-")).toList();
      String zone =
          zones.size() == 1 && enrolledAzRoles.contains(zones.getFirst())
              ? zones.getFirst()
              : "INVALID";
      var identity =
          java.util.UUID.nameUUIDFromBytes(
              member.uniqueAddress().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
      peers.add(new AuthenticatedPeer(identity, zone, member.getRoles()));
    }
    return List.copyOf(peers);
  }

  public ClusterReadiness.Snapshot refresh() {
    var state = cluster.state();
    var unreachable = new HashSet<>(state.getUnreachable());
    var zones = new HashSet<String>();
    int count = 0;
    boolean localUp = false;
    for (var member : state.getMembers()) {
      if (!member.status().equals(MemberStatus.up())
          || unreachable.contains(member)
          || !member.hasRole("signaling-actor")) continue;
      var memberZones = new HashSet<String>();
      for (String role : member.getRoles()) if (role.startsWith("az-")) memberZones.add(role);
      if (memberZones.size() != 1 || !enrolledAzRoles.containsAll(memberZones)) continue;
      count++;
      zones.addAll(memberZones);
      if (member.uniqueAddress().equals(cluster.selfUniqueAddress())) localUp = true;
    }
    readiness.updateMembership(localUp, count, zones.size());
    return readiness.snapshot();
  }
}
