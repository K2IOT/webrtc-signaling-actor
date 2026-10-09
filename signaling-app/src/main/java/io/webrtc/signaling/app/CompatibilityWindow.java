package io.webrtc.signaling.app;

import java.util.*;

/** Peer data must come from authenticated current Up membership, never a public request. */
public final class CompatibilityWindow {
  public enum Feature {
    SESSION_S1,
    FULL_CONNECTION_ACTIVE,
    CAUSAL_ROUTE_LOSS
  }

  public record Peer(
      UUID memberId,
      String zone,
      String cell,
      int major,
      int minor,
      String fingerprint,
      Set<Feature> features) {
    public Peer {
      Objects.requireNonNull(memberId);
      if (zone == null
          || zone.isBlank()
          || cell == null
          || major < 1
          || minor < 0
          || fingerprint == null
          || !fingerprint.matches("[a-f0-9]{64}"))
        throw new IllegalArgumentException("Invalid authenticated peer descriptor");
      features = Set.copyOf(features);
    }
  }

  private final String cell, fingerprint;
  private final int major, minor;
  private final EnumSet<Feature> enabled = EnumSet.noneOf(Feature.class);

  public CompatibilityWindow(String cell, int major, int minor, String fingerprint) {
    if (cell == null
        || major < 1
        || minor < 1
        || fingerprint == null
        || !fingerprint.matches("[a-f0-9]{64}"))
      throw new IllegalArgumentException("Invalid N/N-1 window");
    this.cell = cell;
    this.major = major;
    this.minor = minor;
    this.fingerprint = fingerprint;
  }

  public synchronized boolean accepts(Peer peer) {
    return peer != null
        && peer.cell().equals(cell)
        && peer.major() == major
        && (peer.minor() == minor || peer.minor() == minor - 1)
        && peer.fingerprint().equals(fingerprint)
        && peer.features().containsAll(enabled);
  }

  public synchronized boolean enable(Feature feature, List<Peer> authenticatedUpPeers) {
    Objects.requireNonNull(feature);
    Objects.requireNonNull(authenticatedUpPeers);
    if (authenticatedUpPeers.size() < 4
        || authenticatedUpPeers.size() > 7
        || authenticatedUpPeers.stream().map(Peer::memberId).distinct().count()
            != authenticatedUpPeers.size()
        || authenticatedUpPeers.stream().map(Peer::zone).distinct().count() < 2
        || authenticatedUpPeers.stream()
            .anyMatch(p -> !accepts(p) || !p.features().contains(feature))) return false;
    enabled.add(feature);
    return true;
  }

  public synchronized boolean enabled(Feature feature) {
    return enabled.contains(feature);
  }

  public synchronized void disableAll() {
    enabled.clear();
  }

  public synchronized boolean rollbackAllowed(
      long incompatibleDurableWork,
      boolean physicalWorkSettled,
      boolean expandContractSchemaRetained) {
    if (incompatibleDurableWork < 0)
      throw new IllegalArgumentException("Invalid native work count");
    return enabled.isEmpty()
        && incompatibleDurableWork == 0
        && physicalWorkSettled
        && expandContractSchemaRetained;
  }
}
