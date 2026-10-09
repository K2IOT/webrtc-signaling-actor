package io.webrtc.signaling.actors.cluster;

import java.util.concurrent.atomic.AtomicReference;

public final class ClusterReadiness {
  private final int minimumFailureDomains;

  public ClusterReadiness() {
    this(2);
  }

  private ClusterReadiness(int minimumFailureDomains) {
    this.minimumFailureDomains = minimumFailureDomains;
  }

  /** Explicit single-host qualification exception; quorum and live safety remain mandatory. */
  public static ClusterReadiness localMinikube(String acknowledgement) {
    if (!"LOCAL_TEST_ONLY".equals(acknowledgement))
      throw new IllegalArgumentException(
          "Local readiness requires LOCAL_TEST_ONLY acknowledgement");
    return new ClusterReadiness(1);
  }

  private final AtomicReference<java.util.function.BooleanSupplier> liveSafety =
      new AtomicReference<>();

  /**
   * Installed once by the native source owner; expiry is checked on every admission without I/O.
   */
  public void installSafetyGate(java.util.function.BooleanSupplier gate) {
    java.util.Objects.requireNonNull(gate);
    if (!liveSafety.compareAndSet(null, gate))
      throw new IllegalStateException("Native safety source already installed");
  }

  private boolean sourceReady() {
    var gate = liveSafety.get();
    try {
      return gate == null || gate.getAsBoolean();
    } catch (RuntimeException unavailable) {
      return false;
    }
  }

  public record Snapshot(
      boolean localUp,
      boolean regionsRegistered,
      boolean fingerprintValid,
      boolean cellActive,
      boolean safetyPoolUsable,
      boolean clockBoundValid,
      int upActors,
      int reachableAzCount,
      boolean draining) {
    public Snapshot {
      if (upActors < 0 || reachableAzCount < 0)
        throw new IllegalArgumentException("Invalid membership counts");
    }
  }

  private final AtomicReference<Snapshot> state =
      new AtomicReference<>(new Snapshot(false, false, false, false, false, false, 0, 0, false));

  public void update(Snapshot snapshot) {
    java.util.Objects.requireNonNull(snapshot);
    state.updateAndGet(
        previous ->
            new Snapshot(
                snapshot.localUp(),
                snapshot.regionsRegistered(),
                snapshot.fingerprintValid(),
                snapshot.cellActive(),
                snapshot.safetyPoolUsable(),
                snapshot.clockBoundValid(),
                snapshot.upActors(),
                snapshot.reachableAzCount(),
                previous.draining() || snapshot.draining()));
  }

  public void beginDrain() {
    state.updateAndGet(
        s ->
            new Snapshot(
                s.localUp(),
                s.regionsRegistered(),
                s.fingerprintValid(),
                s.cellActive(),
                s.safetyPoolUsable(),
                s.clockBoundValid(),
                s.upActors(),
                s.reachableAzCount(),
                true));
  }

  public Snapshot snapshot() {
    return state.get();
  }

  public void updateMembership(boolean localUp, int upActors, int reachableAzCount) {
    if (upActors < 0 || reachableAzCount < 0)
      throw new IllegalArgumentException("Invalid membership counts");
    state.updateAndGet(
        s ->
            new Snapshot(
                localUp,
                s.regionsRegistered(),
                s.fingerprintValid(),
                s.cellActive(),
                s.safetyPoolUsable(),
                s.clockBoundValid(),
                upActors,
                reachableAzCount,
                s.draining()));
  }

  public void updateSafety(
      boolean fingerprintValid,
      boolean cellActive,
      boolean safetyPoolUsable,
      boolean clockBoundValid) {
    state.updateAndGet(
        s ->
            new Snapshot(
                s.localUp(),
                s.regionsRegistered(),
                fingerprintValid,
                cellActive,
                safetyPoolUsable,
                clockBoundValid,
                s.upActors(),
                s.reachableAzCount(),
                s.draining()));
  }

  private boolean safety(Snapshot s) {
    return sourceReady()
        && s.localUp()
        && s.regionsRegistered()
        && s.fingerprintValid()
        && s.cellActive()
        && s.safetyPoolUsable()
        && s.clockBoundValid();
  }

  public boolean safetyReady() {
    return safety(state.get());
  }

  public boolean businessReady() {
    var s = state.get();
    return safety(s)
        && !s.draining()
        && s.upActors() >= 4
        && s.reachableAzCount() >= minimumFailureDomains;
  }

  void registered() {
    state.updateAndGet(
        s ->
            new Snapshot(
                s.localUp(),
                true,
                s.fingerprintValid(),
                s.cellActive(),
                s.safetyPoolUsable(),
                s.clockBoundValid(),
                s.upActors(),
                s.reachableAzCount(),
                s.draining()));
  }
}
