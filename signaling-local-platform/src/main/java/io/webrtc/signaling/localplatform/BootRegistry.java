package io.webrtc.signaling.localplatform;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Binds a pod UID to the certificate identity that first claimed it, and orders boots per pod.
 *
 * <p>A pod UID supplied by a caller means nothing on its own: it is bound to one SPIFFE id, and a
 * different certificate presenting the same UID is rejected. A new boot for the same pod and
 * identity replaces the current one; a boot that a newer boot already replaced is rejected. State
 * is bounded: {@value #MAX_PODS} pods, and the last {@value #RETIRED_BOOTS_PER_POD} retired boots
 * per pod. A boot older than that window is no longer remembered.
 *
 * <p>All state is guarded by one lock. Status: LOCAL_TEST_ONLY, NOT_QUALIFIED.
 */
public final class BootRegistry {

  static final int MAX_PODS = 4096;
  static final int RETIRED_BOOTS_PER_POD = 8;

  /** An accepted registration. */
  public record Registration(PeerIdentity peer, UUID pod, UUID boot) {
    @Override
    public String toString() {
      return "Registration";
    }
  }

  /** Registration refused. Carries no detail by design. */
  public static final class BootRejected extends Exception {
    private static final long serialVersionUID = 1L;

    BootRejected() {
      super(null, null, false, false);
    }
  }

  private static final class PodState {
    private final String spiffeId;
    private UUID current;
    private final ArrayDeque<UUID> retired = new ArrayDeque<>();

    private PodState(String spiffeId, UUID current) {
      this.spiffeId = spiffeId;
      this.current = current;
    }
  }

  private final Object lock = new Object();
  private final Map<UUID, PodState> pods = new HashMap<>();

  /**
   * Registers {@code boot} of {@code pod} for {@code peer}.
   *
   * @throws BootRejected if the pod is bound to another identity, the boot was replaced by a newer
   *     one, a new pod would exceed the bound, or an argument is null
   */
  public Registration register(PeerIdentity peer, UUID pod, UUID boot) throws BootRejected {
    if (peer == null || peer.spiffeId() == null || pod == null || boot == null) {
      throw new BootRejected();
    }
    synchronized (lock) {
      PodState state = pods.get(pod);
      if (state == null) {
        if (pods.size() >= MAX_PODS) {
          throw new BootRejected();
        }
        pods.put(pod, new PodState(peer.spiffeId(), boot));
      } else if (!state.spiffeId.equals(peer.spiffeId())) {
        throw new BootRejected();
      } else if (!state.current.equals(boot)) {
        if (state.retired.contains(boot)) {
          throw new BootRejected();
        }
        state.retired.addLast(state.current);
        while (state.retired.size() > RETIRED_BOOTS_PER_POD) {
          state.retired.removeFirst();
        }
        state.current = boot;
      }
    }
    return new Registration(peer, pod, boot);
  }
}
