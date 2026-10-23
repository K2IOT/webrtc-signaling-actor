package io.webrtc.signaling.rpc;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

/**
 * Chooses the recovery groups one scan may visit: an even share of the Up members plus every group
 * this host currently holds. The result is a scheduling hint only; waking a call still re-checks
 * authority per call.
 *
 * <p>The safety gate is read once per selection and applies to the whole set. It must not be read
 * again per group: the gate is a full compatibility evaluation over cluster membership, and a read
 * for each of the 1024 groups on every 100 ms sweep kept an idle actor busy.
 */
final class RecoveryGroupSelector {
    static final int GROUPS = 1024;
    private final BooleanSupplier safe;
    private final Supplier<Members> members;
    private final IntPredicate holdsGrant;

    /**
     * @param safe the complete safety gate; closed means no group is selected
     * @param members reachable Up actor members in a stable order, and this member's identity
     * @param holdsGrant whether this host holds an unexpired grant for a group; this must not
     *     evaluate the safety gate again
     */
    RecoveryGroupSelector(
            BooleanSupplier safe, Supplier<Members> members, IntPredicate holdsGrant) {
        this.safe = Objects.requireNonNull(safe);
        this.members = Objects.requireNonNull(members);
        this.holdsGrant = Objects.requireNonNull(holdsGrant);
    }

    Set<Integer> select() {
        if (!safe.getAsBoolean()) return Set.of();
        var view = members.get();
        int index = view.up().indexOf(view.self());
        if (index < 0 || view.up().isEmpty()) return Set.of();
        var groups = new HashSet<Integer>();
        for (int group = 0; group < GROUPS; group++)
            if (group % view.up().size() == index || holdsGrant.test(group)) groups.add(group);
        return Set.copyOf(groups);
    }

    record Members(List<String> up, String self) {
        Members {
            up = List.copyOf(up);
            Objects.requireNonNull(self);
        }
    }
}
