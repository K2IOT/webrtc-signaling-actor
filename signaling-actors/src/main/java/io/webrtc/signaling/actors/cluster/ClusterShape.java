package io.webrtc.signaling.actors.cluster;

import java.util.Objects;

/**
 * How many actor members a cell has and how many of them make it ready. Production is fixed at six
 * members with a quorum of four. A LOCAL_TEST_ONLY process may declare a smaller cell through
 * {@link #localMajority(int)}, the only way to obtain a non-production shape: its quorum is a strict
 * majority of its members, and that is what business readiness then requires.
 *
 * <p>Known property of a smaller odd-sized cell: with one extra (surge or stale-incarnation) member
 * present, a 2/2 partition of a three-member cell leaves both halves at quorum until the
 * split-brain resolver downs one of them. The authority fence is the PostgreSQL epoch, not this
 * readiness flag; production's six-with-four has the same property with two stale incarnations.
 */
public final class ClusterShape {
    public static final ClusterShape PRODUCTION = new ClusterShape(6, 4);
    private static final int SMALLEST_LOCAL_CELL = 2;
    private final int members;
    private final int quorum;

    private ClusterShape(int members, int quorum) {
        if (members < 1 || quorum < 1 || quorum > members || quorum * 2 <= members)
            throw new IllegalArgumentException("Invalid cluster shape");
        this.members = members;
        this.quorum = quorum;
    }

    /**
     * A local cell of {@code members} members (two to six), ready on a strict majority of them.
     */
    public static ClusterShape localMajority(int members) {
        if (members < SMALLEST_LOCAL_CELL || members > PRODUCTION.members)
            throw new IllegalArgumentException("Invalid local cell size");
        return new ClusterShape(members, members / 2 + 1);
    }

    public int members() {
        return members;
    }

    public int quorum() {
        return quorum;
    }

    /**
     * Whether a cell may report {@code peers} authenticated Up members to the compatibility gate:
     * at least the quorum, and at most the members plus one surge member. Production: four to seven.
     */
    public boolean admitsPeerCount(int peers) {
        return peers >= quorum && peers <= members + 1;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ClusterShape shape && shape.members == members && shape.quorum == quorum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(members, quorum);
    }

    @Override
    public String toString() {
        return "ClusterShape[members=" + members + ",quorum=" + quorum + "]";
    }
}
