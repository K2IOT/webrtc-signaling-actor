package io.webrtc.signaling.rpc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryGroupSelectorTest {
    private static final String SELF = "pekko://s@10.0.0.2:25520";

    private static RecoveryGroupSelector.Members members(String... up) {
        return new RecoveryGroupSelector.Members(List.of(up), SELF);
    }

    /**
     * The selection as it was written before: the whole safety gate is re-read for every group that
     * is not in the member's static share.
     */
    private static Set<Integer> perGroupReference(
            BooleanSupplier safe, RecoveryGroupSelector.Members members, IntPredicate holdsGrant) {
        if (!safe.getAsBoolean()) return Set.of();
        int index = members.up().indexOf(members.self());
        if (index < 0 || members.up().isEmpty()) return Set.of();
        var groups = new HashSet<Integer>();
        for (int group = 0; group < 1024; group++)
            if (group % members.up().size() == index
                    || (safe.getAsBoolean() && holdsGrant.test(group))) groups.add(group);
        return Set.copyOf(groups);
    }

    private record Case(String name, boolean safe, RecoveryGroupSelector.Members members, Set<Integer> owned) {
    }

    private static List<Case> table() {
        var a = "pekko://s@10.0.0.1:25520";
        var c = "pekko://s@10.0.0.3:25520";
        return List.of(
                new Case("gate closed", false, members(a, SELF, c), Set.of(0, 7)),
                new Case("gate closed, nothing owned", false, members(a, SELF, c), Set.of()),
                new Case("self not Up", true, members(a, c), Set.of(3)),
                new Case("no members", true, members(), Set.of(3)),
                new Case("single member owns all", true, members(SELF), Set.of()),
                new Case("share only", true, members(a, SELF, c), Set.of()),
                new Case("share plus owned", true, members(a, SELF, c), Set.of(0, 5, 1023)),
                new Case("owned inside share", true, members(a, SELF, c), Set.of(1, 4)),
                new Case("membership changed", true, members(a, SELF), Set.of(2)),
                new Case("all owned", true, members(a, SELF, c), allGroups()));
    }

    private static Set<Integer> allGroups() {
        var all = new HashSet<Integer>();
        for (int group = 0; group < 1024; group++) all.add(group);
        return all;
    }

    @Test
    @DisplayName("select matches the per-group reference on every state of the table")
    void select_tableOfStates_equalsPerGroupReference() {
        for (var state : table()) {
            var selector =
                    new RecoveryGroupSelector(() -> state.safe(), () -> state.members(), state.owned()::contains);

            assertThat(selector.select())
                    .as(state.name())
                    .isEqualTo(perGroupReference(() -> state.safe(), state.members(), state.owned()::contains));
        }
    }

    @Test
    @DisplayName("select reads the safety gate exactly once however many groups are owned")
    void select_manyGroups_evaluatesGateOnce() {
        var reads = new AtomicInteger();
        var selector =
                new RecoveryGroupSelector(
                        () -> {
                            reads.incrementAndGet();
                            return true;
                        },
                        () -> members("pekko://s@10.0.0.1:25520", SELF, "pekko://s@10.0.0.3:25520"),
                        group -> true);

        var groups = selector.select();

        assertThat(groups).hasSize(1024);
        assertThat(reads).hasValue(1);
    }

    @Test
    @DisplayName("select does not read membership or grants when the gate is closed")
    void select_gateClosed_readsNothingElse() {
        var membershipReads = new AtomicInteger();
        var grantReads = new AtomicInteger();
        var selector =
                new RecoveryGroupSelector(
                        () -> false,
                        () -> {
                            membershipReads.incrementAndGet();
                            return members(SELF);
                        },
                        group -> {
                            grantReads.incrementAndGet();
                            return true;
                        });

        assertThat(selector.select()).isEmpty();
        assertThat(membershipReads).hasValue(0);
        assertThat(grantReads).hasValue(0);
    }

    @Test
    @DisplayName("select consults grants only for groups outside the static share")
    void select_staticShare_consultsGrantsOnlyOutsideIt() {
        var lookups = new AtomicInteger();
        var selector =
                new RecoveryGroupSelector(
                        () -> true,
                        () -> members("pekko://s@10.0.0.1:25520", SELF),
                        group -> {
                            lookups.incrementAndGet();
                            return false;
                        });

        assertThat(selector.select()).hasSize(512);
        assertThat(lookups).hasValue(512);
    }

    @Test
    @DisplayName("select decides once: a gate that closes after the read does not shrink the set")
    void select_gateClosesAfterRead_keepsTheDecisionOfTheSingleRead() {
        var reads = new AtomicInteger();
        var owned = Set.of(1, 3, 1023);
        var selector =
                new RecoveryGroupSelector(
                        () -> reads.incrementAndGet() == 1,
                        () -> members("pekko://s@10.0.0.1:25520", SELF),
                        owned::contains);

        var groups = selector.select();

        var expected = new HashSet<Integer>(owned);
        for (int group = 1; group < 1024; group += 2) expected.add(group);
        assertThat(groups).isEqualTo(expected);
        assertThat(reads).hasValue(1);
    }

    @Test
    @DisplayName("each select reads the gate again, so a closed gate empties the next selection")
    void select_gateClosesBetweenSelections_failsClosed() {
        var open = new java.util.concurrent.atomic.AtomicBoolean(true);
        var selector =
                new RecoveryGroupSelector(open::get, () -> members(SELF), group -> false);

        assertThat(selector.select()).hasSize(1024);
        open.set(false);
        assertThat(selector.select()).isEmpty();
    }

    @Test
    @DisplayName("select returns an immutable set")
    void select_result_isImmutable() {
        var selector = new RecoveryGroupSelector(() -> true, () -> members(SELF), group -> false);

        var groups = selector.select();

        assertThatThrownBy(() -> groups.add(5))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
