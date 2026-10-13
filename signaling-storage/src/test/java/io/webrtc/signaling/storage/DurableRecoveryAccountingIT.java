package io.webrtc.signaling.storage;

import io.webrtc.signaling.observability.SignalingMetrics.RecoverySli;
import io.webrtc.signaling.protocol.Identity.CallId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Actual JDBC/Flyway journal behavior; TEST_ONLY complete cohorts, never production qualification.
 */
class DurableRecoveryAccountingIT {
    static final String SOURCE = "b".repeat(64);

    static DurableRecoveryAccounting journal(LocalInviteAtomicIT.Fixture f) {
        return new DurableRecoveryAccounting(
                f.runtime.sql,
                "c001",
                1,
                "TEST_ONLY",
                new byte[32],
                call -> Optional.ofNullable(f.tokens.get(HomeParticipationService.group(call))));
    }

    static CallId ringing(LocalInviteAtomicIT.Fixture f) {
        var caller = f.sender("accounting-caller");
        var callee = f.sender("accounting-callee");
        return f.service()
                .executeCallCommand(f.invite(caller, callee.userId()))
                .toCompletableFuture()
                .join()
                .callId();
    }

    static DurableRecoveryAccounting.Cohort cohort(CallId call) {
        return new DurableRecoveryAccounting.Cohort(
                UUID.randomUUID(),
                SOURCE,
                DurableRecoveryAccounting.FaultKind.PROCESS,
                List.of(new DurableRecoveryAccounting.Baseline(call, false, false, Set.of())));
    }

    static <T> T done(DbOperation<T> operation) {
        return NegotiationGrantIT.done(operation);
    }

    static DurableRecoveryAccounting.Observation hydration(
            CallId call, AuthoritySql.GroupToken group) {
        return new DurableRecoveryAccounting.Observation(
                call, 0, 0, RecoverySli.WORKFLOW_CONVERGENCE, null, group, Instant.now());
    }

    @Test
    void captureFaultAndOutcomeRetriesSurviveReconstructionWithoutDoubleCounting() throws Exception {
        try (var f = new LocalInviteAtomicIT.Fixture()) {
            var call = ringing(f);
            var cohort = cohort(call);
            var journal = journal(f);
            done(journal.capture(cohort));
            done(journal.capture(cohort));
            assertThat(done(journal.totals(Set.of())).get(RecoverySli.WORKFLOW_CONVERGENCE).known())
                    .isFalse();
            Instant fault = Instant.now();
            done(journal.start(cohort.id(), fault));
            done(journal.start(cohort.id(), fault));
            try (var c = f.connection();
                 var q =
                         c.prepareStatement(
                                 "SELECT extract(epoch FROM (m.deadline_at-r.fault_at)) FROM recovery_member m JOIN recovery_cohort r USING(cohort_id) WHERE r.cohort_id=?")) {
                q.setObject(1, cohort.id());
                try (var r = q.executeQuery()) {
                    assertThat(r.next()).isTrue();
                    assertThat(r.getLong(1)).isEqualTo(30);
                }
            }
            done(journal.observe(hydration(call, f.token(call)), Set.of(cohort.id())));
            done(journal.observe(hydration(call, f.token(call)), Set.of(cohort.id())));
            var total = done(journal(f).totals(Set.of())).get(RecoverySli.WORKFLOW_CONVERGENCE);
            assertThat(total).isEqualTo(new DurableRecoveryAccounting.Total(1, 1, true));
            // Redundant loss after every eligible member succeeded must not invalidate the result.
            assertThat(done(journal.totals(Set.of(cohort.id()))).get(RecoverySli.WORKFLOW_CONVERGENCE))
                    .isEqualTo(total);
            assertThatThrownBy(() -> done(journal.start(cohort.id(), fault.plusMillis(1))))
                    .hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
        }
    }

    @Test
    void liveSnapshotAndExpiredDeadlineCannotFabricateConvergence() throws Exception {
        try (var f = new LocalInviteAtomicIT.Fixture()) {
            var call = ringing(f);
            var cohort = cohort(call);
            var journal = journal(f);
            done(journal.capture(cohort));
            done(journal.start(cohort.id(), Instant.now()));
            // Use the actual committed version: a raw live read is not an owner hydration result.
            long version;
            try (var c = f.connection();
                 var q = c.prepareStatement("SELECT version FROM call_state WHERE call_id=?")) {
                q.setString(1, call.value());
                try (var r = q.executeQuery()) {
                    r.next();
                    version = r.getLong(1);
                }
            }
            done(
                    journal.observe(
                            new DurableRecoveryAccounting.Observation(
                                    call, version, 0, RecoverySli.WORKFLOW_CONVERGENCE, null, null, Instant.now()),
                            Set.of(cohort.id())));
            assertThat(done(journal.totals(Set.of())).get(RecoverySli.WORKFLOW_CONVERGENCE).successful())
                    .isZero();
            try (var c = f.connection();
                 var q =
                         c.prepareStatement(
                                 "UPDATE recovery_member SET deadline_at=clock_timestamp()-interval '1 second' WHERE cohort_id=?")) {
                q.setObject(1, cohort.id());
                q.executeUpdate();
            }
            done(journal.observe(hydration(call, f.token(call)), Set.of(cohort.id())));
            assertThat(done(journal.totals(Set.of())).get(RecoverySli.WORKFLOW_CONVERGENCE))
                    .isEqualTo(new DurableRecoveryAccounting.Total(1, 0, false));
        }
    }

    @Test
    void lossAndQueuedObservationTargetsCannotContaminateAnotherCohort() throws Exception {
        try (var f = new LocalInviteAtomicIT.Fixture()) {
            var call = ringing(f);
            var a = cohort(call);
            var b = cohort(call);
            var journal = journal(f);
            done(journal.capture(a));
            done(journal.capture(b));
            done(journal.start(a.id(), Instant.now()));
            done(journal.start(b.id(), Instant.now()));
            done(journal.totals(Set.of(a.id())));
            done(journal.observe(hydration(call, f.token(call)), Set.of(b.id())));
            try (var c = f.connection();
                 var q =
                         c.prepareStatement(
                                 "SELECT r.accounting_unknown,m.successful_at IS NOT NULL FROM recovery_cohort r JOIN recovery_member m USING(cohort_id) WHERE r.cohort_id=?")) {
                q.setObject(1, a.id());
                try (var r = q.executeQuery()) {
                    r.next();
                    assertThat(r.getBoolean(1)).isTrue();
                    assertThat(r.getBoolean(2)).isFalse();
                }
                q.setObject(1, b.id());
                try (var r = q.executeQuery()) {
                    r.next();
                    assertThat(r.getBoolean(1)).isFalse();
                    assertThat(r.getBoolean(2)).isTrue();
                }
            }
            assertThat(done(journal.totals(Set.of())).get(RecoverySli.WORKFLOW_CONVERGENCE))
                    .isEqualTo(new DurableRecoveryAccounting.Total(2, 1, false));
        }
    }

    @Test
    void actualActiveEligibilityAndEpochPrivacyBindingFailClosed() throws Exception {
        try (var f = new LocalInviteAtomicIT.Fixture()) {
            var caller = f.sender("active-caller");
            var callee = f.sender("active-callee");
            var call = NegotiationGrantIT.ready(f, caller, callee);
            var journal = journal(f);
            assertThatThrownBy(() -> done(journal.capture(cohort(call))))
                    .hasRootCauseInstanceOf(AuthoritySql.FencedException.class);
            var valid =
                    new DurableRecoveryAccounting.Cohort(
                            UUID.randomUUID(),
                            SOURCE,
                            DurableRecoveryAccounting.FaultKind.AZ,
                            List.of(
                                    new DurableRecoveryAccounting.Baseline(
                                            call, true, false, Set.of(caller.incarnation(), callee.incarnation()))));
            done(journal.capture(valid));
            done(journal.start(valid.id(), Instant.now()));
            var changedKey =
                    new DurableRecoveryAccounting(
                            f.runtime.sql, "c001", 1, "CHANGED", new byte[32], c -> Optional.of(f.token(c)));
            assertThat(done(changedKey.totals(Set.of())).get(RecoverySli.WORKFLOW_CONVERGENCE).known())
                    .isFalse();
            try (var c = f.connection();
                 var q =
                         c.prepareStatement(
                                 "SELECT extract(epoch FROM (m.deadline_at-r.fault_at)) FROM recovery_member m JOIN recovery_cohort r USING(cohort_id) WHERE r.cohort_id=? AND m.sli='SIGNALING_REATTACHMENT'")) {
                q.setObject(1, valid.id());
                try (var r = q.executeQuery()) {
                    while (r.next()) assertThat(r.getLong(1)).isEqualTo(180);
                }
            }
        }
    }
}
