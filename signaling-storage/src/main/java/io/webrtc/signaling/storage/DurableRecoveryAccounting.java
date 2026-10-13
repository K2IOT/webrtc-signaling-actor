package io.webrtc.signaling.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.webrtc.signaling.observability.SignalingMetrics.RecoverySli;
import io.webrtc.signaling.protocol.Identity.AuthenticatedSession;
import io.webrtc.signaling.protocol.Identity.CallId;
import io.webrtc.signaling.protocol.Identity.SessionIncarnation;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

/**
 * Trusted native cohort journal. Admission and dedup are serialized across all writers in this DB.
 */
public final class DurableRecoveryAccounting {
    private static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();
    private final SqlTransactions sql;
    private final String cell, keyId;
    private final long epoch;
    private final byte[] key;
    private final Function<CallId, Optional<AuthoritySql.GroupToken>> authority;
    private final CallSnapshotRepository calls = new CallSnapshotRepository();

    public DurableRecoveryAccounting(
            SqlTransactions sql,
            String cell,
            long epoch,
            String keyId,
            byte[] enrolledKey,
            Function<CallId, Optional<AuthoritySql.GroupToken>> authority) {
        this.sql = Objects.requireNonNull(sql);
        this.cell = Objects.requireNonNull(cell);
        this.epoch = epoch;
        if (epoch < 1
                || keyId == null
                || !keyId.matches("[A-Za-z0-9_-]{1,64}")
                || enrolledKey == null
                || enrolledKey.length < 32
                || enrolledKey.length > 64)
            throw new IllegalArgumentException("Enrolled recovery privacy key required");
        this.keyId = keyId;
        key = enrolledKey.clone();
        this.authority = Objects.requireNonNull(authority);
    }

    private static void lockCapacity(Connection c) throws SQLException {
        try (var q = c.createStatement();
             var r =
                     q.executeQuery(
                             "SELECT singleton_id FROM recovery_accounting_capacity WHERE singleton_id=1 FOR UPDATE NOWAIT")) {
            if (!r.next()) throw new AuthoritySql.FencedException();
        }
    }

    /**
     * A single bounded authority-guarded page captures and seals the exact trusted fault scope.
     */
    public DbOperation<UUID> capture(Cohort cohort) {
        Objects.requireNonNull(cohort);
        return sql.submitTracked(
                DbClass.RECOVERY,
                Duration.ofSeconds(2),
                c -> {
                    root(c);
                    // Permanent capacity includes sealed and unresolved cohorts; there is no automatic
                    // eviction.
                    lockCapacity(c);
                    byte[] manifest =
                            digest(
                                    List.of(
                                            cohort.faultKind().name(),
                                            cohort.completeRoster().stream()
                                                    .map(
                                                            b ->
                                                                    List.of(
                                                                            b.call().value(),
                                                                            b.active(),
                                                                            b.media(),
                                                                            b.disconnected().stream()
                                                                                    .map(i -> i.value().toString())
                                                                                    .sorted()
                                                                                    .toList()))
                                                    .toList()));
                    try (var q =
                                 c.prepareStatement(
                                         "SELECT storage_epoch,source_hash,manifest_hash,privacy_key_id FROM recovery_cohort WHERE cohort_id=?")) {
                        q.setObject(1, cohort.id());
                        try (var r = q.executeQuery()) {
                            if (r.next()) {
                                if (r.getLong(1) != epoch
                                        || !Arrays.equals(r.getBytes(2), HexFormat.of().parseHex(cohort.sourceHash()))
                                        || !Arrays.equals(r.getBytes(3), manifest)
                                        || !keyId.equals(r.getString(4))) throw new AuthoritySql.FencedException();
                                return cohort.id();
                            }
                        }
                    }
                    var members = new ArrayList<Member>();
                    for (var baseline : cohort.completeRoster()) {
                        var snapshot = guarded(c, baseline.call());
                        if (snapshot.terminalAt() != null) throw new AuthoritySql.FencedException();
                        var metadata = JSON.readTree(snapshot.deadlines());
                        boolean active =
                                Set.of("CONNECTING", "ESTABLISHED").contains(snapshot.state())
                                        && snapshot.activationId() != null
                                        && snapshot.winner() != null;
                        if (baseline.active() != active) throw new AuthoritySql.FencedException();
                        if (baseline.media()
                                && (!metadata.path("mediaCallerConnected").asBoolean()
                                || !metadata.path("mediaWinnerConnected").asBoolean()))
                            throw new AuthoritySql.FencedException();
                        members.add(member(cohort.id(), snapshot, RecoverySli.WORKFLOW_CONVERGENCE));
                        if (baseline.active())
                            members.add(member(cohort.id(), snapshot, RecoverySli.ACTIVE_CALL_PRESERVATION));
                        if (baseline.media())
                            members.add(member(cohort.id(), snapshot, RecoverySli.MEDIA_CONTINUITY));
                        for (var disconnected : baseline.disconnected()) {
                            var participant =
                                    java.util.stream.Stream.of(snapshot.caller(), snapshot.winner())
                                            .filter(Objects::nonNull)
                                            .filter(p -> p.incarnation().equals(disconnected))
                                            .findFirst()
                                            .orElseThrow(AuthoritySql.FencedException::new);
                            members.add(
                                    new Member(
                                            participantHash(cohort.id(), snapshot.callId(), participant),
                                            RecoverySli.SIGNALING_REATTACHMENT,
                                            snapshot.version(),
                                            snapshot.negotiationId()));
                        }
                    }
                    try (var q =
                                 c.prepareStatement(
                                         "UPDATE recovery_accounting_capacity SET cohorts=cohorts+1,members=members+? WHERE singleton_id=1 AND cohorts<64 AND members+?<=8192")) {
                        q.setInt(1, members.size());
                        q.setInt(2, members.size());
                        if (q.executeUpdate() != 1) throw new DbOverloadedException();
                    }
                    try (var q =
                                 c.prepareStatement(
                                         "INSERT INTO recovery_cohort(cohort_id,storage_epoch,source_hash,manifest_hash,privacy_key_id,member_count,fault_kind) VALUES(?,?,?,?,?,?,?)")) {
                        q.setObject(1, cohort.id());
                        q.setLong(2, epoch);
                        q.setBytes(3, HexFormat.of().parseHex(cohort.sourceHash()));
                        q.setBytes(4, manifest);
                        q.setString(5, keyId);
                        q.setInt(6, members.size());
                        q.setString(7, cohort.faultKind().name());
                        q.executeUpdate();
                    }
                    try (var q =
                                 c.prepareStatement(
                                         "INSERT INTO recovery_member(cohort_id,member_hash,sli,baseline_version,baseline_round) VALUES(?,?,?,?,?)")) {
                        for (var member : members) {
                            q.setObject(1, cohort.id());
                            q.setBytes(2, member.hash());
                            q.setString(3, member.sli().name());
                            q.setLong(4, member.version());
                            q.setLong(5, member.round());
                            q.addBatch();
                        }
                        q.executeBatch();
                    }
                    return cohort.id();
                });
    }

    /**
     * Actual fault timestamp is immutable; retries never move it or reset any deadline.
     */
    public DbOperation<UUID> start(UUID id, Instant actualFaultAt) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(actualFaultAt);
        final Instant faultAt = actualFaultAt.truncatedTo(java.time.temporal.ChronoUnit.MICROS);
        return sql.submitTracked(
                DbClass.RECOVERY,
                Duration.ofSeconds(2),
                c -> {
                    root(c);
                    lockCapacity(c);
                    try (var q =
                                 c.prepareStatement(
                                         "SELECT storage_epoch,fault_at,privacy_key_id,captured_at<=?::timestamptz AND captured_at>=?::timestamptz-interval '5 seconds',?::timestamptz<=clock_timestamp() AND ?::timestamptz>=clock_timestamp()-interval '5 seconds' FROM recovery_cohort WHERE cohort_id=? FOR UPDATE")) {
                        for (int i = 1; i <= 4; i++) q.setTimestamp(i, Timestamp.from(faultAt));
                        q.setObject(5, id);
                        try (var r = q.executeQuery()) {
                            if (!r.next() || r.getLong(1) != epoch || !keyId.equals(r.getString(3)))
                                throw new AuthoritySql.FencedException();
                            if (r.getTimestamp(2) != null) {
                                if (!r.getTimestamp(2).toInstant().equals(faultAt))
                                    throw new AuthoritySql.FencedException();
                                return id;
                            }
                            if (!r.getBoolean(4) || !r.getBoolean(5)) throw new AuthoritySql.FencedException();
                        }
                    }
                    try (var q =
                                 c.prepareStatement(
                                         "UPDATE recovery_cohort SET fault_at=? WHERE cohort_id=? AND fault_at IS NULL")) {
                        q.setTimestamp(1, Timestamp.from(faultAt));
                        q.setObject(2, id);
                        q.executeUpdate();
                    }
                    try (var q =
                                 c.prepareStatement(
                                         "UPDATE recovery_member SET deadline_at=?::timestamptz + CASE sli WHEN 'WORKFLOW_CONVERGENCE' THEN interval '30 seconds' WHEN 'MEDIA_CONTINUITY' THEN interval '30 seconds' WHEN 'SIGNALING_REATTACHMENT' THEN CASE (SELECT fault_kind FROM recovery_cohort WHERE cohort_id=?) WHEN 'AZ' THEN interval '180 seconds' ELSE interval '60 seconds' END ELSE interval '60 seconds' END WHERE cohort_id=?")) {
                        q.setTimestamp(1, Timestamp.from(faultAt));
                        q.setObject(2, id);
                        q.setObject(3, id);
                        q.executeUpdate();
                    }
                    try (var q =
                                 c.prepareStatement(
                                         "UPDATE recovery_totals t SET eligible=eligible+x.n FROM (SELECT sli,count(*) n FROM recovery_member WHERE cohort_id=? GROUP BY sli) x WHERE t.sli=x.sli")) {
                        q.setObject(1, id);
                        q.executeUpdate();
                    }
                    return id;
                });
    }

    /**
     * Native hooks are revalidated at the current owner; fresh facts cannot cross a domain/round.
     */
    public DbOperation<Void> observe(Observation observation, Set<UUID> admittedCohorts) {
        Objects.requireNonNull(observation);
        final Set<UUID> targets = Set.copyOf(admittedCohorts);
        if (targets.isEmpty() || targets.size() > 64)
            throw new IllegalArgumentException("Invalid observation scope");
        return sql.submitTracked(
                DbClass.RECOVERY,
                Duration.ofSeconds(2),
                c -> {
                    var snapshot = guarded(c, observation.call());
                    boolean mediaEvent =
                            observation.sli() == RecoverySli.MEDIA_CONTINUITY
                                    && observation.participant() != null;
                    if (observation.version() != 0
                            && (mediaEvent
                            ? snapshot.version() < observation.version()
                            : snapshot.version() != observation.version())) return null;
                    if (mediaEvent && observation.round() != snapshot.negotiationId()) return null;
                    if (observation.version() == 0
                            && (observation.originalGroup() == null
                            || observation.sli() != RecoverySli.WORKFLOW_CONVERGENCE)) return null;
                    if (observation.sli() == RecoverySli.WORKFLOW_CONVERGENCE
                            && observation.originalGroup() == null
                            && (snapshot.terminalAt() == null || snapshot.terminalReason() == null)) return null;
                    var local = current(observation.call());
                    if (observation.originalGroup() != null && !local.equals(observation.originalGroup()))
                        return null;
                    var metadata = JSON.readTree(snapshot.deadlines());
                    if (observation.sli() == RecoverySli.ACTIVE_CALL_PRESERVATION
                            || observation.sli() == RecoverySli.MEDIA_CONTINUITY) {
                        if (observation.originalGroup() == null
                                || snapshot.terminalAt() != null
                                || snapshot.activationId() == null
                                || snapshot.winner() == null
                                || !Set.of("CONNECTING", "ESTABLISHED").contains(snapshot.state())) return null;
                        if (observation.sli() == RecoverySli.MEDIA_CONTINUITY
                                && (!metadata.path("mediaCallerConnected").asBoolean()
                                || !metadata.path("mediaWinnerConnected").asBoolean())
                                && !mediaEvent) return null;
                    }
                    CallSnapshotRepository.Participant participant = null;
                    if (observation.sli() == RecoverySli.SIGNALING_REATTACHMENT || mediaEvent) {
                        if (snapshot.terminalAt() != null || observation.participant() == null) return null;
                        boolean caller = snapshot.caller().sameBinding(observation.participant());
                        participant = caller ? snapshot.caller() : snapshot.winner();
                        if (participant == null || !participant.sameBinding(observation.participant()))
                            return null;
                        if (mediaEvent
                                && !metadata
                                .path(caller ? "mediaCallerConnected" : "mediaWinnerConnected")
                                .asBoolean()) return null;
                        var route = metadata.get(caller ? "callerRoute" : "winnerRoute");
                        if (route == null
                                || !observation
                                .participant()
                                .equals(JSON.treeToValue(route, AuthenticatedSession.class))) return null;
                    }
                    // <=64 cohorts; no per-call scan and no raw identity in the journal.
                    try (var q =
                                 c.prepareStatement(
                                         "SELECT cohort_id FROM recovery_cohort WHERE fault_at IS NOT NULL AND storage_epoch=? AND privacy_key_id=? AND fault_at<=?::timestamptz AND ?::timestamptz<=clock_timestamp() AND ?::timestamptz>=clock_timestamp()-interval '5 seconds' ORDER BY cohort_id LIMIT 64")) {
                        q.setLong(1, epoch);
                        q.setString(2, keyId);
                        for (int i = 3; i <= 5; i++)
                            q.setTimestamp(i, Timestamp.from(observation.observedAt()));
                        try (var r = q.executeQuery()) {
                            while (r.next()) {
                                UUID id = r.getObject(1, UUID.class);
                                if (!targets.contains(id)) continue;
                                byte[] hash =
                                        participant == null || mediaEvent
                                                ? callHash(id, observation.call())
                                                : participantHash(id, observation.call(), participant);
                                if (mediaEvent) {
                                    boolean caller = snapshot.caller().sameBinding(observation.participant());
                                    String own = caller ? "media_caller_at" : "media_winner_at",
                                            other = caller ? "media_winner_at" : "media_caller_at";
                                    try (var update =
                                                 c.prepareStatement(
                                                         "UPDATE recovery_member SET "
                                                                 + own
                                                                 + "=?,"
                                                                 + other
                                                                 + "=CASE WHEN media_round=? THEN "
                                                                 + other
                                                                 + " ELSE NULL END,media_round=? WHERE cohort_id=? AND sli='MEDIA_CONTINUITY' AND member_hash=? AND successful_at IS NULL AND deadline_at>=clock_timestamp()")) {
                                        update.setTimestamp(1, Timestamp.from(observation.observedAt()));
                                        update.setLong(2, snapshot.negotiationId());
                                        update.setLong(3, snapshot.negotiationId());
                                        update.setObject(4, id);
                                        update.setBytes(5, hash);
                                        update.executeUpdate();
                                    }
                                    continue; // Actual media observation alone does not establish both ACTIVE homes.
                                }
                                String mediaProof =
                                        observation.sli() == RecoverySli.MEDIA_CONTINUITY
                                                ? " AND media_round=? AND media_caller_at>=(SELECT fault_at FROM recovery_cohort WHERE cohort_id=?) AND media_winner_at>=(SELECT fault_at FROM recovery_cohort WHERE cohort_id=?)"
                                                : "";
                                try (var update =
                                             c.prepareStatement(
                                                     "WITH won AS (UPDATE recovery_member SET successful_at=? WHERE cohort_id=? AND sli=? AND member_hash=? AND successful_at IS NULL AND deadline_at>=clock_timestamp() AND deadline_at>=? AND baseline_version<=? AND baseline_round<=?"
                                                             + mediaProof
                                                             + " RETURNING sli) UPDATE recovery_totals t SET successful=successful+1 FROM won WHERE t.sli=won.sli")) {
                                    update.setTimestamp(1, Timestamp.from(observation.observedAt()));
                                    update.setObject(2, id);
                                    update.setString(3, observation.sli().name());
                                    update.setBytes(4, hash);
                                    update.setTimestamp(5, Timestamp.from(observation.observedAt()));
                                    update.setLong(6, snapshot.version());
                                    update.setLong(7, snapshot.negotiationId());
                                    if (!mediaProof.isEmpty()) {
                                        update.setLong(8, snapshot.negotiationId());
                                        update.setObject(9, id);
                                        update.setObject(10, id);
                                    }
                                    update.executeUpdate();
                                }
                            }
                        }
                    }
                    return null;
                });
    }

    public DbOperation<Totals> totals(Set<UUID> observationLost) {
        final Set<UUID> lost = Set.copyOf(observationLost);
        if (lost.size() > 64) throw new IllegalArgumentException("Loss scope exceeds bound");
        return sql.submitTracked(
                DbClass.RECOVERY,
                Duration.ofSeconds(2),
                c -> {
                    root(c);
                    if (!lost.isEmpty())
                        try (var q =
                                     c.prepareStatement(
                                             "UPDATE recovery_cohort SET accounting_unknown=true WHERE cohort_id=? AND storage_epoch=? AND privacy_key_id=? AND fault_at IS NOT NULL AND NOT accounting_unknown AND EXISTS(SELECT 1 FROM recovery_member m WHERE m.cohort_id=recovery_cohort.cohort_id AND m.successful_at IS NULL)")) {
                            for (var id : lost) {
                                q.setObject(1, id);
                                q.setLong(2, epoch);
                                q.setString(3, keyId);
                                q.addBatch();
                            }
                            q.executeBatch();
                        }
                    var result = new EnumMap<RecoverySli, Total>(RecoverySli.class);
                    try (var q =
                                 c.prepareStatement(
                                         "SELECT t.sli,t.eligible,t.successful,EXISTS(SELECT 1 FROM recovery_cohort WHERE fault_at IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM recovery_cohort WHERE fault_at IS NOT NULL AND (accounting_unknown OR storage_epoch<>? OR privacy_key_id<>?)) AND NOT EXISTS(SELECT 1 FROM recovery_member m WHERE m.sli=t.sli AND m.deadline_at<clock_timestamp() AND m.successful_at IS NULL) FROM recovery_totals t")) {
                        q.setLong(1, epoch);
                        q.setString(2, keyId);
                        try (var r = q.executeQuery()) {
                            while (r.next())
                                result.put(
                                        RecoverySli.valueOf(r.getString(1)),
                                        new Total(r.getLong(2), r.getLong(3), r.getBoolean(4)));
                        }
                    }
                    return new Totals(
                            result.get(RecoverySli.WORKFLOW_CONVERGENCE),
                            result.get(RecoverySli.ACTIVE_CALL_PRESERVATION),
                            result.get(RecoverySli.SIGNALING_REATTACHMENT),
                            result.get(RecoverySli.MEDIA_CONTINUITY));
                });
    }

    private AuthoritySql.GroupToken current(CallId call) {
        return authority
                .apply(call)
                .filter(t -> cell.equals(t.cell()) && epoch == t.storageEpoch())
                .orElseThrow(AuthoritySql.FencedException::new);
    }

    private CallSnapshotRepository.Snapshot guarded(Connection c, CallId call) throws SQLException {
        root(c);
        var hint = calls.find(c, call);
        if (hint == null) throw new AuthoritySql.FencedException();
        var token = current(call);
        if (hint.group() != token.group() || hint.hashVersion() != token.hashVersion())
            throw new AuthoritySql.FencedException();
        AuthoritySql.coordinatorGrantCurrentBucket(c, token, hint.bucket(), call.value());
        var snapshot = calls.find(c, call);
        if (snapshot == null) throw new AuthoritySql.FencedException();
        return snapshot;
    }

    private void root(Connection c) throws SQLException {
        AuthoritySql.cellBarrier(c, false);
        AuthoritySql.validateCell(c, cell, epoch);
    }

    private Member member(UUID cohort, CallSnapshotRepository.Snapshot snapshot, RecoverySli sli) {
        return new Member(
                callHash(cohort, snapshot.callId()), sli, snapshot.version(), snapshot.negotiationId());
    }

    private byte[] callHash(UUID cohort, CallId call) {
        return digest(List.of("call", keyId, cohort.toString(), call.value()));
    }

    private byte[] participantHash(UUID cohort, CallId call, CallSnapshotRepository.Participant p) {
        return digest(
                List.of(
                        "participant",
                        keyId,
                        cohort.toString(),
                        call.value(),
                        p.user().value(),
                        p.key().issuer(),
                        p.key().jti(),
                        p.incarnation().value().toString()));
    }

    private byte[] digest(Object value) {
        try {
            var mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(JSON.writeValueAsBytes(value));
        } catch (Exception failure) {
            throw new IllegalStateException("Recovery privacy digest unavailable");
        }
    }

    /**
     * Supplied by an enrolled operator fault source, never decoded from public traffic.
     */
    public enum FaultKind {
        PROCESS,
        AZ
    }

    public record Baseline(
            CallId call, boolean active, boolean media, Set<SessionIncarnation> disconnected) {
        public Baseline {
            Objects.requireNonNull(call);
            disconnected = Set.copyOf(disconnected);
            if (disconnected.size() > 2 || media && !active)
                throw new IllegalArgumentException("Invalid eligibility");
        }
    }

    public record Cohort(
            UUID id, String sourceHash, FaultKind faultKind, List<Baseline> completeRoster) {
        public Cohort {
            Objects.requireNonNull(id);
            Objects.requireNonNull(faultKind);
            if (sourceHash == null
                    || !sourceHash.matches("[a-f0-9]{64}")
                    || completeRoster.isEmpty()
                    || completeRoster.size() > 128)
                throw new IllegalArgumentException("Invalid bounded complete cohort");
            completeRoster =
                    completeRoster.stream().sorted(Comparator.comparing(b -> b.call().value())).toList();
            if (completeRoster.stream().map(Baseline::call).distinct().count() != completeRoster.size())
                throw new IllegalArgumentException("Duplicate cohort call");
        }
    }

    public record Total(long eligible, long successful, boolean known) {
    }

    public record Totals(Total convergence, Total preservation, Total reattachment, Total media) {
        public Totals {
            Objects.requireNonNull(convergence);
            Objects.requireNonNull(preservation);
            Objects.requireNonNull(reattachment);
            Objects.requireNonNull(media);
        }

        public Total get(RecoverySli sli) {
            return switch (sli) {
                case WORKFLOW_CONVERGENCE -> convergence;
                case ACTIVE_CALL_PRESERVATION -> preservation;
                case SIGNALING_REATTACHMENT -> reattachment;
                case MEDIA_CONTINUITY -> media;
            };
        }
    }

    public record Observation(
            CallId call,
            long version,
            long round,
            RecoverySli sli,
            AuthenticatedSession participant,
            AuthoritySql.GroupToken originalGroup,
            Instant observedAt) {
    }

    private record Member(byte[] hash, RecoverySli sli, long version, long round) {
    }
}
