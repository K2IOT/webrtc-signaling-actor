package io.webrtc.signaling.storage;

import io.webrtc.signaling.storage.HomeParticipationService.AuthorizationIntent;
import io.webrtc.signaling.storage.HomeParticipationService.Participation;
import io.webrtc.signaling.storage.HomeParticipationService.Phase;
import io.webrtc.signaling.storage.HomeParticipationService.Request;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public final class UserReservationService {
    private final HomeParticipationService home;

    public UserReservationService(HomeParticipationService home) {
        this.home = home;
    }

    private static int ttl(Request r) {
        return r.phase() == Phase.PREPARING ? 15 : 30;
    }

    public CompletionStage<Participation> reserveUser(Request r) {
        return reserveUserTracked(r, java.time.Duration.ofSeconds(2)).logical();
    }

    public DbOperation<Participation> reserveUserTracked(Request r, java.time.Duration budget) {
        return home.submitTracked(
                r, DbClass.CRITICAL, budget, AuthorizationIntent.reserve(), c -> reserve(c, r));
    }

    public CompletionStage<List<Participation>> reservePair(Request a, Request b) {
        return home.pair(a, b, c -> List.of(reserve(c, a), reserve(c, b)));
    }

    Participation reserve(Connection c, Request r) throws SQLException {
        expireCurrent(c, r.user().value());
        Participation previous = home.find(c, r);
        if (previous != null) {
            if (previous.terminal()) return previous;
            if (previous.leaseUntil() == null) {
                home.terminalize(c, r.call().value(), r.user().value(), "EXPIRED");
                return home.find(c, r);
            }
            return previous;
        }
        try (var s = c.prepareStatement("SELECT call_id FROM user_reservation WHERE user_id=?")) {
            s.setString(1, r.user().value());
            try (var result = s.executeQuery()) {
                if (result.next()) throw new UserBusy();
            }
        }
        UUID reservation = UUID.randomUUID();
        home.insert(c, r, "RESERVED", reservation);
        // A replay that won the insertion retains its original identity, never a new reservation.
        Participation inserted = home.find(c, r);
        if (inserted.terminal()) return inserted;
        if (!reservation.equals(inserted.reservationId())) throw new AuthoritySql.RetryableConflict();
        try (var s =
                     c.prepareStatement(
                             "INSERT INTO"
                                     + " user_reservation(user_id,call_id,reservation_id,reservation_version,lease_until,coordinator_cell,coordinator_storage_epoch,ownership_hash_version,ownership_group_id,highest_group_epoch,highest_lease_sequence,last_renew_operation,last_granted_expiry,phase)"
                                     + " VALUES(?,?,?,1,clock_timestamp()+(? * interval '1"
                                     + " second'),?,?,?,?,?,?,?,clock_timestamp()+(? * interval '1 second'),?)")) {
            s.setString(1, r.user().value());
            s.setString(2, r.call().value());
            s.setObject(3, reservation);
            s.setInt(4, ttl(r));
            s.setString(5, r.grant().cell());
            s.setLong(6, r.grant().storageEpoch());
            s.setLong(7, r.grant().hashVersion());
            s.setInt(8, r.grant().group());
            s.setLong(9, r.grant().groupEpoch());
            s.setLong(10, r.grant().sequence());
            s.setObject(11, r.grant().operation());
            s.setInt(12, ttl(r));
            s.setString(13, r.phase().name());
            s.executeUpdate();
        }
        return home.find(c, r);
    }

    private void expireCurrent(Connection c, String user) throws SQLException {
        try (var s =
                     c.prepareStatement(
                             "SELECT call_id,reservation_id,lease_until<=clock_timestamp() FROM user_reservation"
                                     + " WHERE user_id=?")) {
            s.setString(1, user);
            try (var r = s.executeQuery()) {
                if (r.next() && r.getBoolean(3)) {
                    String call = r.getString(1);
                    UUID id = r.getObject(2, UUID.class);
                    home.terminalize(c, call, user, "EXPIRED");
                    try (var d =
                                 c.prepareStatement(
                                         "DELETE FROM user_reservation WHERE user_id=? AND call_id=? AND"
                                                 + " reservation_id=?")) {
                        d.setString(1, user);
                        d.setString(2, call);
                        d.setObject(3, id);
                        d.executeUpdate();
                    }
                }
            }
        }
    }

    public CompletionStage<Participation> renewReservation(
            Request r, UUID reservation, long version) {
        return renewReservationTracked(r, reservation, version, java.time.Duration.ofSeconds(2))
                .logical();
    }

    public DbOperation<Participation> renewReservationTracked(
            Request r, UUID reservation, long version, java.time.Duration budget) {
        return home.submitTracked(
                r,
                DbClass.RENEWAL,
                budget,
                new AuthorizationIntent("RENEW", reservation, version, null, 0, null, null),
                c -> {
                    expireCurrent(c, r.user().value());
                    var current = home.find(c, r, true);
                    if (current == null
                            || current.terminal()
                            || !reservation.equals(current.reservationId())
                            || current.leaseUntil() == null) throw new AuthoritySql.FencedException();
                    long epoch, sequence, storage, historyStorage;
                    UUID operation;
                    String reservationPhase;
                    try (var s =
                                 c.prepareStatement(
                                         "SELECT"
                                                 + " r.highest_group_epoch,r.highest_lease_sequence,r.last_renew_operation,r.coordinator_storage_epoch,h.coordinator_storage_epoch,r.phase"
                                                 + " FROM user_reservation r JOIN home_participation h ON h.user_id=r.user_id"
                                                 + " AND h.call_id=r.call_id AND h.reservation_id=r.reservation_id WHERE"
                                                 + " r.user_id=? AND r.call_id=? AND r.reservation_id=?")) {
                        s.setString(1, r.user().value());
                        s.setString(2, r.call().value());
                        s.setObject(3, reservation);
                        try (var result = s.executeQuery()) {
                            if (!result.next()) throw new AuthoritySql.FencedException();
                            epoch = result.getLong(1);
                            sequence = result.getLong(2);
                            operation = result.getObject(3, UUID.class);
                            storage = result.getLong(4);
                            historyStorage = result.getLong(5);
                            reservationPhase = result.getString(6);
                        }
                    }
                    if (r.grant().storageEpoch() < storage
                            || r.grant().storageEpoch() == storage
                            && (r.grant().groupEpoch() < epoch
                            || r.grant().groupEpoch() == epoch && r.grant().sequence() < sequence))
                        throw new AuthoritySql.FencedException();
                    if (r.grant().storageEpoch() == storage
                            && r.grant().groupEpoch() == epoch
                            && r.grant().sequence() == sequence) {
                        if (!r.grant().operation().equals(operation))
                            throw new HomeParticipationService.IntentConflict();
                        return current;
                    }
                    // Preserve exact same-cycle replay above. A new PREPARING request cannot downgrade
                    // a RINGING/CLAIMED/ACTIVE reservation or assign a shorter phase TTL to it.
                    if (r.phase() == Phase.PREPARING && !reservationPhase.equals("PREPARING"))
                        throw new AuthoritySql.FencedException();
                    if (current.version() != version) throw new AuthoritySql.FencedException();
                    long next = Math.addExact(version, 1);
                    try (var s =
                                 c.prepareStatement(
                                         "UPDATE user_reservation SET"
                                                 + " coordinator_storage_epoch=?,reservation_version=?,highest_group_epoch=?,highest_lease_sequence=?,last_renew_operation=?,lease_until=GREATEST(lease_until,clock_timestamp()+(?"
                                                 + " * interval '1"
                                                 + " second')),last_granted_expiry=GREATEST(lease_until,clock_timestamp()+(? *"
                                                 + " interval '1 second')),phase=CASE WHEN phase='PREPARING' AND ?='RINGING'"
                                                 + " THEN 'RINGING' ELSE phase END WHERE user_id=? AND call_id=? AND"
                                                 + " reservation_id=? AND reservation_version=? AND"
                                                 + " lease_until>clock_timestamp()")) {
                        s.setLong(1, r.grant().storageEpoch());
                        s.setLong(2, next);
                        s.setLong(3, r.grant().groupEpoch());
                        s.setLong(4, r.grant().sequence());
                        s.setObject(5, r.grant().operation());
                        s.setInt(6, ttl(r));
                        s.setInt(7, ttl(r));
                        s.setString(8, r.phase().name());
                        s.setString(9, r.user().value());
                        s.setString(10, r.call().value());
                        s.setObject(11, reservation);
                        s.setLong(12, version);
                        if (s.executeUpdate() != 1) throw new AuthoritySql.FencedException();
                    }
                    if (r.grant().storageEpoch() != historyStorage
                            || r.grant().groupEpoch() != current.highestGroupEpoch()) {
                        try (var s =
                                     c.prepareStatement(
                                             "UPDATE home_participation SET"
                                                     + " coordinator_storage_epoch=?,highest_group_epoch=? WHERE call_id=? AND"
                                                     + " user_id=? AND reservation_id=? AND phase NOT IN"
                                                     + " ('RELEASED','EXPIRED')")) {
                            s.setLong(1, r.grant().storageEpoch());
                            s.setLong(2, r.grant().groupEpoch());
                            s.setString(3, r.call().value());
                            s.setString(4, r.user().value());
                            s.setObject(5, reservation);
                            if (s.executeUpdate() != 1) throw new AuthoritySql.FencedException();
                        }
                    }
                    return home.find(c, r);
                });
    }

    public CompletionStage<Participation> releaseIfCallVersion(
            Request r, UUID reservation, long version) {
        return releaseIfCallVersionTracked(r, reservation, version, java.time.Duration.ofSeconds(2))
                .logical();
    }

    public DbOperation<Participation> releaseIfCallVersionTracked(
            Request r, UUID reservation, long version, java.time.Duration budget) {
        return home.submitTracked(
                r,
                DbClass.TERMINATION,
                budget,
                new AuthorizationIntent("RELEASE", reservation, version, null, 0, null, null),
                c -> {
                    Participation current = home.find(c, r);
                    if (current == null) {
                        home.insert(c, r, "RELEASED", null);
                        return home.find(c, r);
                    }
                    if (current.terminal()) return current;
                    if (r.grant().groupEpoch() < current.highestGroupEpoch()
                            || reservation != null && !reservation.equals(current.reservationId())
                            || version > 0 && version != current.version())
                        throw new AuthoritySql.FencedException();
                    home.terminalize(c, r.call().value(), r.user().value(), "RELEASED");
                    try (var s =
                                 c.prepareStatement(
                                         "DELETE FROM user_reservation WHERE user_id=? AND call_id=? AND"
                                                 + " reservation_id=?")) {
                        s.setString(1, r.user().value());
                        s.setString(2, r.call().value());
                        s.setObject(3, current.reservationId());
                        s.executeUpdate();
                    }
                    return home.find(c, r);
                });
    }

    public static final class UserBusy extends RuntimeException {
        public UserBusy() {
            super("User has a live reservation");
        }
    }
}
