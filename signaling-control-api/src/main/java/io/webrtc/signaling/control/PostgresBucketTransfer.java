package io.webrtc.signaling.control;

import io.webrtc.signaling.storage.AuthoritySql;
import io.webrtc.signaling.storage.DbClass;
import io.webrtc.signaling.storage.DbOperation;
import io.webrtc.signaling.storage.SqlTransactions;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/**
 * Two independently admitted primaries. No connection or advisory lock crosses a database call.
 */
public final class PostgresBucketTransfer implements BucketTransferService.AuthorityTransfer {
    private static final Duration BUDGET = Duration.ofSeconds(2);
    private static final String[] TABLES = {
            "user_guard", "session_registry", "home_participation", "command_result"
    };
    private static final String[] KEYS = {
            "jsonb_build_array(user_id)::text", "jsonb_build_array(issuer,jti)::text",
            "jsonb_build_array(call_id,user_id)::text",
            "jsonb_build_array(issuer,jti,command_scope,request_id)::text"
    };
    private static final String[] ORDERS = {
            "user_id", "issuer,jti", "call_id,user_id", "issuer,jti,command_scope,request_id"
    };
    private final Store source, target;

    public PostgresBucketTransfer(Store source, Store target) {
        this.source = Objects.requireNonNull(source);
        this.target = Objects.requireNonNull(target);
        if (source.sql() == target.sql() || source.cell().equals(target.cell()))
            throw new IllegalArgumentException("Transfer requires distinct authorities");
    }

    private static String after(int dataset) {
        String[] fields = ORDERS[dataset].split(",");
        var right = new ArrayList<String>();
        for (int i = 0; i < fields.length; i++)
            right.add(
                    "(cursor_value->>"
                            + i
                            + ")"
                            + (fields[i].equals("request_id") ? "::uuid" : " COLLATE \"C\""));
        return "EXISTS(SELECT 1 FROM (SELECT ?::jsonb AS cursor_value,?::boolean AS first_page) x WHERE"
                + " first_page OR ("
                + ORDERS[dataset]
                + ")>("
                + String.join(",", right)
                + "))";
    }

    private static String hash(String previous, int dataset, Item row) throws Exception {
        var md = MessageDigest.getInstance("SHA-256");
        for (String component :
                List.of(previous, Integer.toString(dataset), row.key(), row.payload())) {
            byte[] b = component.getBytes(StandardCharsets.UTF_8);
            md.update(java.nio.ByteBuffer.allocate(4).putInt(b.length).array());
            md.update(b);
        }
        return HexFormat.of().formatHex(md.digest());
    }

    /**
     * Never begin a new step until the preceding admitted transaction has physically finished.
     */
    private static <T> CompletionStage<T> tracked(Store store, SqlTransactions.Work<T> work) {
        DbOperation<T> operation = store.sql().submitTracked(DbClass.MAINTENANCE, BUDGET, work);
        return operation
                .logical()
                .handle((value, error) -> new Logical<>(value, error))
                .thenCompose(
                        result ->
                                operation
                                        .physicalCompletion()
                                        .thenApply(
                                                v -> {
                                                    if (result.error() != null)
                                                        throw new CompletionException(result.error());
                                                    return result.value();
                                                }));
    }

    private static void roots(Connection c, Store store, int bucket, boolean exclusive)
            throws SQLException {
        AuthoritySql.cellBarrier(c, false);
        AuthoritySql.validateCell(c, store.cell(), store.storageEpoch());
        AuthoritySql.bucketBarrier(c, bucket, exclusive);
    }

    private static void checkBucket(
            Connection c, int bucket, long epoch, String state, UUID id, long recovery)
            throws SQLException {
        try (var q =
                     c.prepareStatement(
                             "SELECT directory_epoch,status,transfer_id,recovery_epoch FROM bucket_authority WHERE"
                                     + " bucket_id=?")) {
            q.setInt(1, bucket);
            try (var r = q.executeQuery()) {
                if (!r.next()
                        || r.getLong(1) != epoch
                        || !state.equals(r.getString(2))
                        || !id.equals(r.getObject(3, UUID.class))
                        || r.getLong(4) != recovery) throw new AuthoritySql.FencedException();
            }
        }
    }

    private static boolean exists(Connection c, String sql, int b) throws SQLException {
        try (var q = c.prepareStatement(sql)) {
            q.setInt(1, b);
            try (var r = q.executeQuery()) {
                return r.next();
            }
        }
    }

    private static BucketTransferService.FreezeReceipt receipt(Journal j) {
        return new BucketTransferService.FreezeReceipt(
                j.source().bucket(), j.source().cell(), j.source().epoch(), j.id());
    }

    private static void binding(Journal j, HomeRoute from, HomeRoute to) {
        if (!j.source().equals(from) || !j.target().equals(to))
            throw new IllegalStateException("Transfer identity cannot be reused");
    }

    private static void checkpoint(Connection c, UUID id, Checkpoint p) throws SQLException {
        try (var q =
                     c.prepareStatement(
                             "UPDATE bucket_transfer SET"
                                     + " dataset=?,cursor=?,row_count=?,digest=?,updated_at=clock_timestamp() WHERE"
                                     + " transfer_id=?")) {
            q.setInt(1, p.dataset());
            q.setString(2, p.cursor());
            q.setLong(3, p.count());
            q.setString(4, p.digest());
            q.setObject(5, id);
            if (q.executeUpdate() != 1) throw new AuthoritySql.FencedException();
        }
    }

    private static void state(Connection c, UUID id, String state) throws SQLException {
        try (var q =
                     c.prepareStatement(
                             "UPDATE bucket_transfer SET state=?,updated_at=clock_timestamp() WHERE"
                                     + " transfer_id=?")) {
            q.setString(1, state);
            q.setObject(2, id);
            if (q.executeUpdate() != 1) throw new AuthoritySql.FencedException();
        }
    }

    public List<SqlTransactions> transactionOwners() {
        return List.of(source.sql(), target.sql());
    }

    /**
     * Legacy freeze cannot bind an immutable destination and is intentionally unavailable.
     */
    @Override
    public CompletionStage<BucketTransferService.FreezeReceipt> freeze(HomeRoute route) {
        return CompletableFuture.failedFuture(
                new IllegalStateException("Use transfer identity and destination"));
    }

    @Override
    public CompletionStage<BucketTransferService.FreezeReceipt> prepare(
            UUID id, HomeRoute from, HomeRoute to) {
        validate(from, to);
        return tracked(
                source,
                c -> {
                    roots(c, source, from.bucket(), true);
                    Journal old = journal(c, id);
                    if (old != null) {
                        binding(old, from, to);
                        return old;
                    }
                    AuthoritySql.validateBuckets(c, Map.of(from.bucket(), from.epoch()));
                    long recovery;
                    try (var q =
                                 c.prepareStatement(
                                         "SELECT recovery_epoch FROM bucket_authority WHERE bucket_id=?")) {
                        q.setInt(1, from.bucket());
                        try (var r = q.executeQuery()) {
                            if (!r.next()) throw new AuthoritySql.FencedException();
                            recovery = r.getLong(1);
                        }
                    }
                    var j = new Journal(id, from, to, "DRAINING", recovery, new Checkpoint(0, "", 0, ""));
                    insertJournal(c, j, "SOURCE");
                    return j;
                })
                .thenCompose(j -> source.sql().boundary().settleAdmitted().thenApply(v -> j))
                .thenCompose(
                        j ->
                                tracked(
                                        source,
                                        c -> {
                                            roots(c, source, from.bucket(), true);
                                            Journal current = require(c, id, from, to);
                                            if (!current.state().equals("DRAINING")) {
                                                requireSourceFence(c, current);
                                                return receipt(current);
                                            }
                                            AuthoritySql.validateBuckets(c, Map.of(from.bucket(), from.epoch()));
                                            // Expired rows are still work until their ordinary guarded expiry path has
                                            // retired them.
                                            if (exists(
                                                    c,
                                                    "SELECT 1 FROM call_state WHERE authority_bucket_id=? AND terminal_at"
                                                            + " IS NULL LIMIT 1",
                                                    from.bucket())
                                                    || exists(
                                                    c,
                                                    "SELECT 1 FROM user_reservation WHERE"
                                                            + " native_authority_bucket(user_id)=? LIMIT 1",
                                                    from.bucket())
                                                    || exists(
                                                    c,
                                                    "SELECT 1 FROM home_participation WHERE"
                                                            + " native_authority_bucket(user_id)=? AND terminal_at IS NULL"
                                                            + " LIMIT 1",
                                                    from.bucket())
                                                    || exists(
                                                    c,
                                                    "SELECT 1 FROM command_result WHERE authority_bucket_id=? AND"
                                                            + " status='PENDING' LIMIT 1",
                                                    from.bucket())
                                                    || exists(
                                                    c,
                                                    "SELECT 1 FROM control_outbox WHERE authority_bucket_id=? AND"
                                                            + " delivery_state<>'DELIVERED' AND expires_at>clock_timestamp()"
                                                            + " LIMIT 1",
                                                    from.bucket())
                                                    || exists(
                                                    c,
                                                    "SELECT 1 FROM bucket_transfer_item i JOIN bucket_transfer t ON"
                                                            + " t.transfer_id=i.transfer_id WHERE t.bucket_id=? LIMIT 1",
                                                    from.bucket())
                                                    || exists(
                                                    c,
                                                    "SELECT 1 FROM privacy_deletion_request WHERE authority_bucket_id=?"
                                                            + " AND completed_at IS NULL LIMIT 1",
                                                    from.bucket())) throw new TransferDraining();
                                            try (var q =
                                                         c.prepareStatement(
                                                                 "UPDATE bucket_authority SET"
                                                                         + " status='FROZEN',transfer_id=?,updated_at=clock_timestamp()"
                                                                         + " WHERE bucket_id=? AND directory_epoch=? AND"
                                                                         + " status='ACTIVE'")) {
                                                q.setObject(1, id);
                                                q.setInt(2, from.bucket());
                                                q.setLong(3, from.epoch());
                                                if (q.executeUpdate() != 1) throw new AuthoritySql.FencedException();
                                            }
                                            state(c, id, "FROZEN");
                                            return receipt(current);
                                        }));
    }

    @Override
    public CompletionStage<Void> copyAndInstall(
            BucketTransferService.FreezeReceipt fence, HomeRoute to) {
        return readSource(fence.fenceId())
                .thenCompose(
                        j -> {
                            if (!receipt(j).equals(fence) || !j.target().equals(to))
                                throw new AuthoritySql.FencedException();
                            return initializeTarget(j).thenCompose(v -> copy(j, 32));
                        });
    }

    @Override
    public CompletionStage<Void> verifyInstalled(
            BucketTransferService.FreezeReceipt fence, HomeRoute to) {
        return readSource(fence.fenceId())
                .thenCompose(
                        j -> {
                            if (!receipt(j).equals(fence)
                                    || !j.target().equals(to)
                                    || j.checkpoint().dataset() != 4) throw new AuthoritySql.FencedException();
                            return tracked(
                                    target,
                                    c -> {
                                        roots(c, target, to.bucket(), false);
                                        Journal installed = require(c, j.id(), j.source(), j.target());
                                        if (!Set.of("INSTALLED", "PUBLISHED").contains(installed.state())
                                                || !installed.checkpoint().equals(j.checkpoint()))
                                            throw new AuthoritySql.FencedException();
                                        checkBucket(c, to.bucket(), to.epoch(), "ACTIVE", j.id(), j.recovery());
                                        return null;
                                    });
                        });
    }

    @Override
    public CompletionStage<Void> publicationConfirmed(
            BucketTransferService.FreezeReceipt fence, HomeRoute to) {
        return verifyInstalled(fence, to)
                .thenCompose(v -> readSource(fence.fenceId()))
                .thenCompose(
                        j ->
                                tracked(
                                        source,
                                        c -> {
                                            roots(c, source, j.source().bucket(), true);
                                            requireSourceFence(c, j);
                                            var current = require(c, j.id(), j.source(), j.target());
                                            if (!current.state().equals("PUBLISHED")) state(c, j.id(), "PUBLISHED");
                                            return j;
                                        }))
                .thenCompose(
                        j ->
                                tracked(
                                        target,
                                        c -> {
                                            roots(c, target, to.bucket(), true);
                                            var current = require(c, j.id(), j.source(), j.target());
                                            if (!Set.of("INSTALLED", "PUBLISHED").contains(current.state())
                                                    || !current.checkpoint().equals(j.checkpoint()))
                                                throw new AuthoritySql.FencedException();
                                            checkBucket(c, to.bucket(), to.epoch(), "ACTIVE", j.id(), j.recovery());
                                            if (!current.state().equals("PUBLISHED")) state(c, j.id(), "PUBLISHED");
                                            return null;
                                        }));
    }

    private CompletionStage<Void> initializeTarget(Journal j) {
        return tracked(
                target,
                c -> {
                    roots(c, target, j.target().bucket(), true);
                    Journal old = journal(c, j.id());
                    if (old != null) {
                        binding(old, j.source(), j.target());
                        if (old.recovery() != j.recovery()) throw new AuthoritySql.FencedException();
                        return null;
                    }
                    try (var q = c.prepareStatement("SELECT 1 FROM bucket_authority WHERE bucket_id=?")) {
                        q.setInt(1, j.source().bucket());
                        try (var r = q.executeQuery()) {
                            if (r.next()) throw new IllegalStateException("Target authority already exists");
                        }
                    }
                    int b = j.source().bucket();
                    if (exists(
                            c, "SELECT 1 FROM user_guard WHERE native_authority_bucket(user_id)=? LIMIT 1", b)
                            || exists(
                            c,
                            "SELECT 1 FROM session_registry WHERE native_authority_bucket(user_id)=? LIMIT 1",
                            b)
                            || exists(
                            c,
                            "SELECT 1 FROM home_participation WHERE native_authority_bucket(user_id)=? LIMIT"
                                    + " 1",
                            b)
                            || exists(
                            c,
                            "SELECT 1 FROM user_reservation WHERE native_authority_bucket(user_id)=? LIMIT 1",
                            b)
                            || exists(c, "SELECT 1 FROM command_result WHERE authority_bucket_id=? LIMIT 1", b)
                            || exists(c, "SELECT 1 FROM call_state WHERE authority_bucket_id=? LIMIT 1", b))
                        throw new IllegalStateException("Target contains conflicting bucket history");
                    insertJournal(
                            c,
                            new Journal(
                                    j.id(),
                                    j.source(),
                                    j.target(),
                                    "COPYING",
                                    j.recovery(),
                                    new Checkpoint(0, "", 0, "")),
                            "TARGET");
                    try (var q =
                                 c.prepareStatement(
                                         "INSERT INTO"
                                                 + " bucket_authority(bucket_id,directory_epoch,status,recovery_epoch,transfer_id)"
                                                 + " VALUES(?,?,'INSTALLED',?,?)")) {
                        q.setInt(1, b);
                        q.setLong(2, j.target().epoch());
                        q.setLong(3, j.recovery());
                        q.setObject(4, j.id());
                        q.executeUpdate();
                    }
                    return null;
                });
    }

    private CompletionStage<Void> copy(Journal original, int pagesLeft) {
        return readSource(original.id())
                .thenCompose(
                        j -> {
                            if (j.checkpoint().dataset() == 4) return activate(j);
                            if (pagesLeft == 0)
                                return CompletableFuture.failedFuture(new TransferResumeRequired(j.id()));
                            return tracked(
                                    source,
                                    c -> {
                                        roots(c, source, j.source().bucket(), false);
                                        requireSourceFence(c, j);
                                        Journal current = require(c, j.id(), j.source(), j.target());
                                        return page(c, current);
                                    })
                                    .thenCompose(
                                            page ->
                                                    install(j, page)
                                                            .thenCompose(
                                                                    v ->
                                                                            tracked(
                                                                                    source,
                                                                                    c -> {
                                                                                        roots(c, source, j.source().bucket(), true);
                                                                                        requireSourceFence(c, j);
                                                                                        Journal current =
                                                                                                require(c, j.id(), j.source(), j.target());
                                                                                        if (current.checkpoint().equals(page.end()))
                                                                                            return null;
                                                                                        if (!current.checkpoint().equals(page.start()))
                                                                                            throw new IllegalStateException(
                                                                                                    "Source checkpoint conflict");
                                                                                        checkpoint(c, j.id(), page.end());
                                                                                        return null;
                                                                                    })))
                                    .thenCompose(v -> copy(j, pagesLeft - 1));
                        });
    }

    private CompletionStage<Void> install(Journal j, Page page) {
        return tracked(
                target,
                c -> {
                    roots(c, target, j.target().bucket(), true);
                    Journal current = require(c, j.id(), j.source(), j.target());
                    if (current.checkpoint().equals(page.end())) return null;
                    if (!current.state().equals("COPYING") || !current.checkpoint().equals(page.start()))
                        throw new IllegalStateException("Target checkpoint conflict");
                    checkBucket(
                            c, j.target().bucket(), j.target().epoch(), "INSTALLED", j.id(), j.recovery());
                    int d = page.start().dataset();
                    String table = TABLES[d];
                    String digest = page.start().digest();
                    for (Item item : page.items()) digest = hash(digest, d, item);
                    if (!digest.equals(page.end().digest())
                            || page.end().count() != Math.addExact(page.start().count(), page.items().size())
                            || (page.items().isEmpty()
                            ? page.end().dataset() != d + 1 || !page.end().cursor().isEmpty()
                            : page.end().dataset() != d
                            || !page.end().cursor().equals(page.items().getLast().key())))
                        throw new IllegalStateException("Transfer manifest differs from page contents");
                    for (Item row : page.items()) {
                        try (var q =
                                     c.prepareStatement(
                                             "INSERT INTO bucket_transfer_item(transfer_id,dataset,row_key,payload)"
                                                     + " VALUES(?,?,?,?::jsonb)")) {
                            q.setObject(1, j.id());
                            q.setInt(2, d);
                            q.setString(3, row.key());
                            q.setString(4, row.payload());
                            q.executeUpdate();
                        }
                        try (var q =
                                     c.prepareStatement(
                                             "INSERT INTO "
                                                     + table
                                                     + " SELECT (jsonb_populate_record(NULL::"
                                                     + table
                                                     + ",?::jsonb)).*")) {
                            q.setString(1, row.payload());
                            if (q.executeUpdate() != 1)
                                throw new IllegalStateException("Transfer row not installed");
                        }
                    }
                    checkpoint(c, j.id(), page.end());
                    return null;
                });
    }

    private CompletionStage<Void> activate(Journal j) {
        // A source fresh fence read has committed and physically finished before target activation.
        return tracked(
                target,
                c -> {
                    roots(c, target, j.target().bucket(), true);
                    Journal current = require(c, j.id(), j.source(), j.target());
                    if (!current.checkpoint().equals(j.checkpoint()))
                        throw new IllegalStateException("Incomplete target manifest");
                    if (Set.of("INSTALLED", "PUBLISHED").contains(current.state())) {
                        checkBucket(c, j.target().bucket(), j.target().epoch(), "ACTIVE", j.id(), j.recovery());
                        return null;
                    }
                    if (!current.state().equals("COPYING") || current.checkpoint().dataset() != 4)
                        throw new AuthoritySql.FencedException();
                    checkBucket(
                            c, j.target().bucket(), j.target().epoch(), "INSTALLED", j.id(), j.recovery());
                    try (var q =
                                 c.prepareStatement(
                                         "UPDATE bucket_authority SET status='ACTIVE',updated_at=clock_timestamp() WHERE"
                                                 + " bucket_id=? AND transfer_id=? AND status='INSTALLED'")) {
                        q.setInt(1, j.target().bucket());
                        q.setObject(2, j.id());
                        if (q.executeUpdate() != 1) throw new AuthoritySql.FencedException();
                    }
                    state(c, j.id(), "INSTALLED");
                    return null;
                });
    }

    private Page page(Connection c, Journal j) throws Exception {
        Checkpoint start = j.checkpoint();
        int d = start.dataset();
        String table = TABLES[d], key = KEYS[d];
        String payload = "to_jsonb(t)";
        if (d == 1)
            payload =
                    "to_jsonb(t) ||"
                            + " jsonb_build_object('gateway_id',NULL,'boot_id',NULL,'connection_id',NULL,'closed_at',coalesce(closed_at,updated_at))";
        String filter =
                d == 3
                        ? "authority_bucket_id=? AND status='FINAL' AND command_scope='INVITE'"
                        : "native_authority_bucket(user_id)=?";
        // Canonical source JSONB is also the digest input. Cap physical rows and bytes in SQL.
        String order = ORDERS[d];
        String after = after(d);
        String sql =
                "WITH candidates AS (SELECT "
                        + key
                        + " AS k,"
                        + payload
                        + " AS p,row_number() OVER(ORDER BY "
                        + order
                        + ") AS ordinal FROM "
                        + table
                        + " t WHERE "
                        + filter
                        + " AND "
                        + after
                        + " ORDER BY "
                        + order
                        + " LIMIT 128), sized AS (SELECT k,p,ordinal,sum(octet_length(k)+octet_length(p::text))"
                        + " OVER(ORDER BY ordinal) AS bytes FROM candidates) SELECT k,p::text FROM sized WHERE"
                        + " bytes<=1048576 ORDER BY ordinal";
        var rows = new ArrayList<Item>();
        String cursor = start.cursor(), digest = start.digest();
        try (var q = c.prepareStatement(sql)) {
            q.setInt(1, j.source().bucket());
            q.setString(2, cursor.isEmpty() ? "[]" : cursor);
            q.setBoolean(3, cursor.isEmpty());
            try (var r = q.executeQuery()) {
                while (r.next()) {
                    var row = new Item(r.getString(1), r.getString(2));
                    rows.add(row);
                    cursor = row.key();
                    digest = hash(digest, d, row);
                }
            }
        }
        if (rows.isEmpty()) {
            // A single oversized row must stop the workflow, never be mistaken for completion.
            try (var q =
                         c.prepareStatement(
                                 "SELECT 1 FROM " + table + " WHERE " + filter + " AND " + after + " LIMIT 1")) {
                q.setInt(1, j.source().bucket());
                q.setString(2, cursor.isEmpty() ? "[]" : cursor);
                q.setBoolean(3, cursor.isEmpty());
                try (var r = q.executeQuery()) {
                    if (r.next()) throw new IllegalStateException("Transfer row exceeds page byte bound");
                }
            }
            return new Page(start, new Checkpoint(d + 1, "", start.count(), digest), List.of());
        }
        return new Page(
                start,
                new Checkpoint(d, cursor, Math.addExact(start.count(), rows.size()), digest),
                List.copyOf(rows));
    }

    private CompletionStage<Journal> readSource(UUID id) {
        return tracked(
                source,
                c -> {
                    Journal j = journal(c, id);
                    if (j == null) throw new IllegalStateException("Unknown transfer identity");
                    roots(c, source, j.source().bucket(), false);
                    j = require(c, id, j.source(), j.target());
                    requireSourceFence(c, j);
                    return j;
                });
    }

    private void validate(HomeRoute from, HomeRoute to) {
        if (!source.cell().equals(from.cell())
                || !target.cell().equals(to.cell())
                || from.bucket() != to.bucket()
                || to.epoch() != Math.addExact(from.epoch(), 1))
            throw new IllegalArgumentException("Routes differ from enrolled transfer stores");
    }

    private void requireSourceFence(Connection c, Journal j) throws SQLException {
        if (!Set.of("FROZEN", "PUBLISHED").contains(j.state()))
            throw new AuthoritySql.FencedException();
        checkBucket(c, j.source().bucket(), j.source().epoch(), "FROZEN", j.id(), j.recovery());
    }

    private Journal require(Connection c, UUID id, HomeRoute from, HomeRoute to) throws SQLException {
        Journal j = journal(c, id);
        if (j == null) throw new AuthoritySql.FencedException();
        binding(j, from, to);
        return j;
    }

    private Journal journal(Connection c, UUID id) throws SQLException {
        try (var q =
                     c.prepareStatement(
                             "SELECT"
                                     + " bucket_id,source_cell,source_epoch,source_storage_epoch,target_cell,target_epoch,target_storage_epoch,source_wss,target_wss,state,recovery_epoch,dataset,cursor,row_count,digest,role"
                                     + " FROM bucket_transfer WHERE transfer_id=?")) {
            q.setObject(1, id);
            try (var r = q.executeQuery()) {
                if (!r.next()) return null;
                if (r.getLong(4) != source.storageEpoch() || r.getLong(7) != target.storageEpoch())
                    throw new AuthoritySql.FencedException();
                var from = new HomeRoute(r.getInt(1), r.getString(2), r.getLong(3), r.getString(8));
                var to = new HomeRoute(r.getInt(1), r.getString(5), r.getLong(6), r.getString(9));
                validate(from, to);
                return new Journal(
                        id,
                        from,
                        to,
                        r.getString(10),
                        r.getLong(11),
                        new Checkpoint(r.getInt(12), r.getString(13), r.getLong(14), r.getString(15)));
            }
        }
    }

    private void insertJournal(Connection c, Journal j, String role) throws SQLException {
        try (var q =
                     c.prepareStatement(
                             "INSERT INTO"
                                     + " bucket_transfer(transfer_id,bucket_id,source_cell,source_epoch,source_storage_epoch,target_cell,target_epoch,target_storage_epoch,source_wss,target_wss,role,state,recovery_epoch)"
                                     + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            q.setObject(1, j.id());
            q.setInt(2, j.source().bucket());
            q.setString(3, j.source().cell());
            q.setLong(4, j.source().epoch());
            q.setLong(5, source.storageEpoch());
            q.setString(6, j.target().cell());
            q.setLong(7, j.target().epoch());
            q.setLong(8, target.storageEpoch());
            q.setString(9, j.source().wssUrl());
            q.setString(10, j.target().wssUrl());
            q.setString(11, role);
            q.setString(12, j.state());
            q.setLong(13, j.recovery());
            q.executeUpdate();
        }
    }

    public record Store(SqlTransactions sql, String cell, long storageEpoch) {
        public Store {
            Objects.requireNonNull(sql);
            if (cell == null || !cell.matches("[a-z][a-z0-9-]{0,23}") || storageEpoch < 1)
                throw new IllegalArgumentException("Invalid transfer store");
        }
    }

    private record Checkpoint(int dataset, String cursor, long count, String digest) {
    }

    private record Journal(
            UUID id,
            HomeRoute source,
            HomeRoute target,
            String state,
            long recovery,
            Checkpoint checkpoint) {
    }

    private record Item(String key, String payload) {
    }

    private record Page(Checkpoint start, Checkpoint end, List<Item> items) {
    }

    private record Logical<T>(T value, Throwable error) {
    }

    public static final class TransferDraining extends RuntimeException {
        public TransferDraining() {
            super("Bucket admission stopped; existing workflows must drain before retry");
        }
    }

    public static final class TransferResumeRequired extends RuntimeException {
        public TransferResumeRequired(UUID id) {
            super("Transfer page budget reached; resume the same transfer " + id);
        }
    }
}
