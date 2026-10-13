package io.webrtc.signaling.storage;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Admitted read-only PG17 observations. No checkpoint timeline or promotion authority is inferred.
 */
public final class PostgresWalFacts {
    private final SqlTransactions sql;
    private final String cell, systemIdentifier;
    private final long epoch, timeline;

    public PostgresWalFacts(
            SqlTransactions sql, String cell, long epoch, String systemIdentifier, long timeline) {
        this.sql = Objects.requireNonNull(sql);
        if (cell == null
                || !cell.matches("[a-z][a-z0-9-]{0,23}")
                || epoch < 1
                || systemIdentifier == null
                || !systemIdentifier.matches("[0-9]{1,20}")
                || timeline < 1
                || timeline > 0xffffffffL)
            throw new IllegalArgumentException("Explicit writer WAL identity required");
        this.cell = cell;
        this.epoch = epoch;
        this.systemIdentifier = systemIdentifier;
        this.timeline = timeline;
    }

    /**
     * Deliberately restricted PG grammar: explicit FIRST/ANY count and concrete bounded names.
     */
    private static SynchronousPolicy synchronousPolicy(String setting) {
        if (setting == null || setting.length() > 1024)
            throw new IllegalStateException("Unknown synchronous WAL policy");
        if (setting.isBlank()) return new SynchronousPolicy(setting, "NONE", 0, List.of());
        var match =
                java.util.regex.Pattern.compile("(?i)\\s*(FIRST|ANY)\\s+([1-8])\\s*\\(([^()]*)\\)\\s*")
                        .matcher(setting);
        if (!match.matches()) throw new IllegalStateException("Unsupported synchronous WAL policy");
        String[] tokens = match.group(3).split(",", -1);
        if (tokens.length > 8)
            throw new IllegalStateException("Synchronous WAL policy capacity exceeded");
        List<String> identities = new ArrayList<>();
        Set<String> distinct = new HashSet<>();
        for (String token : tokens) {
            String name = token.trim();
            if (name.matches("\"[A-Za-z0-9_.-]{1,63}\"")) name = name.substring(1, name.length() - 1);
            else if (!name.matches("[A-Za-z_][A-Za-z0-9_]{0,62}"))
                throw new IllegalStateException("Unsupported synchronous WAL identity");
            // PG matches standby application names case-insensitively. Reject ambiguous case aliases.
            if (!distinct.add(name.toLowerCase(Locale.ROOT)))
                throw new IllegalStateException("Duplicate synchronous WAL identity");
            identities.add(name);
        }
        int required = Integer.parseInt(match.group(2));
        if (required > identities.size())
            throw new IllegalStateException("Incomplete synchronous WAL policy");
        return new SynchronousPolicy(
                setting, match.group(1).toUpperCase(Locale.ROOT), required, identities);
    }

    private static BigInteger position(String value) {
        if (value == null) return null;
        if (!value.matches("[0-9A-F]{1,8}/[0-9A-F]{1,8}"))
            throw new IllegalArgumentException("Invalid WAL position");
        var parts = value.split("/");
        return new BigInteger(parts[0], 16).shiftLeft(32).add(new BigInteger(parts[1], 16));
    }

    private static Duration duration(java.math.BigDecimal seconds) {
        if (seconds == null) return null;
        long nanos = seconds.movePointRight(9).longValueExact();
        if (nanos < 0) throw new IllegalArgumentException("Invalid replication duration");
        return Duration.ofNanos(nanos);
    }

    public DbOperation<Facts> poll(Duration budget) {
        if (budget == null
                || budget.isNegative()
                || budget.isZero()
                || budget.compareTo(Duration.ofSeconds(2)) > 0)
            throw new IllegalArgumentException("WAL probe budget outside bound");
        long started = System.nanoTime();
        return sql.submitTracked(
                DbClass.RECOVERY,
                budget,
                c -> {
                    // Both identity samples and replication rows use the original admitted physical
                    // connection.
                    var first = identity(c, started);
                    List<Replica> replicas = new ArrayList<>();
                    try (var statement = c.createStatement();
                         var rows =
                                 statement.executeQuery(
                                         "SELECT application_name,state,sync_state,flush_lsn::text,replay_lsn::text,"
                                                 + "extract(epoch FROM flush_lag),extract(epoch FROM replay_lag),reply_time FROM pg_stat_replication LIMIT 9")) {
                        while (rows.next()) {
                            if (replicas.size() == 8)
                                throw new IllegalStateException("WAL replica capacity exceeded");
                            var reply = rows.getTimestamp(8);
                            replicas.add(
                                    new Replica(
                                            rows.getString(1),
                                            rows.getString(2),
                                            rows.getString(3),
                                            position(rows.getString(4)),
                                            position(rows.getString(5)),
                                            duration(rows.getBigDecimal(6)),
                                            duration(rows.getBigDecimal(7)),
                                            reply == null ? null : reply.toInstant()));
                        }
                    }
                    var last = identity(c, started);
                    if (!first.systemIdentifier().equals(last.systemIdentifier())
                            || first.timeline() != last.timeline()
                            || !first.synchronousPolicy().equals(last.synchronousPolicy())
                            || last.flush().compareTo(first.flush()) < 0)
                        throw new IllegalStateException("WAL writer identity changed");
                    return new Facts(
                            last.cell(),
                            last.epoch(),
                            last.systemIdentifier(),
                            last.timeline(),
                            last.flush(),
                            last.checkedAt(),
                            started,
                            replicas,
                            last.synchronousPolicy());
                });
    }

    private Facts identity(java.sql.Connection c, long started) throws java.sql.SQLException {
        // pg_walfile_name is evaluated for the actual current flush position on a writer. Recovery
        // nodes and privilege failures cannot return a valid writer fact. pg_control_checkpoint is
        // unused.
        try (var statement = c.createStatement();
             var row =
                     statement.executeQuery(
                             "WITH p AS MATERIALIZED (SELECT pg_current_wal_flush_lsn() AS lsn) "
                                     + "SELECT a.cell_id,a.storage_epoch,a.status,a.ownership_mode,a.ownership_schema_version,"
                                     + "pg_is_in_recovery(),s.system_identifier::text,pg_walfile_name(p.lsn),p.lsn::text,clock_timestamp(),"
                                     + "current_setting('server_version_num')::int,current_setting('synchronous_standby_names') FROM cell_authority a CROSS JOIN pg_control_system() s CROSS JOIN p WHERE singleton_id=1")) {
            if (!row.next()
                    || !cell.equals(row.getString(1))
                    || epoch != row.getLong(2)
                    || !"ACTIVE".equals(row.getString(3))
                    || !"GROUPED".equals(row.getString(4))
                    || row.getLong(5) != 1
                    || row.getBoolean(6)
                    || !systemIdentifier.equals(row.getString(7))
                    || row.getInt(11) / 10000 != 17)
                throw new IllegalStateException("Unproven WAL writer identity");
            String filename = row.getString(8);
            if (filename == null
                    || !filename.matches("[0-9A-F]{24}")
                    || Long.parseUnsignedLong(filename.substring(0, 8), 16) != timeline)
                throw new IllegalStateException("Unproven current WAL timeline");
            return new Facts(
                    cell,
                    epoch,
                    systemIdentifier,
                    timeline,
                    Objects.requireNonNull(position(row.getString(9))),
                    row.getTimestamp(10).toInstant(),
                    started,
                    List.of(),
                    synchronousPolicy(row.getString(12)));
        }
    }

    public record Replica(
            String application,
            String state,
            String syncState,
            BigInteger flush,
            BigInteger replay,
            Duration flushDuration,
            Duration replayDuration,
            Instant replyAt) {
    }

    public record SynchronousPolicy(
            String setting, String mode, int required, List<String> identities) {
        public SynchronousPolicy {
            identities = List.copyOf(identities);
        }
    }

    public record Facts(
            String cell,
            long epoch,
            String systemIdentifier,
            long timeline,
            BigInteger flush,
            Instant checkedAt,
            long sampledAtNanos,
            List<Replica> replicas,
            SynchronousPolicy synchronousPolicy) {
        public Facts {
            replicas = List.copyOf(replicas);
            Objects.requireNonNull(synchronousPolicy);
        }
    }
}
