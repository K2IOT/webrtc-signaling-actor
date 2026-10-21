package io.webrtc.signaling.storage;

import org.flywaydb.core.Flyway;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;

/**
 * TEST_ONLY PostgreSQL 17.6 for one local cell: a TLS primary and a real streaming standby with
 * {@code synchronous_standby_names = 'ANY 1 (c001_standby)'}, as spec §4.5 requires (the WAL
 * enrollment refuses a standby-less writer). The primary holds three databases: the c001 cell, an
 * empty-but-migrated c002 cell for the control's transfer owner, and the regional directory.
 */
final class LocalCompositionDatabase implements AutoCloseable {
    static final String USER = "postgres";
    static final String PASSWORD = "TEST_ONLY_LOCAL_COMPOSITION_DB";
    static final String STANDBY = "c001_standby";
    static final String C001 = "signaling_c001";
    static final String C002 = "signaling_c002";
    static final String DIRECTORY = "directory";
    /** Buckets 0..8191 belong to c001, 8192..16383 to c002 (spec §4.5). */
    static final int SPLIT = 8192;
    private static final String IMAGE = "postgres:17.6";
    private static final String PRIMARY_ALIAS = "pg-c001-primary";

    final Network network = Network.newNetwork();
    final GenericContainer<?> primary;
    GenericContainer<?> standby;
    String systemIdentifier;
    long timeline;

    LocalCompositionDatabase(LocalCompositionPki.Leaf server) throws Exception {
        primary =
                new GenericContainer<>(IMAGE)
                        .withNetwork(network)
                        .withNetworkAliases(PRIMARY_ALIAS)
                        .withEnv("POSTGRES_USER", USER)
                        .withEnv("POSTGRES_PASSWORD", PASSWORD)
                        .withEnv("POSTGRES_DB", "postgres")
                        .withExposedPorts(5432)
                        .withCopyToContainer(
                                Transferable.of(Files.readAllBytes(server.certificate()), 0644),
                                "/tmp/tls/server.crt")
                        .withCopyToContainer(
                                Transferable.of(Files.readAllBytes(server.key()), 0600), "/tmp/tls/server.key")
                        .withCommand(
                                "bash",
                                "-ceu",
                                "install -o postgres -g postgres -m 0644 /tmp/tls/server.crt /var/lib/postgresql/server.crt; "
                                        + "install -o postgres -g postgres -m 0600 /tmp/tls/server.key /var/lib/postgresql/server.key; "
                                        + "exec docker-entrypoint.sh postgres -c ssl=on "
                                        + "-c ssl_cert_file=/var/lib/postgresql/server.crt "
                                        + "-c ssl_key_file=/var/lib/postgresql/server.key "
                                        + "-c ssl_min_protocol_version=TLSv1.3 "
                                        + "-c wal_level=replica -c max_wal_senders=4 -c max_connections=400 "
                                        + "-c synchronous_commit=on")
                        .waitingFor(
                                Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2))
                        .withStartupTimeout(Duration.ofSeconds(60));
        try {
            primary.start();
            var hba =
                    primary.execInContainer(
                            "bash",
                            "-ceu",
                            "printf '%s\\n' 'host replication postgres all scram-sha-256' >> \"$PGDATA/pg_hba.conf\"");
            if (hba.getExitCode() != 0) throw new IllegalStateException(hba.getStderr());
            try (var c = admin("postgres");
                 var q = c.createStatement()) {
                q.execute("SELECT pg_reload_conf()");
                q.execute("CREATE DATABASE " + C001);
                q.execute("CREATE DATABASE " + C002);
                q.execute("CREATE DATABASE " + DIRECTORY);
            }
            migrateCell(C001, "c001", 0, SPLIT - 1);
            migrateCell(C002, "c002", SPLIT, 16383);
            migrateDirectory();
            startStandby();
            try (var c = admin(C001);
                 var q = c.createStatement();
                 var r =
                         q.executeQuery(
                                 "SELECT system_identifier::text,('x'||substring(pg_walfile_name(pg_current_wal_flush_lsn()),1,8))::bit(32)::bigint FROM pg_control_system()")) {
                r.next();
                systemIdentifier = r.getString(1);
                timeline = r.getLong(2);
            }
        } catch (Exception | AssertionError failure) {
            close();
            throw failure;
        }
    }

    /** The URL the processes use: TLS with full hostname verification against the fixture CA. */
    String jdbcUrl(String database, Path rootCertificate) {
        return "jdbc:postgresql://localhost:"
                + primary.getMappedPort(5432)
                + "/"
                + database
                + "?sslmode=verify-full&sslrootcert="
                + rootCertificate;
    }

    /** Fixture-side administration only; the processes never use this URL. */
    Connection admin(String database) throws Exception {
        return DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + primary.getMappedPort(5432) + "/" + database + "?sslmode=disable",
                USER,
                PASSWORD);
    }

    private void migrateCell(String database, String cell, int firstActive, int lastActive) throws Exception {
        Flyway.configure()
                .dataSource(
                        "jdbc:postgresql://localhost:" + primary.getMappedPort(5432) + "/" + database + "?sslmode=disable",
                        USER,
                        PASSWORD)
                .locations("classpath:db/migration")
                .load()
                .migrate();
        try (var c = admin(database);
             var q = c.createStatement()) {
            q.execute("INSERT INTO cell_authority VALUES(1,'" + cell + "',1,'GROUPED',1,'ACTIVE')");
            q.execute(
                    "INSERT INTO bucket_authority(bucket_id,directory_epoch,status,recovery_epoch) SELECT n,1,"
                            + "CASE WHEN n BETWEEN "
                            + firstActive
                            + " AND "
                            + lastActive
                            + " THEN 'ACTIVE' ELSE 'FROZEN' END,1 FROM generate_series(0,16383) n");
            q.execute(
                    "INSERT INTO group_owner(cell_id,ownership_hash_version,group_id,storage_epoch,group_epoch,lease_sequence,status) SELECT '"
                            + cell
                            + "',1,n,1,1,0,'IDLE' FROM generate_series(0,1023) n");
        }
    }

    private void migrateDirectory() throws Exception {
        Flyway.configure()
                .dataSource(
                        "jdbc:postgresql://localhost:" + primary.getMappedPort(5432) + "/" + DIRECTORY + "?sslmode=disable",
                        USER,
                        PASSWORD)
                .table("directory_schema_history")
                .locations("classpath:db/directory/migration")
                .load()
                .migrate();
    }

    /** Seeds the 16384 regional rows: each cell's half points at that cell's gateway URL. */
    void seedDirectory(String c001Url, String c002Url) throws Exception {
        try (var c = admin(DIRECTORY);
             var q = c.prepareStatement(
                     "INSERT INTO directory_bucket(bucket_id,cell_id,directory_epoch,wss_url) SELECT n,"
                             + "CASE WHEN n < ? THEN 'c001' ELSE 'c002' END,1,"
                             + "CASE WHEN n < ? THEN ? ELSE ? END FROM generate_series(0,16383) n")) {
            q.setInt(1, SPLIT);
            q.setInt(2, SPLIT);
            q.setString(3, c001Url);
            q.setString(4, c002Url);
            q.execute();
        }
    }

    private void startStandby() throws Exception {
        standby =
                new GenericContainer<>(IMAGE)
                        .withNetwork(network)
                        .withEnv("PGDATA", "/var/lib/postgresql/data/replica")
                        .withEnv("PGPASSWORD", PASSWORD)
                        .withExposedPorts(5432)
                        .withCommand(
                                "bash",
                                "-ceu",
                                "install -d -m 0700 -o postgres -g postgres \"$PGDATA\"; "
                                        + "runuser -u postgres -- pg_basebackup -D \"$PGDATA\" -R -X stream --checkpoint=fast "
                                        + "-d 'host="
                                        + PRIMARY_ALIAS
                                        + " user=postgres application_name="
                                        + STANDBY
                                        + "'; "
                                        + "exec runuser -u postgres -- postgres -D \"$PGDATA\" -c hot_standby=on -c max_connections=400")
                        .waitingFor(
                                Wait.forLogMessage(".*database system is ready to accept read-only connections.*\\n", 1))
                        .withStartupTimeout(Duration.ofSeconds(60));
        standby.start();
        try (var c = admin("postgres");
             var q = c.createStatement()) {
            q.execute("ALTER SYSTEM SET synchronous_standby_names='ANY 1 (" + STANDBY + ")'");
            q.execute("SELECT pg_reload_conf()");
        }
        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .until(
                        () -> {
                            try (var c = admin("postgres");
                                 var q = c.createStatement();
                                 var r =
                                         q.executeQuery(
                                                 "SELECT count(*) FROM pg_stat_replication WHERE application_name='"
                                                         + STANDBY
                                                         + "' AND state='streaming' AND sync_state='quorum'")) {
                                r.next();
                                return r.getInt(1) == 1;
                            }
                        });
    }

    @Override
    public void close() {
        try {
            if (standby != null) standby.stop();
        } finally {
            try {
                primary.stop();
            } finally {
                network.close();
            }
        }
    }
}
