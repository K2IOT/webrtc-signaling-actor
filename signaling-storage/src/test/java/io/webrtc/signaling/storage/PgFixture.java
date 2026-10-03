package io.webrtc.signaling.storage;
import java.sql.*;
import org.flywaydb.core.Flyway;
import org.testcontainers.containers.PostgreSQLContainer;
public final class PgFixture {
    public static final PostgreSQLContainer<?> PG=new PostgreSQLContainer<>("postgres:17.6");
    static {PG.start();Flyway.configure().dataSource(PG.getJdbcUrl(),PG.getUsername(),PG.getPassword()).locations("classpath:db/migration").load().migrate();}
    public static Connection connection() throws SQLException {return DriverManager.getConnection(PG.getJdbcUrl(),PG.getUsername(),PG.getPassword());}
}
