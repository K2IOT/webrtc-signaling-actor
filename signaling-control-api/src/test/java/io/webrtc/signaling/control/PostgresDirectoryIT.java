package io.webrtc.signaling.control;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.storage.*;
import java.time.Duration;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;

class PostgresDirectoryIT {
    static final class Fixture implements AutoCloseable {
        final String url,localUrl;
        final DbBoundary regionalBoundary;final DbPools regionalPools;
        final DbTestRuntime runtime;
        final AtomicBoolean writer=new AtomicBoolean(true);
        final PostgresDirectoryRepository directory;
        Fixture()throws Exception {
            String schema="directory_"+UUID.randomUUID().toString().replace("-","");
            url=PgFixture.PG.getJdbcUrl()+"&currentSchema="+schema;
            Flyway.configure().dataSource(url,PgFixture.PG.getUsername(),PgFixture.PG.getPassword()).schemas(schema).defaultSchema(schema).locations("classpath:db/directory/migration").load().migrate();
            localUrl=PgFixture.PG.getJdbcUrl()+"&currentSchema="+schema+"_cell";
            Flyway.configure().dataSource(localUrl,PgFixture.PG.getUsername(),PgFixture.PG.getPassword()).schemas(schema+"_cell").defaultSchema(schema+"_cell").locations("classpath:db/migration").load().migrate();
            runtime=new DbTestRuntime(localUrl,PgFixture.PG.getUsername(),PgFixture.PG.getPassword());
            var quotas=new DbAdmission(Map.of(DbClass.CRITICAL,8,DbClass.RECOVERY,2));regionalBoundary=new DbBoundary(quotas);regionalPools=new DbPools(url,PgFixture.PG.getUsername(),PgFixture.PG.getPassword(),quotas,2,8);
            directory=new PostgresDirectoryRepository(new SqlTransactions(regionalBoundary,regionalPools),runtime.sql,"c001",1,writer::get);
            try(var c=connection();var q=c.createStatement()){
                q.execute("INSERT INTO directory_bucket(bucket_id,cell_id,directory_epoch,wss_url) VALUES(175,'c001',1,'wss://c001.example.invalid/signal')");
            }
        }
        Connection connection()throws SQLException{return DriverManager.getConnection(url,PgFixture.PG.getUsername(),PgFixture.PG.getPassword());}
        Connection localConnection()throws SQLException{return DriverManager.getConnection(localUrl,PgFixture.PG.getUsername(),PgFixture.PG.getPassword());}
        public void close(){regionalPools.close();regionalBoundary.close();runtime.close();}
    }
    @Test void nativePublicationHasOneWinnerAndStalePublicationCannotRollBackTheEpoch()throws Exception{
        try(var f=new Fixture()){
            var before=f.directory.read(175).toCompletableFuture().join().orElseThrow();
            var a=new HomeRoute(175,"c002",2,"wss://c002.example.invalid/signal");
            var b=new HomeRoute(175,"c003",2,"wss://c003.example.invalid/signal");
            var first=f.directory.compareAndPublish(before,a).toCompletableFuture();
            var second=f.directory.compareAndPublish(before,b).toCompletableFuture();
            assertThat(List.of(first.join(),second.join())).containsExactlyInAnyOrder(true,false);
            var committed=f.directory.read(175).toCompletableFuture().join().orElseThrow();
            assertThat(committed).isIn(a,b);
            assertThat(f.directory.compareAndPublish(before,new HomeRoute(175,"c004",3,"wss://c004.example.invalid/signal")).toCompletableFuture().join()).isFalse();
            assertThatThrownBy(()->f.directory.compareAndPublish(committed,before)).isInstanceOf(IllegalArgumentException.class);
            try(var c=f.connection();var q=c.createStatement()){
                assertThatThrownBy(()->q.execute("UPDATE directory_bucket SET directory_epoch=1 WHERE bucket_id=175")).isInstanceOf(SQLException.class);
            }
        }
    }
    @Test void writerFencingStopsRegionalReadsAndPublicationButLocalAuthorityUsesItsOwnPrimary()throws Exception{
        try(var f=new Fixture()){
            var route=f.directory.read(175).toCompletableFuture().join().orElseThrow();
            f.writer.set(false);
            assertThatThrownBy(()->f.directory.read(175).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThatThrownBy(()->f.directory.compareAndPublish(route,new HomeRoute(175,"c002",2,"wss://c002.example.invalid/signal")).toCompletableFuture().join()).hasCauseInstanceOf(AuthoritySql.FencedException.class);
            assertThat(f.directory.localActive(175,"c001",1).toCompletableFuture().join()).isTrue();
            assertThat(f.directory.localActive(175,"c002",1).toCompletableFuture().join()).isFalse();
            try(var c=f.localConnection();var q=c.createStatement()){q.execute("UPDATE bucket_authority SET status='FROZEN' WHERE bucket_id=175");}
            assertThat(f.directory.localActive(175,"c001",1).toCompletableFuture().join()).isFalse();
        }
    }
    @Test void directoryRowLockTimeoutIsFiniteAndDoesNotPublishAnUnacknowledgedHint()throws Exception{
        try(var f=new Fixture();var lock=f.connection()){
            var before=f.directory.read(175).toCompletableFuture().join().orElseThrow();lock.setAutoCommit(false);
            try(var q=lock.createStatement()){q.execute("SELECT bucket_id FROM directory_bucket WHERE bucket_id=175 FOR UPDATE");}
            long start=System.nanoTime();
            assertThatThrownBy(()->f.directory.compareAndPublish(before,new HomeRoute(175,"c002",2,"wss://c002.example.invalid/signal")).toCompletableFuture().join()).isInstanceOf(java.util.concurrent.CompletionException.class);
            assertThat(Duration.ofNanos(System.nanoTime()-start)).isLessThan(Duration.ofSeconds(2));lock.rollback();
            assertThat(f.directory.read(175).toCompletableFuture().join()).contains(before);
        }
    }
}
