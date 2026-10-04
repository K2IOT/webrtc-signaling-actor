package io.webrtc.signaling.storage;
import static org.assertj.core.api.Assertions.*;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
class AuthoritySqlIT {
    @Test void barriersValidateFreshRootsAndFrozenBucketsFailClosed() throws Exception {
        try(var c=PgFixture.connection();var s=c.createStatement()) {
            s.execute("INSERT INTO cell_authority VALUES(1,'c001',1,'GROUPED',1,'ACTIVE') ON CONFLICT DO NOTHING");
            s.execute("INSERT INTO bucket_authority(bucket_id,directory_epoch,status,recovery_epoch) VALUES(42,1,'ACTIVE',1) ON CONFLICT DO NOTHING");
            c.setAutoCommit(false);
            AuthoritySql.home(c,"c001",1,Map.of(42,1L),List.of("guard-z","guard-a"));
            assertThat(c.createStatement().executeQuery("SELECT count(*) FROM user_guard WHERE user_id IN ('guard-a','guard-z')").next()).isTrue();c.commit();
            s.execute("UPDATE bucket_authority SET status='FROZEN' WHERE bucket_id=42");c.commit();
            assertThatThrownBy(()->AuthoritySql.home(c,"c001",1,Map.of(42,1L),List.of("never-authorized"))).isInstanceOf(AuthoritySql.FencedException.class);c.rollback();
            s.execute("UPDATE bucket_authority SET status='ACTIVE' WHERE bucket_id=42");c.commit();
        }
    }
    @Test void sharedBusinessBarrierConflictsWithExclusiveFreeze() throws Exception {
        try(var first=PgFixture.connection();var second=PgFixture.connection()) {
            first.setAutoCommit(false);second.setAutoCommit(false);
            AuthoritySql.bucketBarrier(first,77,false);
            assertThatThrownBy(()->AuthoritySql.bucketBarrier(second,77,true)).isInstanceOf(AuthoritySql.RetryableConflict.class);
            second.rollback();first.commit();AuthoritySql.bucketBarrier(second,77,true);second.commit();
        }
    }
    @Test void jpaBoundConnectionCommitAndRollbackAreInsideAdmittedVirtualThread() throws Exception {
        var admission=new DbAdmission(Map.of(DbClass.CRITICAL,1,DbClass.RENEWAL,1));
        try(var boundary=new DbBoundary(admission);var pools=new DbPools(PgFixture.PG.getJdbcUrl(),PgFixture.PG.getUsername(),PgFixture.PG.getPassword(),admission,1,1)) {
            var sql=new SqlTransactions(boundary,pools);
            record Receipt(boolean virtual,boolean same,int backend) {}
            var committed=sql.submitTracked(DbClass.CRITICAL,Duration.ofSeconds(2),c->{
                try(var s=c.createStatement()) {s.execute("INSERT INTO user_guard(user_id) VALUES('tx-committed') ON CONFLICT DO NOTHING");var r=s.executeQuery("SELECT pg_backend_pid()");r.next();return new Receipt(Thread.currentThread().isVirtual(),c==pools.currentConnection(DbClass.CRITICAL),r.getInt(1));}
            });
            var receipt=committed.logical().toCompletableFuture().join();committed.physicalCompletion().toCompletableFuture().join();
            assertThat(receipt.virtual()).isTrue();assertThat(receipt.same()).isTrue();
            assertThatThrownBy(()->sql.submit(DbClass.CRITICAL,Duration.ofSeconds(2),c->{c.createStatement().execute("INSERT INTO user_guard(user_id) VALUES('tx-rolledback')");throw new IllegalStateException("abort");}).toCompletableFuture().join()).hasCauseInstanceOf(IllegalStateException.class);
            try(var c=PgFixture.connection();var s=c.createStatement();var r=s.executeQuery("SELECT user_id FROM user_guard WHERE user_id IN ('tx-committed','tx-rolledback')")){r.next();assertThat(r.getString(1)).isEqualTo("tx-committed");assertThat(r.next()).isFalse();}
        }
    }
    @Test void contendedNativeUserGuardReturnsTypedRetryWithoutWaiting()throws Exception{String user="contention-"+UUID.randomUUID();try(var seed=PgFixture.connection();var q=seed.prepareStatement("INSERT INTO user_guard(user_id) VALUES(?)")){q.setString(1,user);q.executeUpdate();}try(var first=PgFixture.connection();var second=PgFixture.connection()){first.setAutoCommit(false);second.setAutoCommit(false);AuthoritySql.userGuards(first,List.of(user));assertThatThrownBy(()->AuthoritySql.userGuards(second,List.of(user))).isInstanceOf(AuthoritySql.RetryableConflict.class);second.rollback();first.commit();AuthoritySql.userGuards(second,List.of(user));second.commit();}}
}
