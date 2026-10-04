package io.webrtc.signaling.storage.worker;
import io.webrtc.signaling.storage.*;
import java.sql.*;
import java.time.*;
import java.util.*;
/** Per-bucket SKIP LOCKED cleanup. Live origins and the terminal replay window remain authoritative. */
public final class RetentionWorker {
    public record Report(int commandResults,int terminalCalls,int homeHistory,int outbox){public int deleted(){return commandResults+terminalCalls+homeHistory+outbox;}}
    private final SqlTransactions sql;private final String cell;private final long epoch;
    public RetentionWorker(SqlTransactions sql,String cell,long epoch){this.sql=Objects.requireNonNull(sql);this.cell=Objects.requireNonNull(cell);this.epoch=epoch;}
    public DbOperation<Report> prune(int bucket,int limit){WorkerFence.limit(limit,128);return sql.submitTracked(DbClass.MAINTENANCE,Duration.ofSeconds(2),c->{WorkerFence.cell(c,cell,epoch);WorkerFence.bucket(c,bucket);int results;try(var q=c.prepareStatement("WITH due AS (SELECT issuer,jti,command_scope,request_id FROM command_result r WHERE authority_bucket_id=? AND status='FINAL' AND expires_at<clock_timestamp() AND NOT EXISTS (SELECT 1 FROM call_state s WHERE s.call_id=r.call_id AND (s.terminal_at IS NULL OR s.expires_at>=clock_timestamp())) ORDER BY expires_at,issuer,jti,command_scope,request_id LIMIT ? FOR UPDATE SKIP LOCKED) DELETE FROM command_result r USING due d WHERE (r.issuer,r.jti,r.command_scope,r.request_id)=(d.issuer,d.jti,d.command_scope,d.request_id)")){q.setInt(1,bucket);q.setInt(2,limit);results=q.executeUpdate();}int outbox=delete(c,"control_outbox","expires_at<clock_timestamp()",bucket,limit);int history=delete(c,"home_participation","terminal_at IS NOT NULL AND expires_at<clock_timestamp()",bucket,limit);int calls=delete(c,"call_state","terminal_at IS NOT NULL AND expires_at<clock_timestamp() AND NOT EXISTS (SELECT 1 FROM command_result r WHERE r.call_id=call_state.call_id)",bucket,limit);return new Report(results,calls,history,outbox);});}
    private static int delete(Connection c,String table,String predicate,int bucket,int limit)throws SQLException{try(var q=c.prepareStatement("WITH due AS (SELECT ctid FROM "+table+" WHERE "+(table.equals("home_participation")?"native_authority_bucket(user_id)":"authority_bucket_id")+"=? AND "+predicate+" ORDER BY expires_at LIMIT ? FOR UPDATE SKIP LOCKED) DELETE FROM "+table+" t USING due d WHERE t.ctid=d.ctid")){q.setInt(1,bucket);q.setInt(2,limit);return q.executeUpdate();}}
}
