package io.webrtc.signaling.storage.worker;
import io.webrtc.signaling.storage.*;
import io.webrtc.signaling.protocol.Identity.CallId;
import java.sql.*;
import java.time.*;
import java.util.*;
/** Index hints have no ownership meaning. A fixed upper key bounds each sweep under insert traffic. */
public final class RecoveryScanner {
    public record Row(long hashVersion,int group,CallId call){}
    public record Cursor(Row after,Row upper,Instant sweepStartedAt){public Cursor{Objects.requireNonNull(after);Objects.requireNonNull(upper);Objects.requireNonNull(sweepStartedAt);}}
    public record Page(List<Row> rows,Cursor next,Instant checkedAt,Duration elapsed){public Page{rows=List.copyOf(rows);}}
    private final SqlTransactions sql;private final String cell;private final long epoch;
    public RecoveryScanner(SqlTransactions sql,String cell,long epoch){this.sql=Objects.requireNonNull(sql);this.cell=Objects.requireNonNull(cell);this.epoch=epoch;}
    public DbOperation<Page> scan(Cursor cursor,int limit){WorkerFence.limit(limit,512);return sql.submitTracked(DbClass.RECOVERY,Duration.ofSeconds(2),c->{WorkerFence.cell(c,cell,epoch);Instant now;try(var q=c.createStatement();var r=q.executeQuery("SELECT clock_timestamp()")){r.next();now=r.getTimestamp(1).toInstant();}Row upper=cursor==null?null:cursor.upper();if(cursor==null)try(var q=c.createStatement();var r=q.executeQuery("SELECT ownership_hash_version,ownership_group_id,call_id FROM call_state WHERE terminal_at IS NULL ORDER BY ownership_hash_version DESC,ownership_group_id DESC,call_id DESC LIMIT 1")){if(r.next())upper=row(r);}Instant start=cursor==null?now:cursor.sweepStartedAt();if(upper==null)return new Page(List.of(),null,now,Duration.ZERO);var rows=new ArrayList<Row>();String after=cursor==null?"":" AND (ownership_hash_version,ownership_group_id,call_id)>(?,?,?)";try(var q=c.prepareStatement("SELECT ownership_hash_version,ownership_group_id,call_id FROM call_state WHERE terminal_at IS NULL AND (ownership_hash_version,ownership_group_id,call_id)<=(?,?,?)"+after+" ORDER BY ownership_hash_version,ownership_group_id,call_id LIMIT ?")){int i=bind(q,1,upper);if(cursor!=null)i=bind(q,i,cursor.after());q.setInt(i,limit);try(var r=q.executeQuery()){while(r.next())rows.add(row(r));}}boolean finished=rows.size()<limit||rows.getLast().equals(upper);return new Page(rows,finished?null:new Cursor(rows.getLast(),upper,start),now,Duration.between(start,now));});}
    private static Row row(ResultSet r)throws SQLException{return new Row(r.getLong(1),r.getInt(2),new CallId(r.getString(3)));}
    private static int bind(PreparedStatement q,int i,Row row)throws SQLException{q.setLong(i++,row.hashVersion());q.setInt(i++,row.group());q.setString(i++,row.call().value());return i;}
}
