package io.webrtc.signaling.storage.worker;
import io.webrtc.signaling.storage.*;
import java.sql.*;
import java.util.*;
final class WorkerFence {
    static void cell(Connection c,String cell,long epoch)throws SQLException{AuthoritySql.cellBarrier(c,false);AuthoritySql.validateCell(c,cell,epoch);}
    static void bucket(Connection c,int bucket)throws SQLException{AuthoritySql.bucketBarrier(c,bucket,false);try(var q=c.prepareStatement("SELECT status FROM bucket_authority WHERE bucket_id=?")){q.setInt(1,bucket);try(var r=q.executeQuery()){if(!r.next()||!"ACTIVE".equals(r.getString(1)))throw new AuthoritySql.FencedException();}}}
    static void limit(int limit,int maximum){if(limit<1||limit>maximum)throw new IllegalArgumentException("Invalid worker batch bound");}
}
