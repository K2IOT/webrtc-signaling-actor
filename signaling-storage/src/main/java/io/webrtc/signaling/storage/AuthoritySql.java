package io.webrtc.signaling.storage;
import java.sql.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
/** Namespace v1. Each lock acquisition and protected read is a distinct statement. */
public final class AuthoritySql {
    private AuthoritySql() {}
    public record GroupToken(String cell,long storageEpoch,long hashVersion,int group,long epoch,String node,UUID incarnation) {
        public GroupToken {if(storageEpoch<1||hashVersion<1||group<0||group>1023||epoch<1)throw new IllegalArgumentException("Invalid group token");Objects.requireNonNull(cell);Objects.requireNonNull(node);Objects.requireNonNull(incarnation);}
    }
    public static void cellBarrier(Connection c,boolean exclusive)throws SQLException {barrier(c,100,1,exclusive);}
    public static void bucketBarrier(Connection c,int bucket,boolean exclusive)throws SQLException {if(bucket<0||bucket>16383)throw new IllegalArgumentException("Invalid bucket");barrier(c,101,bucket,exclusive);}
    public static void groupBarrier(Connection c,int group,boolean exclusive)throws SQLException {if(group<0||group>1023)throw new IllegalArgumentException("Invalid group");barrier(c,102,group,exclusive);}
    public static void callBarrier(Connection c,String call)throws SQLException {barrier(c,103,callKey(call),true);}
    public static void callReadBarrier(Connection c,String call)throws SQLException {barrier(c,103,callKey(call),false);}
    public static int callKey(String call) {try{return ByteBuffer.wrap(MessageDigest.getInstance("SHA-256").digest(call.getBytes(StandardCharsets.UTF_8))).getInt();}catch(NoSuchAlgorithmException e){throw new IllegalStateException(e);}}
    private static void barrier(Connection c,int namespace,int key,boolean exclusive)throws SQLException {
        if(c.getAutoCommit())throw new IllegalStateException("Authority barriers require a transaction");
        String sql=exclusive?"SELECT pg_try_advisory_xact_lock(?,?)":"SELECT pg_try_advisory_xact_lock_shared(?,?)";
        try(var s=c.prepareStatement(sql)){s.setInt(1,namespace);s.setInt(2,key);try(var r=s.executeQuery()){if(!r.next()||!r.getBoolean(1))throw new RetryableConflict();}}
    }
    public static void validateCell(Connection c,String cell,long storageEpoch)throws SQLException {
        try(var s=c.prepareStatement("SELECT cell_id,storage_epoch,ownership_mode,ownership_schema_version,status FROM cell_authority WHERE singleton_id=1");var r=s.executeQuery()){
            if(!r.next()||!cell.equals(r.getString(1))||storageEpoch!=r.getLong(2)||!"GROUPED".equals(r.getString(3))||r.getLong(4)!=1||!"ACTIVE".equals(r.getString(5)))throw new FencedException();
        }
    }
    public static void validateBuckets(Connection c,Map<Integer,Long> buckets)throws SQLException {
        for(var e:new TreeMap<>(buckets).entrySet())try(var s=c.prepareStatement("SELECT directory_epoch,status FROM bucket_authority WHERE bucket_id=?")){
            s.setInt(1,e.getKey());try(var r=s.executeQuery()){if(!r.next()||r.getLong(1)!=e.getValue()||!"ACTIVE".equals(r.getString(2)))throw new FencedException();}
        }
    }
    public static void validateGroup(Connection c,GroupToken token)throws SQLException {
        try(var s=c.prepareStatement("SELECT storage_epoch,group_epoch,owner_node,owner_incarnation,status,lease_until>clock_timestamp()+interval '5 seconds' FROM group_owner WHERE cell_id=? AND ownership_hash_version=? AND group_id=?")){
            s.setString(1,token.cell());s.setLong(2,token.hashVersion());s.setInt(3,token.group());try(var r=s.executeQuery()){
                if(!r.next()||r.getLong(1)!=token.storageEpoch()||r.getLong(2)!=token.epoch()||!token.node().equals(r.getString(3))||!token.incarnation().equals(r.getObject(4,UUID.class))||!"OWNED".equals(r.getString(5))||!r.getBoolean(6))throw new FencedException();
            }
        }
    }
    public static void userGuards(Connection c,List<String> users)throws SQLException {
        if(users.size()>16)throw new IllegalArgumentException("Guard batch exceeds limit");
        var sorted=new TreeSet<>(users);for(String user:sorted)try(var s=c.prepareStatement("INSERT INTO user_guard(user_id) VALUES(?) ON CONFLICT DO NOTHING")){s.setString(1,user);s.executeUpdate();}
        for(String user:sorted)try(var s=c.prepareStatement("SELECT user_id FROM user_guard WHERE user_id=? FOR UPDATE NOWAIT")){s.setString(1,user);try(var r=s.executeQuery()){if(!r.next())throw new RetryableConflict();}}
    }
    public static void home(Connection c,String cell,long storageEpoch,Map<Integer,Long> buckets,List<String> users)throws SQLException {
        roots(c,cell,storageEpoch,buckets);userGuards(c,users);
    }
    public static void coordinator(Connection c,GroupToken token,Map<Integer,Long> buckets,List<String> users,List<String> calls)throws SQLException {
        roots(c,token.cell(),token.storageEpoch(),buckets);groupBarrier(c,token.group(),false);validateGroup(c,token);userGuards(c,users);
        if(calls.size()>16)throw new IllegalArgumentException("Call batch exceeds limit");for(String call:new TreeSet<>(calls))callBarrier(c,call);
    }
    public static void coordinatorGrant(Connection c,GroupToken token,Map<Integer,Long> buckets,String call)throws SQLException {
        roots(c,token.cell(),token.storageEpoch(),buckets);groupBarrier(c,token.group(),false);validateGroup(c,token);callReadBarrier(c,call);
    }
    private static void roots(Connection c,String cell,long epoch,Map<Integer,Long> buckets)throws SQLException {
        if(buckets.size()>128)throw new IllegalArgumentException("Bucket batch exceeds limit");cellBarrier(c,false);validateCell(c,cell,epoch);
        for(int bucket:new TreeSet<>(buckets.keySet()))bucketBarrier(c,bucket,false);validateBuckets(c,buckets);
    }
    public static final class FencedException extends RuntimeException {public FencedException(){super("Authority fence rejected");}}
    public static final class RetryableConflict extends RuntimeException {public RetryableConflict(){super("Retryable authority barrier contention");}}
}
