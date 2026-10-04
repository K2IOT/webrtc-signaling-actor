package io.webrtc.signaling.storage;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletionStage;
public final class GroupOwnerRepository implements GroupOwnership {
    public static final Duration TTL=Duration.ofSeconds(15),PULSE_INTERVAL=Duration.ofSeconds(5);
    public record Grant(AuthoritySql.GroupToken token,long sequence,UUID operation,UUID acquireOperation,Instant leaseUntil,Instant databaseTime) {}
    private record Root(long storageEpoch,long epoch,String node,UUID incarnation,String status,long sequence,UUID operation,UUID acquireOperation,Instant expiry,Instant now) {
        boolean live(){return status.equals("OWNED")&&expiry!=null&&expiry.isAfter(now);}
    }
    private final SqlTransactions sql;private final String cell;private final long storageEpoch;
    public GroupOwnerRepository(SqlTransactions sql,String cell,long storageEpoch){this.sql=Objects.requireNonNull(sql);this.cell=cell;if(storageEpoch<1)throw new IllegalArgumentException("Invalid storage epoch");this.storageEpoch=storageEpoch;}
    public CompletionStage<Optional<Grant>> acquire(int group,String node,UUID incarnation,UUID operation){return acquireTracked(group,node,incarnation,operation).logical();}
    public DbOperation<Optional<Grant>> acquireTracked(int group,String node,UUID incarnation,UUID operation){return sql.submitTracked(DbClass.RECOVERY,Duration.ofSeconds(2),c->{
        roots(c,group,true);Root current=read(c,group);
        if(current.live()&&current.storageEpoch()==storageEpoch){
            if(node.equals(current.node())&&incarnation.equals(current.incarnation())&&operation.equals(current.acquireOperation()))return Optional.of(grant(group,current));return Optional.empty();
        }
        if(incarnation.equals(current.incarnation())||current.storageEpoch()>storageEpoch)throw new AuthoritySql.FencedException();long epoch=Math.addExact(current.epoch(),1);
        try(var s=c.prepareStatement("WITH stamp AS (SELECT clock_timestamp() AS tick) UPDATE group_owner g SET storage_epoch=?,group_epoch=?,owner_node=?,owner_incarnation=?,status='OWNED',lease_sequence=1,last_pulse_operation=?,acquire_operation_id=?,lease_until=stamp.tick+interval '15 seconds',last_granted_expiry=stamp.tick+interval '15 seconds' FROM stamp WHERE g.cell_id=? AND g.ownership_hash_version=1 AND g.group_id=? AND g.group_epoch=?")){
            s.setLong(1,storageEpoch);s.setLong(2,epoch);s.setString(3,node);s.setObject(4,incarnation);s.setObject(5,operation);s.setObject(6,operation);s.setString(7,cell);s.setInt(8,group);s.setLong(9,current.epoch());if(s.executeUpdate()!=1)throw new AuthoritySql.RetryableConflict();
        }return Optional.of(grant(group,read(c,group)));
    });}
    public CompletionStage<Grant> pulse(Grant previous,long sequence,UUID operation){return pulseTracked(previous,sequence,operation).logical();}
    public DbOperation<Grant> pulseTracked(Grant previous,long sequence,UUID operation){return sql.submitTracked(DbClass.RENEWAL,Duration.ofSeconds(2),c->{
        AuthoritySql.GroupToken token=previous.token();validateDomain(token);roots(c,token.group(),true);Root current=read(c,token.group());requireToken(token,current);
        if(!current.live())throw new AuthoritySql.FencedException();
        if(sequence==current.sequence()&&operation.equals(current.operation()))return grant(token.group(),current);
        if(sequence!=Math.addExact(current.sequence(),1)||operation.equals(current.operation()))throw new AuthoritySql.FencedException();
        try(var s=c.prepareStatement("WITH stamp AS (SELECT clock_timestamp() AS tick) UPDATE group_owner g SET lease_sequence=?,last_pulse_operation=?,lease_until=stamp.tick+interval '15 seconds',last_granted_expiry=stamp.tick+interval '15 seconds' FROM stamp WHERE g.cell_id=? AND g.ownership_hash_version=1 AND g.group_id=? AND g.storage_epoch=? AND g.group_epoch=? AND g.owner_node=? AND g.owner_incarnation=? AND g.status='OWNED' AND g.lease_sequence=? AND g.lease_until>clock_timestamp()")){
            s.setLong(1,sequence);s.setObject(2,operation);bindToken(s,3,token);s.setLong(9,current.sequence());if(s.executeUpdate()!=1)throw new AuthoritySql.FencedException();
        }return grant(token.group(),read(c,token.group()));
    });}
    public CompletionStage<Boolean> release(AuthoritySql.GroupToken token){return releaseTracked(token).logical();}
    public DbOperation<Boolean> releaseTracked(AuthoritySql.GroupToken token){return sql.submitTracked(DbClass.TERMINATION,Duration.ofSeconds(2),c->{
        validateDomain(token);roots(c,token.group(),true);Root current=read(c,token.group());
        if(!sameToken(token,current)||!current.status().equals("OWNED"))return true;
        try(var s=c.prepareStatement("UPDATE group_owner SET status='RELEASED',lease_until=clock_timestamp() WHERE cell_id=? AND ownership_hash_version=1 AND group_id=? AND storage_epoch=? AND group_epoch=? AND owner_node=? AND owner_incarnation=? AND status='OWNED'")){
            bindToken(s,1,token);return s.executeUpdate()==1;
        }
    });}
    public CompletionStage<Optional<Grant>> reconcile(int group,String node,UUID incarnation){return reconcileTracked(group,node,incarnation).logical();}
    public DbOperation<Optional<Grant>> reconcileTracked(int group,String node,UUID incarnation){return sql.submitTracked(DbClass.RECOVERY,Duration.ofSeconds(2),c->{
        roots(c,group,false);Root current=read(c,group);return current.live()&&current.storageEpoch()==storageEpoch&&node.equals(current.node())&&incarnation.equals(current.incarnation())?Optional.of(grant(group,current)):Optional.empty();
    });}
    private void roots(Connection c,int group,boolean exclusive)throws SQLException {AuthoritySql.cellBarrier(c,false);AuthoritySql.validateCell(c,cell,storageEpoch);AuthoritySql.groupBarrier(c,group,exclusive);}
    private Root read(Connection c,int group)throws SQLException {
        try(var s=c.prepareStatement("SELECT storage_epoch,group_epoch,owner_node,owner_incarnation,status,lease_sequence,last_pulse_operation,acquire_operation_id,lease_until,clock_timestamp() FROM group_owner WHERE cell_id=? AND ownership_hash_version=1 AND group_id=?")){
            s.setString(1,cell);s.setInt(2,group);try(var r=s.executeQuery()){if(!r.next())throw new AuthoritySql.FencedException();Timestamp expiry=r.getTimestamp(9);return new Root(r.getLong(1),r.getLong(2),r.getString(3),r.getObject(4,UUID.class),r.getString(5),r.getLong(6),r.getObject(7,UUID.class),r.getObject(8,UUID.class),expiry==null?null:expiry.toInstant(),r.getTimestamp(10).toInstant());}
        }
    }
    private Grant grant(int group,Root root){if(!root.live())throw new AuthoritySql.FencedException();return new Grant(new AuthoritySql.GroupToken(cell,root.storageEpoch(),1,group,root.epoch(),root.node(),root.incarnation()),root.sequence(),root.operation(),root.acquireOperation(),root.expiry(),root.now());}
    private void validateDomain(AuthoritySql.GroupToken token){if(!cell.equals(token.cell())||storageEpoch!=token.storageEpoch()||token.hashVersion()!=1)throw new AuthoritySql.FencedException();}
    private static boolean sameToken(AuthoritySql.GroupToken token,Root root){return token.storageEpoch()==root.storageEpoch()&&token.epoch()==root.epoch()&&token.node().equals(root.node())&&token.incarnation().equals(root.incarnation());}
    private static void requireToken(AuthoritySql.GroupToken token,Root root){if(!sameToken(token,root))throw new AuthoritySql.FencedException();}
    private static void bindToken(PreparedStatement s,int first,AuthoritySql.GroupToken token)throws SQLException {s.setString(first,token.cell());s.setInt(first+1,token.group());s.setLong(first+2,token.storageEpoch());s.setLong(first+3,token.epoch());s.setString(first+4,token.node());s.setObject(first+5,token.incarnation());}
}
