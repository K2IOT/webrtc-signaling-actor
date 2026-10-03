package io.webrtc.signaling.storage;
import java.sql.*;
import java.time.Instant;
import java.util.UUID;
public final class GatewayLeaseRepository {
    public record Renewal(Boot boot,long sequence,UUID operation) {}
    public record Boot(String gatewayId,UUID bootId,long storageEpoch,String region,String cell,long renewalSequence,Instant leaseUntil,UUID operationId) {}
    public Boot start(Connection c,String gateway,UUID boot,String region,String cell,long storageEpoch,UUID operation)throws SQLException {
        try(var s=c.prepareStatement("INSERT INTO gateway_lease(gateway_id,boot_id,lease_until,renewal_sequence,last_operation_id,last_granted_expiry,storage_epoch,region,cell) VALUES(?,?,clock_timestamp()+interval '15 seconds',1,?,clock_timestamp()+interval '15 seconds',?,?,?) ON CONFLICT DO NOTHING")) {
            s.setString(1,gateway);s.setObject(2,boot);s.setObject(3,operation);s.setLong(4,storageEpoch);s.setString(5,region);s.setString(6,cell);s.executeUpdate();
        }
        Boot result=live(c,gateway,boot,cell,storageEpoch);if(!operation.equals(result.operationId()))throw new AuthoritySql.FencedException();return result;
    }
    public Boot renew(Connection c,Boot expected,long sequence,UUID operation)throws SQLException {
        Boot current=live(c,expected.gatewayId(),expected.bootId(),expected.cell(),expected.storageEpoch());
        if(sequence==current.renewalSequence()&&operation.equals(current.operationId()))return current;
        if(sequence!=Math.addExact(current.renewalSequence(),1))throw new AuthoritySql.FencedException();
        try(var s=c.prepareStatement("UPDATE gateway_lease SET renewal_sequence=?,last_operation_id=?,lease_until=clock_timestamp()+interval '15 seconds',last_granted_expiry=clock_timestamp()+interval '15 seconds' WHERE gateway_id=? AND boot_id=? AND renewal_sequence=? AND lease_until>clock_timestamp() AND expired_at IS NULL")){
            s.setLong(1,sequence);s.setObject(2,operation);s.setString(3,current.gatewayId());s.setObject(4,current.bootId());s.setLong(5,current.renewalSequence());if(s.executeUpdate()!=1)throw new AuthoritySql.RetryableConflict();
        }
        return live(c,current.gatewayId(),current.bootId(),current.cell(),current.storageEpoch());
    }
    public Boot live(Connection c,String gateway,UUID boot,String cell,long storageEpoch)throws SQLException {
        try(var s=c.prepareStatement("SELECT g.storage_epoch,g.region,g.cell,g.renewal_sequence,g.lease_until,g.last_operation_id,g.lease_until>clock_timestamp() AND g.expired_at IS NULL FROM gateway_lease g JOIN cell_authority a ON a.singleton_id=1 AND a.cell_id=g.cell AND a.storage_epoch=g.storage_epoch AND a.status='ACTIVE' WHERE g.gateway_id=? AND g.boot_id=?")){
            s.setString(1,gateway);s.setObject(2,boot);try(var r=s.executeQuery()){
                if(!r.next()||r.getLong(1)!=storageEpoch||!cell.equals(r.getString(3))||!r.getBoolean(7))throw new AuthoritySql.FencedException();
                return new Boot(gateway,boot,r.getLong(1),r.getString(2),r.getString(3),r.getLong(4),r.getTimestamp(5).toInstant(),r.getObject(6,UUID.class));
            }
        }
    }
}
