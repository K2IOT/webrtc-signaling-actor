package io.webrtc.signaling.storage;
import io.webrtc.signaling.protocol.*;
import io.webrtc.signaling.protocol.Identity.*;
import java.sql.*;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
public final class CommandResultRepository {
    private static final ObjectMapper JSON=new ObjectMapper().findAndRegisterModules();
    public record Stored(int bucket,String hash,String status,CallId callId,CallCommandService.Outcome outcome) {}
    public Stored find(Connection c,SessionKey key,CommandScope scope,RequestId request)throws Exception {
        try(var s=c.prepareStatement("SELECT authority_bucket_id,payload_hash,status,call_id,result FROM command_result WHERE issuer=? AND jti=? AND command_scope=? AND request_id=?")){
            s.setString(1,key.issuer());s.setString(2,key.jti());s.setString(3,scope.value());s.setObject(4,request.value());try(var r=s.executeQuery()){if(!r.next())return null;String call=r.getString(4),result=r.getString(5);return new Stored(r.getInt(1),HexFormat.of().formatHex(r.getBytes(2)),r.getString(3),call==null?null:new CallId(call),result==null?null:JSON.readValue(result,CallCommandService.Outcome.class));}
        }
    }
    /** Upgrade only the original, freshly authorized pending ACCEPT under its existing native guard. */
    public void upgradePendingAccept(Connection c,CallCommand command,CallId call)throws SQLException {
        try(var q=c.prepareStatement("UPDATE command_result SET command_type='ACCEPT' WHERE issuer=? AND jti=? AND command_scope=? AND request_id=? AND payload_hash=? AND call_id=? AND status='PENDING' AND command_type IS NULL")){
            q.setString(1,command.sender().key().issuer());q.setString(2,command.sender().key().jti());q.setString(3,command.scope().value());q.setObject(4,command.requestId().value());q.setBytes(5,HexFormat.of().parseHex(command.intentHash()));q.setString(6,call.value());q.executeUpdate();
        }
    }
    public boolean insertPending(Connection c,CallCommand command,CallId call,int bucket)throws SQLException {
        if(!command.intentHash().matches("[0-9a-f]{64}"))throw new IllegalArgumentException("Invalid normalized command hash");
        try(var s=c.prepareStatement("INSERT INTO command_result(issuer,jti,command_scope,request_id,authority_bucket_id,payload_hash,status,call_id,command_type) VALUES(?,?,?,?,?,?,'PENDING',?,?) ON CONFLICT DO NOTHING")){
            s.setString(1,command.sender().key().issuer());s.setString(2,command.sender().key().jti());s.setString(3,command.scope().value());s.setObject(4,command.requestId().value());s.setInt(5,bucket);s.setBytes(6,HexFormat.of().parseHex(command.intentHash()));s.setString(7,call.value());s.setString(8,command.type().name());return s.executeUpdate()==1;
        }
    }
    public void finalizeResult(Connection c,SessionKey key,CommandScope scope,RequestId request,CallCommandService.Outcome outcome)throws Exception {
        String result=JSON.writeValueAsString(outcome);if(result.getBytes(java.nio.charset.StandardCharsets.UTF_8).length>8192)throw new IllegalArgumentException("Outcome exceeds bound");
        try(var s=c.prepareStatement("UPDATE command_result SET status='FINAL',result=?::jsonb,finalized_at=clock_timestamp(),expires_at=clock_timestamp()+interval '24 hours 5 seconds' WHERE issuer=? AND jti=? AND command_scope=? AND request_id=? AND status='PENDING'")){
            s.setString(1,result);s.setString(2,key.issuer());s.setString(3,key.jti());s.setString(4,scope.value());s.setObject(5,request.value());if(s.executeUpdate()!=1)throw new AuthoritySql.RetryableConflict();
        }
    }
    public static final class IntentConflict extends RuntimeException {public IntentConflict(){super("Scoped idempotency intent conflict");}}
}
