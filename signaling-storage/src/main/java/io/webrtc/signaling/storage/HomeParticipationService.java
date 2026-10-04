package io.webrtc.signaling.storage;
import io.webrtc.signaling.protocol.Identity.*;
import java.sql.*;
import java.time.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CompletionStage;
public final class HomeParticipationService {
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON=new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();
    public enum Phase { PREPARING,RINGING }
    public record Grant(String cell,long storageEpoch,long hashVersion,int group,long groupEpoch,long sequence,UUID operation,Instant issuedAt,Instant expiresAt,String proof,long authorizedCallVersion) {
        public Grant(String cell,long storageEpoch,long hashVersion,int group,long groupEpoch,long sequence,UUID operation,Instant issuedAt,Instant expiresAt,String proof){this(cell,storageEpoch,hashVersion,group,groupEpoch,sequence,operation,issuedAt,expiresAt,proof,0);}
        public Grant {if(authorizedCallVersion<0||cell==null||storageEpoch<1||hashVersion<1||group<0||group>1023||groupEpoch<1||sequence<1||proof==null||proof.length()>4096)throw new IllegalArgumentException("Invalid home grant");Objects.requireNonNull(operation);Objects.requireNonNull(issuedAt);Objects.requireNonNull(expiresAt);}
    }
    public record Request(UserId user,CallId call,UUID acquireOperation,String payloadHash,long directoryEpoch,Phase phase,Grant grant) {
        public Request {Objects.requireNonNull(user);Objects.requireNonNull(call);Objects.requireNonNull(acquireOperation);Objects.requireNonNull(phase);Objects.requireNonNull(grant);if(directoryEpoch<1||payloadHash==null||!payloadHash.matches("[0-9a-f]{64}"))throw new IllegalArgumentException("Invalid home intent");}
    }
    public record AuthorizationIntent(String action,UUID reservation,long version,UUID activation,long callVersion,Winner winner,SessionRepository.Route route){public AuthorizationIntent{if(!Set.of("RESERVE","RENEW","RELEASE","CLAIM","CONFIRM","QUERY").contains(action)||version<0||callVersion<0)throw new IllegalArgumentException("Invalid home action");}public static AuthorizationIntent reserve(){return new AuthorizationIntent("RESERVE",null,0,null,0,null,null);}}
    public static String authorizationHash(Request request,AuthorizationIntent action){try{var body=new TreeMap<String,Object>();body.put("acquisition",request.acquireOperation());body.put("user",request.user());body.put("call",request.call());body.put("hash",request.payloadHash());body.put("phase",request.phase());body.put("directoryEpoch",request.directoryEpoch());body.put("action",action);byte[] encoded=JSON.writeValueAsBytes(body);return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(encoded));}catch(Exception invalid){throw new IllegalArgumentException("Invalid home authorization intent",invalid);}}
    @FunctionalInterface public interface GrantVerifier { boolean verify(Request request);default boolean verify(Request request,AuthorizationIntent action){return verify(request);} }
    public record Winner(SessionKey key,SessionIncarnation incarnation,long generation) {}
    public record Participation(CallId call,UserId user,UUID acquireOperation,String payloadHash,String phase,UUID reservationId,long version,Instant leaseUntil,Winner winner,long highestGroupEpoch) {public boolean terminal(){return phase.equals("RELEASED")||phase.equals("EXPIRED");}}
    /** Cached authenticated promotion/migration approval; must never perform network I/O under SQL guards. */
    @FunctionalInterface public interface EpochAdoptionVerifier {boolean permits(String coordinatorCell,long previousEpoch,long nextEpoch);}
    private final EpochAdoptionVerifier epochAdoption;
    private final SqlTransactions sql;private final String cell;private final long epoch;private final GrantVerifier verifier;
    public HomeParticipationService(SqlTransactions sql,String cell,long epoch,GrantVerifier verifier){this(sql,cell,epoch,verifier,(source,previous,next)->false);}
    public HomeParticipationService(SqlTransactions sql,String cell,long epoch,GrantVerifier verifier,EpochAdoptionVerifier epochAdoption){this.sql=Objects.requireNonNull(sql);this.cell=cell;this.epoch=epoch;this.verifier=Objects.requireNonNull(verifier);this.epochAdoption=Objects.requireNonNull(epochAdoption);}
    <T> CompletionStage<T> submit(Request r,DbClass clazz,SqlTransactions.Work<T> work){return submitTracked(r,clazz,Duration.ofSeconds(2),work).logical();}
    <T> DbOperation<T> submitTracked(Request r,DbClass clazz,Duration budget,SqlTransactions.Work<T> work){return submitTracked(r,clazz,budget,new AuthorizationIntent("QUERY",null,0,null,0,null,null),work);}
    <T> DbOperation<T> submitTracked(Request r,DbClass clazz,Duration budget,AuthorizationIntent action,SqlTransactions.Work<T> work){return sql.submitTracked(clazz,budget,c->{guard(c,List.of(r),action);return work.apply(c);});}
    <T> CompletionStage<T> pair(Request a,Request b,SqlTransactions.Work<T> work){return sql.submit(DbClass.CRITICAL,Duration.ofSeconds(2),c->{if(!a.call().equals(b.call())||a.user().equals(b.user()))throw new IllegalArgumentException("Invalid local participant pair");guard(c,List.of(a,b),AuthorizationIntent.reserve());return work.apply(c);});}
    private void guard(Connection c,List<Request> requests,AuthorizationIntent action)throws SQLException {
        var buckets=new TreeMap<Integer,Long>();for(Request r:requests){int bucket=SessionRegistryService.bucket(r.user());Long previous=buckets.put(bucket,r.directoryEpoch());if(previous!=null&&previous!=r.directoryEpoch())throw new AuthoritySql.FencedException();}
        AuthoritySql.home(c,cell,epoch,buckets,requests.stream().map(r->r.user().value()).toList());
        for(Request r:requests)validateGrant(c,r,action);
    }
    private void validateGrant(Connection c,Request r,AuthorizationIntent action)throws SQLException {
        var g=r.grant();if(!verifier.verify(r,action)||!r.call().coordinatorCell().equals(g.cell())||g.hashVersion()!=1||g.group()!=group(r.call()))throw new AuthoritySql.FencedException();
        try(var s=c.prepareStatement("SELECT clock_timestamp()" );var result=s.executeQuery()){result.next();Instant now=result.getTimestamp(1).toInstant();
            if(!g.expiresAt().isAfter(now)||g.issuedAt().isAfter(now.plusSeconds(1))||g.issuedAt().isAfter(g.expiresAt())||Duration.between(g.issuedAt(),g.expiresAt()).compareTo(Duration.ofSeconds(5))>0)throw new AuthoritySql.FencedException();}
    }
    public CompletionStage<Participation> queryParticipation(Request r){return submit(r,DbClass.CRITICAL,c->find(c,r,true));}
    Participation find(Connection c,Request request)throws SQLException {return find(c,request,false);}
    Participation find(Connection c,Request request,boolean allowAdoption)throws SQLException {
        try(var s=c.prepareStatement("SELECT h.acquire_operation_id,h.payload_hash,h.phase,h.reservation_id,COALESCE(r.reservation_version,0),r.lease_until,h.winner_issuer,h.winner_jti,h.winner_incarnation,h.winner_generation,h.highest_group_epoch,h.coordinator_cell,h.coordinator_storage_epoch,h.ownership_hash_version,h.ownership_group_id,r.lease_until>clock_timestamp() FROM home_participation h LEFT JOIN user_reservation r ON r.call_id=h.call_id AND r.user_id=h.user_id AND r.reservation_id=h.reservation_id WHERE h.call_id=? AND h.user_id=?")){
            s.setString(1,request.call().value());s.setString(2,request.user().value());try(var r=s.executeQuery()){if(!r.next())return null;
                if(!request.acquireOperation().equals(r.getObject(1,UUID.class))||!request.payloadHash().equals(HexFormat.of().formatHex(r.getBytes(2)))||!request.grant().cell().equals(r.getString(12))||request.grant().hashVersion()!=r.getLong(14)||request.grant().group()!=r.getInt(15))throw new IntentConflict();
                long previous=r.getLong(13),next=request.grant().storageEpoch();
                if(next<previous||next>previous&&(!allowAdoption||!r.getBoolean(16)||!epochAdoption.permits(request.grant().cell(),previous,next)))throw new AuthoritySql.FencedException();
                Winner winner=r.getString(7)==null?null:new Winner(new SessionKey(r.getString(7),r.getString(8)),new SessionIncarnation(r.getObject(9,UUID.class)),r.getLong(10));
                Timestamp expiry=r.getTimestamp(6);return new Participation(request.call(),request.user(),request.acquireOperation(),request.payloadHash(),r.getString(3),r.getObject(4,UUID.class),r.getLong(5),expiry==null?null:expiry.toInstant(),winner,r.getLong(11));}
        }
    }
    void insert(Connection c,Request r,String phase,UUID reservation)throws SQLException {
        try(var s=c.prepareStatement("INSERT INTO home_participation(call_id,user_id,acquire_operation_id,payload_hash,reservation_id,coordinator_cell,coordinator_storage_epoch,ownership_hash_version,ownership_group_id,highest_group_epoch,phase,terminal_at,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,CASE WHEN ? THEN clock_timestamp() END,CASE WHEN ? THEN clock_timestamp()+interval '24 hours 5 seconds' END) ON CONFLICT DO NOTHING")){
            s.setString(1,r.call().value());s.setString(2,r.user().value());s.setObject(3,r.acquireOperation());s.setBytes(4,HexFormat.of().parseHex(r.payloadHash()));s.setObject(5,reservation);s.setString(6,r.grant().cell());s.setLong(7,r.grant().storageEpoch());s.setLong(8,r.grant().hashVersion());s.setInt(9,r.grant().group());s.setLong(10,r.grant().groupEpoch());s.setString(11,phase);boolean terminal=phase.equals("RELEASED")||phase.equals("EXPIRED");s.setBoolean(12,terminal);s.setBoolean(13,terminal);s.executeUpdate();
        }
        // Subsequent statement handles invisible ON CONFLICT winners at READ COMMITTED.
        if(find(c,r)==null)throw new AuthoritySql.RetryableConflict();
    }
    void terminalize(Connection c,String call,String user,String phase)throws SQLException {
        try(var s=c.prepareStatement("UPDATE home_participation SET phase=?,terminal_at=clock_timestamp(),expires_at=clock_timestamp()+interval '24 hours 5 seconds' WHERE call_id=? AND user_id=? AND phase NOT IN ('RELEASED','EXPIRED')")){s.setString(1,phase);s.setString(2,call);s.setString(3,user);s.executeUpdate();}
    }
    public static int group(CallId call){try{byte[] digest=MessageDigest.getInstance("SHA-256").digest(call.value().getBytes(StandardCharsets.UTF_8));return (digest[6]&3)<<8|(digest[7]&255);}catch(NoSuchAlgorithmException e){throw new IllegalStateException(e);}}
    public static final class IntentConflict extends RuntimeException {public IntentConflict(){super("Participation intent conflict");}}
}
