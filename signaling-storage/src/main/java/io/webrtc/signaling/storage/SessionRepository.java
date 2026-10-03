package io.webrtc.signaling.storage;
import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.Identity.*;
import java.sql.*;
import java.time.Instant;
import java.util.*;
public final class SessionRepository {
    public record Route(UserId user,SessionKey key,SessionIncarnation incarnation,long connectionGeneration,String gatewayId,UUID bootId,UUID connectionId,Instant tokenExpiresAt,String signingKeyId,long securityEpoch) {}
    private static final String FIELDS="user_id,session_incarnation,connection_generation,gateway_id,boot_id,connection_id,token_exp,signing_key_id,security_epoch";
    private final GatewayLeaseRepository gateways=new GatewayLeaseRepository();
    public Route register(Connection c,AuthPrincipal principal,GatewayLeaseRepository.Boot boot,UUID connection)throws SQLException {
        gateways.live(c,boot.gatewayId(),boot.bootId(),boot.cell(),boot.storageEpoch());
        expireStale(c,principal.userId());
        Route previous=find(c,principal.key());
        if(previous!=null&&!previous.user().equals(principal.userId()))throw new BindingRejected();
        if(previous!=null&&connection.equals(previous.connectionId())&&boot.bootId().equals(previous.bootId()))return refresh(c,previous,principal);
        if(liveRoutes(c,principal.userId()).size()>=5&&(previous==null||!isLive(c,principal.key())))throw new SessionLimit();
        long generation=previous==null?1:Math.addExact(previous.connectionGeneration(),1);
        UUID incarnation=previous==null?UUID.randomUUID():previous.incarnation().value();
        if(previous==null)try(var s=c.prepareStatement("INSERT INTO session_registry(issuer,jti,user_id,session_incarnation,connection_generation,gateway_id,boot_id,connection_id,token_exp,signing_key_id,security_epoch,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,clock_timestamp()) ON CONFLICT DO NOTHING")) {
            bindRegistration(s,principal,boot,connection,generation,incarnation);if(s.executeUpdate()!=1)throw new AuthoritySql.RetryableConflict();
        }else try(var s=c.prepareStatement("UPDATE session_registry SET connection_generation=?,gateway_id=?,boot_id=?,connection_id=?,token_exp=?,signing_key_id=?,security_epoch=?,closed_at=NULL,updated_at=clock_timestamp() WHERE issuer=? AND jti=? AND user_id=? AND session_incarnation=? AND connection_generation=?")){
            s.setLong(1,generation);s.setString(2,boot.gatewayId());s.setObject(3,boot.bootId());s.setObject(4,connection);s.setTimestamp(5,Timestamp.from(principal.expiresAt()));s.setString(6,principal.signingKeyId());s.setLong(7,Math.max(previous.securityEpoch(),principal.securityEpoch()));
            s.setString(8,principal.key().issuer());s.setString(9,principal.key().jti());s.setString(10,principal.userId().value());s.setObject(11,incarnation);s.setLong(12,previous.connectionGeneration());if(s.executeUpdate()!=1)throw new AuthoritySql.RetryableConflict();
        }
        if(!isLive(c,principal.key()))throw new AuthoritySql.FencedException();return Objects.requireNonNull(find(c,principal.key()));
    }
    private static void bindRegistration(PreparedStatement s,AuthPrincipal p,GatewayLeaseRepository.Boot b,UUID connection,long generation,UUID incarnation)throws SQLException {
        s.setString(1,p.key().issuer());s.setString(2,p.key().jti());s.setString(3,p.userId().value());s.setObject(4,incarnation);s.setLong(5,generation);s.setString(6,b.gatewayId());s.setObject(7,b.bootId());s.setObject(8,connection);s.setTimestamp(9,Timestamp.from(p.expiresAt()));s.setString(10,p.signingKeyId());s.setLong(11,p.securityEpoch());
    }
    public Route refresh(Connection c,Route expected,AuthPrincipal principal)throws SQLException {
        if(!expected.user().equals(principal.userId())||!expected.key().equals(principal.key()))throw new BindingRejected();
        Route current=requireCurrent(c,expected);
        if(!principal.expiresAt().isAfter(current.tokenExpiresAt()))return current;
        if(principal.securityEpoch()<current.securityEpoch())throw new AuthoritySql.FencedException();
        try(var s=c.prepareStatement("UPDATE session_registry SET token_exp=?,signing_key_id=?,security_epoch=?,updated_at=clock_timestamp() WHERE issuer=? AND jti=? AND session_incarnation=? AND connection_generation=? AND connection_id=? AND closed_at IS NULL")){
            s.setTimestamp(1,Timestamp.from(principal.expiresAt()));s.setString(2,principal.signingKeyId());s.setLong(3,principal.securityEpoch());s.setString(4,expected.key().issuer());s.setString(5,expected.key().jti());s.setObject(6,expected.incarnation().value());s.setLong(7,expected.connectionGeneration());s.setObject(8,expected.connectionId());if(s.executeUpdate()!=1)throw new AuthoritySql.FencedException();
        }return Objects.requireNonNull(find(c,expected.key()));
    }
    public boolean close(Connection c,Route expected)throws SQLException {
        try(var s=c.prepareStatement("UPDATE session_registry SET closed_at=clock_timestamp(),updated_at=clock_timestamp() WHERE issuer=? AND jti=? AND user_id=? AND session_incarnation=? AND connection_generation=? AND connection_id=? AND closed_at IS NULL")){
            s.setString(1,expected.key().issuer());s.setString(2,expected.key().jti());s.setString(3,expected.user().value());s.setObject(4,expected.incarnation().value());s.setLong(5,expected.connectionGeneration());s.setObject(6,expected.connectionId());return s.executeUpdate()==1;
        }
    }
    public Route requireCurrent(Connection c,Route expected)throws SQLException {
        Route current=find(c,expected.key());if(current==null||!current.equals(expected)||!isLive(c,expected.key()))throw new AuthoritySql.FencedException();return current;
    }
    public Route find(Connection c,SessionKey key)throws SQLException {
        try(var s=c.prepareStatement("SELECT "+FIELDS+" FROM session_registry WHERE issuer=? AND jti=?")){
            s.setString(1,key.issuer());s.setString(2,key.jti());try(var r=s.executeQuery()){return r.next()?route(r,key):null;}
        }
    }
    public List<Route> liveRoutes(Connection c,UserId user)throws SQLException {
        var result=new ArrayList<Route>();
        try(var s=c.prepareStatement("SELECT s.issuer,s.jti,s.user_id,s.session_incarnation,s.connection_generation,s.gateway_id,s.boot_id,s.connection_id,s.token_exp,s.signing_key_id,s.security_epoch FROM session_registry s JOIN gateway_lease g ON g.gateway_id=s.gateway_id AND g.boot_id=s.boot_id WHERE s.user_id=? AND s.closed_at IS NULL AND s.token_exp>clock_timestamp() AND g.lease_until>clock_timestamp() AND g.expired_at IS NULL ORDER BY s.issuer,s.jti LIMIT 6")){
            s.setString(1,user.value());try(var r=s.executeQuery()){while(r.next())result.add(new Route(user,new SessionKey(r.getString(1),r.getString(2)),new SessionIncarnation(r.getObject(4,UUID.class)),r.getLong(5),r.getString(6),r.getObject(7,UUID.class),r.getObject(8,UUID.class),r.getTimestamp(9).toInstant(),r.getString(10),r.getLong(11)));}
        }
        if(result.size()>5)throw new SessionLimit();return List.copyOf(result);
    }
    private boolean isLive(Connection c,SessionKey key)throws SQLException {
        try(var s=c.prepareStatement("SELECT s.token_exp>clock_timestamp() AND s.closed_at IS NULL AND g.lease_until>clock_timestamp() AND g.expired_at IS NULL FROM session_registry s JOIN gateway_lease g ON g.gateway_id=s.gateway_id AND g.boot_id=s.boot_id WHERE s.issuer=? AND s.jti=?")){
            s.setString(1,key.issuer());s.setString(2,key.jti());try(var r=s.executeQuery()){return r.next()&&r.getBoolean(1);}
        }
    }
    private void expireStale(Connection c,UserId user)throws SQLException {
        var stale=new ArrayList<SessionKey>();
        try(var s=c.prepareStatement("SELECT s.issuer,s.jti FROM session_registry s LEFT JOIN gateway_lease g ON g.gateway_id=s.gateway_id AND g.boot_id=s.boot_id WHERE s.user_id=? AND s.closed_at IS NULL AND (s.token_exp<=clock_timestamp() OR g.boot_id IS NULL OR g.lease_until<=clock_timestamp() OR g.expired_at IS NOT NULL) ORDER BY s.issuer,s.jti LIMIT 6")){
            s.setString(1,user.value());try(var r=s.executeQuery()){while(r.next())stale.add(new SessionKey(r.getString(1),r.getString(2)));}
        }
        if(stale.size()>5)throw new SessionLimit();
        for(SessionKey key:stale)try(var s=c.prepareStatement("UPDATE session_registry SET closed_at=clock_timestamp(),updated_at=clock_timestamp() WHERE issuer=? AND jti=? AND closed_at IS NULL")){s.setString(1,key.issuer());s.setString(2,key.jti());s.executeUpdate();}
    }
    private static Route route(ResultSet r,SessionKey key)throws SQLException {
        return new Route(new UserId(r.getString(1)),key,new SessionIncarnation(r.getObject(2,UUID.class)),r.getLong(3),r.getString(4),r.getObject(5,UUID.class),r.getObject(6,UUID.class),r.getTimestamp(7).toInstant(),r.getString(8),r.getLong(9));
    }
    public static final class BindingRejected extends RuntimeException {public BindingRejected(){super("Session binding rejected");}}
    public static final class SessionLimit extends RuntimeException {public SessionLimit(){super("Session limit reached");}}
}
