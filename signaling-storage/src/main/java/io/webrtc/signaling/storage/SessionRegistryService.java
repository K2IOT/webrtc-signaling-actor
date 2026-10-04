package io.webrtc.signaling.storage;
import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.Identity.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
public final class SessionRegistryService {
    @FunctionalInterface public interface NativeSecurityPolicy {boolean allowed(java.sql.Connection connection,AuthPrincipal principal)throws java.sql.SQLException;}
    public record SessionProofView(SessionRepository.Route route,java.time.Instant checkedAt,java.time.Instant proofUntil,String sourceCell,long sourceStorageEpoch,long directoryEpoch) {}
    public record BootGrant(GatewayLeaseRepository.Boot boot,long remainingMillis) {}
    private final NativeSecurityPolicy security;
    public boolean nativeSecurityConfigured(){return security!=null;}
    private void authorize(java.sql.Connection c,AuthPrincipal principal)throws java.sql.SQLException {if(security!=null&&!security.allowed(c,principal))throw new AuthoritySql.FencedException();}

    public static final Duration GATEWAY_PULSE_INTERVAL=Duration.ofSeconds(5);
    private final SqlTransactions sql;private final String cell;private final long storageEpoch;
    private final SessionRepository sessions=new SessionRepository();private final GatewayLeaseRepository gateways=new GatewayLeaseRepository();
    public SessionRegistryService(SqlTransactions sql,String cell,long storageEpoch){this(sql,cell,storageEpoch,null);}
    public SessionRegistryService(SqlTransactions sql,String cell,long storageEpoch,NativeSecurityPolicy security){this.sql=Objects.requireNonNull(sql);this.cell=cell;this.storageEpoch=storageEpoch;this.security=security;}
    public CompletionStage<GatewayLeaseRepository.Boot> startGatewayBoot(String gateway,UUID boot,String region,UUID operation){return startGatewayBootTracked(gateway,boot,region,operation,Duration.ofSeconds(2)).logical().thenApply(BootGrant::boot);}
    public DbOperation<BootGrant> startGatewayBootTracked(String gateway,UUID boot,String region,UUID operation,Duration budget){return sql.submitTracked(DbClass.RENEWAL,budget,c->{AuthoritySql.cellBarrier(c,false);AuthoritySql.validateCell(c,cell,storageEpoch);return grant(c,gateways.start(c,gateway,boot,region,cell,storageEpoch,operation));});}
    public CompletionStage<GatewayLeaseRepository.Boot> renewGatewayBoot(GatewayLeaseRepository.Boot boot,long sequence,UUID operation){try{validateBoot(boot);}catch(AuthoritySql.FencedException fenced){return java.util.concurrent.CompletableFuture.failedFuture(fenced);}return renewGatewayBootTracked(boot.gatewayId(),boot.bootId(),sequence,operation,Duration.ofSeconds(2)).logical().thenApply(BootGrant::boot);}
    public DbOperation<BootGrant> renewGatewayBootTracked(String gateway,UUID boot,long sequence,UUID operation,Duration budget){return sql.submitTracked(DbClass.RENEWAL,budget,c->{AuthoritySql.cellBarrier(c,false);AuthoritySql.validateCell(c,cell,storageEpoch);return grant(c,gateways.renew(c,gateways.live(c,gateway,boot,cell,storageEpoch),sequence,operation));});}
    private static BootGrant grant(java.sql.Connection c,GatewayLeaseRepository.Boot boot)throws java.sql.SQLException {try(var statement=c.prepareStatement("SELECT floor(extract(epoch FROM (?::timestamptz-clock_timestamp()))*1000)::bigint")){statement.setTimestamp(1,java.sql.Timestamp.from(boot.leaseUntil()));try(var result=statement.executeQuery()){result.next();long remaining=result.getLong(1);if(remaining<=0||remaining>15000)throw new AuthoritySql.FencedException();return new BootGrant(boot,remaining);}}}
    public CompletionStage<List<GatewayLeaseRepository.Boot>> renewGatewayBootBatch(List<GatewayLeaseRepository.Renewal> renewals){
        if(renewals.isEmpty()||renewals.size()>128)throw new IllegalArgumentException("Invalid gateway pulse batch");
        var sorted=renewals.stream().sorted(Comparator.comparing((GatewayLeaseRepository.Renewal r)->r.boot().gatewayId()).thenComparing(r->r.boot().bootId())).toList();
        if(sorted.stream().map(r->r.boot().gatewayId()+":"+r.boot().bootId()).distinct().count()!=sorted.size())throw new IllegalArgumentException("Duplicate boot pulse");
        return sql.submit(DbClass.RENEWAL,Duration.ofSeconds(2),c->{AuthoritySql.cellBarrier(c,false);AuthoritySql.validateCell(c,cell,storageEpoch);var result=new ArrayList<GatewayLeaseRepository.Boot>();for(var renewal:sorted){validateBoot(renewal.boot());result.add(gateways.renew(c,renewal.boot(),renewal.sequence(),renewal.operation()));}return List.copyOf(result);});
    }
    public CompletionStage<SessionRepository.Route> registerSession(AuthPrincipal principal,GatewayLeaseRepository.Boot boot,UUID connection,long directoryEpoch){return registerSessionTracked(principal,boot,connection,directoryEpoch,Duration.ofSeconds(2)).logical();}
    public DbOperation<SessionRepository.Route> registerSessionTracked(AuthPrincipal principal,GatewayLeaseRepository.Boot boot,UUID connection,long directoryEpoch,Duration budget){return sql.submitTracked(DbClass.CRITICAL,budget,c->{guard(c,principal.userId(),directoryEpoch);authorize(c,principal);validateBoot(boot);return sessions.register(c,principal,boot,connection);});}
    public CompletionStage<SessionRepository.Route> refreshSession(SessionRepository.Route route,AuthPrincipal principal,long directoryEpoch){return refreshSessionTracked(route,principal,directoryEpoch,Duration.ofSeconds(2)).logical();}
    public DbOperation<SessionRepository.Route> refreshSessionTracked(SessionRepository.Route route,AuthPrincipal principal,long directoryEpoch,Duration budget){return sql.submitTracked(DbClass.CRITICAL,budget,c->{guard(c,route.user(),directoryEpoch);authorize(c,principal);return sessions.refresh(c,route,principal);});}
    public CompletionStage<Boolean> closeSessionIfGeneration(SessionRepository.Route route,long directoryEpoch){return closeSessionIfGenerationTracked(route,directoryEpoch,Duration.ofSeconds(2)).logical();}
    public DbOperation<Boolean> closeSessionIfGenerationTracked(SessionRepository.Route route,long directoryEpoch,Duration budget){return sql.submitTracked(DbClass.CRITICAL,budget,c->{guard(c,route.user(),directoryEpoch);return sessions.close(c,route);});}
    public CompletionStage<List<SessionRepository.Route>> lookupLiveRoutes(UserId user,long directoryEpoch){return sql.submit(DbClass.CRITICAL,Duration.ofSeconds(2),c->{guard(c,user,directoryEpoch);return sessions.liveRoutes(c,user);});}
    /** Auth-only native read: terminal/result recovery must not depend on a live call reservation. */
    public DbOperation<SessionProofView> readCurrentSessionTracked(SessionRepository.Route expected,AuthPrincipal principal,long directoryEpoch,Duration budget){
        if(security==null)throw new IllegalStateException("Native session security policy required");
        return sql.submitTracked(DbClass.CRITICAL,budget,c->{
            guard(c,expected.user(),directoryEpoch);authorize(c,principal);
            var route=sessions.find(c,expected.key());
            if(route==null||!route.user().equals(principal.userId())||!route.key().equals(principal.key())
                    ||!route.user().equals(expected.user())||!route.incarnation().equals(expected.incarnation())
                    ||route.connectionGeneration()!=expected.connectionGeneration()||!route.connectionId().equals(expected.connectionId())
                    ||!route.gatewayId().equals(expected.gatewayId())||!route.bootId().equals(expected.bootId()))throw new AuthoritySql.FencedException();
            sessions.requireCurrent(c,route);var boot=gateways.live(c,route.gatewayId(),route.bootId(),cell,storageEpoch);
            java.time.Instant now;try(var q=c.createStatement();var r=q.executeQuery("SELECT clock_timestamp()")){r.next();now=r.getTimestamp(1).toInstant();}
            java.time.Instant until=now.plusSeconds(5);for(var expiry:List.of(principal.expiresAt(),route.tokenExpiresAt(),boot.leaseUntil()))if(expiry.isBefore(until))until=expiry;
            if(!until.isAfter(now))throw new AuthoritySql.FencedException();return new SessionProofView(route,now,until,cell,storageEpoch,directoryEpoch);
        });
    }
    private void validateBoot(GatewayLeaseRepository.Boot boot){if(!cell.equals(boot.cell())||storageEpoch!=boot.storageEpoch())throw new AuthoritySql.FencedException();}
    private void guard(java.sql.Connection c,UserId user,long directoryEpoch)throws java.sql.SQLException {AuthoritySql.home(c,cell,storageEpoch,Map.of(bucket(user),directoryEpoch),List.of(user.value()));}
    public static int bucket(UserId user){try{byte[] digest=MessageDigest.getInstance("SHA-256").digest(user.value().getBytes(StandardCharsets.UTF_8));return (digest[6]&63)<<8|(digest[7]&255);}catch(NoSuchAlgorithmException e){throw new IllegalStateException(e);}}
}
