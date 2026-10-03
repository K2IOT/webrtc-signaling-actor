package io.webrtc.signaling.storage;
import io.webrtc.signaling.auth.AuthPrincipal;
import io.webrtc.signaling.protocol.Identity.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
public final class SessionRegistryService {
    public static final Duration GATEWAY_PULSE_INTERVAL=Duration.ofSeconds(5);
    private final SqlTransactions sql;private final String cell;private final long storageEpoch;
    private final SessionRepository sessions=new SessionRepository();private final GatewayLeaseRepository gateways=new GatewayLeaseRepository();
    public SessionRegistryService(SqlTransactions sql,String cell,long storageEpoch){this.sql=sql;this.cell=cell;this.storageEpoch=storageEpoch;}
    public CompletionStage<GatewayLeaseRepository.Boot> startGatewayBoot(String gateway,UUID boot,String region,UUID operation){return sql.submit(DbClass.RENEWAL,Duration.ofSeconds(2),c->{AuthoritySql.cellBarrier(c,false);AuthoritySql.validateCell(c,cell,storageEpoch);return gateways.start(c,gateway,boot,region,cell,storageEpoch,operation);});}
    public CompletionStage<GatewayLeaseRepository.Boot> renewGatewayBoot(GatewayLeaseRepository.Boot boot,long sequence,UUID operation){return sql.submit(DbClass.RENEWAL,Duration.ofSeconds(2),c->{AuthoritySql.cellBarrier(c,false);AuthoritySql.validateCell(c,cell,storageEpoch);validateBoot(boot);return gateways.renew(c,boot,sequence,operation);});}
    public CompletionStage<List<GatewayLeaseRepository.Boot>> renewGatewayBootBatch(List<GatewayLeaseRepository.Renewal> renewals){
        if(renewals.isEmpty()||renewals.size()>128)throw new IllegalArgumentException("Invalid gateway pulse batch");
        var sorted=renewals.stream().sorted(Comparator.comparing((GatewayLeaseRepository.Renewal r)->r.boot().gatewayId()).thenComparing(r->r.boot().bootId())).toList();
        if(sorted.stream().map(r->r.boot().gatewayId()+":"+r.boot().bootId()).distinct().count()!=sorted.size())throw new IllegalArgumentException("Duplicate boot pulse");
        return sql.submit(DbClass.RENEWAL,Duration.ofSeconds(2),c->{AuthoritySql.cellBarrier(c,false);AuthoritySql.validateCell(c,cell,storageEpoch);var result=new ArrayList<GatewayLeaseRepository.Boot>();for(var renewal:sorted){validateBoot(renewal.boot());result.add(gateways.renew(c,renewal.boot(),renewal.sequence(),renewal.operation()));}return List.copyOf(result);});
    }
    public CompletionStage<SessionRepository.Route> registerSession(AuthPrincipal principal,GatewayLeaseRepository.Boot boot,UUID connection,long directoryEpoch){return registerSessionTracked(principal,boot,connection,directoryEpoch,Duration.ofSeconds(2)).logical();}
    public DbOperation<SessionRepository.Route> registerSessionTracked(AuthPrincipal principal,GatewayLeaseRepository.Boot boot,UUID connection,long directoryEpoch,Duration budget){return sql.submitTracked(DbClass.CRITICAL,budget,c->{guard(c,principal.userId(),directoryEpoch);validateBoot(boot);return sessions.register(c,principal,boot,connection);});}
    public CompletionStage<SessionRepository.Route> refreshSession(SessionRepository.Route route,AuthPrincipal principal,long directoryEpoch){return refreshSessionTracked(route,principal,directoryEpoch,Duration.ofSeconds(2)).logical();}
    public DbOperation<SessionRepository.Route> refreshSessionTracked(SessionRepository.Route route,AuthPrincipal principal,long directoryEpoch,Duration budget){return sql.submitTracked(DbClass.CRITICAL,budget,c->{guard(c,route.user(),directoryEpoch);return sessions.refresh(c,route,principal);});}
    public CompletionStage<Boolean> closeSessionIfGeneration(SessionRepository.Route route,long directoryEpoch){return closeSessionIfGenerationTracked(route,directoryEpoch,Duration.ofSeconds(2)).logical();}
    public DbOperation<Boolean> closeSessionIfGenerationTracked(SessionRepository.Route route,long directoryEpoch,Duration budget){return sql.submitTracked(DbClass.CRITICAL,budget,c->{guard(c,route.user(),directoryEpoch);return sessions.close(c,route);});}
    public CompletionStage<List<SessionRepository.Route>> lookupLiveRoutes(UserId user,long directoryEpoch){return sql.submit(DbClass.CRITICAL,Duration.ofSeconds(2),c->{guard(c,user,directoryEpoch);return sessions.liveRoutes(c,user);});}
    private void validateBoot(GatewayLeaseRepository.Boot boot){if(!cell.equals(boot.cell())||storageEpoch!=boot.storageEpoch())throw new AuthoritySql.FencedException();}
    private void guard(java.sql.Connection c,UserId user,long directoryEpoch)throws java.sql.SQLException {AuthoritySql.home(c,cell,storageEpoch,Map.of(bucket(user),directoryEpoch),List.of(user.value()));}
    public static int bucket(UserId user){try{byte[] digest=MessageDigest.getInstance("SHA-256").digest(user.value().getBytes(StandardCharsets.UTF_8));return (digest[6]&63)<<8|(digest[7]&255);}catch(NoSuchAlgorithmException e){throw new IllegalStateException(e);}}
}
