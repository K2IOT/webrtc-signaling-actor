package io.webrtc.signaling.actors.cluster;
import com.typesafe.config.*;
import io.webrtc.signaling.actors.lease.PostgresShardLeaseProvider;
import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.Adapter;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.sharding.typed.*;
import org.apache.pekko.cluster.sharding.typed.javadsl.*;
import org.apache.pekko.coordination.lease.LeaseUsageSettings;
import org.apache.pekko.http.javadsl.HttpsConnectionContext;
import org.apache.pekko.management.javadsl.PekkoManagement;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;
public final class ShardingBootstrap {
    public static final EntityTypeKey<UserMessage> USER_TYPE=EntityTypeKey.create(UserMessage.class,"SignalingUserV1");
    public static final EntityTypeKey<CallMessage> CALL_TYPE=EntityTypeKey.create(CallMessage.class,"SignalingCallV1");
    public record Regions(ActorRef<ShardingEnvelope<UserMessage>> users,ActorRef<ShardingEnvelope<CallMessage>> calls) {}
    private ShardingBootstrap() {}
    public static Config baseConfig(){return ConfigFactory.parseResources("application-actor.conf").withFallback(ConfigFactory.load());}
    public static ClusterShardingSettings userSettings(Config config){var settings=ClusterShardingSettings.fromConfig(config.getConfig("pekko.cluster.sharding"));if(!settings.stateStoreMode().name().equals("ddata"))throw new IllegalArgumentException("DData state store is required");if(!settings.leaseSettings().isEmpty())throw new IllegalArgumentException("Global/UserActor lease is forbidden");return settings.withRole("signaling-actor").withRememberEntities(false);}
    public static ClusterShardingSettings callSettings(Config config){return userSettings(config).withNoPassivationStrategy().withLeaseSettings(new LeaseUsageSettings("signaling.postgres-lease",scala.concurrent.duration.Duration.fromNanos(Duration.ofSeconds(1).toNanos())));}
    public static Regions registerBoth(ActorSystem<?> system,org.apache.pekko.japi.function.Function<EntityContext<UserMessage>,Behavior<UserMessage>> users,UserMessage stopUser,org.apache.pekko.japi.function.Function<EntityContext<CallMessage>,Behavior<CallMessage>> calls,CallMessage stopCall,ClusterReadiness readiness){
        var classic=Adapter.toClassic(system);var cluster=org.apache.pekko.cluster.Cluster.get(classic);
        if(!cluster.selfMember().status().equals(MemberStatus.up())||!cluster.selfMember().hasRole("signaling-actor"))throw new IllegalStateException("Local actor membership must be Up before region registration");
        PostgresShardLeaseProvider.requireInstalled(classic);
        var config=system.settings().config();var sharding=ClusterSharding.get(system);
        var props=Props.empty().withDispatcherFromConfig("signaling.actor-dispatcher").withMailboxFromConfig("signaling.control-mailbox");
        var allocation=org.apache.pekko.cluster.sharding.ShardCoordinator.leastShardAllocationStrategy(2,0.01);
        var userRegion=sharding.init(Entity.of(USER_TYPE,users).withSettings(userSettings(config)).withEntityProps(props).withStopMessage(stopUser).withMessageExtractor(new UserShardExtractor<UserMessage>()).withAllocationStrategy(allocation));
        var callRegion=sharding.init(Entity.of(CALL_TYPE,entity->org.apache.pekko.actor.typed.javadsl.Behaviors.setup(context->{
            int group=io.webrtc.signaling.storage.HomeParticipationService.group(new io.webrtc.signaling.protocol.Identity.CallId(entity.getEntityId()));
            PostgresShardLeaseProvider.bindPlacement(classic,group,Adapter.toClassic(context).parent());return calls.apply(entity);
        })).withSettings(callSettings(config)).withEntityProps(props).withStopMessage(stopCall).withMessageExtractor(new CallShardExtractor<CallMessage>()).withAllocationStrategy(allocation));
        readiness.registered();return new Regions(userRegion,callRegion);
    }
    public static void validateProduction(Config config){
        String cell=config.getString("signaling.cell-id"),host=config.getString("pekko.remote.artery.canonical.hostname"),namespace=config.getString("pekko.discovery.kubernetes-api.pod-namespace");
        String selector="app=webrtc-signaling,plane=actor,cell="+cell;
        if(!cell.matches("[a-z][a-z0-9-]{0,23}")||!config.getString("signaling.cluster-fingerprint").matches("[a-f0-9]{64}")||host.isBlank()||Set.of("0.0.0.0","::","localhost").contains(host)||!namespace.matches("[a-z0-9][a-z0-9-]{0,62}")||!selector.equals(config.getString("pekko.discovery.kubernetes-api.pod-label-selector"))||!config.getString("pekko.management.cluster.bootstrap.contact-point-discovery.service-name").equals("signaling-"+cell))throw new IllegalArgumentException("Invalid cell-scoped discovery/member identity");
        if(config.getBoolean("pekko.actor.allow-java-serialization")||!config.getString("pekko.remote.artery.transport").equals("tls-tcp")||config.getInt("pekko.cluster.sharding.number-of-shards")!=1024||config.getInt("signaling.ownership-hash-version")!=1||config.getInt("signaling.user-hash-version")!=1||config.getBoolean("pekko.cluster.sharding.remember-entities")||!config.getString("pekko.cluster.sharding.state-store-mode").equals("ddata"))throw new IllegalArgumentException("Incompatible serializer/sharding contract");
        var tls=config.getConfig("pekko.remote.artery.ssl.config-ssl-engine");
        if(!tls.getBoolean("require-mutual-authentication")||!tls.getString("protocol").equals("TLSv1.3")
                ||!config.getString("pekko.remote.artery.ssl.ssl-engine-provider").equals("org.apache.pekko.remote.artery.tcp.ConfigSSLEngineProvider"))
            throw new IllegalArgumentException("Remoting requires mutual TLS 1.3");
        if(!config.getString("pekko.discovery.method").equals("kubernetes-api")||!config.getStringList("pekko.cluster.seed-nodes").isEmpty()||config.getInt("pekko.cluster.role.signaling-actor.min-nr-of-members")!=4||config.getBoolean("pekko.cluster.allow-weakly-up-members"))throw new IllegalArgumentException("Invalid formation/readiness policy");
        var discovery=config.getConfig("pekko.management.cluster.bootstrap.contact-point-discovery");
        if(!config.getString("pekko.discovery.kubernetes-api.class").equals("org.apache.pekko.discovery.kubernetes.KubernetesApiServiceDiscovery")
                ||!discovery.getString("discovery-method").equals("pekko.discovery")
                ||!discovery.getString("port-name").equals("management")||discovery.getInt("required-contact-point-nr")!=6
                ||!config.getBoolean("pekko.cluster.configuration-compatibility-check.enforce-on-join"))
            throw new IllegalArgumentException("Native Kubernetes discovery and compatibility enforcement are required");
        if(config.getBoolean("pekko.management.cluster.bootstrap.new-cluster-enabled")&&(!config.hasPath("signaling.controlled-initial-formation")||!config.getBoolean("signaling.controlled-initial-formation")))throw new IllegalArgumentException("Unapproved automatic cluster formation");
        for(String setting:List.of("shutdown-flush-timeout","shutdown-streams-timeout")){
            var timeout=config.getDuration("pekko.remote.artery.advanced."+setting);
            if(timeout.isZero()||timeout.isNegative()||timeout.compareTo(java.time.Duration.ofSeconds(1))>0)
                throw new IllegalArgumentException("Native remoting retirement must preserve the original termination phase");
        }
        userSettings(config);callSettings(config);
    }
    /** Kubernetes discovery is the only production formation path; contexts are required PKI inputs. */
    public static CompletionStage<org.apache.pekko.http.javadsl.model.Uri> startManagement(ActorSystem<?> system,HttpsConnectionContext serverTls,HttpsConnectionContext clientTls){
        return bindManagement(system,serverTls,clientTls)
            .thenApply(uri->{org.apache.pekko.management.cluster.bootstrap.ClusterBootstrap.get(system).start();return uri;});
    }
    /** Preserve the original binding receipt independently of subsequent bootstrap failure. */
    public static CompletionStage<org.apache.pekko.http.javadsl.model.Uri> bindManagement(ActorSystem<?> system,HttpsConnectionContext serverTls,HttpsConnectionContext clientTls){
        validateProduction(system.settings().config());Objects.requireNonNull(serverTls);Objects.requireNonNull(clientTls);
        org.apache.pekko.http.javadsl.Http.get(system).setDefaultClientHttpsContext(clientTls);
        return PekkoManagement.get(system).start(settings->settings.withHttpsConnectionContext(serverTls).withReadOnly(true));
    }
}
