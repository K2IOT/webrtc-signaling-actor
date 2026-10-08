package io.webrtc.signaling.actors.cluster;
import static org.assertj.core.api.Assertions.*;
import com.typesafe.config.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
class ShardingConfigurationTest {
    static Config config(){return ConfigFactory.parseString("""
        signaling.cell-id="c001"
        signaling.cluster-fingerprint="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        pekko.remote.artery.canonical.hostname="10.0.1.5"
        pekko.management.http.hostname="10.0.1.5"
        pekko.discovery.kubernetes-api.pod-namespace="signaling-c001"
        pekko.discovery.kubernetes-api.pod-label-selector="app=webrtc-signaling,plane=actor,cell=c001"
        pekko.management.cluster.bootstrap.contact-point-discovery.service-name="signaling-c001"
        """).withFallback(ShardingBootstrap.baseConfig()).resolve();}
    @Test void effectiveSettingsKeepUserUnleasedAndCallLeaseBoundToItsType(){
        var config=config();var user=ShardingBootstrap.userSettings(config);var call=ShardingBootstrap.callSettings(config);
        assertThat(org.apache.pekko.cluster.sharding.typed.ClusterShardingSettings$.MODULE$.toClassicSettings(user).stateStoreMode()).isEqualTo("ddata");assertThat(org.apache.pekko.cluster.sharding.typed.ClusterShardingSettings$.MODULE$.toClassicSettings(call).stateStoreMode()).isEqualTo("ddata");
        assertThat(call.passivationStrategySettings().idleEntitySettings().isEmpty()).isTrue();
        assertThat(user.numberOfShards()).isEqualTo(1024);assertThat(call.numberOfShards()).isEqualTo(1024);assertThat(user.rememberEntities()).isFalse();assertThat(call.rememberEntities()).isFalse();
        assertThat(user.stateStoreMode().name()).isEqualTo("ddata");assertThat(user.leaseSettings().isEmpty()).isTrue();assertThat(call.leaseSettings().get().leaseImplementation()).isEqualTo("signaling.postgres-lease");
        assertThat(ShardingBootstrap.USER_TYPE.name()).isEqualTo("SignalingUserV1");assertThat(ShardingBootstrap.CALL_TYPE.name()).isEqualTo("SignalingCallV1");assertThat(user.role().get()).isEqualTo("signaling-actor");assertThat(config.getString("pekko.cluster.sharding.use-lease")).isEmpty();
    }
    @Test void isolationAndFormationRemainFailClosed(){
        var c=config();assertThat(c.getDuration("pekko.cluster.split-brain-resolver.stable-after")).isEqualTo(Duration.ofSeconds(10));assertThat(c.getBoolean("pekko.cluster.split-brain-resolver.down-all-when-unstable")).isTrue();assertThat(c.getDuration("pekko.cluster.down-removal-margin")).isEqualTo(Duration.ofSeconds(10));
        assertThat(c.getBoolean("pekko.actor.allow-java-serialization")).isFalse();assertThat(c.getString("pekko.remote.artery.transport")).isEqualTo("tls-tcp");assertThat(c.getBytes("pekko.remote.artery.advanced.maximum-frame-size")).isEqualTo(262144);
        assertThat(c.getInt("pekko.cluster.sharding.buffer-size")).isEqualTo(256);assertThat(c.getInt("signaling.control-mailbox.mailbox-capacity")).isEqualTo(128);assertThat(c.getInt("pekko.remote.artery.canonical.port")).isEqualTo(25520);assertThat(c.getInt("pekko.remote.artery.advanced.outbound-message-queue-size")).isEqualTo(256);
        assertThat(c.getString("pekko.discovery.method")).isEqualTo("kubernetes-api");assertThat(c.getString("pekko.discovery.kubernetes-api.pod-namespace")).isEqualTo("signaling-c001");assertThat(c.getBoolean("pekko.management.cluster.bootstrap.new-cluster-enabled")).isFalse();assertThat(c.getInt("pekko.management.cluster.bootstrap.contact-point-discovery.required-contact-point-nr")).isEqualTo(6);
        assertThat(c.getInt("pekko.cluster.role.signaling-actor.min-nr-of-members")).isEqualTo(4);assertThat(c.getString("pekko.cluster.allow-weakly-up-members")).isEqualTo("off");
        ShardingBootstrap.validateProduction(c);
        assertThatThrownBy(()->ShardingBootstrap.validateProduction(ConfigFactory.parseString("pekko.remote.artery.canonical.hostname=\"0.0.0.0\"").withFallback(c))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(()->ShardingBootstrap.validateProduction(ConfigFactory.parseString("pekko.discovery.kubernetes-api.pod-label-selector=\"app=webrtc-signaling\"").withFallback(c))).isInstanceOf(IllegalArgumentException.class);
    }
    @ParameterizedTest @ValueSource(strings={
        "pekko.remote.artery.ssl.config-ssl-engine.require-mutual-authentication=off",
        "pekko.remote.artery.ssl.config-ssl-engine.protocol=TLSv1.2",
        "pekko.remote.artery.ssl.ssl-engine-provider=invalid.Provider",
        "pekko.discovery.kubernetes-api.class=org.apache.pekko.discovery.config.ConfigServiceDiscovery",
        "pekko.management.cluster.bootstrap.contact-point-discovery.discovery-method=config",
        "pekko.management.cluster.bootstrap.contact-point-discovery.port-name=grpc",
        "pekko.management.cluster.bootstrap.contact-point-discovery.required-contact-point-nr=1",
        "pekko.cluster.configuration-compatibility-check.enforce-on-join=off"
    })
    void productionFormationCannotWeakenPinnedTransportOrDiscovery(String override){
        assertThatThrownBy(()->ShardingBootstrap.validateProduction(ConfigFactory.parseString(override).withFallback(config())))
            .isInstanceOf(IllegalArgumentException.class);
    }
    @Test void thePinnedNativeRemotingEngineReadsTheTls13Contract(){
        var tls=config().getConfig("pekko.remote.artery.ssl.config-ssl-engine");
        assertThat(tls.getString("protocol")).isEqualTo("TLSv1.3");
        assertThat(tls.getBoolean("require-mutual-authentication")).isTrue();
        assertThat(tls.getStringList("enabled-algorithms")).containsExactly("TLS_AES_128_GCM_SHA256","TLS_AES_256_GCM_SHA384");
    }
    @Test void nativeStreamRetirementFitsTheOriginalTerminationPhase(){
        var advanced=config().getConfig("pekko.remote.artery.advanced");
        assertThat(advanced.getDuration("shutdown-flush-timeout")).isEqualTo(Duration.ofSeconds(1));
        assertThat(advanced.getDuration("shutdown-streams-timeout")).isEqualTo(Duration.ofSeconds(1));
    }
    @ParameterizedTest @ValueSource(strings={
        "pekko.remote.artery.advanced.shutdown-flush-timeout=0s",
        "pekko.remote.artery.advanced.shutdown-flush-timeout=2s",
        "pekko.remote.artery.advanced.shutdown-streams-timeout=0s",
        "pekko.remote.artery.advanced.shutdown-streams-timeout=2s"
    })
    void nativeRetirementCannotConsumeOrDisableTheOriginalFiveSecondPhase(String override){
        assertThatThrownBy(()->ShardingBootstrap.validateProduction(ConfigFactory.parseString(override).withFallback(config())))
            .isInstanceOf(IllegalArgumentException.class);
    }
    @Test void canonicalHashVectorsAreSeparateFromDirectoryBuckets(){
        var users=new UserShardExtractor<UserMessage>();assertThat(users.shardId("alice")).isEqualTo("175");assertThat(users.shardId("bob")).isEqualTo("730");assertThat(users.shardId("é")).isEqualTo(users.shardId("e\u0301"));
        var calls=new CallShardExtractor<CallMessage>();assertThat(calls.shardId("c001.e1.00000000-0000-0000-0000-000000000001")).isEqualTo("685");assertThat(calls.shardId("c001.e1.ffffffff-ffff-ffff-ffff-ffffffffffff")).isEqualTo("567");
    }
    @Test void fingerprintCheckerRejectsMismatchAndIgnoresMemberAddress(){
        var checker=new FingerprintCompatibilityChecker();var c=config();assertThat(checker.check(c,c)).isEqualTo(org.apache.pekko.cluster.Valid.getInstance());
        assertThat(checker.check(c,ConfigFactory.parseString("signaling.cluster-fingerprint=\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"").withFallback(c))).isInstanceOf(org.apache.pekko.cluster.Invalid.class);
        assertThat(checker.check(c,ConfigFactory.parseString("pekko.remote.artery.canonical.hostname=\"10.0.2.6\"").withFallback(c))).isEqualTo(org.apache.pekko.cluster.Valid.getInstance());
    }
    @Test void localUpAloneDoesNotEnableBusinessReadinessOrInvalidateHeldSafetyWork(){
        var ready=new ClusterReadiness();ready.update(new ClusterReadiness.Snapshot(true,false,true,true,true,true,4,2,false));assertThat(ready.businessReady()).isFalse();
        ready.update(new ClusterReadiness.Snapshot(true,true,true,true,true,true,4,2,false));assertThat(ready.businessReady()).isTrue();assertThat(ready.safetyReady()).isTrue();
        ready.update(new ClusterReadiness.Snapshot(true,true,true,true,true,true,3,2,false));assertThat(ready.businessReady()).isFalse();assertThat(ready.safetyReady()).isTrue();
        ready.update(new ClusterReadiness.Snapshot(true,true,true,true,true,false,4,2,false));assertThat(ready.safetyReady()).isFalse();
    }
}
