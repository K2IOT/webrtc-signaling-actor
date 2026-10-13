package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.rpc.CellRpcClient;
import io.webrtc.signaling.rpc.CellRpcServer;
import io.webrtc.signaling.rpc.GatewayRelayRpcClient;
import io.webrtc.signaling.rpc.GatewayRelayRpcServer;
import io.webrtc.signaling.storage.SqlTransactions;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;

/**
 * Explicit topology enables actual collectors on every process plane, before business readiness.
 */
@AutoConfiguration(before = NativeTelemetryConfiguration.class)
@ConditionalOnBean(NativeCompatibilityObservationEnrollment.class)
public class NativeCompatibilityObservationConfiguration {
    @Bean
    static BeanPostProcessor nativeCompatibilityObservationBindings(BeanFactory factory) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String name) {
                if (bean instanceof CellRpcServer server)
                    server.compatibilityFacts(
                            () -> factory.getBean(NativeCompatibilityObservationProducer.class).actorFacts());
                if (bean instanceof GatewayRelayRpcServer server) {
                    server.compatibilityFacts(
                            () -> factory.getBean(NativeCompatibilityObservationProducer.class).gatewayFacts());
                    factory
                            .getBean(NativeCompatibilityCollectors.class)
                            .localGateway(server::localCompatibilityFacts);
                }
                return bean;
            }
        };
    }

    @Bean
    NativeCompatibilityObservationProducer nativeCompatibilityObservationProducer(
            NativeCompatibilityObservationEnrollment inputs,
            ObjectProvider<NativeActorSchedulingEnrollment> actors,
            NativeCompatibility compatibility) {
        return new NativeCompatibilityObservationProducer(
                inputs, actors::getIfAvailable, compatibility);
    }

    @Bean
    NativeCompatibilityCollectors nativeCompatibilityCollectors(
            NativeCompatibilityObservationEnrollment inputs,
            ObjectProvider<CellRpcClient> cells,
            ObjectProvider<GatewayRelayRpcClient> gateways,
            ObjectProvider<NativeActorIngressEnrollment> ingress,
            ObjectProvider<NativeRollbackCollector> rollback) {
        return new NativeCompatibilityCollectors(
                inputs,
                cells::getIfAvailable,
                () -> {
                    var direct = gateways.getIfAvailable();
                    var actor = ingress.getIfAvailable();
                    return direct != null ? direct : actor == null ? null : actor.gateways();
                },
                () -> {
                    var collector = rollback.getIfAvailable();
                    return collector == null ? null : collector.observation();
                });
    }

    @Bean
    @ConditionalOnMissingBean(NativeCompatibilityEnrollment.class)
    NativeCompatibilityEnrollment nativeCompatibilityEnrollment(
            NativeCompatibilityCollectors collectors) {
        return collectors.enrollment();
    }

    @Bean(destroyMethod = "")
    NativeCompatibilityObservationLifecycle nativeCompatibilityObservationLifecycle(
            NativeCompatibilityCollectors collectors,
            NativeCompatibilityObservationProducer producer,
            NativeCompatibility compatibility,
            ObjectProvider<SqlTransactions> sql,
            NativeTelemetry telemetry,
            ObjectProvider<NativeRollbackCollector> rollback) {
        var jobs = new ArrayList<NativeWorkerScheduler.Job>();
        jobs.add(producer.membershipJob());
        jobs.add(collectors.pollJob());
        var database = sql.getIfAvailable();
        if (database != null) jobs.add(compatibility.schemaJob(database));
        var rollbackCollector = rollback.getIfAvailable();
        if (rollbackCollector != null) jobs.add(rollbackCollector.job());
        return new NativeCompatibilityObservationLifecycle(jobs, telemetry);
    }

    @Bean
    @ConditionalOnBean(NativeRollbackCollector.Enrollment.class)
    @ConditionalOnMissingBean(NativeRollbackCollector.class)
    NativeRollbackCollector nativeRollbackCollector(NativeRollbackCollector.Enrollment inputs) {
        return new NativeRollbackCollector(inputs);
    }
}
