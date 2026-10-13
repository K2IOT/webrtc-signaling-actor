package io.webrtc.signaling.app.runtime;

import ch.qos.logback.classic.LoggerContext;
import io.micrometer.core.instrument.Gauge;
import io.webrtc.signaling.app.SignalingApplication;
import io.webrtc.signaling.app.config.SignalingProperties;
import io.webrtc.signaling.observability.BoundedAsyncLogAppender;
import io.webrtc.signaling.observability.SignalingMetrics;
import io.webrtc.signaling.storage.DbBoundary;
import io.webrtc.signaling.storage.SqlTransactions;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * One process registry is mandatory; private health binds its measured scrape.
 */
@AutoConfiguration(
        before = {
                NativeActorProcessConfiguration.class,
                NativeCellRpcConfiguration.class,
                NativeGatewayBusinessConfiguration.class,
                NativeControlBusinessConfiguration.class
        })
public class NativeTelemetryConfiguration {
    @Bean
    static BeanPostProcessor nativeTelemetryBindings(
            org.springframework.beans.factory.BeanFactory factory) {
        return new BeanPostProcessor() {
            public Object postProcessAfterInitialization(Object bean, String name) {
                if (bean instanceof io.webrtc.signaling.gateway.NativeGatewayServices services)
                    services.compatibility(factory.getBean(NativeCompatibility.class));
                if (bean instanceof io.webrtc.signaling.auth.BoundedTokenVerifier verifier)
                    verifier.observe(factory.getBean(NativeTelemetry.class));
                if (bean instanceof io.webrtc.signaling.rpc.CellRpcClient client)
                    client
                            .observe(factory.getBean(NativeTelemetry.class))
                            .compatibility(factory.getBean(NativeCompatibility.class));
                if (bean instanceof io.webrtc.signaling.rpc.GatewayRelayRpcClient client)
                    client
                            .observe(factory.getBean(NativeTelemetry.class))
                            .compatibility(factory.getBean(NativeCompatibility.class));
                if (bean instanceof io.webrtc.signaling.rpc.CellRpcServer server)
                    server
                            .observe(factory.getBean(NativeTelemetry.class))
                            .compatibility(factory.getBean(NativeCompatibility.class));
                if (bean instanceof io.webrtc.signaling.rpc.GatewayRelayRpcServer server)
                    server
                            .observe(factory.getBean(NativeTelemetry.class))
                            .compatibility(factory.getBean(NativeCompatibility.class));
                if (bean instanceof DbBoundary boundary)
                    boundary.observe(factory.getBean(NativeTelemetry.class));
                if (bean instanceof SqlTransactions sql) {
                    var telemetry = factory.getBean(NativeTelemetry.class);
                    sql.observe(telemetry);
                    telemetry.pools(sql.pools());
                }
                return bean;
            }
        };
    }

    @Bean(destroyMethod = "close")
    SignalingMetrics nativeSignalingMetrics(
            SignalingProperties properties, SignalingApplication.Plane plane, Environment environment) {
        String cell = environment.getProperty("SIGNALING_CELL_ID", environment.getProperty("CELL_ID"));
        if (cell == null) cell = environment.getProperty("signaling.cell-id");
        var metrics =
                new SignalingMetrics(
                        cell, SignalingMetrics.Plane.valueOf(plane.name()), properties.fingerprint());
        if (LoggerFactory.getILoggerFactory() instanceof LoggerContext context) {
            var appender = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).getAppender("SAFE_ASYNC");
            if (appender instanceof BoundedAsyncLogAppender async) {
                Gauge.builder("signaling.log.export.dropped", async, BoundedAsyncLogAppender::droppedEvents)
                        .register(metrics.registry());
                Gauge.builder(
                                "signaling.log.export.queued",
                                async,
                                BoundedAsyncLogAppender::getNumberOfElementsInQueue)
                        .register(metrics.registry());
                Gauge.builder(
                                "signaling.log.export.shutdown.unresolved",
                                async,
                                BoundedAsyncLogAppender::unresolvedEvents)
                        .register(metrics.registry());
            }
        }
        return metrics;
    }

    @Bean(destroyMethod = "close")
    NativeLogExportOwner nativeLogExportOwner() {
        return new NativeLogExportOwner();
    }

    @Bean
    NativeTelemetry nativeTelemetry(SignalingMetrics metrics) {
        return new NativeTelemetry(metrics);
    }

    @Bean
    NativeCompatibility nativeCompatibility(
            SignalingProperties properties,
            Environment environment,
            org.springframework.beans.factory.ObjectProvider<NativeActorBusinessEnrollment> local,
            org.springframework.beans.factory.ObjectProvider<NativeActorSchedulingEnrollment> scheduling,
            org.springframework.beans.factory.ObjectProvider<NativeCompatibilityEnrollment> enrollment) {
        String cell = environment.getProperty("SIGNALING_CELL_ID", environment.getProperty("CELL_ID"));
        if (cell == null) cell = environment.getProperty("signaling.cell-id");
        String fingerprint =
                environment.getProperty(
                        "SIGNALING_CLUSTER_FINGERPRINT",
                        environment.getProperty("CLUSTER_FINGERPRINT", properties.fingerprint()));
        return new NativeCompatibility(
                cell,
                fingerprint,
                local::getIfAvailable,
                scheduling::getIfAvailable,
                enrollment::getIfAvailable);
    }
}
