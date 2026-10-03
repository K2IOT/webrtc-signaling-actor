package io.webrtc.signaling.app.config;

import io.webrtc.signaling.app.SignalingApplication;
import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.FileSystemResource;
import static org.assertj.core.api.Assertions.assertThat;

class SignalingPropertiesTest {
    private ApplicationContextRunner context(String... overrides) throws IOException {
        var yaml = new YamlPropertySourceLoader().load("production", new FileSystemResource("../config/production-defaults.yaml"));
        return new ApplicationContextRunner().withUserConfiguration(SignalingApplication.class)
            .withInitializer(ctx -> {
                yaml.forEach(source -> ctx.getEnvironment().getPropertySources().addLast(source));
                ctx.getEnvironment().setActiveProfiles("actor");
            })
            .withPropertyValues("signaling.identity.issuer=https://identity.example.test", "signaling.identity.audience=signaling-test")
            .withPropertyValues(overrides);
    }
    @Test void loadsSpecDefaultsAndImmutableFingerprint() throws IOException {
        context().run(ctx -> {
            assertThat(ctx).hasNotFailed();
            var p = ctx.getBean(SignalingProperties.class);
            assertThat(p.lease().ttl()).isEqualTo(Duration.ofSeconds(15));
            assertThat(p.lease().renewal()).isEqualTo(Duration.ofSeconds(5));
            assertThat(p.cluster().sbrStableAfter()).isEqualTo(Duration.ofSeconds(10));
            assertThat(p.cluster().downRemovalMargin()).isEqualTo(Duration.ofSeconds(10));
            assertThat(p.cluster().terminationGrace()).isEqualTo(Duration.ofSeconds(90));
            assertThat(p.transport().authTimeout()).isEqualTo(Duration.ofSeconds(5));
            assertThat(p.protocol().frameBytes()).isEqualTo(80 * 1024);
            assertThat(p.fingerprint()).matches("[a-f0-9]{64}").isEqualTo(p.fingerprint());
            assertThat(ctx.getBean(SignalingApplication.Plane.class)).isEqualTo(SignalingApplication.Plane.ACTOR);
        });
    }
    @ParameterizedTest @CsvSource({
        "signaling.lease.renewal,15s", "signaling.lease.renewal,16s", "signaling.lease.ttl,0s",
        "signaling.queues.entity-messages,-1", "signaling.queues.entity-bytes,0",
        "signaling.protocol.version,2", "signaling.identity.issuer,''", "signaling.identity.audience,''",
        "signaling.cluster.sbr-stable-after,5s", "signaling.cluster.down-removal-margin,5s",
        "signaling.cluster.termination-grace,30s", "signaling.protocol.frame-bytes,81921",
        "signaling.protocol.ice-batch-count,21", "signaling.identity.clock-skew,31s",
        "signaling.transport.auth-timeout,6s", "signaling.queues.entity-messages,129"
    }) void rejectsUnsafeConfigAtStartup(String key, String value) throws IOException {
        context(key + "=" + value).run(ctx -> assertThat(ctx).hasFailed());
    }
    @Test void rejectsUnresolvedIdentityPlaceholders() throws IOException {
        context("signaling.identity.issuer=${MISSING_ISSUER}").run(ctx -> assertThat(ctx).hasFailed());
    }
    @Test void fingerprintChangesWithEffectiveConfiguration() throws IOException {
        context().run(a -> contextUnchecked("signaling.queues.entity-messages=32").run(b ->
            assertThat(a.getBean(SignalingProperties.class).fingerprint()).isNotEqualTo(b.getBean(SignalingProperties.class).fingerprint())));
    }
    @Test void fingerprintCannotAliasIdentityRecordDelimiters() throws IOException {
        context("signaling.identity.issuer=x, audience=y", "signaling.identity.audience=z").run(a ->
            contextUnchecked("signaling.identity.issuer=x", "signaling.identity.audience=y, audience=z").run(b ->
                assertThat(a.getBean(SignalingProperties.class).fingerprint())
                    .isNotEqualTo(b.getBean(SignalingProperties.class).fingerprint())));
    }
    @Test void mapsEffectiveConfigurationIntoProtocolLimits() throws IOException {
        context().run(ctx -> assertThat(ctx.getBean(SignalingProperties.class).protocol().limits())
            .isEqualTo(io.webrtc.signaling.protocol.ProtocolLimits.v1()));
    }
    private ApplicationContextRunner contextUnchecked(String... values) {
        try { return context(values); } catch (IOException e) { throw new IllegalStateException(e); }
    }
    @Test void rejectsMultipleDeploymentPlanes() throws IOException {
        context().withInitializer(ctx -> ctx.getEnvironment().setActiveProfiles("actor", "gateway"))
            .run(ctx -> assertThat(ctx).hasFailed());
    }
    @Test void rejectsMissingDeploymentPlane() throws IOException {
        context().withInitializer(ctx -> ctx.getEnvironment().setActiveProfiles())
            .run(ctx -> assertThat(ctx).hasFailed());
    }
}
