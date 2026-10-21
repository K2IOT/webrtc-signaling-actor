package io.webrtc.signaling.app.config;

import io.webrtc.signaling.app.SignalingApplication;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.FileSystemResource;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class SignalingPropertiesTest {
    static java.util.stream.Stream<Arguments> malformedIdentityScalars() {
        return java.util.stream.Stream.of("signaling.identity.issuer", "signaling.identity.audience")
                .flatMap(
                        field ->
                                java.util.stream.Stream.of("x\ud800", "x\udc00", "x\ud800y")
                                        .map(value -> Arguments.of(field, value)));
    }

    private ApplicationContextRunner context(String... overrides) throws IOException {
        var yaml =
                new YamlPropertySourceLoader()
                        .load("production", new FileSystemResource("../config/production-defaults.yaml"));
        return new ApplicationContextRunner()
                .withUserConfiguration(SignalingApplication.class)
                .withInitializer(
                        ctx -> {
                            yaml.forEach(source -> ctx.getEnvironment().getPropertySources().addLast(source));
                            ctx.getEnvironment().setActiveProfiles("actor");
                        })
                .withPropertyValues(
                        "CELL_ID=c001",
                        "signaling.identity.issuer=https://identity.example.test",
                        "signaling.identity.audience=signaling-test")
                .withPropertyValues(overrides);
    }

    @Test
    void loadsSpecDefaultsAndImmutableFingerprint() throws IOException {
        context()
                .run(
                        ctx -> {
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
                            assertThat(ctx.getBean(SignalingApplication.Plane.class))
                                    .isEqualTo(SignalingApplication.Plane.ACTOR);
                        });
    }

    @ParameterizedTest
    @CsvSource({
            "signaling.lease.renewal,15s",
            "signaling.lease.renewal,16s",
            "signaling.lease.ttl,0s",
            "signaling.queues.entity-messages,-1",
            "signaling.queues.entity-bytes,0",
            "signaling.protocol.version,2",
            "signaling.identity.issuer,''",
            "signaling.identity.audience,''",
            "signaling.cluster.sbr-stable-after,5s",
            "signaling.cluster.down-removal-margin,5s",
            "signaling.cluster.termination-grace,30s",
            "signaling.protocol.frame-bytes,81921",
            "signaling.protocol.ice-batch-count,21",
            "signaling.identity.clock-skew,31s",
            "signaling.transport.auth-timeout,6s",
            "signaling.queues.entity-messages,129"
    })
    void rejectsUnsafeConfigAtStartup(String key, String value) throws IOException {
        context(key + "=" + value).run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void callTimersAndRateLimitsLoadFromTheDefaultsFile() throws IOException {
        context()
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            var p = ctx.getBean(SignalingProperties.class);
                            assertThat(p.calls()).isEqualTo(io.webrtc.signaling.storage.CallTimers.V1);
                            assertThat(p.rateLimits())
                                    .isEqualTo(io.webrtc.signaling.gateway.GatewayRateLimits.V1);
                        });
    }

    @ParameterizedTest
    @CsvSource({
            "signaling.calls.ringing,31s",
            "signaling.calls.activation,6s",
            "signaling.calls.negotiation,21s",
            "signaling.calls.reconnect-grace,76s",
            "signaling.calls.media-recovery-window,76s",
            "signaling.calls.media-disconnect-debounce,30s",
            "signaling.calls.media-restart-attempts,0",
            "signaling.calls.media-restart-attempts,4",
            "signaling.calls.ringing,0s",
            "signaling.rate-limits.socket-rate,0",
            "signaling.rate-limits.socket-rate,31",
            "signaling.rate-limits.user-burst,0",
            "signaling.rate-limits.tracked-users,0"
    })
    void rejectsCallTimersAndRateLimitsBeyondTheContract(String key, String value)
            throws IOException {
        context(key + "=" + value).run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void shorterCallTimersAreAcceptedAndChangeTheFingerprint() throws IOException {
        context()
                .run(
                        a ->
                                contextUnchecked("signaling.calls.ringing=20s")
                                        .run(
                                                b -> {
                                                    assertThat(b).hasNotFailed();
                                                    assertThat(b.getBean(SignalingProperties.class).calls().ringing())
                                                            .isEqualTo(Duration.ofSeconds(20));
                                                    assertThat(a.getBean(SignalingProperties.class).fingerprint())
                                                            .isNotEqualTo(
                                                                    b.getBean(SignalingProperties.class).fingerprint());
                                                }));
    }

    @Test
    void rateLimitChangesAlterTheFingerprint() throws IOException {
        context()
                .run(
                        a ->
                                contextUnchecked("signaling.rate-limits.user-rate=10")
                                        .run(
                                                b ->
                                                        assertThat(a.getBean(SignalingProperties.class).fingerprint())
                                                                .isNotEqualTo(
                                                                        b.getBean(SignalingProperties.class).fingerprint())));
    }

    @Test
    void rejectsUnresolvedIdentityPlaceholders() throws IOException {
        context("signaling.identity.issuer=${MISSING_ISSUER}").run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void fingerprintChangesWithEffectiveConfiguration() throws IOException {
        context()
                .run(
                        a ->
                                contextUnchecked("signaling.queues.entity-messages=32")
                                        .run(
                                                b ->
                                                        assertThat(a.getBean(SignalingProperties.class).fingerprint())
                                                                .isNotEqualTo(b.getBean(SignalingProperties.class).fingerprint())));
    }

    @Test
    void fingerprintCannotAliasIdentityRecordDelimiters() throws IOException {
        context("signaling.identity.issuer=x, audience=y", "signaling.identity.audience=z")
                .run(
                        a ->
                                contextUnchecked(
                                        "signaling.identity.issuer=x", "signaling.identity.audience=y, audience=z")
                                        .run(
                                                b ->
                                                        assertThat(a.getBean(SignalingProperties.class).fingerprint())
                                                                .isNotEqualTo(b.getBean(SignalingProperties.class).fingerprint())));
    }

    @Test
    void mapsEffectiveConfigurationIntoProtocolLimits() throws IOException {
        context()
                .run(
                        ctx ->
                                assertThat(ctx.getBean(SignalingProperties.class).protocol().limits())
                                        .isEqualTo(io.webrtc.signaling.protocol.ProtocolLimits.v1()));
    }

    @ParameterizedTest
    @MethodSource("malformedIdentityScalars")
    void malformedUtf16CannotEnterTheEffectiveConfigurationFingerprint(String field, String value)
            throws IOException {
        context(field + "=" + value).run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void validSupplementaryUnicodeRemainsAnUnambiguousIdentity() throws IOException {
        context("signaling.identity.issuer=https://identity.example.test/\ud83d\ude00")
                .run(
                        a ->
                                contextUnchecked("signaling.identity.issuer=https://identity.example.test/?")
                                        .run(
                                                b -> {
                                                    assertThat(a).hasNotFailed();
                                                    assertThat(b).hasNotFailed();
                                                    assertThat(a.getBean(SignalingProperties.class).identity().issuer())
                                                            .endsWith("\ud83d\ude00");
                                                    assertThat(a.getBean(SignalingProperties.class).fingerprint())
                                                            .isNotEqualTo(b.getBean(SignalingProperties.class).fingerprint());
                                                }));
    }

    private ApplicationContextRunner contextUnchecked(String... values) {
        try {
            return context(values);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void rejectsMultipleDeploymentPlanes() throws IOException {
        context()
                .withInitializer(ctx -> ctx.getEnvironment().setActiveProfiles("actor", "gateway"))
                .run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void rejectsMissingDeploymentPlane() throws IOException {
        context()
                .withInitializer(ctx -> ctx.getEnvironment().setActiveProfiles())
                .run(ctx -> assertThat(ctx).hasFailed());
    }
}
