package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.SignalingApplication;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.io.FileSystemResource;
import org.springframework.mock.env.MockEnvironment;

import java.nio.file.Files;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.function.Consumer;

import static io.webrtc.signaling.app.runtime.local.LocalRuntimeFixtures.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalRuntimeInitializerTest {
    @TempDir
    Path mount;

    @Test
    void registersNothingWithoutAllThreeMarkers() throws Exception {
        var valid = LocalRuntimeFixtures.install(mount, "ACTOR");
        assertThat(LocalRuntimeInitializer.active(valid)).isTrue();

        assertInactive(env -> env.setProperty("SIGNALING_DEPLOYMENT_MODE", "production"));
        assertInactive(
                env ->
                        ((java.util.Properties) env.getPropertySources().get("mockProperties").getSource())
                                .remove("SIGNALING_LOCAL_ACKNOWLEDGEMENT"));
        assertInactive(env -> env.setProperty("SIGNALING_LOCAL_ACKNOWLEDGEMENT", "local_test_only"));

        var document = LocalRuntimeFixtures.contract(mount);
        document.put("environment", "production");
        Files.writeString(mount.resolve("provider.json"), JSON.writeValueAsString(document));
        var wrongEnvironment = LocalRuntimeFixtures.environment(mount);
        assertThat(LocalRuntimeInitializer.active(wrongEnvironment)).isFalse();
        assertNoEnrollment(wrongEnvironment, "actor");

        Files.delete(mount.resolve("provider.json"));
        assertThat(LocalRuntimeInitializer.active(LocalRuntimeFixtures.environment(mount))).isFalse();
    }

    @Test
    void plainContextStillFailsWithNativeRuntimeNotInstalled() throws Exception {
        var yaml =
                new YamlPropertySourceLoader()
                        .load(
                                "TEST_ONLY_defaults", new FileSystemResource("../config/production-defaults.yaml"));
        new ApplicationContextRunner()
                .withUserConfiguration(SignalingApplication.class)
                .withInitializer(new LocalRuntimeInitializer())
                .withInitializer(
                        ctx -> {
                            yaml.forEach(source -> ctx.getEnvironment().getPropertySources().addLast(source));
                            ctx.getEnvironment().setActiveProfiles("actor");
                        })
                .withPropertyValues(
                        "CELL_ID=c001",
                        "signaling.identity.issuer=TEST_ONLY_ISSUER",
                        "signaling.identity.audience=TEST_ONLY_AUDIENCE")
                .run(
                        ctx -> {
                            assertThat(ctx).hasNotFailed();
                            assertThatThrownBy(
                                    () ->
                                            ctx.getBean(ApplicationRunner.class)
                                                    .run(new DefaultApplicationArguments()))
                                    .isInstanceOf(IllegalStateException.class)
                                    .hasMessageContaining("Native runtime not installed");
                        });
    }

    @ParameterizedTest
    @EnumSource(SignalingApplication.Plane.class)
    void openGateDispatchesToThePlaneInstaller(SignalingApplication.Plane plane) throws Exception {
        LocalRuntimeFixtures.install(mount, plane.name());
        var context = context(plane.name().toLowerCase(Locale.ROOT));
        assertThatThrownBy(() -> new LocalRuntimeInitializer().initialize(context))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("not installed until Task 4/5/6");
    }

    @Test
    void profilePlaneMustMatchTheActiveProfile() throws Exception {
        LocalRuntimeFixtures.install(mount, "GATEWAY");
        var context = context("actor");
        assertThatThrownBy(() -> new LocalRuntimeInitializer().initialize(context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("plane");
        var none = context();
        assertThatThrownBy(() -> new LocalRuntimeInitializer().initialize(none))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void activeGateRefusesAnInvalidContractInsteadOfSilentlyContinuing() throws Exception {
        LocalRuntimeFixtures.install(mount, "ACTOR");
        Files.writeString(mount.resolve("process.json"), "{}");
        var context = context("actor");
        assertThatThrownBy(() -> new LocalRuntimeInitializer().initialize(context))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void activeGateRequiresAGenericApplicationContext() throws Exception {
        LocalRuntimeFixtures.install(mount, "ACTOR");
        var env = LocalRuntimeFixtures.environment(mount);
        var nonGeneric =
                (ConfigurableApplicationContext)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {ConfigurableApplicationContext.class},
                                (proxy, method, args) -> method.getName().equals("getEnvironment") ? env : null);
        assertThatThrownBy(() -> new LocalRuntimeInitializer().initialize(nonGeneric))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("GenericApplicationContext");
    }

    @Test
    void initializerIsRegisteredInSpringFactories() throws Exception {
        var factories =
                org.springframework.core.io.support.SpringFactoriesLoader.loadFactoryNames(
                        org.springframework.context.ApplicationContextInitializer.class,
                        getClass().getClassLoader());
        assertThat(factories).contains(LocalRuntimeInitializer.class.getName());
    }

    private AnnotationConfigApplicationContext context(String... profiles) {
        var context = new AnnotationConfigApplicationContext();
        var env = LocalRuntimeFixtures.environment(mount);
        context.getEnvironment().merge(env);
        context.getEnvironment().setActiveProfiles(profiles);
        return context;
    }

    private void assertInactive(Consumer<MockEnvironment> mutate) throws Exception {
        var env = LocalRuntimeFixtures.environment(mount);
        mutate.accept(env);
        assertThat(LocalRuntimeInitializer.active(env)).isFalse();
        assertNoEnrollment(env, "actor");
    }

    private void assertNoEnrollment(MockEnvironment env, String plane) {
        var context = new AnnotationConfigApplicationContext();
        context.getEnvironment().merge(env);
        context.getEnvironment().setActiveProfiles(plane);
        var before = Set.of(context.getBeanDefinitionNames());
        new LocalRuntimeInitializer().initialize(context);
        assertThat(context.getBeanDefinitionNames()).containsExactlyInAnyOrderElementsOf(before);
        assertThat(
                Arrays.stream(context.getBeanDefinitionNames())
                        .map(context::getBeanDefinition)
                        .map(d -> String.valueOf(d.getBeanClassName()))
                        .filter(n -> n.contains("Enrollment")))
                .isEmpty();
    }
}
