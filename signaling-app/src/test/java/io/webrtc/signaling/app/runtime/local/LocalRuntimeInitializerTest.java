package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.SignalingApplication;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Category;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure.Role;
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
import java.util.List;
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
    void registersNothingWithoutBothOperatorMarkers() throws Exception {
        var valid = LocalRuntimeFixtures.install(mount, "ACTOR");
        assertThat(LocalRuntimeInitializer.active(valid)).isTrue();

        assertInactive(env -> env.setProperty("SIGNALING_DEPLOYMENT_MODE", "production"));
        assertInactive(env -> env.setProperty("SIGNALING_DEPLOYMENT_MODE", "LOCAL-MINIKUBE"));
        assertInactive(
                env ->
                        ((java.util.Properties) env.getPropertySources().get("mockProperties").getSource())
                                .remove("SIGNALING_DEPLOYMENT_MODE"));
        assertInactive(
                env ->
                        ((java.util.Properties) env.getPropertySources().get("mockProperties").getSource())
                                .remove("SIGNALING_LOCAL_ACKNOWLEDGEMENT"));
        assertInactive(env -> env.setProperty("SIGNALING_LOCAL_ACKNOWLEDGEMENT", "local_test_only"));
        // Unusable files are irrelevant while a marker is absent: nothing is read at all.
        Files.delete(mount.resolve("provider.json"));
        assertInactive(env -> env.setProperty("SIGNALING_DEPLOYMENT_MODE", "production"));
    }

    @Test
    void markersWithoutAnExplicitContractLocationRegisterNothing() {
        for (var location : new String[] {null, "", "   "}) {
            var env =
                    new MockEnvironment()
                            .withProperty("SIGNALING_DEPLOYMENT_MODE", "local-minikube")
                            .withProperty("SIGNALING_LOCAL_ACKNOWLEDGEMENT", "LOCAL_TEST_ONLY");
            if (location != null) {
                env.setProperty("SIGNALING_NATIVE_PROVIDER_CONTRACT", location);
                env.setProperty("SIGNALING_LOCAL_PROCESS_PROFILE", location);
            }
            assertThat(LocalRuntimeInitializer.active(env)).as("[" + location + "]").isFalse();
            assertNoEnrollment(env, "actor");
        }
    }

    @Test
    void markersWithoutALocationStillFailWithTheGenericNativeRuntimeNotInstalled() throws Exception {
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
                            ctx.getEnvironment().setActiveProfiles("gateway");
                        })
                .withPropertyValues(
                        "SIGNALING_DEPLOYMENT_MODE=local-minikube",
                        "SIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY",
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

    @Test
    void anExplicitContractLocationActivatesTheGateAndAMissingFileFailsLoudly() {
        var absent = mount.resolve("absent");
        var contractOnly =
                markers().withProperty("SIGNALING_NATIVE_PROVIDER_CONTRACT", absent + "/provider.json");
        assertThat(LocalRuntimeInitializer.active(contractOnly)).isTrue();
        assertLoud(contractOnly, Role.CONTRACT, Category.MISSING);

        var profileOnly =
                markers().withProperty("SIGNALING_LOCAL_PROCESS_PROFILE", absent + "/process.json");
        assertThat(LocalRuntimeInitializer.active(profileOnly)).isTrue();
        // The contract falls back to its default location, which holds nothing in a test JVM.
        assertLoud(profileOnly, Role.CONTRACT, Category.MISSING);
    }

    @Test
    void anExplicitProfileLocationWithAMissingProfileFailsLoudly() throws Exception {
        LocalRuntimeFixtures.install(mount, "ACTOR");
        Files.delete(mount.resolve("process.json"));
        var env = LocalRuntimeFixtures.environment(mount);
        assertLoud(env, Role.PROFILE, Category.MISSING);
    }

    private static MockEnvironment markers() {
        return new MockEnvironment()
                .withProperty("SIGNALING_DEPLOYMENT_MODE", "local-minikube")
                .withProperty("SIGNALING_LOCAL_ACKNOWLEDGEMENT", "LOCAL_TEST_ONLY");
    }

    private static void assertLoud(MockEnvironment env, Role role, Category category) {
        var context = new AnnotationConfigApplicationContext();
        context.getEnvironment().merge(env);
        context.getEnvironment().setActiveProfiles("actor");
        var failure =
                org.junit.jupiter.api.Assertions.assertThrows(
                        LocalRuntimeFailure.class, () -> new LocalRuntimeInitializer().initialize(context));
        assertThat(failure.role()).isEqualTo(role);
        assertThat(failure.category()).isEqualTo(category);
    }

    @Test
    void readableContractForAnotherEnvironmentRegistersNothing() throws Exception {
        LocalRuntimeFixtures.install(mount, "ACTOR");
        var document = LocalRuntimeFixtures.contract(mount);
        document.put("environment", "production");
        Files.writeString(mount.resolve("provider.json"), JSON.writeValueAsString(document));
        var env = LocalRuntimeFixtures.environment(mount);
        assertThat(LocalRuntimeInitializer.active(env)).isTrue();
        assertNoEnrollment(env, "actor");
    }

    @Test
    void bothMarkersSetMakeEveryUnusableInputFailLoudlyAndSpecifically() throws Exception {
        LocalRuntimeFixtures.install(mount, "ACTOR");
        String contract = JSON.writeValueAsString(LocalRuntimeFixtures.contract(mount));
        String profile = JSON.writeValueAsString(LocalRuntimeFixtures.profile(mount, "ACTOR"));
        String secret = "TEST_ONLY_PRIVATE_PASSWORD";
        record Case(String file, String content, Role role, Category category) {
        }
        for (var c :
                List.of(
                        new Case("provider.json", null, Role.CONTRACT, Category.MISSING),
                        new Case("provider.json", "{\"password\":\"" + secret + "\"", Role.CONTRACT, Category.MALFORMED),
                        new Case(
                                "provider.json",
                                contract.substring(0, contract.length() - 1) + ",\"cell\":\"c002\"}",
                                Role.CONTRACT,
                                Category.DUPLICATE_KEY),
                        new Case("provider.json", " ".repeat(65537), Role.CONTRACT, Category.TOO_LARGE),
                        new Case("provider.json", "{\"password\":\"" + secret + "\"}", Role.CONTRACT, Category.INVALID),
                        new Case("process.json", null, Role.PROFILE, Category.MISSING),
                        new Case("process.json", "{", Role.PROFILE, Category.MALFORMED),
                        new Case("process.json", "{}", Role.PROFILE, Category.INVALID),
                        new Case(
                                "process.json",
                                profile.substring(0, profile.length() - 1) + ",\"plane\":\"ACTOR\"}",
                                Role.PROFILE,
                                Category.DUPLICATE_KEY))) {
            LocalRuntimeFixtures.install(mount, "ACTOR");
            if (c.content() == null) Files.delete(mount.resolve(c.file()));
            else Files.writeString(mount.resolve(c.file()), c.content());
            var env = LocalRuntimeFixtures.environment(mount);
            assertThat(LocalRuntimeInitializer.active(env)).isTrue();
            var context = new AnnotationConfigApplicationContext();
            context.getEnvironment().merge(env);
            context.getEnvironment().setActiveProfiles("actor");
            var failure =
                    org.junit.jupiter.api.Assertions.assertThrows(
                            LocalRuntimeFailure.class, () -> new LocalRuntimeInitializer().initialize(context));
            assertThat(failure.role()).as(c.file() + c.category()).isEqualTo(c.role());
            assertThat(failure.category()).as(c.file() + c.category()).isEqualTo(c.category());
            assertThat(failure).hasMessageNotContaining(secret).hasMessageNotContaining(mount.toString());
            assertThat(failure).hasNoCause();
        }
    }

    @Test
    void identityMismatchFailsLoudlyThroughTheInitializer() throws Exception {
        LocalRuntimeFixtures.install(mount, "ACTOR");
        var context = context("actor");
        context.getEnvironment().getPropertySources().addFirst(
                new org.springframework.core.env.MapPropertySource(
                        "override", java.util.Map.of("SIGNALING_CELL_ID", "c002")));
        var failure =
                org.junit.jupiter.api.Assertions.assertThrows(
                        LocalRuntimeFailure.class, () -> new LocalRuntimeInitializer().initialize(context));
        assertThat(failure.category()).isEqualTo(Category.IDENTITY_MISMATCH);
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
                .isInstanceOf(LocalRuntimeFailure.class)
                .hasMessage("Local runtime profile: INVALID");
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
