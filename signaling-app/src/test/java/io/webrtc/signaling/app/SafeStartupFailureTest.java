package io.webrtc.signaling.app;

import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeInitializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextException;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class SafeStartupFailureTest {
    @Test
    void realBindingFailureEmitsOnlyABoundedConfigurationCode() {
        var binder = new Binder(new MapConfigurationPropertySource(Map.of("test.value", -1)));
        var failure = catchThrowable(() -> binder.bind("test", Bindable.of(Positive.class)));
        assertThat(failure).isInstanceOf(BindException.class);
        assertThat(SafeStartupFailure.render(failure))
                .isEqualTo(
                        "{\"component\":\"PLATFORM\",\"level\":\"ERROR\",\"errorCode\":\"CONFIGURATION_REJECTED\"}");
    }

    @Test
    void missingNativeRuntimeHasItsOwnCodeWithoutExceptionText() {
        var failure =
                new RuntimeException(
                        "TEST_ONLY_PRIVATE",
                        new NativeRuntimeStartup.MissingRuntime(SignalingApplication.Plane.ACTOR));
        assertThat(SafeStartupFailure.render(failure))
                .isEqualTo(
                        "{\"component\":\"PLATFORM\",\"level\":\"ERROR\",\"errorCode\":\"NATIVE_RUNTIME_NOT_INSTALLED\"}");
    }

    @Test
    void unknownAndCyclicFailuresDoNotExposeMessagesOrHang() {
        assertThat(SafeStartupFailure.render(new IllegalStateException("TEST_ONLY_PRIVATE")))
                .contains("STARTUP_FAILED")
                .doesNotContain("TEST_ONLY_PRIVATE");
        var cyclic =
                new RuntimeException("TEST_ONLY_PRIVATE") {
                    @Override
                    public synchronized Throwable getCause() {
                        return this;
                    }
                };
        assertThat(SafeStartupFailure.render(cyclic))
                .contains("STARTUP_FAILED")
                .doesNotContain("TEST_ONLY_PRIVATE");
    }

    @ParameterizedTest
    @EnumSource(LocalRuntimeFailure.Category.class)
    void localRuntimeFailureRendersRoleAndCategoryOnly(LocalRuntimeFailure.Category category) {
        for (var role : LocalRuntimeFailure.Role.values()) {
            assertThat(SafeStartupFailure.render(new LocalRuntimeFailure(role, category)))
                    .isEqualTo(
                            "{\"component\":\"PLATFORM\",\"level\":\"ERROR\",\"errorCode\":"
                                    + "\"LOCAL_RUNTIME_REJECTED\",\"role\":\""
                                    + role.name().toLowerCase(Locale.ROOT)
                                    + "\",\"category\":\""
                                    + category.name()
                                    + "\"}");
        }
    }

    @Test
    void localRuntimeFailureIsFoundInsideSpringWrappers() {
        var inner = new LocalRuntimeFailure(LocalRuntimeFailure.Role.CONTRACT, LocalRuntimeFailure.Category.MISSING);
        var beanCreation =
                new BeanCreationException("TEST_ONLY_PRIVATE_BEAN", "TEST_ONLY_PRIVATE", inner);
        var wrapped =
                new ApplicationContextException("TEST_ONLY_PRIVATE_CONTEXT", beanCreation);
        for (Throwable failure : new Throwable[] {beanCreation, wrapped, new RuntimeException(wrapped)}) {
            assertThat(SafeStartupFailure.render(failure))
                    .contains("\"role\":\"contract\"", "\"category\":\"MISSING\"")
                    .doesNotContain("STARTUP_FAILED", "TEST_ONLY_PRIVATE");
        }
    }

    @Test
    void localRuntimeFailureNeverRendersItsCauseOrSiblingMessages() {
        var failure = new LocalRuntimeFailure(LocalRuntimeFailure.Role.PROFILE, LocalRuntimeFailure.Category.INVALID);
        failure.initCause(new IllegalArgumentException("TEST_ONLY_PRIVATE_PASSWORD /run/signaling/private.key"));
        failure.addSuppressed(new IllegalStateException("TEST_ONLY_PRIVATE_SUPPRESSED"));
        assertThat(SafeStartupFailure.render(new RuntimeException("TEST_ONLY_PRIVATE_OUTER", failure)))
                .isEqualTo(SafeStartupFailure.render(failure))
                .contains("\"role\":\"profile\"", "\"category\":\"INVALID\"")
                .doesNotContain("TEST_ONLY_PRIVATE", "/run/signaling");
    }

    @Test
    void bothMarkersWithAMissingContractRenderTheMissingCategoryNotAGenericCode() {
        var absent = "/nonexistent-local-runtime-mount";
        new ApplicationContextRunner()
                .withInitializer(new LocalRuntimeInitializer())
                .withPropertyValues(
                        "SIGNALING_DEPLOYMENT_MODE=local-minikube",
                        "SIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY",
                        "SIGNALING_NATIVE_PROVIDER_CONTRACT=" + absent + "/provider.json",
                        "SIGNALING_LOCAL_PROCESS_PROFILE=" + absent + "/process.json")
                .run(
                        ctx -> {
                            assertThat(ctx).hasFailed();
                            assertThat(SafeStartupFailure.render(ctx.getStartupFailure()))
                                    .contains("\"role\":\"contract\"", "\"category\":\"MISSING\"")
                                    .doesNotContain("STARTUP_FAILED", absent);
                        });
    }

    public record Positive(int value) {
        public Positive {
            if (value < 1)
                throw new IllegalArgumentException("TEST_ONLY_PRIVATE_TOKEN_URL_AND_CONFIGURATION");
        }
    }
}
