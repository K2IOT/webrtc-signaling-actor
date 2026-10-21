package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.SignalingApplication.Plane;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Environment;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Registers the native enrollments for a LOCAL_TEST_ONLY container from its mounted contract.
 *
 * <p>Inactive unless all three markers hold: {@code SIGNALING_DEPLOYMENT_MODE=local-minikube},
 * {@code SIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY} and a contract declaring environment
 * {@code local-test-only}. When inactive nothing is registered and startup keeps failing with
 * "Native runtime not installed". Enrollments must be registered with
 * {@code registerBean(Class, Supplier)}, never {@code registerSingleton}, so
 * {@code @ConditionalOnBean} sees bean definitions.
 */
public final class LocalRuntimeInitializer
        implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    private static final String MODE = "local-minikube";
    private static final String ACKNOWLEDGEMENT = "LOCAL_TEST_ONLY";
    private static final String CONTRACT_ENVIRONMENT = "local-test-only";

    static boolean active(Environment env) {
        return MODE.equals(env.getProperty("SIGNALING_DEPLOYMENT_MODE"))
                && ACKNOWLEDGEMENT.equals(env.getProperty("SIGNALING_LOCAL_ACKNOWLEDGEMENT"))
                && CONTRACT_ENVIRONMENT.equals(LocalRuntimeContract.declaredEnvironment(env));
    }

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        var env = context.getEnvironment();
        if (!active(env)) return;
        if (!(context instanceof GenericApplicationContext generic))
            throw new IllegalStateException(
                    "Local runtime requires a GenericApplicationContext: " + context.getClass().getName());
        var local = LocalRuntimeContract.load(env);
        var plane = activePlane(env);
        if (local.profile().plane() != plane)
            throw new IllegalStateException("Local process profile plane does not match active profile");
        switch (plane) {
            case ACTOR -> installActor(generic, local);
            case GATEWAY -> installGateway(generic, local);
            case CONTROL -> installControl(generic, local);
        }
    }

    // Tasks 4-6 replace these with the per-plane installers.
    static void installActor(GenericApplicationContext ctx, LocalRuntimeContract c) {
        throw new UnsupportedOperationException("not installed until Task 4/5/6");
    }

    static void installGateway(GenericApplicationContext ctx, LocalRuntimeContract c) {
        throw new UnsupportedOperationException("not installed until Task 4/5/6");
    }

    static void installControl(GenericApplicationContext ctx, LocalRuntimeContract c) {
        throw new UnsupportedOperationException("not installed until Task 4/5/6");
    }

    private static Plane activePlane(Environment env) {
        List<String> planes =
                Arrays.stream(env.getActiveProfiles())
                        .filter(p -> List.of("gateway", "actor", "control").contains(p))
                        .toList();
        if (planes.size() != 1)
            throw new IllegalStateException(
                    "Select exactly one deployment plane: gateway, actor or control");
        return Plane.valueOf(planes.getFirst().toUpperCase(Locale.ROOT));
    }
}
