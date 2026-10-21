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
 * <p>The gate is the two operator markers, {@code SIGNALING_DEPLOYMENT_MODE=local-minikube} and
 * {@code SIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY}, plus an explicit contract location: a
 * non-blank {@code SIGNALING_NATIVE_PROVIDER_CONTRACT} or {@code SIGNALING_LOCAL_PROCESS_PROFILE}
 * (Helm and compose always set the former). Markers alone, as in legacy tests and tooling, register
 * nothing and startup keeps failing with "Native runtime not installed". Once the gate is open the
 * contract and the profile are always loaded and any defect throws a specific
 * {@link LocalRuntimeFailure}; the only quiet exit is a readable contract that declares an
 * environment other than {@code local-test-only}.
 * Enrollments must be registered with
 * {@code registerBean(Class, Supplier)}, never {@code registerSingleton}, so
 * {@code @ConditionalOnBean} sees bean definitions.
 */
public final class LocalRuntimeInitializer
        implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    private static final String MODE = "local-minikube";
    private static final String ACKNOWLEDGEMENT = "LOCAL_TEST_ONLY";

    /**
     * Both operator markers and an explicit, non-blank contract location. The defaults of the two
     * location variables only resolve a path once the gate is open.
     */
    static boolean active(Environment env) {
        return MODE.equals(env.getProperty("SIGNALING_DEPLOYMENT_MODE"))
                && ACKNOWLEDGEMENT.equals(env.getProperty("SIGNALING_LOCAL_ACKNOWLEDGEMENT"))
                && (explicit(env, LocalRuntimeContract.CONTRACT_VARIABLE)
                || explicit(env, LocalRuntimeContract.PROFILE_VARIABLE));
    }

    private static boolean explicit(Environment env, String name) {
        var value = env.getProperty(name);
        return value != null && !value.isBlank();
    }

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        var env = context.getEnvironment();
        if (!active(env)) return;
        if (!(context instanceof GenericApplicationContext generic))
            throw new IllegalStateException(
                    "Local runtime requires a GenericApplicationContext: " + context.getClass().getName());
        LocalRuntimeContract local;
        try {
            local = LocalRuntimeContract.load(env);
        } catch (LocalRuntimeFailure failure) {
            // A readable contract for another environment is not ours: register nothing.
            if (failure.category() == LocalRuntimeFailure.Category.ENVIRONMENT_MISMATCH
                    && failure.role() == LocalRuntimeFailure.Role.CONTRACT) return;
            throw failure;
        }
        var plane = activePlane(env);
        if (local.profile().plane() != plane)
            throw new IllegalStateException("Local process profile plane does not match active profile");
        switch (plane) {
            case ACTOR -> installActor(generic, local);
            case GATEWAY -> installGateway(generic, local);
            case CONTROL -> installControl(generic, local);
        }
    }

    static void installActor(GenericApplicationContext ctx, LocalRuntimeContract c) {
        LocalActorInstaller.install(ctx, c);
    }

    static void installGateway(GenericApplicationContext ctx, LocalRuntimeContract c) {
        LocalGatewayInstaller.install(ctx, c);
    }

    // Task 6 replaces this with the control installer.
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
