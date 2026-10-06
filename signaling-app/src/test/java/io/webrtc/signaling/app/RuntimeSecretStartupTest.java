package io.webrtc.signaling.app;

import static org.assertj.core.api.Assertions.*;
import io.webrtc.signaling.app.config.RuntimeSecretEnvironment;
import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringApplication;

/** Uses Boot's actual environment lifecycle and SPI, without test-installed runtime beans. */
class RuntimeSecretStartupTest {
    @TempDir Path directory;
    private Throwable start(String yaml) throws Exception {
        var file=directory.resolve("runtime.yaml");Files.writeString(file,yaml);
        var app=new SpringApplication(SignalingApplication.class);
        app.setRegisterShutdownHook(false);
        return catchThrowable(()->app.run("--spring.profiles.active=gateway",
            "--spring.main.banner-mode=off","--SIGNALING_RUNTIME_CONTRACT="+file));
    }
    private Throwable cause(Throwable failure,Class<? extends Throwable> type) {
        for(int depth=0;failure!=null&&depth<16;depth++,failure=failure.getCause())if(type.isInstance(failure))return failure;
        return null;
    }
    @Test void bootLoadsSecretIdentityBeforeBindingButStillRequiresRealRuntime() throws Exception {
        var failure=start("signaling: {identity: {issuer: TEST_ONLY_PRIVATE_ISSUER, audience: TEST_ONLY_PRIVATE_AUDIENCE}}\n");
        assertThat(cause(failure,NativeRuntimeStartup.MissingRuntime.class)).isNotNull();
        assertThat(SafeStartupFailure.render(failure)).contains("NATIVE_RUNTIME_NOT_INSTALLED").doesNotContain("TEST_ONLY_PRIVATE");
    }
    @Test void parserFailureStopsBeforeApplicationContextAndUsesSafeConfigurationCode() throws Exception {
        var failure=start("signaling: {identity: {issuer: TEST_ONLY_PRIVATE, issuer: TEST_ONLY_OTHER}}\n");
        assertThat(cause(failure,RuntimeSecretEnvironment.Rejected.class)).isNotNull();
        assertThat(SafeStartupFailure.render(failure)).contains("CONFIGURATION_REJECTED").doesNotContain("TEST_ONLY_PRIVATE",directory.toString());
    }
    @Test void unsafeSecretValuesStillPassThroughNativeTypedValidation() throws Exception {
        var failure=start("signaling: {identity: {issuer: TEST_ONLY_ISSUER, audience: TEST_ONLY_AUDIENCE}, lease: {ttl: 16s}}\n");
        assertThat(SafeStartupFailure.render(failure)).contains("CONFIGURATION_REJECTED");
        assertThat(cause(failure,NativeRuntimeStartup.MissingRuntime.class)).isNull();
    }
    @Test void unknownSecretPropertiesCannotSilentlyDisappear() throws Exception {
        var failure=start("signaling: {identity: {issuer: TEST_ONLY_ISSUER, audience: TEST_ONLY_AUDIENCE}, guessed-policy: permit-all}\n");
        assertThat(SafeStartupFailure.render(failure)).contains("CONFIGURATION_REJECTED");
    }
}
