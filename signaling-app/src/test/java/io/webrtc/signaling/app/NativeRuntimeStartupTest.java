package io.webrtc.signaling.app;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.FileSystemResource;

class NativeRuntimeStartupTest {
    @ParameterizedTest @EnumSource(SignalingApplication.Plane.class)
    void validIdentityCannotReportSuccessfulStartupWithoutInstalledNativePlane(SignalingApplication.Plane plane)throws Exception {
        var yaml=new YamlPropertySourceLoader().load("TEST_ONLY_defaults",new FileSystemResource("../config/production-defaults.yaml"));
        new ApplicationContextRunner().withUserConfiguration(SignalingApplication.class)
            .withInitializer(ctx->{yaml.forEach(source->ctx.getEnvironment().getPropertySources().addLast(source));ctx.getEnvironment().setActiveProfiles(plane.name().toLowerCase(java.util.Locale.ROOT));})
            .withPropertyValues("signaling.identity.issuer=TEST_ONLY_ISSUER","signaling.identity.audience=TEST_ONLY_AUDIENCE")
            .run(ctx->{
                assertThat(ctx).hasNotFailed();
                assertThat(ctx.getBeansOfType(ApplicationRunner.class)).hasSize(1);
                assertThatThrownBy(()->ctx.getBean(ApplicationRunner.class).run(new DefaultApplicationArguments())).isInstanceOf(IllegalStateException.class).hasMessageContaining("Native runtime not installed");
            });
    }
}
