package io.webrtc.signaling.app;

import static org.assertj.core.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.*;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class SafeStartupFailureTest {
  public record Positive(int value) {
    public Positive {
      if (value < 1)
        throw new IllegalArgumentException("TEST_ONLY_PRIVATE_TOKEN_URL_AND_CONFIGURATION");
    }
  }

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
}
