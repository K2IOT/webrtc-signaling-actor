package io.webrtc.signaling.app;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NativeControlMissingEnrollmentTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void missingControlRuntimeRetainsSafeNativeDiagnosticAndIdentityValidationOrder(
      boolean validIdentity) {
    var application = SignalingApplication.application();
    application.setRegisterShutdownHook(false);
    var failure =
        catchThrowable(
            () ->
                application.run(
                    "--spring.profiles.active=control",
                    "--server.port=0",
                    "--signaling.identity.issuer=" + (validIdentity ? "TEST_ONLY_ISSUER" : ""),
                    "--signaling.identity.audience=TEST_ONLY_AUDIENCE"));
    assertThat(failure).isNotNull();
    assertThat(SafeStartupFailure.render(failure))
        .contains(validIdentity ? "NATIVE_RUNTIME_NOT_INSTALLED" : "CONFIGURATION_REJECTED");
    assertThat(SafeStartupFailure.render(failure)).doesNotContain("TEST_ONLY");
  }
}
