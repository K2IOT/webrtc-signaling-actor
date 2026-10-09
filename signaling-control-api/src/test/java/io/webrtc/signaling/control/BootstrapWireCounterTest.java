package io.webrtc.signaling.control;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BootstrapWireCounterTest {
  @ParameterizedTest
  @ValueSource(longs = {7, 9007199254740993L, Long.MAX_VALUE})
  void directoryVersionUsesCanonicalDecimalStringWithoutBrowserPrecisionLoss(long epoch)
      throws Exception {
    var response =
        new BootstrapController.BootstrapResponse(
            "wss://cell.test/ws", "webrtc-signaling.v1", epoch, 0);
    var json = new ObjectMapper().readTree(new ObjectMapper().writeValueAsBytes(response));
    assertThat(json.path("directoryEpoch").isTextual()).isTrue();
    assertThat(json.path("directoryEpoch").textValue()).isEqualTo(Long.toString(epoch));
    assertThat(json.path("retryAfterMillis").isInt()).isTrue();
  }
}
