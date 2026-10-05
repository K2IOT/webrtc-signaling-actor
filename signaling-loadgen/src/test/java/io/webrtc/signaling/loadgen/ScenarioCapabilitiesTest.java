package io.webrtc.signaling.loadgen;
import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
class ScenarioCapabilitiesTest {
    final ObjectMapper json=new ObjectMapper();
    @Test void ordinaryTrafficAndImplementedTwoTimesBurstRemainAvailable(){var plain=json.createObjectNode();assertThatCode(()->ScenarioRunner.requireImplementedWorkload(plain)).doesNotThrowAnyException();plain.putObject("burst").put("multiplier",2).put("seconds",60);assertThatCode(()->ScenarioRunner.requireImplementedWorkload(plain)).doesNotThrowAnyException();}
    @Test void nativeFiveTimesSkewIsAvailableAndUnknownMultipliersFail(){for(var field:new String[]{"hotDestinationMultiplier","hotBucketMultiplier"}){var scenario=json.createObjectNode();scenario.putObject("burst").put("multiplier",2).put("seconds",60).put(field,5);assertThatCode(()->ScenarioRunner.requireImplementedWorkload(scenario)).doesNotThrowAnyException();scenario.withObject("burst").put(field,2);assertThatThrownBy(()->ScenarioRunner.requireImplementedWorkload(scenario)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("SKEW_PROFILE_NOT_IMPLEMENTED");}}
    @Test void everyUnimplementedSecurityModeFailsBeforeStartingOrdinaryWorker(){for(var mode:new String[]{"malformed","slowConsumer","oversized","staleGeneration","revokedJti","retiredSigningKey"}){var scenario=json.createObjectNode();scenario.putArray("abuse").add(mode);scenario.put("abuseFraction",.01);assertThatThrownBy(()->ScenarioRunner.requireImplementedWorkload(scenario)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ABUSE_PROFILE_NOT_IMPLEMENTED");}}
}
