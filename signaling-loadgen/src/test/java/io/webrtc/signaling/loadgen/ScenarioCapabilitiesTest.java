package io.webrtc.signaling.loadgen;
import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
class ScenarioCapabilitiesTest {
    final ObjectMapper json=new ObjectMapper();
    @Test void ordinaryTrafficAndImplementedTwoTimesBurstRemainAvailable(){var plain=json.createObjectNode();assertThatCode(()->ScenarioRunner.requireImplementedWorkload(plain)).doesNotThrowAnyException();plain.putObject("burst").put("multiplier",2).put("seconds",60);assertThatCode(()->ScenarioRunner.requireImplementedWorkload(plain)).doesNotThrowAnyException();}
    @Test void nativeFiveTimesSkewIsAvailableAndUnknownMultipliersFail(){for(var field:new String[]{"hotDestinationMultiplier","hotBucketMultiplier"}){var scenario=json.createObjectNode();scenario.putObject("burst").put("multiplier",2).put("seconds",60).put(field,5);assertThatCode(()->ScenarioRunner.requireImplementedWorkload(scenario)).doesNotThrowAnyException();scenario.withObject("burst").put(field,2);assertThatThrownBy(()->ScenarioRunner.requireImplementedWorkload(scenario)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("SKEW_PROFILE_NOT_IMPLEMENTED");}}
    @Test void everyUnimplementedSecurityModeFailsBeforeStartingOrdinaryWorker(){for(var mode:new String[]{"revokedJti","retiredSigningKey"}){var scenario=json.createObjectNode();scenario.putArray("abuse").add(mode);scenario.put("abuseFraction",.01);assertThatThrownBy(()->ScenarioRunner.requireImplementedWorkload(scenario)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ABUSE_PROFILE_NOT_IMPLEMENTED");}}
    @Test void nativeSlowConsumerProfileIsAvailable(){var scenario=json.createObjectNode();scenario.putArray("abuse").add("slowConsumer");scenario.put("abuseFraction",.01);assertThatCode(()->ScenarioRunner.requireImplementedWorkload(scenario)).doesNotThrowAnyException();}
    @Test void nativeStaleGenerationProfileIsAvailable(){var scenario=json.createObjectNode();scenario.putArray("abuse").add("staleGeneration");scenario.put("abuseFraction",.01);assertThatCode(()->ScenarioRunner.requireImplementedWorkload(scenario)).doesNotThrowAnyException();}
    @Test void nativeRawAbuseProfilesAreAvailable(){for(var mode:new String[]{"malformed","oversized"}){var scenario=json.createObjectNode();scenario.putArray("abuse").add(mode);scenario.put("abuseFraction",.01);assertThatCode(()->ScenarioRunner.requireImplementedWorkload(scenario)).doesNotThrowAnyException();}}
    @Test void invalidAbuseShapeCannotSilentlySelectOrdinaryTraffic()throws Exception {
        for(var text:new String[]{"{\"abuse\":\"malformed\",\"abuseFraction\":0.01}","{\"abuse\":[\"malformed\",\"malformed\"],\"abuseFraction\":0.01}","{\"abuse\":[\"malformed\"],\"abuseFraction\":0}","{\"abuse\":[\"malformed\"],\"abuseFraction\":1.1}","{\"abuse\":[\"malformed\"],\"abuseFraction\":\"0.01\"}","{\"abuseFraction\":0.01}"}) {
            var scenario=json.readTree(text);assertThatThrownBy(()->ScenarioRunner.requireImplementedWorkload(scenario)).isInstanceOf(IllegalArgumentException.class);
        }
    }
    @Test void skewOnlyProfilePreservesOriginalOpenLoopArrival()throws Exception{
        var runner=new ScenarioRunner();var scenario=json.createObjectNode();scenario.putObject("burst").put("hotBucketMultiplier",5);
        var field=ScenarioRunner.class.getDeclaredField("scenario");field.setAccessible(true);field.set(runner,scenario);
        var planned=ScenarioRunner.class.getDeclaredMethod("planned",long.class,long.class,long.class);planned.setAccessible(true);
        assertThat(planned.invoke(runner,123L,200L,100L)).isEqualTo(2_000_000_123L);
    }
    @Test void invalidArrivalBurstFailsBeforeWorkerPreparation(){
        for(var bad:new String[]{"{\"multiplier\":0,\"seconds\":60}","{\"multiplier\":2.5,\"seconds\":60}","{\"multiplier\":2,\"seconds\":-1}","{\"multiplier\":2,\"seconds\":\"60\"}"}){
            try {var scenario=json.createObjectNode();scenario.set("burst",json.readTree(bad));assertThatThrownBy(()->ScenarioRunner.requireImplementedWorkload(scenario)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Invalid burst");}catch(java.io.IOException invalid){throw new AssertionError(invalid);}
        }
    }
}
