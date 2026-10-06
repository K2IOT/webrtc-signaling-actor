package io.webrtc.signaling.loadgen;

import static org.assertj.core.api.Assertions.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.file.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ScenarioInputTest {
    @TempDir Path directory;
    @ParameterizedTest @ValueSource(strings={
        "abuse: [retiredSigningKey]\nabuse: [malformed]\nabuseFraction: 1\n",
        "burst: {multiplier: 2, seconds: 60}\nburst: {multiplier: 1, seconds: 0}\n",
        "{\"name\":\"first\",\"name\":\"second\"}"
    }) void duplicateRequestedLabelsCannotBeReplacedBeforePreflight(String yaml)throws Exception {
        var scenario=directory.resolve("scenario.yaml");Files.writeString(scenario,yaml);
        var config=directory.resolve("config.json");Files.writeString(config,"{}");
        var runner=new ScenarioRunner();
        assertThatThrownBy(()->runner.run(scenario,config,directory.resolve("evidence")))
            .isInstanceOf(JsonProcessingException.class).hasMessageContaining("Duplicate field");
        var loops=ScenarioRunner.class.getDeclaredField("loops");loops.setAccessible(true);
        assertThat(loops.get(runner)).isNull();
    }
}
