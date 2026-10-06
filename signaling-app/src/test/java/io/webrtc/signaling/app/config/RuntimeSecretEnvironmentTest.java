package io.webrtc.signaling.app.config;

import static org.assertj.core.api.Assertions.*;
import java.nio.file.*;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.*;

class RuntimeSecretEnvironmentTest {
    @TempDir Path directory;
    private StandardEnvironment environment(String location) {
        var env=new StandardEnvironment();
        env.getPropertySources().addFirst(new MapPropertySource("TEST_ONLY_contract",Map.of("SIGNALING_RUNTIME_CONTRACT",location)));
        return env;
    }
    private void load(StandardEnvironment env) {
        new RuntimeSecretEnvironment().postProcessEnvironment(env,new SpringApplication());
    }
    @Test void readsActualMountedFileAndOverridesPackagedDefaults() throws Exception {
        var file=directory.resolve("runtime.yaml");
        Files.writeString(file,"signaling:\n  queues:\n    entity-messages: 32\n  identity:\n    issuer: TEST_ONLY_ISSUER\n");
        var env=environment(file.toString());
        env.getPropertySources().addLast(new MapPropertySource("Config resource TEST_ONLY_defaults",Map.of("signaling.queues.entity-messages",64)));
        load(env);
        assertThat(env.getProperty("signaling.queues.entity-messages",Integer.class)).isEqualTo(32);
        assertThat(env.getProperty("signaling.identity.issuer")).isEqualTo("TEST_ONLY_ISSUER");
    }
    @Test void absentContractKeepsExistingFailClosedNativeGuard() {
        var env=new StandardEnvironment();
        env.getPropertySources().remove("systemEnvironment");env.getPropertySources().remove("systemProperties");
        load(env);assertThat(env.getPropertySources().contains("native-runtime-secret")).isFalse();
    }
    @Test void deploymentPropertiesKeepBootPrecedence() throws Exception {
        var file=directory.resolve("runtime.yaml");Files.writeString(file,"signaling: {queues: {entity-messages: 32}}\n");
        var env=environment(file.toString());
        env.getPropertySources().addFirst(new MapPropertySource("TEST_ONLY_deployment",Map.of("signaling.queues.entity-messages",16)));
        load(env);assertThat(env.getProperty("signaling.queues.entity-messages",Integer.class)).isEqualTo(16);
    }
    @Test void rejectsExcessiveDepthAndKeys() throws Exception {
        var file=directory.resolve("runtime.yaml");
        Files.writeString(file,"signaling: "+"{a: ".repeat(18)+"TEST_ONLY"+"}".repeat(18));
        assertThatThrownBy(()->load(environment(file.toString()))).isInstanceOf(RuntimeSecretEnvironment.Rejected.class);
        var yaml=new StringBuilder("signaling:\n");for(int i=0;i<513;i++)yaml.append("  k").append(i).append(": TEST_ONLY\n");
        Files.writeString(file,yaml);
        assertThatThrownBy(()->load(environment(file.toString()))).isInstanceOf(RuntimeSecretEnvironment.Rejected.class);
    }
    @Test void explicitMissingOrRelativeOrEmptyPathFailsWithoutLeakingPath() {
        for(var location:new String[]{directory.resolve("TEST_ONLY_PRIVATE").toString(),"relative.yaml",""}) {
            assertThatThrownBy(()->load(environment(location)))
                .isInstanceOf(RuntimeSecretEnvironment.Rejected.class)
                .hasMessage("Runtime configuration rejected");
        }
    }
    @Test void configuredDirectoryIsNotAContract() {
        assertThatThrownBy(()->load(environment(directory.toString()))).isInstanceOf(RuntimeSecretEnvironment.Rejected.class);
    }
    @ParameterizedTest @ValueSource(strings={
        "signaling: {identity: {issuer: first, issuer: second}}",
        "signaling: {identity: {issuer: TEST_ONLY}}\n---\nsignaling: {}",
        "spring: {config: {import: 'https://TEST_ONLY_PRIVATE'}}",
        "signaling: &x {identity: *x}",
        "signaling: {identity: !!java.net.URL 'https://TEST_ONLY_PRIVATE'}",
        "signaling: {identity: [TEST_ONLY]}",
        "signaling: {identity: {issuer: null}}",
        "signaling: {identity.issuer: TEST_ONLY}"
    }) void rejectsAmbiguousOrExecutableOrNonScalarConfiguration(String yaml) throws Exception {
        var file=directory.resolve("runtime.yaml");Files.writeString(file,yaml);
        assertThatThrownBy(()->load(environment(file.toString())))
            .isInstanceOf(RuntimeSecretEnvironment.Rejected.class).hasMessage("Runtime configuration rejected");
    }
    @Test void rejectsOversizedAndInvalidUtf8BeforeParsing() throws Exception {
        var file=directory.resolve("runtime.yaml");
        Files.writeString(file,"#"+"x".repeat(65536));
        assertThatThrownBy(()->load(environment(file.toString()))).isInstanceOf(RuntimeSecretEnvironment.Rejected.class);
        Files.write(file,new byte[]{(byte)0xc3,(byte)0x28});
        assertThatThrownBy(()->load(environment(file.toString()))).isInstanceOf(RuntimeSecretEnvironment.Rejected.class);
    }
    @Test void acceptsKubernetesProjectedSecretSymlink() throws Exception {
        var target=directory.resolve("..data");Files.createDirectory(target);
        Files.writeString(target.resolve("runtime.yaml"),"signaling: {queues: {entity-messages: 32}}\n");
        var link=directory.resolve("runtime.yaml");Files.createSymbolicLink(link,Path.of("..data/runtime.yaml"));
        var env=environment(link.toString());load(env);
        assertThat(env.getProperty("signaling.queues.entity-messages")).isEqualTo("32");
    }
}
