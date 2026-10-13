package io.webrtc.signaling.identity;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IdentityStartupTest {
    private static IdentityProperties validProperties() {
        return new IdentityProperties(
                "http://localhost:8081",
                "webrtc-signaling",
                Duration.ofMinutes(15),
                "test-key-1",
                List.of(new IdentityProperties.Account("alice", "alice123", "alice")));
    }

    private static String causeMessages(Throwable error) {
        StringBuilder messages = new StringBuilder();
        for (Throwable current = error; current != null; current = current.getCause()) {
            messages.append(current.getMessage()).append('\n');
        }
        return messages.toString();
    }

    @Test
    void rejectsInvalidIdentityConfigurationDuringStartup() {
        new ApplicationContextRunner()
                .withUserConfiguration(InvalidIdentityConfiguration.class)
                .run(
                        context -> {
                            assertThat(context).hasFailed();
                            assertThat(causeMessages(context.getStartupFailure()))
                                    .contains("token TTL must be positive whole seconds");
                        });
    }

    @Test
    void rejectsMissingKeyResourcesDuringStartup() {
        new ApplicationContextRunner()
                .withUserConfiguration(IdentityConfiguration.class, MissingKeyResources.class)
                .withBean(IdentityProperties.class, IdentityStartupTest::validProperties)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .run(
                        context -> {
                            assertThat(context).hasFailed();
                            assertThat(causeMessages(context.getStartupFailure()))
                                    .contains("Unable to load configured RSA key pair");
                        });
    }

    @Configuration(proxyBeanMethods = false)
    static class InvalidIdentityConfiguration {
        @Bean
        IdentityProperties identityProperties() {
            return new IdentityProperties(
                    "http://localhost:8081",
                    "webrtc-signaling",
                    Duration.ZERO,
                    "test-key-1",
                    List.of(new IdentityProperties.Account("alice", "alice123", "alice")));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MissingKeyResources {
        @Bean
        ResourceLoader resourceLoader() {
            return new DefaultResourceLoader() {
                @Override
                public org.springframework.core.io.Resource getResource(String location) {
                    return new org.springframework.core.io.ClassPathResource("keys/missing.pem");
                }
            };
        }
    }
}
