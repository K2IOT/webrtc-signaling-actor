package io.webrtc.signaling.identity;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;

@Configuration
public class IdentityConfiguration {
  @Bean
  Clock identityClock() {
    return Clock.systemUTC();
  }

  @Bean
  TestAccountRegistry testAccountRegistry(IdentityProperties properties) {
    return new TestAccountRegistry(properties);
  }

  @Bean
  RsaKeyMaterial rsaKeyMaterial(IdentityProperties properties, ResourceLoader resourceLoader) {
    return RsaKeyMaterial.load(
        resourceLoader.getResource("classpath:keys/private.pem"),
        resourceLoader.getResource("classpath:keys/public.pem"),
        properties.keyId());
  }
}
