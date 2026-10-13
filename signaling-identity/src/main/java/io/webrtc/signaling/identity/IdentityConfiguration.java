package io.webrtc.signaling.identity;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
}
