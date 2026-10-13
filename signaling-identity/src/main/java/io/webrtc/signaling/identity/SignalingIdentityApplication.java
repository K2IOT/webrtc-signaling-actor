package io.webrtc.signaling.identity;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(IdentityProperties.class)
public class SignalingIdentityApplication {
    public static void main(String[] args) {
        SpringApplication.run(SignalingIdentityApplication.class, args);
    }
}
