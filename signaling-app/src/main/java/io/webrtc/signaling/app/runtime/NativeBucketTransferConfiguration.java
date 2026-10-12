package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.control.*;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.context.annotation.*;

@AutoConfiguration(before = NativeControlBusinessConfiguration.class)
@Profile("control")
@ConditionalOnBean(NativeBucketTransferEnrollment.class)
public class NativeBucketTransferConfiguration {
  @Bean
  PostgresBucketTransfer nativeBucketTransfer(NativeBucketTransferEnrollment inputs) {
    return new PostgresBucketTransfer(inputs.source(), inputs.target());
  }

  @Bean
  BucketTransferService nativeBucketTransferService(
      NativeControlBusinessEnrollment inputs, PostgresBucketTransfer transfer) {
    return new BucketTransferService(inputs.directory(), transfer);
  }
}
