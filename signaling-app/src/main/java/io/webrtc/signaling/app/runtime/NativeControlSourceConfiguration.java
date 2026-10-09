package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.ClockSafetyMonitor;
import java.time.*;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.*;

/** Native attested HTTPS clock only; scoped security ingestion remains independently enrolled. */
@AutoConfiguration(before = NativeControlBusinessConfiguration.class)
@Profile("control")
@ConditionalOnBean(NativeControlSourceEnrollment.class)
public class NativeControlSourceConfiguration {
  @Bean
  ClockSafetyMonitor nativeControlClockMonitor(
      NativeControlSourceEnrollment inputs,
      ObjectProvider<NativeControlDatabaseEnrollment> databases) {
    var database = databases.getIfAvailable();
    if (database != null
        && (!database.cell().equals(inputs.cell())
            || database.storageEpoch() != inputs.storageEpoch()))
      throw new IllegalArgumentException(
          "Control clock authority differs from database enrollment");
    return new ClockSafetyMonitor(
        inputs.cell(),
        inputs.storageEpoch(),
        inputs.podUid(),
        inputs.processBoot(),
        inputs.clock().keys(),
        Clock.systemUTC(),
        System::nanoTime);
  }

  @Bean(destroyMethod = "")
  NativeClockSource nativeControlClockSource(
      NativeControlSourceEnrollment inputs, ClockSafetyMonitor monitor) {
    return new NativeClockSource(
        inputs.clock().uri(), inputs.clock().tls(), monitor, inputs.podUid(), inputs.processBoot());
  }

  @Bean(destroyMethod = "")
  NativeWorkerScheduler nativeControlClockWorkers(
      NativeControlSourceEnrollment inputs, NativeClockSource source) {
    return new NativeWorkerScheduler(List.of(source.job(Duration.ofMillis(100))), inputs.events());
  }
}
