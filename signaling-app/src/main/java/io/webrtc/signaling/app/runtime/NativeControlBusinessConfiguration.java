package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.auth.BoundedTokenVerifier;
import io.webrtc.signaling.control.*;
import io.webrtc.signaling.storage.SqlTransactions;
import java.time.Clock;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.context.annotation.*;

@AutoConfiguration
@Profile("control")
@ConditionalOnBean(NativeControlBusinessEnrollment.class)
public class NativeControlBusinessConfiguration {
  @Bean
  DirectoryService nativeControlDirectory(NativeControlBusinessEnrollment inputs) {
    return new DirectoryService(inputs.directory(), 16384, inputs.directoryRefresh());
  }

  @Bean(destroyMethod = "")
  @ConditionalOnMissingBean(BoundedTokenVerifier.class)
  BoundedTokenVerifier nativeControlTokens(NativeControlBusinessEnrollment inputs) {
    return inputs.tokens();
  }

  @Bean
  BootstrapController nativeControlBootstrap(
      NativeControlBusinessEnrollment inputs,
      DirectoryService directory,
      NativeControlReadiness readiness,
      BoundedTokenVerifier tokens,
      NativeTelemetry telemetry) {
    if (tokens != inputs.tokens())
      throw new IllegalArgumentException("Native control verifier ownership differs");
    return new BootstrapController(
            directory, tokens, inputs.cachedSecurity(), Clock.systemUTC(), readiness::safe)
        .observe(telemetry);
  }

  @Bean
  NativeControlReadiness nativeControlReadiness(
      NativeControlProcess process,
      NativeCompatibility compatibility,
      NativeControlBusinessEnrollment inputs) {
    var readiness = process.readiness();
    readiness.compatibility(() -> compatibility.controlReady());
    return readiness;
  }

  @Bean
  NativeControlProcess nativeControlProcess(
      NativeControlBusinessEnrollment inputs,
      BoundedTokenVerifier tokens,
      ObjectProvider<SqlTransactions> sql,
      ObjectProvider<NativeWorkerScheduler> workers,
      ObjectProvider<NativeClockSource> clocks,
      ObjectProvider<NativeRevocationSource> revocations,
      ObjectProvider<NativeCellHealthSource> primaries,
      ObjectProvider<NativeCachedRevocationSource> cachedSources,
      ObjectProvider<PostgresBucketTransfer> transfers) {
    return new NativeControlProcess(
        inputs,
        tokens,
        sql.orderedStream().toList(),
        workers.orderedStream().toList(),
        clocks.orderedStream().toList(),
        revocations.orderedStream().toList(),
        primaries.orderedStream().toList(),
        cachedSources.orderedStream().toList(),
        transfers.getIfAvailable());
  }
}
