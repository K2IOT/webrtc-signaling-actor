package io.webrtc.signaling.app.runtime;

import io.webrtc.signaling.app.*;
import io.webrtc.signaling.app.config.SignalingProperties;
import io.webrtc.signaling.control.NativeBootstrapErrors;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory;
import org.springframework.context.annotation.*;
import org.springframework.http.client.ReactorResourceFactory;
import reactor.netty.resources.LoopResources;

/** Main's control HTTP server requires explicit TLS; bounded native loops are process-owned. */
@AutoConfiguration(
    after = NativeControlBusinessConfiguration.class,
    before =
        org.springframework.boot.autoconfigure.web.reactive
            .ReactiveWebServerFactoryAutoConfiguration.class)
@Profile("control")
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
public class NativeControlIngressConfiguration {
  @Bean
  ReactorResourceFactory nativeControlHttpResources() {
    var resources = new ReactorResourceFactory();
    resources.setUseGlobalResources(false);
    resources.setLoopResourcesSupplier(() -> LoopResources.create("control-http", 1, 2, true));
    return resources;
  }

  @Bean
  NettyReactiveWebServerFactory nativeControlHttps(
      ObjectProvider<NativeControlIngressEnrollment> enrollment,
      ObjectProvider<NativeControlBusinessEnrollment> business,
      ObjectProvider<NativeControlProcess> process,
      SignalingProperties properties,
      ReactorResourceFactory resources) {
    var inputs = enrollment.getIfAvailable();
    if (inputs == null || business.getIfAvailable() == null)
      throw NativeRuntimeStartup.missing(SignalingApplication.Plane.CONTROL);
    if (process.getIfAvailable() == null)
      throw NativeRuntimeStartup.missing(SignalingApplication.Plane.CONTROL);
    var factory = new NettyReactiveWebServerFactory(inputs.address().getPort());
    factory.setAddress(inputs.address().getAddress());
    factory.setResourceFactory(resources);
    factory.addServerCustomizers(
        server ->
            server
                .secure(ssl -> ssl.sslContext(inputs.tls()))
                .httpRequestDecoder(
                    decoder -> decoder.maxInitialLineLength(2048).maxHeaderSize(12288)));
    return factory;
  }

  @Bean(destroyMethod = "")
  PrivateHealthServer nativeControlHealth(
      NativeControlIngressEnrollment inputs,
      NativeControlReadiness readiness,
      NativeControlProcess process)
      throws Exception {
    var health =
        new PrivateHealthServer(
            inputs.healthAddress(), inputs.live(), readiness::ready, inputs.metrics());
    process.installHealth(health);
    try {
      health.start().toCompletableFuture().get(2, TimeUnit.SECONDS);
      return health;
    } catch (Exception failed) {
      health.stop().toCompletableFuture().get(5, TimeUnit.SECONDS);
      throw failed;
    }
  }

  @Bean
  NativeBootstrapErrors nativeControlAuthenticationErrors() {
    return new NativeBootstrapErrors();
  }
}
