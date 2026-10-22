package io.webrtc.signaling.app.runtime.local;

import io.webrtc.signaling.app.SignalingApplication.Plane;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * TEST_ONLY bridge for {@code ComposeRenderedContractsTest}: reaches the package-private entry
 * points of the local runtime adapter that the compose-rendered files must satisfy. It sits in the
 * adapter's package name on the test classpath only and ships nowhere.
 *
 * <p>Nothing here opens a database, a socket or an actor system: the installers open databases only
 * inside bean suppliers, and the context they register into is never refreshed.
 */
public final class ComposeContractProbe {
  private ComposeContractProbe() {}

  /**
   * What {@link LocalRuntimeInitializer#initialize} does for an actor, with the OS process
   * environment injected: the adapter requires the Kubernetes API address in the real process
   * environment, which a test JVM cannot set. Returns the number of registered bean definitions.
   */
  public static int installActor(
      ConfigurableEnvironment env, Function<String, String> processEnvironment) {
    try (var ctx = new GenericApplicationContext()) {
      ctx.setEnvironment(env);
      var contract = LocalRuntimeContract.load(env);
      if (contract.profile().plane() != Plane.ACTOR)
        throw new IllegalStateException("TEST_ONLY: not an actor profile");
      LocalActorInstaller.install(
          ctx,
          contract,
          processEnvironment,
          database -> {
            throw new IllegalStateException("TEST_ONLY: no database is opened offline");
          });
      return ctx.getBeanDefinitionCount();
    }
  }

  /**
   * The directory snapshot as every installer loads it (hash over the exact bytes, then strict
   * parsing): the pinned gateway boots, keyed {@code cell/gatewayId}.
   */
  public static Map<String, UUID> gatewayBoots(LocalRuntimeContract contract) {
    var boots = new TreeMap<String, UUID>();
    LocalDirectorySnapshot.load(contract)
        .targets()
        .forEach(target -> boots.put(target.cell() + "/" + target.gatewayId(), target.bootId()));
    return boots;
  }
}
