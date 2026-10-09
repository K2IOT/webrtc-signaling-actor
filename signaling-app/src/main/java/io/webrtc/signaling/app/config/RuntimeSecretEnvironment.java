package io.webrtc.signaling.app.config;

import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.events.*;

/** Loads the explicitly mounted native configuration before application beans exist. */
public final class RuntimeSecretEnvironment implements EnvironmentPostProcessor, Ordered {
  private static final int MAX_BYTES = 65536;

  public static final class Rejected extends IllegalStateException {
    public Rejected() {
      super("Runtime configuration rejected");
    }
  }

  @Override
  public int getOrder() {
    return Ordered.HIGHEST_PRECEDENCE + 11;
  }

  @Override
  public void postProcessEnvironment(
      ConfigurableEnvironment environment, SpringApplication application) {
    String location = environment.getProperty("SIGNALING_RUNTIME_CONTRACT");
    if (location == null) return;
    try {
      Path path = Path.of(location);
      if (location.isBlank() || !path.isAbsolute() || !Files.isRegularFile(path))
        throw new Rejected();
      byte[] bytes;
      try (var input = Files.newInputStream(path)) {
        bytes = input.readNBytes(MAX_BYTES + 1);
      }
      if (bytes.length == 0 || bytes.length > MAX_BYTES) throw new Rejected();
      String text = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
      var options = new LoaderOptions();
      options.setAllowDuplicateKeys(false);
      options.setWarnOnDuplicateKeys(false);
      options.setMaxAliasesForCollections(0);
      options.setNestingDepthLimit(16);
      options.setCodePointLimit(MAX_BYTES);
      var yaml = new Yaml(new SafeConstructor(options));
      // Reject explicit tags and all anchors/aliases before construction, including scalar aliases.
      for (var event : yaml.parse(new StringReader(text))) {
        if (event instanceof AliasEvent
            || event instanceof NodeEvent node && node.getAnchor() != null
            || event instanceof ScalarEvent scalar && scalar.getTag() != null
            || event instanceof CollectionStartEvent collection && collection.getTag() != null)
          throw new Rejected();
      }
      Object document = yaml.load(text); // load requires exactly one document.
      if (!(document instanceof Map<?, ?> root)
          || root.size() != 1
          || !root.containsKey("signaling")) throw new Rejected();
      var values = new LinkedHashMap<String, Object>();
      flatten("", root, values, 0, new int[] {0});
      var source = new MapPropertySource("native-runtime-secret", Map.copyOf(values));
      // Deployment environment/CLI retain Boot precedence; the Secret overrides packaged defaults.
      String before = null;
      for (var existing : environment.getPropertySources()) {
        if (existing.getName().startsWith("Config resource ")) {
          before = existing.getName();
          break;
        }
      }
      if (before == null) environment.getPropertySources().addLast(source);
      else environment.getPropertySources().addBefore(before, source);
    } catch (Exception rejected) {
      // Parser exceptions can contain private values and file paths. Never retain them as causes.
      throw new Rejected();
    }
  }

  private static void flatten(
      String prefix, Object value, Map<String, Object> output, int depth, int[] nodes) {
    if (depth > 16 || ++nodes[0] > 512) throw new Rejected();
    if (value instanceof Map<?, ?> map) {
      if (map.isEmpty()) throw new Rejected();
      for (var entry : map.entrySet()) {
        if (!(entry.getKey() instanceof String key) || !key.matches("[a-z][a-z0-9-]{0,63}"))
          throw new Rejected();
        flatten(
            prefix.isEmpty() ? key : prefix + "." + key,
            entry.getValue(),
            output,
            depth + 1,
            nodes);
      }
    } else if (value instanceof String || value instanceof Number || value instanceof Boolean) {
      if (output.putIfAbsent(prefix, value) != null) throw new Rejected();
    } else throw new Rejected();
  }
}
