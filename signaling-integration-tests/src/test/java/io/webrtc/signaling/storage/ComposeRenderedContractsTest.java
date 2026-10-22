package io.webrtc.signaling.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.webrtc.signaling.app.config.RuntimeSecretEnvironment;
import io.webrtc.signaling.app.runtime.MountedNativeContract;
import io.webrtc.signaling.app.runtime.local.ComposeContractProbe;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeContract;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeFailure;
import io.webrtc.signaling.app.runtime.local.LocalRuntimeInitializer;
import io.webrtc.signaling.localplatform.LocalPlatformConfig;
import io.webrtc.signaling.localplatform.SourceSigner;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

/**
 * The compose renderer's output, read by the real loaders (SP3 Task 3). Stage 1 of the provisioner
 * and {@code render.sh render --offline --mount-prefix <tree>} build the whole mount tree in a
 * temporary directory; every plane is then loaded the way {@code LocalRuntimeInitializer} loads it
 * at startup, with the environment the container gets ({@code env.list} plus what {@code launch.sh}
 * exports) and {@code runtime.yaml} applied as {@code SIGNALING_RUNTIME_CONTRACT}. Nothing opens a
 * database, a socket or an actor system. LOCAL_TEST_ONLY / NOT_QUALIFIED; no Docker.
 */
class ComposeRenderedContractsTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final List<String> TOOLS = List.of("bash", "openssl", "keytool", "jq");
  private static final Pattern LAUNCH_EXPORT =
      Pattern.compile("^export ([A-Z_]+)=([A-Za-z0-9.:-]+)$", Pattern.MULTILINE);
  private static final int BUCKETS = 16384;

  @TempDir static Path temp;
  private static Path repo;
  private static Path run;
  private static Map<String, String> topology;
  private static List<String> planes;

  @BeforeAll
  static void renderTree() throws Exception {
    requireTools();
    repo = repoRoot();
    run = Files.createDirectories(temp.toRealPath().resolve("run"));
    topology = readEnvFile(repo.resolve("deploy/compose/topology.env"));
    planes = planeServices();
    var environment = new LinkedHashMap<String, String>();
    environment.put("RUN_ROOT", run.toString());
    environment.put(
        "IDENTITY_PUBLIC_KEY",
        repo.resolve("signaling-identity/src/main/resources/keys/public.pem").toString());
    exec(environment, "bash", "deploy/compose/provisioner/provision.sh", "stage1");
    render(run);
  }

  @Test
  void everyPlaneStartsThroughTheRealLoaders() throws Exception {
    assertThat(planes).hasSize(15);
    for (var service : planes) {
      var env = environment(service);
      var contract = LocalRuntimeContract.load(env);
      var plane = env.getProperty("SPRING_PROFILES_ACTIVE");
      assertThat(contract.profile().plane().name().toLowerCase(Locale.ROOT))
          .as(service)
          .isEqualTo(plane);
      assertThat(contract.contract().cell())
          .as(service)
          .isEqualTo(env.getProperty("SIGNALING_CELL_ID"));
      assertThat(contract.contract().identity().issuer())
          .isEqualTo(env.getProperty("SIGNALING_IDENTITY_ISSUER"))
          .isEqualTo(env.getProperty("signaling.identity.issuer"))
          .isEqualTo(topology.get("ISSUER"));
      assertThat(contract.contract().identity().audience())
          .isEqualTo(env.getProperty("SIGNALING_IDENTITY_AUDIENCE"))
          .isEqualTo(env.getProperty("signaling.identity.audience"))
          .isEqualTo(topology.get("AUDIENCE"));
      assertThat(
              MountedNativeContract.load(
                  run.resolve(service).resolve("provider.json"),
                  "local-minikube",
                  "LOCAL_TEST_ONLY"))
          .isNotNull();
      // The plane installer's whole pre-registration path: TLS material, keys, snapshot, WAL,
      // Pekko config, compatibility, transfer cell, gateway identity.
      assertThat(install(service, env)).as(service).isPositive();
    }
  }

  @Test
  void snapshotDigestPinsTheExactBytesOfEveryCopy() throws Exception {
    for (var service : planes) {
      var bytes = Files.readAllBytes(run.resolve(service).resolve("directory.json"));
      var digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
      assertThat(provider(service).path("directorySnapshotSha256").asText())
          .as(service)
          .isEqualTo(digest);
    }
  }

  @Test
  void snapshotRangesFollowTheSplitRuleAndCoverEveryBucket() throws Exception {
    var split = Integer.parseInt(topology.get("SPLIT"));
    var snapshot = JSON.readTree(run.resolve("control").resolve("directory.json").toFile());
    var owner = new String[BUCKETS];
    for (var range : snapshot.path("buckets")) {
      for (int bucket = range.path("first").asInt();
          bucket <= range.path("last").asInt();
          bucket++) {
        assertThat(owner[bucket]).as("bucket %d covered once", bucket).isNull();
        owner[bucket] = range.path("cell").asText();
      }
    }
    for (int bucket = 0; bucket < BUCKETS; bucket++)
      assertThat(owner[bucket]).as("bucket %d", bucket).isEqualTo(bucket < split ? "c001" : "c002");
  }

  @Test
  void gatewayBootsAgreeAcrossProfileSnapshotAndProvisioning() throws Exception {
    var boots = readEnvFile(run.resolve("_shared/gateway-boots.env"));
    var expected = new TreeMap<String, UUID>();
    for (var cell : List.of("c001", "c002")) {
      var upper = cell.toUpperCase(Locale.ROOT);
      var boot = UUID.fromString(boots.get("GATEWAY_BOOT_" + upper));
      expected.put(cell + "/" + topology.get("GATEWAY_ID_" + upper), boot);
      var profile = JSON.readTree(run.resolve("gateway-" + cell).resolve("process.json").toFile());
      assertThat(profile.path("gatewayBootId").asText()).isEqualTo(boot.toString());
    }
    for (var service : planes)
      assertThat(ComposeContractProbe.gatewayBoots(LocalRuntimeContract.load(environment(service))))
          .as(service)
          .isEqualTo(expected);
  }

  @Test
  void podUidsAreTheProvisionedIdentities() throws Exception {
    var uids = readEnvFile(run.resolve("_shared/pod-uids.env"));
    for (var service : planes) {
      var expected = uids.get(service.toUpperCase(Locale.ROOT).replace('-', '_') + "_POD_UID");
      assertThat(expected).as(service).isNotBlank();
      var dir = run.resolve(service);
      assertThat(JSON.readTree(dir.resolve("process.json").toFile()).path("podUid").asText())
          .isEqualTo(expected);
      assertThat(readEnvFile(dir.resolve("pod.env"))).containsEntry("SIGNALING_POD_UID", expected);
      assertThat(readEnvFile(dir.resolve("env.list"))).containsEntry("SIGNALING_POD_UID", expected);
      assertThat(launchExports(service)).containsEntry("SIGNALING_POD_UID", expected);
    }
  }

  @Test
  void platformConfigLoadsAndItsKeySignsForThePlanes() throws Exception {
    var dir = run.resolve("local-platform");
    var config = LocalPlatformConfig.load(readEnvFile(dir.resolve("env.list")));
    assertThat(config.cells()).containsExactly(Map.entry("c001", 1L), Map.entry("c002", 1L));
    for (var file :
        List.of(
            config.source().keystore(),
            config.source().keystorePasswordFile(),
            config.source().clientTrust(),
            config.admin().clientTrust(),
            config.database().passwordFile(),
            config.signingKeyFile())) {
      assertThat(file).startsWith(dir).isRegularFile();
    }
    assertThat(config.source().clientTrust()).isNotEqualTo(config.admin().clientTrust());
    var signer = SourceSigner.load(config.signingKeyFile(), config.keyId());
    var message = "TEST_ONLY compose source key".getBytes(StandardCharsets.UTF_8);
    var signature = signer.sign(message);
    for (var service : planes) {
      var keys = provider(service).path("sources").path("publicKeys");
      assertThat(keys.has(config.keyId())).as(service).isTrue();
      var verifier = Signature.getInstance("Ed25519");
      verifier.initVerify(ed25519Public(Path.of(keys.path(config.keyId()).asText())));
      verifier.update(message);
      assertThat(verifier.verify(signature)).as(service).isTrue();
    }
  }

  @Test
  void envListAgreesWithTheContract() throws Exception {
    for (var service : planes) {
      var env = readEnvFile(run.resolve(service).resolve("env.list"));
      var contract = provider(service);
      var profile = JSON.readTree(run.resolve(service).resolve("process.json").toFile());
      assertThat(env.get("SIGNALING_CELL_ID")).isEqualTo(contract.path("cell").asText());
      assertThat(env.get("SIGNALING_IDENTITY_ISSUER"))
          .isEqualTo(contract.path("identity").path("issuer").asText());
      assertThat(env.get("SIGNALING_IDENTITY_AUDIENCE"))
          .isEqualTo(contract.path("identity").path("audience").asText());
      assertThat(env.get("SPRING_PROFILES_ACTIVE").toUpperCase(Locale.ROOT))
          .isEqualTo(profile.path("plane").asText());
      assertThat(env.keySet()).noneMatch(name -> name.contains("PASSWORD"));
    }
  }

  @Test
  void noRenderedFileCarriesAPlaceholder() throws Exception {
    var names =
        List.of(
            "provider.json",
            "process.json",
            "runtime.yaml",
            "directory.json",
            "env.list",
            "pod.env",
            "launch.sh",
            "platform.json");
    List<Path> rendered;
    try (Stream<Path> files = Files.walk(run)) {
      rendered = files.filter(file -> names.contains(file.getFileName().toString())).toList();
    }
    // Seven files per plane, plus platform.json and env.list of local-platform.
    assertThat(rendered).hasSize(15 * 7 + 2);
    for (var file : rendered)
      assertThat(Files.readString(file)).as(file.toString()).doesNotContain("${");
  }

  @Test
  void droppingOneProfileKeyIsProfileInvalid() throws Exception {
    var tree = copyAndRender("negative-profile");
    var profile = tree.resolve("actor-c001-2").resolve("process.json");
    var document = (ObjectNode) JSON.readTree(profile.toFile());
    document.remove("allowedOrigins");
    replace(profile, JSON.writeValueAsBytes(document));
    var env = environment(tree, "actor-c001-2");
    assertThatThrownBy(() -> LocalRuntimeContract.load(env))
        .isInstanceOfSatisfying(
            LocalRuntimeFailure.class,
            failure -> {
              assertThat(failure.role()).isEqualTo(LocalRuntimeFailure.Role.PROFILE);
              assertThat(failure.category()).isEqualTo(LocalRuntimeFailure.Category.INVALID);
            });
  }

  @Test
  void flippingOneSnapshotByteIsAContractDigestMismatch() throws Exception {
    var tree = copyAndRender("negative-snapshot");
    var snapshot = tree.resolve("gateway-c002").resolve("directory.json");
    var bytes = Files.readAllBytes(snapshot);
    // Still valid JSON (a digit of the split changes): only the digest can catch it.
    var text = new String(bytes, StandardCharsets.UTF_8);
    var at = text.indexOf("\"last\": ") + "\"last\": ".length();
    bytes[at] = (byte) (bytes[at] == '1' ? '2' : '1');
    replace(snapshot, bytes);
    var env = environment(tree, "gateway-c002");
    var contract = LocalRuntimeContract.load(env);
    assertThatThrownBy(() -> ComposeContractProbe.gatewayBoots(contract))
        .isInstanceOfSatisfying(
            LocalRuntimeFailure.class,
            failure -> {
              assertThat(failure.role()).isEqualTo(LocalRuntimeFailure.Role.CONTRACT);
              assertThat(failure.category()).isEqualTo(LocalRuntimeFailure.Category.INVALID);
            });
    try (var ctx = new GenericApplicationContext()) {
      ctx.setEnvironment(env);
      assertThatThrownBy(() -> new LocalRuntimeInitializer().initialize(ctx))
          .isInstanceOf(LocalRuntimeFailure.class);
    }
  }

  // ------------------------------------------------------------------------- loading

  /** The plane installer, as the initializer runs it; returns the bean definitions registered. */
  private static int install(String service, ConfigurableEnvironment env) throws IOException {
    if (service.startsWith("actor-")) {
      var process = new LinkedHashMap<String, String>(launchExports(service));
      return ComposeContractProbe.installActor(env, process::get);
    }
    try (var ctx = new GenericApplicationContext()) {
      ctx.setEnvironment(env);
      new LocalRuntimeInitializer().initialize(ctx);
      return ctx.getBeanDefinitionCount();
    }
  }

  private static ConfigurableEnvironment environment(String service) throws IOException {
    return environment(run, service);
  }

  /**
   * The container's environment and nothing of the test JVM's: env.list, what launch.sh exports
   * (the remoting passwords read from the mount, as launch.sh does), then runtime.yaml applied by
   * the real post-processor.
   */
  private static ConfigurableEnvironment environment(Path tree, String service) throws IOException {
    var dir = tree.resolve(service);
    var values = new LinkedHashMap<String, Object>(readEnvFile(dir.resolve("env.list")));
    values.putAll(launchExports(tree, service));
    if (service.startsWith("actor-")) {
      var password = Files.readString(dir.resolve("remoting.pass"));
      for (var name :
          List.of(
              "SIGNALING_REMOTING_KEYSTORE_PASSWORD",
              "SIGNALING_REMOTING_KEY_PASSWORD",
              "SIGNALING_REMOTING_TRUSTSTORE_PASSWORD")) {
        values.put(name, password);
      }
    }
    var env = new StandardEnvironment();
    env.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
    env.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
    env.getPropertySources().addFirst(new MapPropertySource("TEST_ONLY_container", values));
    env.setActiveProfiles(String.valueOf(values.get("SPRING_PROFILES_ACTIVE")));
    new RuntimeSecretEnvironment().postProcessEnvironment(env, null);
    return env;
  }

  /** The literal exports of launch.sh (the password lines read a file and are not literals). */
  private static Map<String, String> launchExports(String service) throws IOException {
    return launchExports(run, service);
  }

  private static Map<String, String> launchExports(Path tree, String service) throws IOException {
    var exports = new LinkedHashMap<String, String>();
    var matcher =
        LAUNCH_EXPORT.matcher(Files.readString(tree.resolve(service).resolve("launch.sh")));
    while (matcher.find()) exports.put(matcher.group(1), matcher.group(2));
    return exports;
  }

  private static JsonNode provider(String service) throws IOException {
    return JSON.readTree(run.resolve(service).resolve("provider.json").toFile());
  }

  private static java.security.PublicKey ed25519Public(Path pem) throws Exception {
    var body =
        Files.readString(pem)
            .replace("-----BEGIN PUBLIC KEY-----", "")
            .replace("-----END PUBLIC KEY-----", "")
            .replaceAll("\\s", "");
    return KeyFactory.getInstance("Ed25519")
        .generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(body)));
  }

  // ------------------------------------------------------------------------- the tree

  /** A copy of the stage-1 tree, rendered again so its paths point into the copy. */
  private static Path copyAndRender(String name) throws Exception {
    var tree = temp.toRealPath().resolve(name);
    exec(Map.of(), "cp", "-a", run.toString(), tree.toString());
    render(tree);
    return tree;
  }

  private static void render(Path tree) throws Exception {
    exec(
        Map.of("RUN_ROOT", tree.toString()),
        "bash",
        "deploy/compose/provisioner/lib/render.sh",
        "render",
        "--offline",
        "--mount-prefix",
        tree.toString());
  }

  /** Rendered files are 0440 in a 0750 directory: replace rather than write in place. */
  private static void replace(Path file, byte[] content) throws IOException {
    Files.delete(file);
    Files.write(file, content);
  }

  private static List<String> planeServices() {
    var services = new ArrayList<String>();
    for (var cell : List.of("c001", "c002"))
      for (int n = 1; n <= 6; n++) services.add("actor-" + cell + "-" + n);
    services.addAll(List.of("gateway-c001", "gateway-c002", "control"));
    return List.copyOf(services);
  }

  /** KEY=value lines; comments and blank lines skipped; surrounding single quotes dropped. */
  private static Map<String, String> readEnvFile(Path file) throws IOException {
    var values = new LinkedHashMap<String, String>();
    for (var line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
      if (line.isBlank() || line.startsWith("#") || !line.contains("=")) continue;
      var key = line.substring(0, line.indexOf('='));
      var value = line.substring(line.indexOf('=') + 1);
      if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'"))
        value = value.substring(1, value.length() - 1);
      values.put(key, value);
    }
    return values;
  }

  // ------------------------------------------------------------------------- processes

  private static void requireTools() throws Exception {
    var missing = new ArrayList<String>();
    for (var tool : TOOLS) {
      var probe = new ProcessBuilder("sh", "-c", "command -v \"$0\"", tool);
      probe.redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD);
      if (probe.start().waitFor() != 0) missing.add(tool);
    }
    if (missing.isEmpty()) return;
    if (System.getenv("CI") != null)
      fail("CI must provide the compose renderer's tools; missing: " + missing);
    Assumptions.abort("TEST_ONLY compose renderer tools missing: " + missing);
  }

  private static Path repoRoot() {
    var dir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
    for (var at = dir; at != null; at = at.getParent())
      if (Files.isRegularFile(at.resolve("deploy/compose/topology.env"))) return at;
    throw new IllegalStateException("repository root not found above " + dir);
  }

  /** Runs a command from the repository root; its output (file names only) is kept on failure. */
  private static void exec(Map<String, String> environment, String... command) throws Exception {
    var builder = new ProcessBuilder(command).directory(repo.toFile()).redirectErrorStream(true);
    builder.environment().putAll(environment);
    var log = Files.createTempFile(temp, "exec", ".log");
    builder.redirectOutput(log.toFile());
    var process = builder.start();
    if (!process.waitFor(5, TimeUnit.MINUTES)) {
      process.destroyForcibly();
      fail("timed out: " + String.join(" ", command));
    }
    if (process.exitValue() != 0)
      fail(
          "exit "
              + process.exitValue()
              + " from "
              + String.join(" ", command)
              + ":\n"
              + Files.readString(log));
  }
}
