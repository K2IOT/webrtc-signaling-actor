package io.webrtc.signaling.localplatform;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.webrtc.signaling.protocol.Identity.SessionKey;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Strictly parsed, immutable configuration of the LOCAL_TEST_ONLY platform process. Every failure
 * is reported as a bare {@link LocalPlatformFailure} category; no value or path is ever echoed, and
 * no {@code toString} exposes configured locations or credentials.
 */
public final class LocalPlatformConfig {

  static final String ENV_CONFIG = "SIGNALING_PLATFORM_CONFIG";
  static final String ENV_ACKNOWLEDGEMENT = "SIGNALING_LOCAL_ACKNOWLEDGEMENT";
  static final String ENV_ISSUER = "SIGNALING_IDENTITY_ISSUER";
  static final String ACKNOWLEDGEMENT_VALUE = "LOCAL_TEST_ONLY";

  private static final Duration CLOCK_VALIDITY = Duration.ofSeconds(2);
  private static final int MAX_CELLS = 8;
  private static final Pattern CELL_NAME = Pattern.compile("[a-z][a-z0-9-]{0,23}");
  // Same pattern as the signer (SourceSigner), which also satisfies both consumer patterns.
  private static final Pattern KEY_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
  private static final String OCTET = "(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])";
  private static final Pattern IPV4 = Pattern.compile(OCTET + "(?:\\." + OCTET + "){3}");
  private static final Pattern IPV6_CHARS = Pattern.compile("[0-9a-fA-F:.]{2,45}");
  private static final String JDBC_PREFIX = "jdbc:postgresql://";

  /**
   * The only driver properties a URL may carry. Anything else (credentials, and the pgjdbc
   * parameters that instantiate a configured class) is refused.
   */
  private static final Set<String> JDBC_PARAMETERS =
      Set.of(
          "sslmode",
          "sslrootcert",
          "sslcert",
          "sslkey",
          "connectTimeout",
          "socketTimeout",
          "ApplicationName");

  private static final long MAX_CONFIG_BYTES = 64 * 1024;
  private static final ObjectMapper MAPPER =
      JsonMapper.builder()
          .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .build();

  /** mTLS listener: bind address, port and the key material locations. */
  public record Listener(
      InetAddress bind, int port, Path keystore, Path keystorePasswordFile, Path clientTrust) {
    @Override
    public String toString() {
      return "Listener[redacted]";
    }
  }

  /** Health listener address. */
  public record Health(InetAddress bind, int port) {
    @Override
    public String toString() {
      return "Health[redacted]";
    }
  }

  /** Database connection; the password is only a file location. */
  public record Database(String jdbcUrl, String user, Path passwordFile) {
    @Override
    public String toString() {
      return "Database[redacted]";
    }
  }

  private final String issuer;
  private final Map<String, Long> cells;
  private final String keyId;
  private final Path signingKeyFile;
  private final Listener source;
  private final Listener admin;
  private final Health health;
  private final Database database;

  private LocalPlatformConfig(
      String issuer,
      Map<String, Long> cells,
      String keyId,
      Path signingKeyFile,
      Listener source,
      Listener admin,
      Health health,
      Database database) {
    this.issuer = issuer;
    this.cells = cells;
    this.keyId = keyId;
    this.signingKeyFile = signingKeyFile;
    this.source = source;
    this.admin = admin;
    this.health = health;
    this.database = database;
  }

  /** Loads and validates the configuration; throws {@link LocalPlatformFailure} on any problem. */
  public static LocalPlatformConfig load(Map<String, String> env) {
    if (!ACKNOWLEDGEMENT_VALUE.equals(env.get(ENV_ACKNOWLEDGEMENT))) {
      throw failure(LocalPlatformFailure.Category.ACKNOWLEDGEMENT);
    }
    String issuer = env.get(ENV_ISSUER);
    if (issuer == null || issuer.isBlank() || issuer.contains("${") || !boundedIssuer(issuer)) {
      throw failure(LocalPlatformFailure.Category.IDENTITY);
    }
    try {
      return parse(issuer, readConfig(env.get(ENV_CONFIG)));
    } catch (LocalPlatformFailure failure) {
      throw failure;
    } catch (IOException | RuntimeException invalid) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
  }

  /** The issuer must be accepted by the session key rules the events and the verifier apply. */
  private static boolean boundedIssuer(String issuer) {
    try {
      new SessionKey(issuer, "x");
      return true;
    } catch (RuntimeException invalid) {
      return false;
    }
  }

  /**
   * Test seam: a copy whose three listener ports are replaced by the given values, including 0
   * (ephemeral), which {@link #load} never accepts.
   */
  public LocalPlatformConfig withPorts(int sourcePort, int adminPort, int healthPort) {
    return new LocalPlatformConfig(
        issuer,
        cells,
        keyId,
        signingKeyFile,
        new Listener(
            source.bind(),
            sourcePort,
            source.keystore(),
            source.keystorePasswordFile(),
            source.clientTrust()),
        new Listener(
            admin.bind(),
            adminPort,
            admin.keystore(),
            admin.keystorePasswordFile(),
            admin.clientTrust()),
        new Health(health.bind(), healthPort),
        database);
  }

  public String issuer() {
    return issuer;
  }

  /** Cell name to storage epoch. */
  public Map<String, Long> cells() {
    return cells;
  }

  public String keyId() {
    return keyId;
  }

  public Path signingKeyFile() {
    return signingKeyFile;
  }

  public Listener source() {
    return source;
  }

  public Listener admin() {
    return admin;
  }

  public Health health() {
    return health;
  }

  public Database database() {
    return database;
  }

  /** Validity of a signed clock report; fixed and not configurable. */
  public Duration clockValidity() {
    return CLOCK_VALIDITY;
  }

  @Override
  public String toString() {
    return "LocalPlatformConfig[LOCAL_TEST_ONLY]";
  }

  private static JsonNode readConfig(String location) throws IOException {
    Path path = absolutePath(location);
    if (Files.size(path) > MAX_CONFIG_BYTES) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
    JsonNode root = MAPPER.readTree(Files.readAllBytes(path));
    if (root == null || !root.isObject()) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
    return root;
  }

  private static LocalPlatformConfig parse(String issuer, JsonNode root) {
    requireKeys(
        root, Set.of("cells", "keyId", "signingKey", "source", "admin", "health", "database"));
    Map<String, Long> cells = cells(root.get("cells"));
    String keyId = text(root, "keyId");
    if (!KEY_ID.matcher(keyId).matches()) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
    Path signingKey = absolutePath(text(root, "signingKey"));
    return new LocalPlatformConfig(
        issuer,
        cells,
        keyId,
        signingKey,
        listener(object(root, "source")),
        listener(object(root, "admin")),
        health(object(root, "health")),
        database(object(root, "database")));
  }

  private static Map<String, Long> cells(JsonNode node) {
    if (node == null || !node.isObject() || node.isEmpty() || node.size() > MAX_CELLS) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
    Map<String, Long> result = new LinkedHashMap<>();
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      JsonNode epoch = field.getValue();
      if (!CELL_NAME.matcher(field.getKey()).matches()
          || !epoch.isIntegralNumber()
          || !epoch.canConvertToLong()
          || epoch.asLong() < 1) {
        throw failure(LocalPlatformFailure.Category.CONFIG);
      }
      result.put(field.getKey(), epoch.asLong());
    }
    return Collections.unmodifiableMap(result);
  }

  private static Listener listener(JsonNode node) {
    requireKeys(node, Set.of("bind", "port", "keystore", "keystorePasswordFile", "clientTrust"));
    return new Listener(
        bind(text(node, "bind")),
        port(node.get("port")),
        absolutePath(text(node, "keystore")),
        absolutePath(text(node, "keystorePasswordFile")),
        absolutePath(text(node, "clientTrust")));
  }

  private static Health health(JsonNode node) {
    requireKeys(node, Set.of("bind", "port"));
    return new Health(bind(text(node, "bind")), port(node.get("port")));
  }

  private static Database database(JsonNode node) {
    requireKeys(node, Set.of("jdbcUrl", "user", "passwordFile"));
    String jdbcUrl = text(node, "jdbcUrl");
    if (!jdbcUrl.startsWith(JDBC_PREFIX) || jdbcUrl.contains("${") || !allowedQuery(jdbcUrl)) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
    return new Database(jdbcUrl, text(node, "user"), absolutePath(text(node, "passwordFile")));
  }

  /**
   * Checks the query part of a JDBC URL: each parameter is {@code name=value} with a non-empty
   * value, appears once and is on the allow-list. This keeps credentials out of the URL and keeps
   * the driver from instantiating a class named in it (the {@code password}, {@code *factory} and
   * {@code *classname} parameters are not on the list).
   */
  private static boolean allowedQuery(String jdbcUrl) {
    int mark = jdbcUrl.indexOf('?');
    if (mark < 0) {
      return true;
    }
    Set<String> seen = new HashSet<>();
    for (String parameter : jdbcUrl.substring(mark + 1).split("&", -1)) {
      int equals = parameter.indexOf('=');
      if (equals < 1 || equals == parameter.length() - 1) {
        return false;
      }
      String name = parameter.substring(0, equals);
      String lower = name.toLowerCase(Locale.ROOT);
      if (lower.equals("password")
          || lower.contains("factory")
          || lower.contains("classname")
          || !JDBC_PARAMETERS.contains(name)
          || !seen.add(name)) {
        return false;
      }
    }
    return true;
  }

  private static JsonNode object(JsonNode parent, String key) {
    JsonNode node = parent.get(key);
    if (node == null || !node.isObject()) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
    return node;
  }

  /** Requires exactly the expected key set: nothing missing, nothing unknown. */
  private static void requireKeys(JsonNode node, Set<String> expected) {
    if (node == null || !node.isObject()) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
    Set<String> actual = new HashSet<>();
    node.fieldNames().forEachRemaining(actual::add);
    if (!actual.equals(expected)) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
  }

  private static String text(JsonNode node, String key) {
    JsonNode value = node.get(key);
    if (value == null || !value.isTextual() || value.asText().isBlank()) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
    return value.asText();
  }

  private static int port(JsonNode node) {
    if (node == null
        || !node.isIntegralNumber()
        || !node.canConvertToInt()
        || node.asInt() < 1
        || node.asInt() > 65535) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
    return node.asInt();
  }

  /**
   * A strict dotted-quad IPv4 address (four decimal octets, no leading zeros, no short or hex
   * forms) or an IPv6 literal. Neither path can reach a name lookup: IPv4 is turned into bytes
   * here, and an IPv6 candidate is handed over in brackets, which the JDK only accepts as a
   * literal.
   */
  private static InetAddress bind(String literal) {
    try {
      InetAddress address;
      if (IPV4.matcher(literal).matches()) {
        address = InetAddress.getByAddress(ipv4Bytes(literal));
      } else if (literal.indexOf(':') >= 0 && IPV6_CHARS.matcher(literal).matches()) {
        address = InetAddress.getByName("[" + literal + "]");
      } else {
        throw failure(LocalPlatformFailure.Category.CONFIG);
      }
      if (address.isAnyLocalAddress()) {
        throw failure(LocalPlatformFailure.Category.CONFIG);
      }
      return address;
    } catch (UnknownHostException invalid) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
  }

  private static byte[] ipv4Bytes(String dottedQuad) {
    String[] octets = dottedQuad.split("\\.");
    byte[] bytes = new byte[4];
    for (int i = 0; i < 4; i++) {
      bytes[i] = (byte) Integer.parseInt(octets[i]);
    }
    return bytes;
  }

  private static Path absolutePath(String value) {
    if (value == null || value.isBlank()) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
    try {
      Path path = Path.of(value);
      if (!path.isAbsolute()) {
        throw failure(LocalPlatformFailure.Category.CONFIG);
      }
      return path;
    } catch (InvalidPathException invalid) {
      throw failure(LocalPlatformFailure.Category.CONFIG);
    }
  }

  private static LocalPlatformFailure failure(LocalPlatformFailure.Category category) {
    return new LocalPlatformFailure(category);
  }
}
