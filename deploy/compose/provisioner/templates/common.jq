# Shared definitions of the contract templates. LOCAL_TEST_ONLY / NOT_QUALIFIED.
# Loaded with `jq -L <templates> 'include "common"; ...'` by provisioner/lib/render.sh.
# Values come from --arg/--argjson only; every path and URL is built here, never in shell.

# The JDBC URL of database <db> on <host>. Only sslmode and sslrootcert are set: the app's
# contract loader requires sslmode=verify-full, and the local platform admits only allow-listed
# parameters (config/native-sources.md, "Local platform source"). pgjdbc verify-full checks the
# host against the server certificate; stage 1 issues the IP SANs (spec 4.6).
def jdbc($host; $port; $db; $mount):
  "jdbc:postgresql://" + $host + ":" + ($port | tostring) + "/" + $db
  + "?sslmode=verify-full&sslrootcert=" + $mount + "/db-ca.crt";

# A secret handed over in the environment of the jq process (never on its command line).
def secret($name):
  $ENV[$name] as $value
  | if ($value | type) == "string" and ($value | length) > 0 then $value
    else error("missing secret " + $name) end;

# A database enrollment with count-only quotas; the parser derives the LOCAL_TEST_ONLY resource
# profiles (same counts as the SP1 LocalCompositionFixture).
def database($url; $user; $password):
  {
    jdbcUrl: $url,
    username: $user,
    password: $password,
    quotas: {
      CRITICAL: 8,
      NORMAL: 2,
      OUTBOX: 2,
      MAINTENANCE: 2,
      RENEWAL: 6,
      TERMINATION: 2,
      RECOVERY: 6
    },
    safetyMax: 14,
    controlMax: 14
  };

# The build identity every plane carries (major 1, minor 1, window 0..1), with the feature set
# of the SP1 fixture.
def compatibility($executableId):
  {
    major: 1,
    minor: 1,
    executableId: $executableId,
    features: [
      "session-s1",
      "full-connection-active",
      "causal-route-loss",
      "native-renewal-v1",
      "native-recovery-receipt-v1",
      "native-wake-receipt-v1",
      "native-control-receipt-v1",
      "home-proof-high-water-v1",
      "storage-domain-adoption-v1",
      "native-actor-trace-v1",
      "native-initial-ice-receipt-v1"
    ],
    lowerMinor: 0,
    upperMinor: 1
  };

# Every key of the process profile, in the documented order, with the optional ones null or
# empty; each plane template overrides what it owns.
def profile($plane; $podUid; $podIp; $businessPort; $healthPort; $relayPort; $executableId):
  {
    schemaVersion: 1,
    plane: $plane,
    podUid: $podUid,
    podIp: $podIp,
    businessPort: $businessPort,
    healthPort: $healthPort,
    relayPort: $relayPort,
    azRole: null,
    cellAzRoles: [],
    gatewayId: null,
    gatewayBootId: null,
    wal: null,
    compatibility: compatibility($executableId),
    kube: null,
    transferCells: [],
    privacyPseudonymKey: null,
    allowedOrigins: []
  };
