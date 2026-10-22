# platform.json: LocalPlatformConfig of the local-platform process (config/native-sources.md,
# "Local platform source"). Exactly the keys cells, keyId, signingKey, source, admin, health,
# database. LOCAL_TEST_ONLY / NOT_QUALIFIED.
# Arguments: $cells [names] $mount $bind $sourcePort $adminPort $healthPort $pgPort $dbHost $dbUser.
# The database password is a file (db.pass) beside this one, never a value here.
include "common";

def listener($port; $trust):
  {
    bind: $bind,
    port: $port,
    keystore: ($mount + "/platform.p12"),
    keystorePasswordFile: ($mount + "/platform.pass"),
    clientTrust: ($mount + "/" + $trust)
  };

{
  cells: ($cells | map({key: ., value: 1}) | from_entries),
  # The key id the planes enroll under sources.publicKeys.
  keyId: "local-source",
  signingKey: ($mount + "/source.key"),
  # Workload callers (CA-workload); the admin listener trusts only the separate admin CA.
  source: listener($sourcePort; "source-client-trust.crt"),
  admin: listener($adminPort; "admin-ca.crt"),
  health: {bind: $bind, port: $healthPort},
  database: {
    jdbcUrl: jdbc($dbHost; $pgPort; "source"; $mount),
    user: $dbUser,
    passwordFile: ($mount + "/db.pass")
  }
}
