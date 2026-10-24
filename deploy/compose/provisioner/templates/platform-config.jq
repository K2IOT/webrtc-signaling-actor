# platform.json: LocalPlatformConfig of the local-platform process (config/native-sources.md,
# "Local platform source"). Exactly the keys cells, homeBuckets, keyId, signingKey, source, admin,
# health, database. LOCAL_TEST_ONLY / NOT_QUALIFIED.
# Arguments: $cells [names] $split $mount $bind $sourcePort $adminPort $healthPort $pgPort $dbHost $dbUser.
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
  # Home buckets per cell, the same split as directory.json (c001 0..split-1, c002 split..16383):
  # each cell's revocation page carries only its home users, as the native source contract requires.
  homeBuckets: {($cells[0]): [0, ($split - 1)], ($cells[1]): [$split, 16383]},
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
