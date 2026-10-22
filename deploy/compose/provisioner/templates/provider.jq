# provider.json: MountedNativeContract schema v1 for one plane process (spec 4.1). Field for field
# as the SP1 LocalCompositionFixture renders it. LOCAL_TEST_ONLY / NOT_QUALIFIED.
#
# Arguments (render.sh): $cell $mount $issuer $audience $pgPort $cellDbHost $cellDbUser
# $dirDbHost $dirDbUser $sourceBase $domain $snapshotSha $cells [names] $actorHosts {cell: host}
# $rpcPort $gatewayIds {cell: id} $gatewayHosts {cell: host} $relayPort. The two passwords come
# from the environment (RENDER_CELL_DB_PASSWORD, RENDER_DIR_DB_PASSWORD), never from argv.
#
# Every contract enrolls BOTH cells and BOTH gateways: the shared directory snapshot pins one boot
# per enrolled gateway (LocalDirectorySnapshot requires exactly one gatewayBoots entry for every
# contract gateway and no other), a gateway routes call commands to the other cell's actors, and
# an actor relays to a callee on the other cell's gateway and admits that gateway as an RPC peer.
include "common";

{
  schemaVersion: 1,
  qualification: "LOCAL_TEST_ONLY",
  cell: $cell,
  storageEpoch: 1,
  routingEpoch: 1,
  environment: "local-test-only",
  identity: {
    issuer: $issuer,
    audience: $audience,
    maximumJwtLifetime: "PT15M",
    clockSkew: "PT30S",
    revocationPropagationSLO: "PT1S",
    hardSafetyBound: "PT5S",
    preserveJtiOnRefresh: true,
    highWaterSource: "TEST_ONLY_LOCAL_SOURCE"
  },
  cellDatabase: database(
    jdbc($cellDbHost; $pgPort; "signaling"; $mount); $cellDbUser; secret("RENDER_CELL_DB_PASSWORD")
  ),
  directoryDatabase: database(
    jdbc($dirDbHost; $pgPort; "directory"; $mount); $dirDbUser; secret("RENDER_DIR_DB_PASSWORD")
  ),
  tls: {
    ca: ($mount + "/ca.crt"),
    certificate: ($mount + "/leaf.crt"),
    privateKey: ($mount + "/leaf.key"),
    # Present on actors only; gateway and control never open the remoting stores.
    remotingKeyStore: ($mount + "/remoting.p12"),
    remotingTrustStore: ($mount + "/trust.p12")
  },
  sources: {
    clockUrl: ($sourceBase + "/v1/clock-bound"),
    revocationUrl: ($sourceBase + "/v1/revocations"),
    publicKeys: {"local-source": ($mount + "/source.pub")}
  },
  proofs: {
    keyId: "local",
    # Present on actors only: the gateway verifies relay proofs with public keys and never opens
    # the signing key (LocalTrust.relayVerifier); control signs no proof.
    privateKey: ($mount + "/proof.key"),
    publicKeys: ($cells | map({key: (. + "/local"), value: ($mount + "/proof-" + . + ".pub")}) | from_entries)
  },
  jwtPublicKeys: {"signaling-identity-key": ($mount + "/jwt.pub")},
  cells: ($cells | map({
    cell: .,
    host: $actorHosts[.],
    port: $rpcPort,
    tlsAuthority: ("actor." + . + "." + $domain),
    storageEpoch: 1
  })),
  gateways: ($cells | map({
    cell: .,
    gatewayId: $gatewayIds[.],
    host: $gatewayHosts[.],
    port: $relayPort,
    tlsAuthority: ("gateway." + . + "." + $domain)
  })),
  directorySnapshot: ($mount + "/directory.json"),
  directorySnapshotSha256: $snapshotSha
}
