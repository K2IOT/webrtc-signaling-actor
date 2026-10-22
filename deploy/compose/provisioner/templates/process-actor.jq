# process.json of an actor (config/native-sources.md, "Process profile"). LOCAL_TEST_ONLY.
# Arguments: $podUid $podIp $businessPort $healthPort $relayPort $executableId $mount $cell
# $azRole $cellAzRoles [roles] $walSystemId $walTimeline $kubeHost $kubePort $namespace.
include "common";

profile("ACTOR"; $podUid; $podIp; $businessPort; $healthPort; $relayPort; $executableId)
+ {
  azRole: $azRole,
  cellAzRoles: $cellAzRoles,
  # Observed writer facts of the cell primary (Task 4 writes _shared/wal.env); never
  # localSingleWriter, which the adapter always sets to false.
  wal: {
    systemIdentifier: $walSystemId,
    timeline: $walTimeline,
    replicas: ["standby-" + $cell],
    synchronousReplicas: 1,
    maximumLagBytes: 16777216,
    freshness: "PT1S",
    maximumReplyAge: "PT5S"
  },
  kube: {
    apiHost: $kubeHost,
    apiPort: $kubePort,
    caPath: ($mount + "/kube-ca.crt"),
    tokenPath: ($mount + "/kube-token"),
    namespace: $namespace
  },
  privacyPseudonymKey: ($mount + "/privacy.key")
}
