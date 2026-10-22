# process.json of a gateway (config/native-sources.md, "Process profile"). LOCAL_TEST_ONLY.
# Arguments: $podUid $podIp $businessPort $healthPort $relayPort $executableId $gatewayId
# $gatewayBootId $origin.
include "common";

profile("GATEWAY"; $podUid; $podIp; $businessPort; $healthPort; $relayPort; $executableId)
+ {
  gatewayId: $gatewayId,
  # Pinned per provisioning run; must equal this gateway's entry in directory.json.
  gatewayBootId: $gatewayBootId,
  allowedOrigins: [$origin]
}
