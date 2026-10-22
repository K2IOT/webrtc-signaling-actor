# process.json of control (config/native-sources.md, "Process profile"). LOCAL_TEST_ONLY.
# Arguments: $podUid $podIp $businessPort $healthPort $relayPort $executableId $mount $pgPort
# $transferCell $transferDbHost $transferDbUser; the password comes from the environment
# (RENDER_TRANSFER_DB_PASSWORD), never from argv.
# Control's own cell is c001; its one transfer cell is the other cell, with that cell's primary.
include "common";

profile("CONTROL"; $podUid; $podIp; $businessPort; $healthPort; $relayPort; $executableId)
+ {
  transferCells: [
    {
      cell: $transferCell,
      storageEpoch: 1,
      database: database(
        jdbc($transferDbHost; $pgPort; "signaling"; $mount);
        $transferDbUser;
        secret("RENDER_TRANSFER_DB_PASSWORD")
      )
    }
  ]
}
