# directory.json: the directory snapshot every plane reads (config/native-sources.md, "Directory
# snapshot"; spec 4.5). LOCAL_TEST_ONLY.
# Arguments: $cells [first, second] $split $gatewayIds {cell: id} $gatewayBoots {cell: uuid}.
# Buckets 0..split-1 belong to the first cell, split..16383 to the second: the same rule Task 4
# seeds into directory_bucket and bucket_authority. One gateway boot per enrolled gateway.
if ($cells | length) != 2 then error("directory.jq needs exactly two cells")
elif ($split | type) != "number" or $split < 1 or $split > 16383 or ($split | floor) != $split
then error("directory.jq split out of range")
else
  {
    schemaVersion: 1,
    qualification: "LOCAL_TEST_ONLY",
    buckets: [
      {first: 0, last: ($split - 1), cell: $cells[0], directoryEpoch: 1, storageEpoch: 1},
      {first: $split, last: 16383, cell: $cells[1], directoryEpoch: 1, storageEpoch: 1}
    ],
    gatewayBoots: ($cells | map({cell: ., gatewayId: $gatewayIds[.], bootId: $gatewayBoots[.]}))
  }
end
