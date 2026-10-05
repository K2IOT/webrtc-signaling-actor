# Original native drill receipts

Indexed JSON receipts retain the candidate/git/image and all five fingerprint
bindings, `testOnly:false`, `receiptVersion:1`, original `cell`, `drillId`,
`sourceIdentity` and `observedAt`. The original observation must be inside the
associated gate window. The approved external manifest signer attests source
enrollment and artifact authenticity; these checks do not establish infrastructure
measurements or authenticate an arbitrary collector by its name.

`faultTimeline` has `receiptType:FAULT_TIMELINE` and 3–4096 events. Each event
has contiguous integer `sequence` starting at1, UTC `at` inside the original
gate/observation window, and bounded uppercase `kind`. Times strictly increase.
The first event is `DRILL_STARTED`, the last `OBSERVATION_COMPLETED`, and an
intermediate `FAULT_INJECTED` records actual fault admission. Retain detailed
native events between these markers; a boolean gate checklist is insufficient.

`physicalFenceReceipt` has `receiptType:PHYSICAL_FENCE`, exact `oldWriter`
(`podUid`, `bootId`, native decimal `systemIdentifier`, integer `storageEpoch`),
`writeAccessRevoked:true` and an enrolled actual `fenceMethod`: `POWER_FENCED`,
`STORAGE_ACCESS_REVOKED` or `NETWORK_WRITE_ACCESS_REVOKED`. Original `fencedAt`
precedes `promotedAt`; promotion precedes or equals `trafficOpenedAt`, all before
`observedAt` and within the gate. A shutdown request or routing update cannot
supply a physical fence receipt.

`acknowledgedWalReceipt` has `receiptType:ACKNOWLEDGED_WAL`, exact original
`oldWriter`, PostgreSQL uppercase `acknowledgedWalLsn` and `recoveredWalLsn`,
positive integer `acknowledgedOperations` and matching `reconciledOperations`.
`recoveryMode:SYNCHRONOUS_FAILOVER` requires containment of every acknowledged
WAL record and zero `lostAcknowledgedOperations`. `recoveryMode:PITR_RESTORE`
can expose lost acknowledged history only with a bounded explicit
`lostAcknowledgedOperations` and `allRestoredNonterminalInvalidated:true`.
The latter does not grant an HA RPO exception or permit resuming restored calls.
Retain original operation-level reconciliation and independent WAL observations
in indexed artifacts; these numerical checks cannot prove their origin.

`outsideBackupEpochHighWaterReceipt` has `receiptType:OUTSIDE_BACKUP_EPOCH`,
`sourcePlacement:OUTSIDE_BACKUP`, positive integer `backupStorageEpoch`,
`outsideBackupHighWater` and `restoredStorageEpoch`. New authority exceeds both
original bounds. Preserve the actual independent enrolled source receipt; the
placement label alone cannot establish backup independence.

Ephemeral unit fixtures exercise parsing and rejection with disposable keys.
They are not collected evidence and never authorize production release.
