# Native storage and deletion ownership

The storage plane owns bounded retirement. The security platform owns issuer lifetime, revocation freshness and accepted issuance policy. The deployment owner supplies approved optional-data and backup retention. Unconfigured policy keeps business readiness closed; this document does not invent a legal retention period.

| Native table / export | Purpose | Retention / deletion mechanism |
|---|---|---|
| cell_authority, bucket_authority | Storage and transfer fencing | Keep current native epochs; restore advances storage epoch before serving. Never privacy-delete a fence. |
| group_owner | Root tenure and renewal high-water | Keep the compact row across idle tenures; native CAS replaces tenure, not its epoch history. |
| user_guard | Native serialization key | Keep while session, participation or call references require it. Contains no optional profile. |
| gateway_lease | Boot identity and expiration barrier | Retain expired boot identity until configured stale-operation / token policy permits guarded retirement; absence never authorizes a boot resurrection. |
| session_registry | Issuer/jti/user and incarnation/generation binding | Preserve closed binding through maximum accepted token lifetime, skew and stale-message window. Recreated rows allocate a new incarnation. Never delete a current route for privacy completion. |
| user_reservation | Current compact live reservation | Native release / expired replacement removes the exact reservation under home barriers while retaining participation history. |
| home_participation | Acquisition, immutable winner, release-before-reserve barrier | RetentionWorker deletes terminal history only after native expiry (at least 24h + safety margin), under the owning user bucket barrier. |
| call_state | Current workflow and terminal replay snapshot | RetentionWorker deletes terminal rows after native expiry and after referenced scoped results are retired. Active rows remain; no SDP/ICE bodies are stored. |
| call_workflow_step | Bounded saga step replay | At most 16 rows/call; deletes cascade only with safe call retirement. |
| command_result | Scoped request replay and original acquisition hash | FINAL rows require at least 24h; PENDING has no guessed expiry. Live-call results and terminal-call results inside the call replay window remain even if their own expiry has passed. |
| control_outbox | Committed control notification | Retry authenticated application receipt independently of socket write; expired events are quarantined with a reason and safely retired. Poison events require operator validation before replay. |
| security_epoch, security_progress, retired_signing_key | Native revocation / key-retirement and authenticated source high-water | Keep security fences across reconnect and restore. Duplicate historical pages cannot refresh freshness. Retire only against an authoritative issuer policy, never because a stream lookup is empty. |
| privacy_deletion_request | Opaque deletion and restore-reapplication marker | HMAC subject key; detach optional data using the configured physical-completion port, retain minimal safety/replay identifiers. Tombstone prevents optional data from being silently reintroduced. |
| Metrics / traces / optional exports | Operations and optional analytics | Bounded labels, privacy-safe structured events; no raw JWT, SDP, ICE or TURN credentials. Deployment policy owns export retention. |
| Backups / PITR / debug captures | Recovery and controlled investigation | Approved deployment retention; no in-place mutation of backups. Reapply privacy / security state before a restored writer serves traffic. Debug capture is disabled by default. |

Maintenance uses a separate nonborrowable DbClass.MAINTENANCE quota. Recovery uses DbClass.RECOVERY; outbox uses DbClass.OUTBOX. Hints, scan cursors and claim leases are work-discovery state and grant no call ownership. The fixed scan upper key bounds a sweep under concurrent insertion; rows inserted behind the cursor are covered by the following full sweep. Report actual sweep duration and oldest-unreconciled age; the six-row local test does not qualify the production 5s target.
