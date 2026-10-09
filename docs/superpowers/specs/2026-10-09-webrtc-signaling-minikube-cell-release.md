# Minikube CELL release — local qualification amendment

The production architecture and contracts in `2026-10-03-webrtc-signaling-10m-design-v1.11-production.md` remain authoritative. The user authorized a complete local release on 2026-10-09 with a 1,000-client campaign and explicitly deferred large stress, spike, 24-hour soak, physical AZ and HA qualification.

## Required behavior

Deploy actual actor, gateway and control planes, cell PostgreSQL, separate regional directory PostgreSQL, and an enrolled local identity/clock/revocation authority. Each cell retains six actors, minimum four Up members, native Kubernetes discovery, lease/SBR/fencing, protected SQL pools, durable sagas/outbox, authenticated internal TLS and WSS, bounded admission, and physical shutdown ownership. Cross-cell tests require a second independently fenced cell; provision it after measuring the first cell within the available 2 CPU / 8 GiB budget.

Local identity, certificates, keys, source logs and enrollment inputs are generated as `LOCAL_TEST_ONLY`, stored in ignored private artifacts and Kubernetes Secrets, and never represented as production enrollment. The source must produce signed observations from actual clocks and an ordered durable revocation log. Gateway/control caches start unknown and fail closed on stale, invalid or incomplete feeds. No constant healthy callbacks or fictitious AZ labels are permitted.

An explicit local deployment mode, requiring a `LOCAL_TEST_ONLY` acknowledgement, permits same-node placement, one gateway/control replica, measured smaller JVM/resources, and one actual failure domain (`az-local`). Production defaults and three-AZ validation remain unchanged. Local readiness still requires four real Up actors and all existing safety facts; only its physical failure-domain minimum is one. Network policies retain namespace/cell isolation and authorize each actual SQL, source, control-delivery and relay path.

Main must compose the mounted enrollment itself. A separate typed provider contract is required because the existing runtime YAML accepts only bounded typed scalar signaling properties. Provider parsing, all source/cache owners, internal gateway event delivery, application `EVENT_RECEIVED` acknowledgements and outbox maintenance must be implemented and tested before claiming serving readiness.

## Acceptance

Record exact image, configuration, compatibility and enrollment fingerprints. All required pods and SQL authorities must be ready. Exercise authenticated bootstrap/WSS, local and cross-cell call setup, relay, hangup, replay/idempotency, competing sessions, resume/sync, malformed and oversized frames, stale generations, wrong-cell/mTLS rejection, source freshness failure/recovery and graceful stop. Run a separate local 1,000-client scenario with actual resource observations; report generator saturation honestly.

The local release and original production qualification have separate statuses. Production remains `NOT_QUALIFIED` until its original gates pass.

## Explicit TODOs

- Original multi-million-client/10M capacity and destructive stress campaigns.
- Large spike and 24-hour soak.
- Physical three-AZ placement, failure isolation and HA/failover durability.
- Production PKI, identity, clock-quality and revocation enrollment.
- Mixed-version Pekko rollout qualification and browser/media interoperability campaigns.
