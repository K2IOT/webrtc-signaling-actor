# Tasks 1–22 audit remediation

Authority: `2026-10-03-webrtc-signaling-10m-design-v1.11-production.md` and findings A01–A15 in `../reviews/2026-10-09-tasks-1-22-audit.md`.
User authorization: fix all audited issues, 2026-10-09. Base: `847745501a1e6cf045876ff745f017440014af15` on `dev-release-minikube`.

## Constraints

- Preserve exact authority/TTL/quorum/replay/UNKNOWN/physical-drain rules and existing work.
- Run GitNexus upstream impact before editing existing symbols; report risk before changes.
- Work on the existing delivery branch; preserve the untracked audit report. No push/merge in this request.
- Higher-priority session instruction prohibits adding/running tests unless explicitly requested. Implementations receive source review and compilation with tests skipped; never reuse historical PASS as evidence for changed code.
- Missing identity attestation, external client deployment and production providers are concrete input dependencies. Supply safe code/contracts and reject missing enrollment; do not invent configuration or qualify unexecuted external gates.

## Tasks

### Task 1: Storage fairness and write/expiry cost (A02/A10/A13)
Bounded keyset outbox traversal advancing deferred pages; skip unchanged home-history renewal writes; matching expiry query/index paths and remove duplicate index with an additive migration. Preserve ordered authority barriers and physical DB admission.

### Task 2: Gateway priority and lifecycle cost (A04/A11/A12)
Protected critical input service/count/bytes while keeping malformed/fragment bounds; one heartbeat close hook per socket; bounded incremental security traversal without full registry copies, retaining freshness/expiry closure semantics.

### Task 3: Native renew/recovery/maintenance composition (A01/A09)
Implement participant renewal and proactive recovery from shipped native actors, fair bounded group/owner scan scheduling, mandatory maintenance capabilities with startup validation/lifecycle draining; connect actual outbox application receipts/retention/privacy work rather than declaring a scheduler bean sufficient.

### Task 4: Native authority migration (A03)
Durable transfer identity/freeze/install receipt and source/target PostgreSQL adapter with authority barriers, bounded copy and replay-history validation; connect control enrollment/API and UNKNOWN resume. No directory publish before verified installation.

### Task 5: Runtime telemetry, compatibility and logging (A05/A06/A14)
Production registry/operation producers and tracing boundaries, exported dashboard/rule resources; authenticated native peer capability gates/durable-work rollback predicate; bounded nonblocking logging with explicit drop/drain policy.

### Task 6: Identity and pinned client contracts (A07/A08/A15)
Keep deployment identity attestation fail-closed; resolve optional iat safely under issuer lifetime contract; supply native client ICE receive/ACK/resend and media continuity integration contract/adapter as repository scope permits. Record external input dependencies explicitly.

### Task 7: Remaining budget/retention/deployment contracts and integration review
Resolve audit section 4: DB byte/service/completion budget inventory, actual receipt retirement, safe session/tombstone retention, runtime constant mapping and provider configuration. Compile production reactor (tests skipped), fresh Java/code review of all changes, fix review findings, update execution ledger with each finding's exact implemented/unverified/dependency status.

## Dependency order

Tasks 1 and 2 can proceed independently. Task 3 consumes existing storage/RPC interfaces and Task 1 fixes. Task 4 owns control/migration files. Task 5 integrates completed runtime/gateway work. Task 6 owns auth/client code. Task 7 reviews the combined change, without claiming benchmark or test evidence.
