-- Separate all-state expiry hints from the INFLIGHT dispatch-reclaim access path.
CREATE INDEX outbox_expiry_hint_idx ON control_outbox(expires_at,event_id) WHERE quarantined=false;
-- Retention includes delivered and quarantined history, within its authoritative bucket.
CREATE INDEX outbox_bucket_retire_idx ON control_outbox(authority_bucket_id,expires_at,event_id);
-- V008 duplicated the V002 dispatch_until/event_id INFLIGHT index exactly.
DROP INDEX outbox_expired_dispatch_idx;
