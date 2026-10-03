-- The origin supports exact pending-INVITE reconciliation without a call_id history scan.
ALTER TABLE call_state ADD COLUMN invite_request_id uuid NOT NULL;
