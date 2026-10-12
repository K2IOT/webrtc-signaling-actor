-- Additive access paths for global due discovery and exact bucket retirement. No hot lease index.
-- PostgreSQL creates/attaches the command_result children under the partitioned parent index.
CREATE INDEX command_bucket_retire_idx ON command_result
 (authority_bucket_id,expires_at,issuer,jti,command_scope,request_id) WHERE status='FINAL';
CREATE INDEX call_bucket_retire_idx ON call_state
 (authority_bucket_id,expires_at,call_id) WHERE terminal_at IS NOT NULL;
CREATE INDEX outbox_retire_due_idx ON control_outbox(expires_at,event_id);
-- Exact outbox bucket expiry lookup is supplied by V013.outbox_bucket_retire_idx.
CREATE INDEX transfer_published_retire_idx ON bucket_transfer(updated_at,transfer_id,bucket_id)
 WHERE state='PUBLISHED';
-- Copy discovery also orders the complete immutable item key through its existing primary key.
