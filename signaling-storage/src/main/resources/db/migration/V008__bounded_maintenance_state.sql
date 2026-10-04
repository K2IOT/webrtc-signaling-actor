-- Cell-local revocation source progress is authoritative security state, never inferred from an empty lookup.
CREATE TABLE security_progress (
 singleton_id integer PRIMARY KEY CHECK(singleton_id=1),source_offset bigint NOT NULL CHECK(source_offset>=0),
 checked_at timestamptz NOT NULL,source_checked_at timestamptz NOT NULL);
CREATE TABLE privacy_deletion_request (
 opaque_subject bytea PRIMARY KEY CHECK(octet_length(opaque_subject)=32),authority_bucket_id bucket_number NOT NULL,
 request_id uuid NOT NULL,requested_at timestamptz NOT NULL,completed_at timestamptz,
 dispatch_incarnation uuid,dispatch_generation bigint NOT NULL DEFAULT 0 CHECK(dispatch_generation>=0),dispatch_until timestamptz);
CREATE INDEX privacy_due_idx ON privacy_deletion_request(requested_at,opaque_subject) WHERE completed_at IS NULL;
CREATE INDEX outbox_expired_dispatch_idx ON control_outbox(dispatch_until,event_id) WHERE delivery_state='INFLIGHT';
-- Hash v1: bytes 6/7 of SHA-256 over the canonical UTF-8 user identifier, matching SessionRegistryService.
CREATE FUNCTION native_authority_bucket(subject text) RETURNS integer LANGUAGE SQL IMMUTABLE STRICT AS $$
 SELECT ((get_byte(sha256(convert_to(subject,'UTF8')),6) & 63) << 8) | get_byte(sha256(convert_to(subject,'UTF8')),7)
$$;
CREATE INDEX home_bucket_retire_idx ON home_participation(native_authority_bucket(user_id),expires_at,call_id,user_id) WHERE terminal_at IS NOT NULL;
CREATE TABLE retired_signing_key (
 issuer issuer_id NOT NULL,signing_key_id node_id NOT NULL,source_offset bigint NOT NULL CHECK(source_offset>=0),
 committed_at timestamptz NOT NULL,PRIMARY KEY(issuer,signing_key_id));
CREATE INDEX session_signing_key_idx ON session_registry(issuer,signing_key_id,security_epoch) WHERE closed_at IS NULL;
