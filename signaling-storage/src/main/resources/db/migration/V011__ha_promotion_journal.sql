-- Accessible only to the separately privileged HA admission role.
CREATE TABLE promotion_epoch_journal (
 storage_epoch positive_counter PRIMARY KEY,previous_epoch positive_counter NOT NULL,
 operation_id uuid UNIQUE NOT NULL,fence_receipt_sha256 text NOT NULL CHECK(fence_receipt_sha256 ~ '^[a-f0-9]{64}$'),
 wal_receipt_sha256 text NOT NULL CHECK(wal_receipt_sha256 ~ '^[a-f0-9]{64}$'),source_key_id text NOT NULL,
 committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),CHECK(storage_epoch>previous_epoch));
