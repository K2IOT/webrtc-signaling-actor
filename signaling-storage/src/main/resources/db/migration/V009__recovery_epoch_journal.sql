-- Administrative recovery is serialized by the native exclusive cell barrier.
-- External pre-disaster epoch high-water and physical fencing remain required signed inputs.
CREATE TABLE recovery_epoch_journal (
 storage_epoch positive_counter PRIMARY KEY, previous_epoch positive_counter NOT NULL,
 external_epoch_high_water nonnegative_counter NOT NULL,operation_id uuid UNIQUE NOT NULL,
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(),completed_at timestamptz,
 security_replay_offset nonnegative_counter,privacy_replay_offset nonnegative_counter,
 CHECK(storage_epoch>previous_epoch AND storage_epoch>external_epoch_high_water));
