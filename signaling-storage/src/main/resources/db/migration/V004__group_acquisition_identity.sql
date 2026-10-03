ALTER TABLE group_owner ADD COLUMN acquire_operation_id uuid;
ALTER TABLE group_owner ADD CONSTRAINT group_acquire_identity CHECK(status<>'OWNED' OR acquire_operation_id IS NOT NULL);
