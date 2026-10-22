-- Cell seed, the same rows as LocalCompositionDatabase.migrateCell (spec 4.5).
-- LOCAL_TEST_ONLY / NOT_QUALIFIED. Run by lib/db.sh as signaling_owner on the cell's `signaling`
-- database after Flyway, with psql variables cell, first_active, last_active (this cell's
-- buckets, from topology SPLIT) and active_count.
-- A database that is already seeded is not seeded again: it is checked against the same shape
-- and a mismatch fails (exit 3) instead of being overwritten.

SELECT count(*) = 0 AS seed_needed FROM cell_authority \gset
\if :seed_needed
BEGIN;
INSERT INTO cell_authority VALUES (1, :'cell', 1, 'GROUPED', 1, 'ACTIVE');
INSERT INTO bucket_authority (bucket_id, directory_epoch, status, recovery_epoch)
SELECT n, 1, CASE WHEN n BETWEEN :first_active AND :last_active THEN 'ACTIVE' ELSE 'FROZEN' END, 1
FROM generate_series(0, 16383) n;
INSERT INTO group_owner (cell_id, ownership_hash_version, group_id, storage_epoch, group_epoch,
                         lease_sequence, status)
SELECT :'cell', 1, n, 1, 1, 0, 'IDLE'
FROM generate_series(0, 1023) n;
COMMIT;
\endif

SELECT (SELECT count(*) FROM cell_authority
        WHERE singleton_id = 1 AND cell_id = :'cell' AND storage_epoch = 1
          AND ownership_mode = 'GROUPED' AND ownership_schema_version = 1) = 1
   AND (SELECT count(*) FROM bucket_authority) = 16384
   AND (SELECT count(*) FROM bucket_authority WHERE status = 'ACTIVE') = :active_count
   AND (SELECT count(*) FROM bucket_authority
        WHERE status = 'ACTIVE' AND bucket_id BETWEEN :first_active AND :last_active) = :active_count
   AND (SELECT count(*) FROM group_owner WHERE cell_id = :'cell') = 1024 AS seed_ok \gset
\if :seed_ok
\else
DO $$ BEGIN RAISE EXCEPTION 'cell seed mismatch: the database holds a different seed; reset the database volumes'; END $$;
\endif
