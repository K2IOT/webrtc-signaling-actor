-- Directory seed, the same rows as LocalCompositionDatabase.seedDirectory (spec 4.5): 16384
-- directory_bucket rows, buckets below :split to the first cell, the rest to the second, each
-- half pointing at its cell's public gateway URL. The same split rule renders directory.json.
-- LOCAL_TEST_ONLY / NOT_QUALIFIED. Run by lib/db.sh as signaling_owner on `directory` after
-- Flyway, with psql variables split, first_cell, second_cell, first_url, second_url.
-- Already seeded: checked against the same shape, a mismatch fails (exit 3).

SELECT count(*) = 0 AS seed_needed FROM directory_bucket \gset
\if :seed_needed
INSERT INTO directory_bucket (bucket_id, cell_id, directory_epoch, wss_url)
SELECT n,
       CASE WHEN n < :split THEN :'first_cell' ELSE :'second_cell' END,
       1,
       CASE WHEN n < :split THEN :'first_url' ELSE :'second_url' END
FROM generate_series(0, 16383) n;
\endif

SELECT (SELECT count(*) FROM directory_bucket) = 16384
   AND (SELECT count(*) FROM directory_bucket
        WHERE bucket_id < :split AND cell_id = :'first_cell' AND wss_url = :'first_url') = :split
   AND (SELECT count(*) FROM directory_bucket
        WHERE bucket_id >= :split AND cell_id = :'second_cell' AND wss_url = :'second_url') = 16384 - :split
       AS seed_ok \gset
\if :seed_ok
\else
DO $$ BEGIN RAISE EXCEPTION 'directory seed mismatch: the database holds a different seed; reset the database volumes'; END $$;
\endif
