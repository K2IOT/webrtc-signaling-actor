-- Databases of one compose PostgreSQL cluster. LOCAL_TEST_ONLY / NOT_QUALIFIED.
-- Run by lib/db.sh as signaling_owner on `postgres`, after roles.sql, with the psql booleans
-- is_cell and is_directory set. Idempotent.
--   cell primary  signaling   owner signaling_owner (Flyway V001-V018, then the cell seed)
--   pg-directory  directory   owner signaling_owner (Flyway directory V001-V002, then the seed)
--                 source      owner source_owner (local-platform migrates it; never seeded here)
-- CONNECT and TEMPORARY are revoked from PUBLIC on every database, so a role reaches only what
-- grants.sql grants it by name (signaling_nodir: nothing).

\if :is_cell
SELECT 'CREATE DATABASE signaling OWNER signaling_owner'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'signaling') \gexec
REVOKE CONNECT, TEMPORARY ON DATABASE signaling FROM PUBLIC;
\endif

\if :is_directory
SELECT 'CREATE DATABASE directory OWNER signaling_owner'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'directory') \gexec
SELECT 'CREATE DATABASE source OWNER source_owner'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'source') \gexec
ALTER DATABASE source OWNER TO source_owner;
REVOKE CONNECT, TEMPORARY ON DATABASE directory FROM PUBLIC;
REVOKE CONNECT, TEMPORARY ON DATABASE source FROM PUBLIC;
\endif

REVOKE CONNECT, TEMPORARY ON DATABASE postgres FROM PUBLIC;
