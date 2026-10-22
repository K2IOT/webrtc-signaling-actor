-- Login roles of one compose PostgreSQL cluster. LOCAL_TEST_ONLY / NOT_QUALIFIED.
-- Run by lib/db.sh as signaling_owner (the cluster superuser created at initialisation) on the
-- `postgres` database, through psql on stdin. Before this file, db.sh sets the psql variables
--   is_cell, is_directory              booleans: which kind of cluster this is
--   app_password                        always
--   replicator_password                 cell primaries
--   nodir_password, source_owner_password   pg-directory
-- so no password is in this file, in argv or in the environment. Idempotent: CREATE only when
-- missing, then ALTER to the exact attributes and password.
--
-- signaling_app    the planes' database user: DML on the cell / directory schema (grants.sql)
--                  and pg_monitor for pg_stat_replication, pg_control_system() and
--                  pg_current_wal_flush_lsn() (WAL enrollment, spec 4.5).
-- replicator       the standby's base backup and streaming (created at initialisation by the
--                  primary's start.sh, so the standby can start before stage 2; refreshed here).
-- signaling_nodir  the directory credential of actors and gateways: LOGIN and nothing else;
--                  databases.sql revokes CONNECT on `directory` from PUBLIC, so it reaches no data.
-- source_owner     owns the `source` database; local-platform migrates it itself.

SELECT 'CREATE ROLE signaling_app'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'signaling_app') \gexec
ALTER ROLE signaling_app WITH LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOBYPASSRLS PASSWORD :'app_password';
GRANT pg_monitor TO signaling_app;

\if :is_cell
SELECT 'CREATE ROLE replicator'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') \gexec
ALTER ROLE replicator WITH LOGIN REPLICATION NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOBYPASSRLS PASSWORD :'replicator_password';
\endif

\if :is_directory
SELECT 'CREATE ROLE signaling_nodir'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'signaling_nodir') \gexec
ALTER ROLE signaling_nodir WITH LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOBYPASSRLS PASSWORD :'nodir_password';
SELECT 'REVOKE pg_monitor FROM signaling_nodir'
WHERE pg_has_role('signaling_nodir', 'pg_monitor', 'MEMBER') \gexec

SELECT 'CREATE ROLE source_owner'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'source_owner') \gexec
ALTER ROLE source_owner WITH LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    NOBYPASSRLS PASSWORD :'source_owner_password';
\endif
