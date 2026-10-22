-- Privileges inside one database. LOCAL_TEST_ONLY / NOT_QUALIFIED.
-- Run by lib/db.sh as signaling_owner, connected to the database it grants on, after Flyway.
-- psql variables: dbname, history_table (the Flyway history table), is_source (boolean).
--
-- signaling / directory: signaling_app gets CONNECT, USAGE on public, DML on every table,
-- sequence use and EXECUTE on every routine, also for objects signaling_owner creates later
-- (default privileges), but only SELECT on the Flyway history (the app reads the V016 checksum
-- and the rollback window there, it never writes it) and no CREATE anywhere.
-- source: source_owner owns the database; it gets CONNECT and USAGE+CREATE on public to run
-- its own migrations.

\if :is_source
GRANT CONNECT, TEMPORARY ON DATABASE source TO source_owner;
GRANT USAGE, CREATE ON SCHEMA public TO source_owner;
\else
GRANT CONNECT ON DATABASE :"dbname" TO signaling_app;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO signaling_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO signaling_app;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO signaling_app;
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA public TO signaling_app;
ALTER DEFAULT PRIVILEGES FOR ROLE signaling_owner IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO signaling_app;
ALTER DEFAULT PRIVILEGES FOR ROLE signaling_owner IN SCHEMA public
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO signaling_app;
ALTER DEFAULT PRIVILEGES FOR ROLE signaling_owner IN SCHEMA public
    GRANT EXECUTE ON ROUTINES TO signaling_app;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLE :"history_table" FROM signaling_app;
\endif
