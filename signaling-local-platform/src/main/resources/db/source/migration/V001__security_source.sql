-- LOCAL_TEST_ONLY / NOT_QUALIFIED: durable, append-only revocation source for the local platform.
--
-- source_state holds the single high-water counter. Every append bumps it with one UPDATE inside
-- the appending transaction; that row lock serialises appenders, so offsets are gapless and are
-- committed in offset order (a reader can never see an offset ahead of a gap).
CREATE TABLE source_state (
    singleton  boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    high_water bigint  NOT NULL DEFAULT 0 CHECK (high_water >= 0)
);

INSERT INTO source_state (singleton, high_water) VALUES (true, 0);

CREATE TABLE revocation_log (
    source_offset bigint      PRIMARY KEY,
    user_id       text        NOT NULL CHECK (octet_length(user_id) BETWEEN 1 AND 1024),
    jti           text        NULL CHECK (jti IS NULL OR octet_length(jti) BETWEEN 1 AND 256),
    epoch         bigint      NOT NULL CHECK (epoch >= 0),
    committed_at  timestamptz NOT NULL
);

-- Latest-epoch lookup per (user, jti) subject.
CREATE INDEX revocation_log_subject ON revocation_log (user_id, jti, epoch);

CREATE FUNCTION revocation_log_append_only() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    RAISE EXCEPTION 'revocation_log is append-only';
END;
$$;

CREATE TRIGGER revocation_log_no_update_delete
    BEFORE UPDATE OR DELETE ON revocation_log
    FOR EACH ROW EXECUTE FUNCTION revocation_log_append_only();

-- TRUNCATE does not fire row triggers, so it gets its own statement trigger (this also binds the
-- table owner, which a REVOKE alone does not).
CREATE TRIGGER revocation_log_no_truncate
    BEFORE TRUNCATE ON revocation_log
    FOR EACH STATEMENT EXECUTE FUNCTION revocation_log_append_only();

REVOKE TRUNCATE ON revocation_log FROM PUBLIC;

-- The high-water mark moves forward by exactly one per append and nothing else touches the row.
CREATE FUNCTION source_state_forward_only() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.high_water = OLD.high_water + 1 THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'source_state high_water is forward-only';
END;
$$;

CREATE TRIGGER source_state_forward_only
    BEFORE INSERT OR UPDATE OR DELETE ON source_state
    FOR EACH ROW EXECUTE FUNCTION source_state_forward_only();

CREATE TRIGGER source_state_no_truncate
    BEFORE TRUNCATE ON source_state
    FOR EACH STATEMENT EXECUTE FUNCTION source_state_forward_only();

REVOKE TRUNCATE ON source_state FROM PUBLIC;
