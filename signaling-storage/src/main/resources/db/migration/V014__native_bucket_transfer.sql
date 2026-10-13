-- Normal transfers are idle-only; the source journal and fence survive process failure.
CREATE TABLE bucket_transfer
(
    transfer_id          uuid PRIMARY KEY,
    bucket_id            bucket_number    NOT NULL,
    source_cell          node_id          NOT NULL,
    source_epoch         positive_counter NOT NULL,
    source_storage_epoch positive_counter NOT NULL,
    target_cell          node_id          NOT NULL,
    target_epoch         positive_counter NOT NULL,
    target_storage_epoch positive_counter NOT NULL,
    source_wss           text             NOT NULL,
    target_wss           text             NOT NULL,
    role                 text             NOT NULL CHECK (role IN ('SOURCE', 'TARGET')),
    state                text             NOT NULL CHECK (state IN ('DRAINING', 'FROZEN', 'COPYING', 'INSTALLED', 'PUBLISHED')),
    dataset              integer          NOT NULL DEFAULT 0 CHECK (dataset BETWEEN 0 AND 4),
    cursor               text COLLATE "C" NOT NULL DEFAULT '',
    row_count            bigint           NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    digest               text             NOT NULL DEFAULT '',
    recovery_epoch       positive_counter NOT NULL,
    updated_at           timestamptz      NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (bucket_id, source_epoch, role),
    CHECK (target_epoch = source_epoch + 1),
    CHECK (source_cell <> target_cell)
);
CREATE TABLE bucket_transfer_item
(
    transfer_id uuid             NOT NULL REFERENCES bucket_transfer (transfer_id),
    dataset     integer          NOT NULL CHECK (dataset BETWEEN 0 AND 3),
    row_key     text COLLATE "C" NOT NULL,
    payload     jsonb            NOT NULL CHECK (octet_length(payload::text) <= 32768),
    PRIMARY KEY (transfer_id, dataset, row_key)
);
CREATE INDEX transfer_guard_bucket_idx ON user_guard (native_authority_bucket(user_id), user_id);
CREATE INDEX transfer_session_bucket_idx ON session_registry (native_authority_bucket(user_id), issuer, jti);
CREATE INDEX transfer_home_bucket_idx ON home_participation (native_authority_bucket(user_id), call_id, user_id);
CREATE INDEX transfer_result_bucket_idx ON command_result (authority_bucket_id, issuer, jti, command_scope, request_id);
CREATE FUNCTION guard_transfer_history() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.transfer_id,NEW.bucket_id,NEW.source_cell,NEW.source_epoch,NEW.source_storage_epoch,NEW.target_cell,NEW.target_epoch,NEW.target_storage_epoch,NEW.source_wss,NEW.target_wss,NEW.role,NEW.recovery_epoch)
 IS DISTINCT FROM (OLD.transfer_id,OLD.bucket_id,OLD.source_cell,OLD.source_epoch,OLD.source_storage_epoch,OLD.target_cell,OLD.target_epoch,OLD.target_storage_epoch,OLD.source_wss,OLD.target_wss,OLD.role,OLD.recovery_epoch)
 OR NEW.dataset<OLD.dataset OR NEW.row_count<OLD.row_count
 OR (OLD.state='PUBLISHED' AND NEW IS DISTINCT FROM OLD)
 OR (OLD.role='SOURCE' AND ((OLD.state='DRAINING' AND NEW.state NOT IN ('DRAINING','FROZEN')) OR (OLD.state='FROZEN' AND NEW.state NOT IN ('FROZEN','PUBLISHED'))))
 OR (OLD.role='TARGET' AND ((OLD.state='COPYING' AND NEW.state NOT IN ('COPYING','INSTALLED')) OR (OLD.state='INSTALLED' AND NEW.state NOT IN ('INSTALLED','PUBLISHED'))))
 OR (OLD.state IN ('INSTALLED','PUBLISHED') AND (NEW.dataset,NEW.cursor,NEW.row_count,NEW.digest) IS DISTINCT FROM (OLD.dataset,OLD.cursor,OLD.row_count,OLD.digest))
 OR (NEW.row_count=OLD.row_count AND NEW.digest<>OLD.digest)
 OR (NEW.state IN ('INSTALLED','PUBLISHED') AND NEW.dataset<>4) THEN
 RAISE EXCEPTION 'Immutable transfer binding or regressed checkpoint' USING ERRCODE='23514';
END IF;
RETURN NEW;
END $$;
CREATE TRIGGER transfer_history
    BEFORE UPDATE
    ON bucket_transfer
    FOR EACH ROW EXECUTE FUNCTION guard_transfer_history();
-- Fresh callers acquire native100/101 before these scalar reads. The receipt binds
-- the current local authority to the exact immutable transfer and recovery epoch.
CREATE FUNCTION native_transfer_source_fenced(b integer) RETURNS boolean
    LANGUAGE SQL STABLE STRICT AS $$
SELECT EXISTS(SELECT 1
              FROM bucket_authority a
                       JOIN bucket_transfer t ON t.transfer_id = a.transfer_id
                       JOIN cell_authority c ON c.singleton_id = 1
              WHERE a.bucket_id = b
                AND a.status = 'FROZEN'
                AND t.role = 'SOURCE'
                AND t.state IN ('FROZEN', 'PUBLISHED')
                AND t.bucket_id = a.bucket_id
                AND t.source_epoch = a.directory_epoch
                AND t.recovery_epoch = a.recovery_epoch
                AND t.source_cell = c.cell_id
                AND t.source_storage_epoch = c.storage_epoch
                AND c.status = 'ACTIVE'
                AND t.target_epoch = t.source_epoch + 1
                AND t.target_cell <> t.source_cell)
           $$;

CREATE FUNCTION native_transfer_published(b integer) RETURNS boolean
    LANGUAGE SQL STABLE STRICT AS $$
SELECT EXISTS(SELECT 1
              FROM bucket_authority a
                       JOIN bucket_transfer t ON t.transfer_id = a.transfer_id
                       JOIN cell_authority c ON c.singleton_id = 1
              WHERE a.bucket_id = b
                AND t.bucket_id = a.bucket_id
                AND t.state = 'PUBLISHED'
                AND t.dataset = 4
                AND t.recovery_epoch = a.recovery_epoch
                AND t.target_epoch = t.source_epoch + 1
                AND t.target_cell <> t.source_cell
                AND c.status = 'ACTIVE'
                AND ((t.role = 'SOURCE' AND a.status = 'FROZEN'
                  AND t.source_epoch = a.directory_epoch AND t.source_cell = c.cell_id
                  AND t.source_storage_epoch = c.storage_epoch) OR
                     (t.role = 'TARGET' AND a.status = 'ACTIVE' AND t.target_epoch = a.directory_epoch
                         AND t.target_cell = c.cell_id AND t.target_storage_epoch = c.storage_epoch)))
           $$;

CREATE FUNCTION guard_transfer_item() RETURNS trigger
    LANGUAGE plpgsql AS $$
DECLARE
b integer;
BEGIN
 IF
TG_OP='DELETE' THEN
SELECT bucket_id
INTO b
FROM bucket_transfer
WHERE transfer_id = OLD.transfer_id;
IF
pg_try_advisory_xact_lock_shared(100,1)
      AND pg_try_advisory_xact_lock_shared(101,b)
      AND native_transfer_published(b)
      AND EXISTS(SELECT 1 FROM bucket_authority WHERE bucket_id=b
        AND transfer_id=OLD.transfer_id) THEN RETURN OLD;
END IF;
END IF;
 RAISE
EXCEPTION 'Transfer items immutable until native publication' USING ERRCODE='23514';
END $$;
CREATE TRIGGER transfer_item_history
    BEFORE UPDATE OR
DELETE
ON bucket_transfer_item
 FOR EACH ROW EXECUTE FUNCTION guard_transfer_item();

-- Partition clones retain TG_ARGV[0], unlike TG_TABLE_NAME (command_result_pN).
-- Every field is derived from JSONB so a generic trigger never resolves a field
-- that is absent from one of its relation row types.
CREATE FUNCTION guard_bucket_transfer_write() RETURNS trigger
    LANGUAGE plpgsql AS $$
DECLARE
row_data jsonb; old_data
jsonb; relation_name
text:=TG_ARGV[0];
 b
integer; a
record; t
record; new_work
boolean; dataset_id
integer; key_text
text;
BEGIN
 row_data
:=CASE WHEN TG_OP='DELETE' THEN to_jsonb(OLD) ELSE to_jsonb(NEW)
END;
 old_data
:=CASE WHEN TG_OP='UPDATE' THEN to_jsonb(OLD) ELSE NULL
END;
 b
:=CASE WHEN relation_name IN ('call_state','command_result','control_outbox')
   THEN (row_data->>'authority_bucket_id')::integer
   ELSE native_authority_bucket(row_data->>'user_id')
END;
 IF
b IS NULL OR NOT pg_try_advisory_xact_lock_shared(100,1)
    OR NOT pg_try_advisory_xact_lock_shared(101,b) THEN
   RAISE EXCEPTION 'Bucket transfer contention' USING ERRCODE='55P03';
END IF;
 IF
NOT EXISTS(SELECT 1 FROM cell_authority WHERE singleton_id=1
   AND status='ACTIVE' AND ownership_mode='GROUPED' AND ownership_schema_version=1) THEN
   RAISE EXCEPTION 'Cell authority unavailable' USING ERRCODE='23514';
END IF;
SELECT *
INTO a
FROM bucket_authority
WHERE bucket_id = b;
SELECT *
INTO t
FROM bucket_transfer
WHERE bucket_id = b
  AND transfer_id = a.transfer_id;
IF
a.status IS DISTINCT FROM 'ACTIVE' THEN
   -- Receipt-backed source cleanup only removes expired terminal replay data.
   -- User/session binding fences cannot be retired without their retention contract.
   IF TG_OP='DELETE' AND native_transfer_published(b) AND t.role='SOURCE'
      AND (row_data->>'expires_at')::timestamptz<clock_timestamp() THEN
     IF relation_name='command_result' AND row_data->>'status'='FINAL'
        AND NOT EXISTS(SELECT 1 FROM call_state c WHERE c.call_id=row_data->>'call_id'
          AND (c.terminal_at IS NULL OR c.expires_at>=clock_timestamp())) THEN RETURN OLD;
     ELSIF
relation_name='call_state' AND row_data->>'terminal_at' IS NOT NULL
        AND NOT EXISTS(SELECT 1 FROM command_result r WHERE r.call_id=row_data->>'call_id') THEN RETURN OLD;
     ELSIF
relation_name='home_participation' AND row_data->>'terminal_at' IS NOT NULL THEN RETURN OLD;
     ELSIF
relation_name='control_outbox' THEN RETURN OLD;
END IF;
END IF;
   IF
TG_OP='INSERT' AND a.status='INSTALLED' AND t.role='TARGET' AND t.state='COPYING' THEN
     dataset_id:=CASE relation_name WHEN 'user_guard' THEN 0 WHEN 'session_registry' THEN 1
       WHEN 'home_participation' THEN 2 WHEN 'command_result' THEN 3 ELSE -1
END;
     key_text
:=CASE dataset_id
       WHEN 0 THEN jsonb_build_array(row_data->>'user_id')::text
       WHEN 1 THEN jsonb_build_array(row_data->>'issuer',row_data->>'jti')::text
       WHEN 2 THEN jsonb_build_array(row_data->>'call_id',row_data->>'user_id')::text
       WHEN 3 THEN jsonb_build_array(row_data->>'issuer',row_data->>'jti',row_data->>'command_scope',row_data->>'request_id')::text
END;
     IF
EXISTS(SELECT 1 FROM bucket_transfer_item WHERE transfer_id=t.transfer_id
       AND dataset=dataset_id AND row_key=key_text AND payload=row_data) THEN RETURN NEW;
END IF;
END IF;
   RAISE
EXCEPTION 'Bucket frozen or installation unproven' USING ERRCODE='23514';
END IF;
 IF
EXISTS(SELECT 1 FROM bucket_transfer WHERE bucket_id=b AND source_epoch=a.directory_epoch
   AND role='SOURCE' AND state='DRAINING') THEN
   new_work:=TG_OP='INSERT' AND relation_name IN
     ('session_registry','call_state','user_reservation','home_participation');
   IF
relation_name='session_registry' AND TG_OP='UPDATE' THEN
     new_work:=(row_data->'connection_generation',row_data->'gateway_id',row_data->'boot_id',row_data->'connection_id')
       IS DISTINCT FROM (old_data->'connection_generation',old_data->'gateway_id',old_data->'boot_id',old_data->'connection_id')
       OR (old_data->>'closed_at' IS NOT NULL AND row_data->>'closed_at' IS NULL);
   ELSIF
relation_name='command_result' AND TG_OP='INSERT' THEN
     new_work:=row_data->>'status'='PENDING' AND row_data->>'command_scope'='INVITE';
END IF;
   IF
new_work THEN RAISE EXCEPTION 'Bucket draining new admission' USING ERRCODE='23514';
END IF;
END IF;
 IF
TG_OP='DELETE' THEN RETURN OLD;
END IF;
RETURN NEW;
END $$;
CREATE TRIGGER transfer_guard
    BEFORE INSERT OR
UPDATE OR
DELETE
ON user_guard
    FOR EACH ROW EXECUTE FUNCTION guard_bucket_transfer_write('user_guard');
CREATE TRIGGER transfer_session
    BEFORE INSERT OR
UPDATE OR
DELETE
ON session_registry
    FOR EACH ROW EXECUTE FUNCTION guard_bucket_transfer_write('session_registry');
CREATE TRIGGER transfer_home
    BEFORE INSERT OR
UPDATE OR
DELETE
ON home_participation
    FOR EACH ROW EXECUTE FUNCTION guard_bucket_transfer_write('home_participation');
CREATE TRIGGER transfer_reservation
    BEFORE INSERT OR
UPDATE OR
DELETE
ON user_reservation
    FOR EACH ROW EXECUTE FUNCTION guard_bucket_transfer_write('user_reservation');
CREATE TRIGGER transfer_call
    BEFORE INSERT OR
UPDATE OR
DELETE
ON call_state
    FOR EACH ROW EXECUTE FUNCTION guard_bucket_transfer_write('call_state');
CREATE TRIGGER transfer_result
    BEFORE INSERT OR
UPDATE OR
DELETE
ON command_result
    FOR EACH ROW EXECUTE FUNCTION guard_bucket_transfer_write('command_result');
CREATE TRIGGER transfer_outbox
    BEFORE INSERT OR
UPDATE OR
DELETE
ON control_outbox
    FOR EACH ROW EXECUTE FUNCTION guard_bucket_transfer_write('control_outbox');

-- An old source epoch is an absorbing write fence. Target activation requires its
-- complete immutable installation checkpoint, not merely a directory publication.
CREATE FUNCTION guard_transfer_bucket_authority() RETURNS trigger
    LANGUAGE plpgsql AS $$
DECLARE
t record;
BEGIN
 IF
TG_OP='UPDATE' AND OLD.status='FROZEN' AND OLD.transfer_id IS NOT NULL
 AND (NEW.status,NEW.directory_epoch,NEW.transfer_id,NEW.recovery_epoch)
 IS DISTINCT FROM (OLD.status,OLD.directory_epoch,OLD.transfer_id,OLD.recovery_epoch) THEN
 RAISE EXCEPTION 'Committed transfer fence is absorbing' USING ERRCODE='23514';
END IF;
 IF
NEW.status='ACTIVE' AND NEW.transfer_id IS NOT NULL THEN
SELECT *
INTO t
FROM bucket_transfer
WHERE transfer_id = NEW.transfer_id;
IF
t.role IS DISTINCT FROM 'TARGET' OR t.state NOT IN ('COPYING','INSTALLED','PUBLISHED')
 OR t.dataset<>4 OR t.bucket_id<>NEW.bucket_id OR t.target_epoch<>NEW.directory_epoch
 OR t.recovery_epoch<>NEW.recovery_epoch OR t.source_epoch+1<>t.target_epoch
 OR (t.row_count>0 AND length(t.digest)<>64) THEN
 RAISE EXCEPTION 'Unproven target activation' USING ERRCODE='23514';
END IF;
END IF;
RETURN NEW;
END $$;
CREATE TRIGGER transfer_bucket_authority
    BEFORE INSERT OR UPDATE ON bucket_authority FOR EACH ROW
EXECUTE FUNCTION guard_transfer_bucket_authority();
