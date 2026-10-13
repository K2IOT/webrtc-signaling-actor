-- Recovery remains an exclusive, journal-bound terminalization path; never bypass bucket fencing for ordinary writers.
CREATE FUNCTION native_disaster_abort_write(relation_name text, operation text, row_data jsonb, old_data jsonb)
    RETURNS boolean
    LANGUAGE plpgsql AS $$
DECLARE
permitted boolean;
BEGIN
 IF
NOT EXISTS(SELECT 1 FROM cell_authority a JOIN recovery_epoch_journal j ON j.storage_epoch=a.storage_epoch
   WHERE a.singleton_id=1 AND a.status='RECOVERING' AND a.ownership_mode='GROUPED' AND a.ownership_schema_version=1
     AND j.completed_at IS NULL AND j.operation_id::text=current_setting('signaling.recovery_abort_operation',true))
   OR NOT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'
     AND classid=100 AND objid=1 AND objsubid=2 AND mode='ExclusiveLock' AND granted) THEN RETURN false;
END IF;
 IF
operation='DELETE' THEN RETURN relation_name='user_reservation';
END IF;
 IF
operation<>'UPDATE' OR old_data IS NULL THEN RETURN false;
END IF;
CASE relation_name
 WHEN 'call_state' THEN
   permitted:=old_data->>'terminal_at' IS NULL AND row_data->>'state'='TERMINAL'
     AND row_data->>'terminal_reason'='DISASTER_RESTORE' AND row_data->>'terminal_at' IS NOT NULL
     AND row_data->>'saga_phase'='COMPENSATE' AND (row_data->>'version')::bigint=(old_data->>'version')::bigint+1
     AND row_data-ARRAY['state','version','saga_phase','terminal_reason','terminal_at','expires_at','updated_at']
       =old_data-ARRAY['state','version','saga_phase','terminal_reason','terminal_at','expires_at','updated_at'];
WHEN 'home_participation' THEN
   permitted:=old_data->>'terminal_at' IS NULL AND row_data->>'phase'='EXPIRED' AND row_data->>'terminal_at' IS NOT NULL
     AND row_data-ARRAY['phase','terminal_at','expires_at']=old_data-ARRAY['phase','terminal_at','expires_at'];
WHEN 'session_registry' THEN
   permitted:=old_data->>'closed_at' IS NULL AND row_data->>'closed_at' IS NOT NULL
     AND row_data-ARRAY['closed_at','updated_at']=old_data-ARRAY['closed_at','updated_at'];
WHEN 'command_result' THEN
   permitted:=old_data->>'status'='PENDING' AND row_data->>'status'='FINAL'
     AND row_data->>'finalized_at' IS NOT NULL AND row_data->'result'->>'code'='DISASTER_RESTORE'
     AND row_data->'result'->>'state'='TERMINAL' AND row_data->'result'->>'callId'=row_data->>'call_id'
     AND EXISTS(SELECT 1 FROM call_state s WHERE s.call_id=row_data->>'call_id'
       AND s.terminal_reason='DISASTER_RESTORE' AND s.terminal_at IS NOT NULL
       AND s.version=(row_data->'result'->>'version')::bigint)
     AND row_data-ARRAY['status','finalized_at','expires_at','result']=old_data-ARRAY['status','finalized_at','expires_at','result'];
WHEN 'control_outbox' THEN
   permitted:=row_data->>'quarantined'='true' AND row_data->>'terminal_reason'='DISASTER_RESTORE'
     AND row_data->>'delivery_state'='PENDING' AND row_data->>'dispatch_owner' IS NULL
     AND row_data->>'dispatch_incarnation' IS NULL AND row_data->>'dispatch_until' IS NULL
     AND row_data-ARRAY['quarantined','terminal_reason','delivery_state','dispatch_owner','dispatch_incarnation','dispatch_until']
       =old_data-ARRAY['quarantined','terminal_reason','delivery_state','dispatch_owner','dispatch_incarnation','dispatch_until'];
ELSE RETURN false;
END
CASE;
RETURN coalesce(permitted, false);
END $$;

CREATE
OR REPLACE FUNCTION guard_bucket_transfer_write() RETURNS trigger LANGUAGE plpgsql AS $$
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
native_disaster_abort_write(relation_name,TG_OP,row_data,old_data) THEN
   IF TG_OP='DELETE' THEN RETURN OLD;
ELSE RETURN NEW;
END IF;
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
