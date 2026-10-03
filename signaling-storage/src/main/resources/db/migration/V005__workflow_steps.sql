-- Compact internal workflow replay, bounded to sixteen entries by the guarded writer.
-- Public command replay remains in command_result under issuer/jti/scope/request_id.
CREATE TABLE call_workflow_step (
 call_id routed_call_id NOT NULL REFERENCES call_state(call_id) ON DELETE CASCADE,
 operation_id uuid NOT NULL, intent_hash bytea NOT NULL CHECK(octet_length(intent_hash)=32),
 outcome jsonb NOT NULL CHECK(octet_length(outcome::text)<=2048),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(call_id,operation_id)
);
CREATE FUNCTION workflow_replay_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'Workflow replay is immutable' USING ERRCODE='23514'; END $$;
CREATE TRIGGER workflow_replay_immutable BEFORE UPDATE ON call_workflow_step
FOR EACH ROW EXECUTE FUNCTION workflow_replay_immutable();
