ALTER TABLE user_reservation ADD COLUMN activation_call_version positive_counter;
ALTER TABLE home_participation ADD COLUMN activation_call_version positive_counter,
 ADD COLUMN activation_operation uuid,ADD COLUMN activation_confirmed_at timestamptz;
ALTER TABLE home_participation ADD CONSTRAINT activation_binding_complete
 CHECK((activation_id IS NULL AND activation_call_version IS NULL AND activation_operation IS NULL AND activation_confirmed_at IS NULL)
 OR (activation_id IS NOT NULL AND activation_call_version IS NOT NULL AND activation_operation IS NOT NULL AND activation_confirmed_at IS NOT NULL));
CREATE FUNCTION guard_activation_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF OLD.activation_id IS NOT NULL AND (NEW.activation_id,NEW.activation_call_version,NEW.activation_operation,NEW.activation_confirmed_at)
 IS DISTINCT FROM (OLD.activation_id,OLD.activation_call_version,OLD.activation_operation,OLD.activation_confirmed_at) THEN
 RAISE EXCEPTION 'Immutable activation binding' USING ERRCODE='23514'; END IF; RETURN NEW; END $$;
CREATE TRIGGER activation_history BEFORE UPDATE ON home_participation FOR EACH ROW EXECUTE FUNCTION guard_activation_history();
