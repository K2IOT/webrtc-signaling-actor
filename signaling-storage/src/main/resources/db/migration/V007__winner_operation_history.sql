ALTER TABLE home_participation ADD COLUMN winner_operation uuid,
 ADD COLUMN winner_call_version bigint CHECK(winner_call_version >= 0);
ALTER TABLE home_participation ADD CONSTRAINT winner_operation_pair
 CHECK((winner_operation IS NULL) = (winner_call_version IS NULL));
CREATE FUNCTION guard_winner_operation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF OLD.winner_operation IS NOT NULL AND (NEW.winner_operation,NEW.winner_call_version)
 IS DISTINCT FROM (OLD.winner_operation,OLD.winner_call_version) THEN
 RAISE EXCEPTION 'Immutable winner operation' USING ERRCODE='23514'; END IF; RETURN NEW; END $$;
CREATE TRIGGER winner_operation_history BEFORE UPDATE ON home_participation
 FOR EACH ROW EXECUTE FUNCTION guard_winner_operation();
