-- Source-domain identity remains immutable; authenticated epoch adoption is checked under native guards.
CREATE OR REPLACE FUNCTION guard_home_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (NEW.call_id,NEW.user_id,NEW.acquire_operation_id,NEW.payload_hash,NEW.coordinator_cell,NEW.ownership_hash_version,NEW.ownership_group_id)
 IS DISTINCT FROM (OLD.call_id,OLD.user_id,OLD.acquire_operation_id,OLD.payload_hash,OLD.coordinator_cell,OLD.ownership_hash_version,OLD.ownership_group_id)
 OR NEW.coordinator_storage_epoch<OLD.coordinator_storage_epoch OR NEW.highest_group_epoch<OLD.highest_group_epoch OR (OLD.phase IN ('RELEASED','EXPIRED') AND NEW IS DISTINCT FROM OLD)
 OR (OLD.reservation_id IS NOT NULL AND NEW.reservation_id IS DISTINCT FROM OLD.reservation_id)
 OR (OLD.winner_issuer IS NOT NULL AND (NEW.winner_issuer,NEW.winner_jti,NEW.winner_incarnation,NEW.winner_generation) IS DISTINCT FROM (OLD.winner_issuer,OLD.winner_jti,OLD.winner_incarnation,OLD.winner_generation)) THEN
 RAISE EXCEPTION 'Absorbing or immutable participation' USING ERRCODE='23514'; END IF; RETURN NEW; END $$;
CREATE FUNCTION guard_reservation_epoch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (NEW.user_id,NEW.call_id,NEW.reservation_id,NEW.coordinator_cell,NEW.ownership_hash_version,NEW.ownership_group_id)
 IS DISTINCT FROM (OLD.user_id,OLD.call_id,OLD.reservation_id,OLD.coordinator_cell,OLD.ownership_hash_version,OLD.ownership_group_id)
 OR NEW.coordinator_storage_epoch<OLD.coordinator_storage_epoch OR NEW.highest_group_epoch<OLD.highest_group_epoch
 OR (NEW.coordinator_storage_epoch=OLD.coordinator_storage_epoch AND NEW.highest_group_epoch=OLD.highest_group_epoch AND NEW.highest_lease_sequence<OLD.highest_lease_sequence)
 THEN RAISE EXCEPTION 'Immutable or regressed reservation source' USING ERRCODE='23514'; END IF; RETURN NEW; END $$;
CREATE TRIGGER reservation_epoch BEFORE UPDATE ON user_reservation FOR EACH ROW EXECUTE FUNCTION guard_reservation_epoch();
