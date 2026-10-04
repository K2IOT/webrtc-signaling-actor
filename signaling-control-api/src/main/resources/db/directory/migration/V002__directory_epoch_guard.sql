CREATE FUNCTION protect_directory_epoch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.bucket_id<>OLD.bucket_id OR NEW.directory_epoch<=OLD.directory_epoch THEN
  RAISE EXCEPTION 'directory epoch must advance' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER directory_epoch_advance BEFORE UPDATE ON directory_bucket
 FOR EACH ROW EXECUTE FUNCTION protect_directory_epoch();
