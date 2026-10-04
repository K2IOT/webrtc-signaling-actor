-- Expand-only: legacy records retain NULL and cannot authorize a new CLAIM.
ALTER TABLE command_result ADD COLUMN command_type varchar(64);
