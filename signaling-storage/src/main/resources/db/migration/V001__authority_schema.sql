CREATE DOMAIN issuer_id AS text COLLATE "C" CHECK (octet_length(VALUE) BETWEEN 1 AND 512);
CREATE DOMAIN identity_id AS text COLLATE "C" CHECK (octet_length(VALUE) BETWEEN 1 AND 256);
CREATE DOMAIN node_id AS text COLLATE "C" CHECK (octet_length(VALUE) BETWEEN 1 AND 128);
CREATE DOMAIN routed_call_id AS text COLLATE "C" CHECK (octet_length(VALUE) BETWEEN 1 AND 96);
CREATE DOMAIN positive_counter AS bigint CHECK (VALUE > 0);
CREATE DOMAIN nonnegative_counter AS bigint CHECK (VALUE >= 0);
CREATE DOMAIN bucket_number AS integer CHECK (VALUE BETWEEN 0 AND 16383);
CREATE DOMAIN group_number AS integer CHECK (VALUE BETWEEN 0 AND 1023);
CREATE DOMAIN intent_hash AS bytea CHECK (octet_length(VALUE)=32);

CREATE TABLE cell_authority (
 singleton_id integer PRIMARY KEY CHECK(singleton_id=1), cell_id node_id NOT NULL,
 storage_epoch positive_counter NOT NULL, ownership_mode text NOT NULL CHECK(ownership_mode='GROUPED'),
 ownership_schema_version positive_counter NOT NULL, status text NOT NULL CHECK(status IN ('ACTIVE','FROZEN','RECOVERING')));
CREATE TABLE bucket_authority (
 bucket_id bucket_number PRIMARY KEY, directory_epoch positive_counter NOT NULL,
 status text NOT NULL CHECK(status IN ('ACTIVE','FROZEN','INSTALLED')), recovery_epoch positive_counter NOT NULL,
 transfer_id uuid, updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE user_guard (user_id identity_id PRIMARY KEY);
CREATE TABLE gateway_lease (
 gateway_id node_id NOT NULL,boot_id uuid NOT NULL,lease_until timestamptz NOT NULL,
 renewal_sequence nonnegative_counter NOT NULL,last_operation_id uuid,last_granted_expiry timestamptz,
 storage_epoch positive_counter NOT NULL,region node_id NOT NULL,cell node_id NOT NULL,
 expired_at timestamptz,PRIMARY KEY(gateway_id,boot_id));
CREATE TABLE session_registry (
 issuer issuer_id NOT NULL,jti identity_id NOT NULL,user_id identity_id NOT NULL REFERENCES user_guard(user_id),
 session_incarnation uuid NOT NULL,connection_generation positive_counter NOT NULL,
 gateway_id node_id,boot_id uuid,connection_id uuid,token_exp timestamptz NOT NULL,
 signing_key_id node_id,security_epoch nonnegative_counter NOT NULL DEFAULT 0,
 closed_at timestamptz,updated_at timestamptz NOT NULL,
 PRIMARY KEY(issuer,jti),FOREIGN KEY(gateway_id,boot_id) REFERENCES gateway_lease(gateway_id,boot_id),
 CHECK((gateway_id IS NULL AND boot_id IS NULL AND connection_id IS NULL) OR (gateway_id IS NOT NULL AND boot_id IS NOT NULL AND connection_id IS NOT NULL)));
CREATE TABLE group_owner (
 cell_id node_id NOT NULL,ownership_hash_version positive_counter NOT NULL,group_id group_number NOT NULL,
 storage_epoch positive_counter NOT NULL,group_epoch positive_counter NOT NULL,
 owner_node node_id,owner_incarnation uuid,status text NOT NULL CHECK(status IN ('IDLE','OWNED','RELEASED')),
 lease_until timestamptz,lease_sequence nonnegative_counter NOT NULL,
 last_pulse_operation uuid,last_granted_expiry timestamptz,
 PRIMARY KEY(cell_id,ownership_hash_version,group_id),
 CHECK(status<>'OWNED' OR (owner_node IS NOT NULL AND owner_incarnation IS NOT NULL AND lease_until IS NOT NULL)));
CREATE TABLE user_reservation (
 user_id identity_id PRIMARY KEY REFERENCES user_guard(user_id),call_id routed_call_id NOT NULL,
 reservation_id uuid NOT NULL,reservation_version positive_counter NOT NULL,
 winner_issuer issuer_id,winner_jti identity_id,winner_incarnation uuid,winner_generation positive_counter,
 lease_until timestamptz NOT NULL,coordinator_cell node_id NOT NULL,coordinator_storage_epoch positive_counter NOT NULL,
 ownership_hash_version positive_counter NOT NULL,ownership_group_id group_number NOT NULL,
 highest_group_epoch positive_counter NOT NULL,highest_lease_sequence nonnegative_counter NOT NULL,
 last_renew_operation uuid,last_granted_expiry timestamptz,activation_id uuid,
 phase text NOT NULL CHECK(phase IN ('PREPARING','RINGING','CLAIMED','ACTIVE')),
 CHECK((winner_issuer IS NULL AND winner_jti IS NULL AND winner_incarnation IS NULL AND winner_generation IS NULL) OR
 (winner_issuer IS NOT NULL AND winner_jti IS NOT NULL AND winner_incarnation IS NOT NULL AND winner_generation IS NOT NULL)));
CREATE TABLE home_participation (
 call_id routed_call_id NOT NULL,user_id identity_id NOT NULL,acquire_operation_id uuid NOT NULL,payload_hash intent_hash NOT NULL,
 reservation_id uuid,coordinator_cell node_id NOT NULL,coordinator_storage_epoch positive_counter NOT NULL,
 ownership_hash_version positive_counter NOT NULL,ownership_group_id group_number NOT NULL,highest_group_epoch positive_counter NOT NULL,
 phase text NOT NULL CHECK(phase IN ('RESERVED','CLAIMED','ACTIVE','RELEASED','EXPIRED')),
 winner_issuer issuer_id,winner_jti identity_id,winner_incarnation uuid,winner_generation positive_counter,
 activation_id uuid,operation_results jsonb NOT NULL DEFAULT '{}',terminal_at timestamptz,expires_at timestamptz,
 PRIMARY KEY(call_id,user_id),CHECK(octet_length(operation_results::text)<=8192),
 CHECK((phase IN ('RELEASED','EXPIRED'))=(terminal_at IS NOT NULL)),
 CHECK(terminal_at IS NULL OR (expires_at IS NOT NULL AND expires_at>=terminal_at+interval '24 hours')),
 CHECK((winner_issuer IS NULL AND winner_jti IS NULL AND winner_incarnation IS NULL AND winner_generation IS NULL) OR
 (winner_issuer IS NOT NULL AND winner_jti IS NOT NULL AND winner_incarnation IS NOT NULL AND winner_generation IS NOT NULL)));
CREATE TABLE call_state (
 call_id routed_call_id PRIMARY KEY,authority_bucket_id bucket_number NOT NULL,
 ownership_hash_version positive_counter NOT NULL,ownership_group_id group_number NOT NULL,
 caller_user identity_id NOT NULL,caller_issuer issuer_id NOT NULL,caller_jti identity_id NOT NULL,
 caller_incarnation uuid NOT NULL,caller_generation positive_counter NOT NULL,callee_user identity_id NOT NULL,
 winner_issuer issuer_id,winner_jti identity_id,winner_incarnation uuid,winner_generation positive_counter,
 state text NOT NULL CHECK(state IN ('PREPARING','RINGING','ACCEPTED','ACTIVATING','CONNECTING','ESTABLISHED','TERMINAL','FAILED')),
 version positive_counter NOT NULL,negotiation_id nonnegative_counter NOT NULL,activation_id uuid,
 saga_phase text NOT NULL,deadlines jsonb NOT NULL DEFAULT '{}',offered_sessions jsonb NOT NULL DEFAULT '[]',rejected_sessions jsonb NOT NULL DEFAULT '[]',
 last_mutation_group_epoch positive_counter NOT NULL,terminal_reason text,terminal_at timestamptz,expires_at timestamptz,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(caller_user<>callee_user),CHECK((state IN ('TERMINAL','FAILED'))=(terminal_at IS NOT NULL)),
 CHECK(terminal_at IS NULL OR (terminal_reason IS NOT NULL AND expires_at IS NOT NULL AND expires_at>=terminal_at+interval '24 hours')),
 CHECK(octet_length(deadlines::text)+octet_length(offered_sessions::text)+octet_length(rejected_sessions::text)<=8192),
 CHECK((winner_issuer IS NULL AND winner_jti IS NULL AND winner_incarnation IS NULL AND winner_generation IS NULL) OR
 (winner_issuer IS NOT NULL AND winner_jti IS NOT NULL AND winner_incarnation IS NOT NULL AND winner_generation IS NOT NULL)));
CREATE TABLE command_result (
 issuer issuer_id NOT NULL,jti identity_id NOT NULL,command_scope text COLLATE "C" NOT NULL CHECK(octet_length(command_scope) BETWEEN 1 AND 128),
 request_id uuid NOT NULL,authority_bucket_id bucket_number NOT NULL,payload_hash intent_hash NOT NULL,
 status text NOT NULL CHECK(status IN ('PENDING','FINAL')),call_id routed_call_id,result jsonb,finalized_at timestamptz,expires_at timestamptz,
 PRIMARY KEY(issuer,jti,command_scope,request_id),CHECK(result IS NULL OR octet_length(result::text)<=8192),
 CHECK((status='PENDING' AND finalized_at IS NULL AND expires_at IS NULL AND result IS NULL) OR
 (status='FINAL' AND finalized_at IS NOT NULL AND result IS NOT NULL AND expires_at IS NOT NULL AND expires_at>=finalized_at+interval '24 hours'))
) PARTITION BY HASH(issuer,jti);
DO $$ BEGIN FOR i IN 0..15 LOOP EXECUTE format('CREATE TABLE command_result_p%s PARTITION OF command_result FOR VALUES WITH (MODULUS 16,REMAINDER %s)',i,i); END LOOP; END $$;
CREATE TABLE control_outbox (
 event_id uuid PRIMARY KEY,authority_bucket_id bucket_number NOT NULL,call_id routed_call_id NOT NULL,version positive_counter NOT NULL,
 destination jsonb NOT NULL,payload bytea NOT NULL CHECK(octet_length(payload)<=8192),
 delivery_state text NOT NULL CHECK(delivery_state IN ('PENDING','INFLIGHT','DELIVERED')),
 dispatch_owner node_id,dispatch_incarnation uuid,dispatch_generation nonnegative_counter NOT NULL DEFAULT 0,
 dispatch_until timestamptz,next_attempt timestamptz NOT NULL,expires_at timestamptz NOT NULL,
 terminal_reason text,quarantined boolean NOT NULL DEFAULT false,
 CHECK(octet_length(destination::text)<=2048),
 CHECK(delivery_state<>'INFLIGHT' OR (dispatch_owner IS NOT NULL AND dispatch_incarnation IS NOT NULL AND dispatch_until IS NOT NULL AND dispatch_generation>0)));
CREATE TABLE security_epoch (
 issuer issuer_id NOT NULL,subject_key text COLLATE "C" NOT NULL CHECK(octet_length(subject_key) BETWEEN 1 AND 1024),
 epoch nonnegative_counter NOT NULL,source_offset nonnegative_counter NOT NULL,committed_at timestamptz NOT NULL,
 PRIMARY KEY(issuer,subject_key));

CREATE FUNCTION guard_session_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.issuer<>OLD.issuer OR NEW.jti<>OLD.jti OR NEW.user_id<>OLD.user_id OR NEW.session_incarnation<>OLD.session_incarnation
 OR NEW.connection_generation<OLD.connection_generation OR NEW.security_epoch<OLD.security_epoch THEN
 RAISE EXCEPTION 'Immutable session binding or regressed fence' USING ERRCODE='23514'; END IF;
 RETURN NEW; END $$;
CREATE TRIGGER session_history BEFORE UPDATE ON session_registry FOR EACH ROW EXECUTE FUNCTION guard_session_history();
CREATE FUNCTION guard_home_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (NEW.call_id,NEW.user_id,NEW.acquire_operation_id,NEW.payload_hash,NEW.coordinator_cell,NEW.coordinator_storage_epoch,NEW.ownership_hash_version,NEW.ownership_group_id)
 IS DISTINCT FROM (OLD.call_id,OLD.user_id,OLD.acquire_operation_id,OLD.payload_hash,OLD.coordinator_cell,OLD.coordinator_storage_epoch,OLD.ownership_hash_version,OLD.ownership_group_id)
 OR NEW.highest_group_epoch<OLD.highest_group_epoch OR (OLD.phase IN ('RELEASED','EXPIRED') AND NEW IS DISTINCT FROM OLD)
 OR (OLD.reservation_id IS NOT NULL AND NEW.reservation_id IS DISTINCT FROM OLD.reservation_id)
 OR (OLD.winner_issuer IS NOT NULL AND (NEW.winner_issuer,NEW.winner_jti,NEW.winner_incarnation,NEW.winner_generation) IS DISTINCT FROM (OLD.winner_issuer,OLD.winner_jti,OLD.winner_incarnation,OLD.winner_generation)) THEN
 RAISE EXCEPTION 'Absorbing or immutable participation' USING ERRCODE='23514'; END IF; RETURN NEW; END $$;
CREATE TRIGGER home_history BEFORE UPDATE ON home_participation FOR EACH ROW EXECUTE FUNCTION guard_home_history();
CREATE FUNCTION guard_call_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (NEW.call_id,NEW.authority_bucket_id,NEW.ownership_hash_version,NEW.ownership_group_id,NEW.caller_user,NEW.caller_issuer,NEW.caller_jti,NEW.callee_user)
 IS DISTINCT FROM (OLD.call_id,OLD.authority_bucket_id,OLD.ownership_hash_version,OLD.ownership_group_id,OLD.caller_user,OLD.caller_issuer,OLD.caller_jti,OLD.callee_user)
 OR NEW.version<OLD.version OR NEW.negotiation_id<OLD.negotiation_id OR NEW.last_mutation_group_epoch<OLD.last_mutation_group_epoch
 OR (OLD.terminal_at IS NOT NULL AND NEW IS DISTINCT FROM OLD)
 OR (OLD.winner_issuer IS NOT NULL AND (NEW.winner_issuer,NEW.winner_jti) IS DISTINCT FROM (OLD.winner_issuer,OLD.winner_jti)) THEN
 RAISE EXCEPTION 'Absorbing call or regressed fence' USING ERRCODE='23514'; END IF; RETURN NEW; END $$;
CREATE TRIGGER call_history BEFORE UPDATE ON call_state FOR EACH ROW EXECUTE FUNCTION guard_call_history();
CREATE FUNCTION guard_command_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (NEW.issuer,NEW.jti,NEW.command_scope,NEW.request_id,NEW.authority_bucket_id,NEW.payload_hash)
 IS DISTINCT FROM (OLD.issuer,OLD.jti,OLD.command_scope,OLD.request_id,OLD.authority_bucket_id,OLD.payload_hash)
 OR (OLD.status='FINAL' AND NEW IS DISTINCT FROM OLD) THEN RAISE EXCEPTION 'Immutable command result' USING ERRCODE='23514'; END IF; RETURN NEW; END $$;
CREATE TRIGGER command_history BEFORE UPDATE ON command_result FOR EACH ROW EXECUTE FUNCTION guard_command_history();
CREATE FUNCTION guard_group_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (NEW.cell_id,NEW.ownership_hash_version,NEW.group_id) IS DISTINCT FROM (OLD.cell_id,OLD.ownership_hash_version,OLD.group_id)
 OR NEW.storage_epoch<OLD.storage_epoch OR NEW.group_epoch<OLD.group_epoch
 OR (NEW.storage_epoch=OLD.storage_epoch AND NEW.group_epoch=OLD.group_epoch AND NEW.lease_sequence<OLD.lease_sequence)
 OR (NEW.group_epoch=OLD.group_epoch AND (NEW.owner_node,NEW.owner_incarnation) IS DISTINCT FROM (OLD.owner_node,OLD.owner_incarnation) AND OLD.owner_incarnation IS NOT NULL) THEN
 RAISE EXCEPTION 'Regressed group authority' USING ERRCODE='23514'; END IF; RETURN NEW; END $$;
CREATE TRIGGER group_history BEFORE UPDATE ON group_owner FOR EACH ROW EXECUTE FUNCTION guard_group_history();
CREATE FUNCTION guard_gateway_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF (NEW.gateway_id,NEW.boot_id,NEW.storage_epoch,NEW.region,NEW.cell) IS DISTINCT FROM (OLD.gateway_id,OLD.boot_id,OLD.storage_epoch,OLD.region,OLD.cell)
 OR NEW.renewal_sequence<OLD.renewal_sequence OR NEW.lease_until<OLD.lease_until
 OR ((OLD.expired_at IS NOT NULL OR OLD.lease_until<=clock_timestamp()) AND NEW.lease_until IS DISTINCT FROM OLD.lease_until) THEN
 RAISE EXCEPTION 'Expired or regressed gateway boot' USING ERRCODE='23514'; END IF; RETURN NEW; END $$;
CREATE TRIGGER gateway_history BEFORE UPDATE ON gateway_lease FOR EACH ROW EXECUTE FUNCTION guard_gateway_history();
CREATE FUNCTION guard_cell_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.singleton_id<>OLD.singleton_id OR NEW.cell_id<>OLD.cell_id OR NEW.storage_epoch<OLD.storage_epoch OR NEW.ownership_schema_version<OLD.ownership_schema_version THEN
 RAISE EXCEPTION 'Regressed cell authority' USING ERRCODE='23514'; END IF; RETURN NEW; END $$;
CREATE TRIGGER cell_history BEFORE UPDATE ON cell_authority FOR EACH ROW EXECUTE FUNCTION guard_cell_history();
CREATE FUNCTION guard_bucket_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.bucket_id<>OLD.bucket_id OR NEW.directory_epoch<OLD.directory_epoch OR NEW.recovery_epoch<OLD.recovery_epoch THEN
 RAISE EXCEPTION 'Regressed bucket authority' USING ERRCODE='23514'; END IF; RETURN NEW; END $$;
CREATE TRIGGER bucket_history BEFORE UPDATE ON bucket_authority FOR EACH ROW EXECUTE FUNCTION guard_bucket_history();
