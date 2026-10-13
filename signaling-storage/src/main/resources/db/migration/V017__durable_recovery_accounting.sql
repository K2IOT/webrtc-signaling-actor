-- Fixed cell-local journal. No call/user/session/network identifiers or payloads retained.
CREATE TABLE recovery_accounting_capacity
(
    singleton_id integer PRIMARY KEY CHECK (singleton_id = 1),
    cohorts      integer NOT NULL CHECK (cohorts BETWEEN 0 AND 64),
    members      integer NOT NULL CHECK (members BETWEEN 0 AND 8192)
);
INSERT INTO recovery_accounting_capacity
VALUES (1, 0, 0);
CREATE TABLE recovery_cohort
(
    cohort_id      uuid PRIMARY KEY,
    storage_epoch  positive_counter NOT NULL,
    source_hash    bytea            NOT NULL CHECK (octet_length(source_hash) = 32),
    manifest_hash  bytea            NOT NULL CHECK (octet_length(manifest_hash) = 32),
    privacy_key_id text             NOT NULL CHECK (privacy_key_id ~ '^[A-Za-z0-9_-]{1,64}$'
) ,
 captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 fault_kind text NOT NULL CHECK(fault_kind IN ('PROCESS','AZ')),
 fault_at timestamptz, accounting_unknown boolean NOT NULL DEFAULT false,
 member_count integer NOT NULL CHECK(member_count BETWEEN 1 AND 640));
CREATE TABLE recovery_member
(
    cohort_id        uuid                NOT NULL REFERENCES recovery_cohort (cohort_id),
    member_hash      bytea               NOT NULL CHECK (octet_length(member_hash) = 32),
    sli              text                NOT NULL CHECK (sli IN ('WORKFLOW_CONVERGENCE', 'ACTIVE_CALL_PRESERVATION',
                                                                 'SIGNALING_REATTACHMENT', 'MEDIA_CONTINUITY')),
    baseline_version positive_counter    NOT NULL,
    baseline_round   nonnegative_counter NOT NULL,
    deadline_at      timestamptz,
    successful_at    timestamptz,
    media_round      nonnegative_counter NOT NULL DEFAULT 0,
    media_caller_at  timestamptz,
    media_winner_at  timestamptz,
    PRIMARY KEY (cohort_id, sli, member_hash),
    CHECK (successful_at IS NULL OR (deadline_at IS NOT NULL AND successful_at <= deadline_at))
);
CREATE INDEX recovery_member_deadline_idx ON recovery_member (deadline_at, cohort_id, sli, member_hash) WHERE successful_at IS NULL;
CREATE TABLE recovery_totals
(
    sli        text PRIMARY KEY CHECK (sli IN
                                       ('WORKFLOW_CONVERGENCE', 'ACTIVE_CALL_PRESERVATION', 'SIGNALING_REATTACHMENT',
                                        'MEDIA_CONTINUITY')),
    eligible   nonnegative_counter NOT NULL DEFAULT 0,
    successful nonnegative_counter NOT NULL DEFAULT 0,
    CHECK (successful <= eligible)
);
INSERT INTO recovery_totals(sli)
VALUES ('WORKFLOW_CONVERGENCE'),
       ('ACTIVE_CALL_PRESERVATION'),
       ('SIGNALING_REATTACHMENT'),
       ('MEDIA_CONTINUITY');
