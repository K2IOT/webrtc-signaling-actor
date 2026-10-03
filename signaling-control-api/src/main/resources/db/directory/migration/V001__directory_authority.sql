CREATE TABLE directory_bucket (
  bucket_id integer PRIMARY KEY CHECK (bucket_id BETWEEN 0 AND 16383),
  cell_id text COLLATE "C" NOT NULL CHECK (octet_length(cell_id) BETWEEN 1 AND 24),
  directory_epoch bigint NOT NULL CHECK (directory_epoch > 0),
  wss_url text NOT NULL CHECK (octet_length(wss_url) <= 512 AND wss_url LIKE 'wss://%'),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- CAS publication is permitted only after proven old-store freeze and destination installation.
-- Directory provisioning is a restricted operator role; runtime routes do not create authority.
