# --- start-standby.sh: cell standby {{SERVER}} (fragment appended to start-common.sh) ---------
# First start: base backup from {{PRIMARY_IP}} as `replicator` over verify-full TLS with
# application_name {{STANDBY_NAME}} (the name the primary's synchronous_standby_names and the
# actors' wal.replicas use); -R writes primary_conninfo and standby.signal. The password comes
# from a pgpass file in tmpfs, re-created on every start, and primary_conninfo refers to that
# file (passfile=), so the password is never written to the data volume. Then the image's
# entrypoint starts postgres as a hot standby; it never initialises a cluster here.
# A data directory counts as a standby only when the base backup finished: -R writes
# standby.signal and primary_conninfo last, so anything without them (an interrupted backup) is
# wiped and backed up again instead of crash-looping on a partial copy.
BASEBACKUP_ATTEMPTS="${BASEBACKUP_ATTEMPTS:-90}"
PRIMARY_CONNINFO="host={{PRIMARY_IP}} port=$PG_PORT user=replicator application_name={{STANDBY_NAME}} connect_timeout=5 sslmode=verify-full sslrootcert=$TLS/db-ca.crt ssl_min_protocol_version=TLSv1.3 passfile=$TLS/pgpass"

write_pgpass() {
  local password
  [ -r "$SRC/replicator.pass" ] || fail "$SRC/replicator.pass is missing"
  password="$(<"$SRC/replicator.pass")"
  printf '%s:%s:replication:replicator:%s\n' '{{PRIMARY_IP}}' "$PG_PORT" "$password" >"$TLS/pgpass"
  chown postgres:postgres "$TLS/pgpass"
  chmod 0600 "$TLS/pgpass"
}

# base_backup: bounded retries while the primary is still starting (compose also orders the
# standby after a healthy primary).
base_backup() {
  local i
  mkdir -p "$PGDATA"
  chown postgres:postgres "$PGDATA"
  chmod 0700 "$PGDATA"
  for ((i = 1; i <= BASEBACKUP_ATTEMPTS; i++)); do
    if gosu postgres pg_basebackup -D "$PGDATA" -R -X stream --checkpoint=fast --no-password \
      -d "$PRIMARY_CONNINFO"; then
      say "base backup from {{PRIMARY_IP}} complete"
      return 0
    fi
    # pg_basebackup removes what it wrote on failure; never leave a partial copy behind
    find "$PGDATA" -mindepth 1 -delete
    say "base backup attempt $i/$BASEBACKUP_ATTEMPTS failed; retrying in 2 s"
    sleep 2
  done
  fail "no base backup from {{PRIMARY_IP}} after $BASEBACKUP_ATTEMPTS attempts"
}

# standby_complete: PG_VERSION, standby.signal and a primary_conninfo written by -R.
standby_complete() {
  cluster_exists && [ -f "$PGDATA/standby.signal" ] &&
    grep -q '^primary_conninfo' "$PGDATA/postgresql.auto.conf" 2>/dev/null
}

install_runtime_files
unset POSTGRES_PASSWORD POSTGRES_PASSWORD_FILE
write_pgpass
if standby_complete; then
  say "standby data complete; no base backup needed"
else
  if [ -d "$PGDATA" ] && [ -n "$(find "$PGDATA" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    say "data directory incomplete (interrupted base backup?); wiping it for a new base backup"
    find "$PGDATA" -mindepth 1 -delete
  fi
  base_backup
fi
exec docker-entrypoint.sh postgres "${SERVER_OPTS[@]}" -c hot_standby=on
