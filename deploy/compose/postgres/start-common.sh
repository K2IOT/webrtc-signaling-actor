#!/usr/bin/env bash
# PostgreSQL container entry of {{SERVER}} ({{KIND}}). LOCAL_TEST_ONLY / NOT_QUALIFIED.
#
# Rendered by provisioner stage 1 (lib/dbprep.sh) from deploy/compose/postgres/start-common.sh
# followed by start-{{KIND}}.sh into /run/signaling/start.sh; compose runs it with
# `entrypoint: ["bash", "/run/signaling/start.sh"]`.
#
# Runs as root (the image's default user) with cap_drop ALL + CHOWN, DAC_OVERRIDE, FOWNER,
# SETGID, SETUID on a read-only root filesystem. The mount files are 0440 root:10001, which the
# postgres user cannot read, so this script copies the TLS files, pg_hba.conf and the password
# files it needs into the tmpfs /run/pgtls (owner postgres), then hands over to the image's
# docker-entrypoint.sh, which drops to the postgres user through gosu. Nothing secret is printed
# or put on a command line: passwords are copied file to file, or written by bash builtins.
#
# Writable places it needs: the data volume at /var/lib/postgresql/data and tmpfs mounts at
# /tmp, /var/run/postgresql and /run/pgtls; servers that initialise a cluster (primaries and
# pg-directory) also need a tmpfs at /docker-entrypoint-initdb.d.
set -euo pipefail
umask 077

SERVER='{{SERVER}}'
SRC=/run/signaling
TLS=/run/pgtls
INITDB_D=/docker-entrypoint-initdb.d
PG_PORT='{{PG_PORT}}'
export PGDATA=/var/lib/postgresql/data/pgdata

say() { printf 'start.sh(%s): %s\n' "$SERVER" "$*" >&2; }
fail() {
  say "FATAL $*"
  exit 1
}

[ "$(id -u)" -eq 0 ] || fail "must start as root: it installs files for the postgres user, then drops privileges"

require_writable() {
  local d
  for d in "$@"; do
    [ -d "$d" ] && [ -w "$d" ] || fail "$d must be writable (tmpfs or volume); see the header of this script"
  done
}

# Server options shared by every server; the kind script appends its own.
SERVER_OPTS=(
  -c "listen_addresses=*"
  -c "port=$PG_PORT"
  -c ssl=on
  -c "ssl_cert_file=$TLS/server.crt"
  -c "ssl_key_file=$TLS/server.key"
  -c ssl_min_protocol_version=TLSv1.3
  -c password_encryption=scram-sha-256
  -c "hba_file=$TLS/hba.conf"
  -c wal_level=replica
  -c max_wal_senders=4
  -c max_connections=400
  -c synchronous_commit=on
)

# install_runtime_files: TLS material and pg_hba.conf, re-copied on every start (tmpfs).
install_runtime_files() {
  require_writable "$TLS" /var/run/postgresql /tmp /var/lib/postgresql/data
  local f
  for f in server.crt server.key db-ca.crt hba.conf; do
    [ -r "$SRC/$f" ] || fail "$SRC/$f is missing (provisioner stage 1 has not run?)"
  done
  chown postgres:postgres "$TLS"
  chmod 0700 "$TLS"
  install -o postgres -g postgres -m 0644 "$SRC/server.crt" "$TLS/server.crt"
  install -o postgres -g postgres -m 0600 "$SRC/server.key" "$TLS/server.key"
  install -o postgres -g postgres -m 0644 "$SRC/db-ca.crt" "$TLS/db-ca.crt"
  install -o postgres -g postgres -m 0600 "$SRC/hba.conf" "$TLS/hba.conf"
}

cluster_exists() { [ -s "$PGDATA/PG_VERSION" ]; }

# prepare_init <init script body>: on the first start only, let docker-entrypoint.sh initialise
# the cluster with signaling_owner as superuser (password from a tmpfs copy of owner.pass) and
# source the given body from the initdb.d tmpfs. On later starts no password reaches the
# entrypoint at all, and the body ends by unsetting POSTGRES_PASSWORD, so the running postmaster
# never carries the owner password in its environment.
prepare_init() {
  unset POSTGRES_PASSWORD POSTGRES_PASSWORD_FILE
  export POSTGRES_USER=signaling_owner POSTGRES_DB=postgres
  if cluster_exists; then
    say "cluster exists; skipping initialisation"
    return 0
  fi
  require_writable "$INITDB_D"
  install -o postgres -g postgres -m 0400 "$SRC/owner.pass" "$TLS/owner.pass"
  export POSTGRES_PASSWORD_FILE="$TLS/owner.pass"
  export POSTGRES_INITDB_ARGS="--auth-local=scram-sha-256 --auth-host=scram-sha-256"
  {
    printf '# Sourced by docker-entrypoint.sh as postgres, first start only. LOCAL_TEST_ONLY.\n'
    printf '%s\n' "$1"
    printf 'rm -f %s/owner.pass\n' "$TLS"
    printf 'unset POSTGRES_PASSWORD\n'
  } >"$INITDB_D/10-local-test-only.sh"
  chmod 0444 "$INITDB_D/10-local-test-only.sh"
  say "first start: the entrypoint initialises the cluster"
}

