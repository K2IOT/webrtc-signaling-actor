#!/usr/bin/env bash
# Provisioner stage 2, databases: db.sh migrate-and-seed | wal-facts
#
#   migrate-and-seed  waits for the five servers and for each cell's standby to stream as a
#                     quorum standby (every commit on a primary waits for it), then per cell
#                     primary: roles, database `signaling`, Flyway V001-V018 (history table
#                     flyway_schema_history), grants, the cell seed; on pg-directory: roles,
#                     databases `directory` and `source`, Flyway directory V001-V002 (history
#                     table directory_schema_history), grants, the directory seed. `source` is
#                     created for source_owner and left empty (local-platform migrates it).
#   wal-facts         per cell, after the standby is a streaming quorum standby again, reads
#                     system_identifier and the timeline exactly as LocalCompositionDatabase does
#                     and writes $RUN_ROOT/_shared/wal.env (WAL_<CELL>_SYSTEM_ID, WAL_<CELL>_TIMELINE),
#                     which render.sh reads.
#
# Runs inside the provisioner image (psql 17, pg_isready, flyway 11.7.2) on all three networks.
# Every connection is TLS verify-full against _shared/ca-db.crt to the server's IP. It connects
# as signaling_owner: psql reads the password from a pgpass file in a private temporary
# directory, Flyway from FLYWAY_PASSWORD; role passwords reach psql as \set lines on stdin. No
# password is passed as an argument or printed; tool output shown on failure is scrubbed of
# every role password first. Failures: one `DB_REFUSED <cause>` line on stderr, exit 1.
# LOCAL_TEST_ONLY / NOT_QUALIFIED.
set -euo pipefail

here="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
prov="$(cd "$here/.." && pwd)"
compose="$(cd "$prov/.." && pwd)"
# shellcheck source=../../lib/topology.sh
source "$compose/lib/topology.sh"

RUN_ROOT="${RUN_ROOT:-/run/signaling-src}"
SHARED="$RUN_ROOT/_shared"
CA="$SHARED/ca-db.crt"
SQL_DIR="$prov/sql"
MIGRATIONS="${PROVISION_MIGRATIONS:-/opt/provisioner/migrations}"
GROUP_ID="${PROVISION_GROUP_ID:-10001}"
WAIT_SECONDS="${PROVISION_DB_WAIT_SECONDS:-180}"
OWNER=signaling_owner
SECRET_RE='^[0-9a-f]{16,128}$'
IP_RE='^[0-9]{1,3}(\.[0-9]{1,3}){3}$'
VAR_VALUE_RE='^[A-Za-z0-9_.:/-]+$'
WAL_QUERY="SELECT system_identifier::text,('x'||substring(pg_walfile_name(pg_current_wal_flush_lsn()),1,8))::bit(32)::bigint FROM pg_control_system()"

refuse() {
  printf 'DB_REFUSED %s\n' "$*" >&2
  exit 1
}
log() { printf 'db: %s\n' "$*"; }
upper() { printf '%s' "$1" | tr '[:lower:]' '[:upper:]'; }

# --- inputs ------------------------------------------------------------------------------

validate_topology() {
  local key
  [ "$CELLS" = "c001 c002" ] || refuse "CELLS must be 'c001 c002' (the seed and the snapshot split two cells)"
  for key in PG_C001_PRIMARY_IP PG_C001_STANDBY_IP PG_C002_PRIMARY_IP PG_C002_STANDBY_IP PG_DIRECTORY_IP; do
    [[ "${!key:-}" =~ $IP_RE ]] || refuse "topology $key is not an IPv4 address"
  done
  [[ "${PG_PORT:-}" =~ ^[1-9][0-9]{0,4}$ ]] || refuse "topology PG_PORT is not a port"
  [[ "${SPLIT:-}" =~ ^[1-9][0-9]{0,4}$ ]] && [ "$SPLIT" -le 16383 ] || refuse "topology SPLIT must be 1..16383"
  for key in HOSTNAME_C001 HOSTNAME_C002 GATEWAY_PUBLISH_C001 GATEWAY_PUBLISH_C002; do
    [[ "${!key:-}" =~ ^[A-Za-z0-9.-]+$ ]] || refuse "topology $key is missing or malformed"
  done
}

# read_passwords: _shared/db-passwords.env into PW[OWNER|APP|NODIR|REPLICATOR|SOURCE_OWNER].
read_passwords() {
  local file="$SHARED/db-passwords.env" line key
  [ -r "$file" ] || refuse "_shared/db-passwords.env is missing (stage 1 not complete?)"
  declare -gA PW=()
  while IFS= read -r line || [ -n "$line" ]; do
    key="${line%%=*}"
    case "$key" in
      DB_PASSWORD_OWNER | DB_PASSWORD_APP | DB_PASSWORD_NODIR | DB_PASSWORD_REPLICATOR | DB_PASSWORD_SOURCE_OWNER)
        PW[${key#DB_PASSWORD_}]="${line#*=}"
        ;;
    esac
  done <"$file"
  for key in OWNER APP NODIR REPLICATOR SOURCE_OWNER; do
    [[ "${PW[$key]:-}" =~ $SECRET_RE ]] || refuse "DB_PASSWORD_$key in _shared/db-passwords.env is missing or malformed"
  done
  [ -r "$CA" ] || refuse "_shared/ca-db.crt is missing (stage 1 not complete?)"
}

# scrub <text>: every role password replaced by [REDACTED] (bash only, nothing on argv).
scrub() {
  local text="$1" key
  for key in "${!PW[@]}"; do text="${text//"${PW[$key]}"/[REDACTED]}"; done
  printf '%s' "$text"
}

# first_problem <text>: the first ERROR/FATAL line, else the last non-empty line.
first_problem() {
  local line last=""
  while IFS= read -r line; do
    case "$line" in *ERROR* | *FATAL* | *error:* | *Error:*)
      printf '%s' "$line"
      return 0
      ;;
    esac
    [ -z "$line" ] || last="$line"
  done <<<"$1"
  printf '%s' "$last"
}

fail_with() { refuse "$1: $(scrub "$(first_problem "$2")")"; }

open_session() {
  WORK="$(mktemp -d)"
  trap 'rm -rf "${WORK:?}"' EXIT
  umask 077
  printf '*:*:*:%s:%s\n' "$OWNER" "${PW[OWNER]}" >"$WORK/pgpass"
  unset PGPASSWORD PGHOST PGHOSTADDR PGDATABASE PGUSER PGSERVICE
  export PGPASSFILE="$WORK/pgpass" PGCONNECT_TIMEOUT=5 PGAPPNAME=provisioner
}

# --- psql / flyway -----------------------------------------------------------------------

conninfo() {
  printf 'host=%s port=%s dbname=%s user=%s sslmode=verify-full sslrootcert=%s' "$1" "$PG_PORT" "$2" "$OWNER" "$CA"
}

# sql <ip> <db> <label> <query>: one query as signaling_owner; unaligned tuples on stdout.
sql() {
  local out
  if ! out="$(psql -X -q -tA -v ON_ERROR_STOP=1 -d "$(conninfo "$1" "$2")" -c "$4" 2>"$WORK/err")"; then
    fail_with "$3" "$(<"$WORK/err")"
  fi
  printf '%s' "$out"
}

# psql_vars NAME=VALUE...: \set lines for a script's stdin; values from a narrow set.
psql_vars() {
  local kv
  for kv in "$@"; do
    [[ "${kv#*=}" =~ $VAR_VALUE_RE ]] || refuse "psql variable ${kv%%=*} has an unexpected value"
    printf '\\set %s %s\n' "${kv%%=*}" "${kv#*=}"
  done
}

# run_script <ip> <db> <label> <sql file> NAME=VALUE...: the file on psql's stdin after the
# variables, as one session; ON_ERROR_STOP makes the first error fatal.
run_script() {
  local ip="$1" db="$2" label="$3" file="$SQL_DIR/$4" vars
  shift 4
  [ -r "$file" ] || refuse "missing $file"
  vars="$(psql_vars "$@")"
  if ! { printf '%s\n' "$vars"; cat "$file"; } |
    psql -X -q -v ON_ERROR_STOP=1 -d "$(conninfo "$ip" "$db")" >/dev/null 2>"$WORK/err"; then
    fail_with "$(basename "$file") on $label/$db" "$(<"$WORK/err")"
  fi
  log "applied $(basename "$file") on $label/$db"
}

# flyway_migrate <ip> <db> <label> <migration dir> <history table>
flyway_migrate() {
  local ip="$1" db="$2" label="$3" dir="$4" table="$5" out applied
  [ -d "$dir" ] || refuse "migration directory $dir is missing"
  if ! out="$(FLYWAY_PASSWORD="${PW[OWNER]}" REDGATE_DISABLE_TELEMETRY=true flyway \
    -url="jdbc:postgresql://$ip:$PG_PORT/$db?sslmode=verify-full&sslrootcert=$CA" \
    -user="$OWNER" -locations="filesystem:$dir" -table="$table" \
    -validateMigrationNaming=true -failOnMissingLocations=true migrate 2>&1)"; then
    fail_with "flyway migrate of $label/$db" "$out"
  fi
  applied="$(grep -Eo 'Successfully applied [0-9]+ migrations?|is up to date' <<<"$out" | head -n1 || true)"
  log "flyway migrate $label/$db ($table): ${applied:-done}"
}

# --- waiting -----------------------------------------------------------------------------

# wait_ready <ip> <label>: bounded wait until the server accepts connections, then one
# authenticated query (a wrong password fails at once, not after the wait).
wait_ready() {
  local deadline=$((SECONDS + WAIT_SECONDS))
  until pg_isready -q -h "$1" -p "$PG_PORT" -t 3; do
    [ "$SECONDS" -lt "$deadline" ] || refuse "$2 ($1:$PG_PORT) not accepting connections after ${WAIT_SECONDS}s"
    sleep 1
  done
  sql "$1" postgres "$2" 'SELECT 1' >/dev/null
}

# wait_quorum <cell>: the primary names standby-<cell> synchronously and it streams as quorum.
wait_quorum() {
  local cell="$1" cu ip label name state deadline=$((SECONDS + WAIT_SECONDS))
  cu="$(upper "$cell")"
  ip="PG_${cu}_PRIMARY_IP"
  ip="${!ip}"
  label="pg-$cell-primary"
  name="standby-$cell"
  state="$(sql "$ip" postgres "$label" 'SHOW synchronous_standby_names')"
  [ "$state" = "ANY 1 (\"$name\")" ] || refuse "$label: synchronous_standby_names is '$state', expected 'ANY 1 (\"$name\")'"
  while :; do
    state="$(sql "$ip" postgres "$label" \
      "SELECT coalesce(string_agg(state || '/' || sync_state, ','), 'none') FROM pg_stat_replication WHERE application_name = '$name'")"
    [ "$state" != "streaming/quorum" ] || return 0
    [ "$SECONDS" -lt "$deadline" ] ||
      refuse "$name is not a streaming quorum standby of $label after ${WAIT_SECONDS}s (observed: $state)"
    sleep 1
  done
}

wait_all() {
  local cell cu v
  for cell in $CELLS; do
    cu="$(upper "$cell")"
    v="PG_${cu}_PRIMARY_IP"
    wait_ready "${!v}" "pg-$cell-primary"
    v="PG_${cu}_STANDBY_IP"
    wait_ready "${!v}" "pg-$cell-standby"
  done
  wait_ready "$PG_DIRECTORY_IP" pg-directory
  for cell in $CELLS; do wait_quorum "$cell"; done
  log "five servers reachable over verify-full TLS; standby-c001 and standby-c002 stream as quorum standbys"
}

# --- stage 2 work ------------------------------------------------------------------------

migrate_cell() {
  local cell="$1" cu ip label first last
  cu="$(upper "$cell")"
  ip="PG_${cu}_PRIMARY_IP"
  ip="${!ip}"
  label="pg-$cell-primary"
  # c001 owns buckets 0..SPLIT-1, c002 SPLIT..16383 (the same rule as directory.json)
  if [ "$cell" = c001 ]; then first=0 last=$((SPLIT - 1)); else first=$SPLIT last=16383; fi
  run_script "$ip" postgres "$label" roles.sql is_cell=true is_directory=false \
    "app_password=${PW[APP]}" "replicator_password=${PW[REPLICATOR]}"
  run_script "$ip" postgres "$label" databases.sql is_cell=true is_directory=false
  flyway_migrate "$ip" signaling "$label" "$MIGRATIONS/storage" flyway_schema_history
  run_script "$ip" signaling "$label" grants.sql dbname=signaling history_table=flyway_schema_history is_source=false
  run_script "$ip" signaling "$label" seed-cell.sql "cell=$cell" "first_active=$first" "last_active=$last" \
    "active_count=$((last - first + 1))"
}

migrate_directory() {
  local ip="$PG_DIRECTORY_IP" label=pg-directory
  run_script "$ip" postgres "$label" roles.sql is_cell=false is_directory=true \
    "app_password=${PW[APP]}" "nodir_password=${PW[NODIR]}" "source_owner_password=${PW[SOURCE_OWNER]}"
  run_script "$ip" postgres "$label" databases.sql is_cell=false is_directory=true
  flyway_migrate "$ip" directory "$label" "$MIGRATIONS/directory" directory_schema_history
  run_script "$ip" directory "$label" grants.sql dbname=directory history_table=directory_schema_history is_source=false
  run_script "$ip" source "$label" grants.sql dbname=source history_table=none is_source=true
  run_script "$ip" directory "$label" seed-directory.sql "split=$SPLIT" first_cell=c001 second_cell=c002 \
    "first_url=wss://$HOSTNAME_C001:$GATEWAY_PUBLISH_C001/ws" "second_url=wss://$HOSTNAME_C002:$GATEWAY_PUBLISH_C002/ws"
}

migrate_and_seed() {
  wait_all
  local cell
  for cell in $CELLS; do migrate_cell "$cell"; done
  migrate_directory
  log "migrate-and-seed done (LOCAL_TEST_ONLY / NOT_QUALIFIED)"
}

wal_facts() {
  local cell cu ip row sys tl out="" tmp="$SHARED/.wal.env.tmp"
  for cell in $CELLS; do
    wait_quorum "$cell"
    cu="$(upper "$cell")"
    ip="PG_${cu}_PRIMARY_IP"
    row="$(sql "${!ip}" signaling "pg-$cell-primary" "$WAL_QUERY")"
    sys="${row%%|*}"
    tl="${row#*|}"
    [[ "$sys" =~ ^[0-9]{1,20}$ ]] || refuse "pg-$cell-primary: system_identifier '$sys' is not 1-20 digits"
    [[ "$tl" =~ ^[1-9][0-9]{0,9}$ ]] || refuse "pg-$cell-primary: timeline '$tl' is not a positive integer"
    out+="WAL_${cu}_SYSTEM_ID=$sys"$'\n'"WAL_${cu}_TIMELINE=$tl"$'\n'
  done
  printf '# WAL facts of the cell primaries, read after their standby streamed as quorum. LOCAL_TEST_ONLY.\n%s' "$out" >"$tmp"
  chmod 0440 "$tmp"
  [ "$(id -u)" -ne 0 ] || chown "root:$GROUP_ID" "$tmp"
  mv -f "$tmp" "$SHARED/wal.env"
  log "wrote _shared/wal.env"
}

main() {
  case "${1:-}" in
    migrate-and-seed | wal-facts) ;;
    *)
      printf 'usage: db.sh migrate-and-seed|wal-facts\n' >&2
      exit 64
      ;;
  esac
  load_topology
  validate_topology
  read_passwords
  open_session
  case "$1" in
    migrate-and-seed) migrate_and_seed ;;
    wal-facts) wal_facts ;;
  esac
}

main "$@"
