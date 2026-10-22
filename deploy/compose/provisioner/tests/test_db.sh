#!/usr/bin/env bash
# REQUIRES_DOCKER
# Database stage against real PostgreSQL 17.6 containers. NEEDS DOCKER.
#
#   tests/run-tests.sh runs this file only with RUN_DOCKER_TESTS=1 (it takes minutes); invoked
#   directly it always runs. Without a `docker` binary it prints SKIP and exits 0, unless CI is
#   set, then it fails.
#
# It creates three throwaway networks with the topology subnets (sp3dbtest-c001|c002|shared),
# a named `run` volume, runs provisioner stage 1 into it, starts the five PostgreSQL servers with
# `docker run` the way compose will (static IPs, `run` volume subpath mounted read-only at
# /run/signaling, read-only rootfs, tmpfs, cap_drop ALL + the five capabilities, entrypoint
# `bash /run/signaling/start.sh`), runs provisioner stage 2 attached to the three networks and
# then checks the databases with psql from the provisioner image (a long-lived probe container).
# The working tree's deploy/compose and migration directories are mounted read-only over the
# image copies, so a stale image never hides a script change; the image is only the toolbox
# (psql 17, flyway 11.7.2, jq, openssl). Everything carrying the test label is removed on exit.
# LOCAL_TEST_ONLY / NOT_QUALIFIED.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
prov="$(cd "$here/.." && pwd)"
compose="$(cd "$prov/.." && pwd)"
repo="$(cd "$compose/../.." && pwd)"
# shellcheck source=../../tests/lib.sh
source "$compose/tests/lib.sh"
# shellcheck source=../../lib/topology.sh
source "$compose/lib/topology.sh"
load_topology

PREFIX=sp3dbtest
LABEL="io.webrtc.signaling.test=$PREFIX"
PROV_IMAGE="${PROVISIONER_IMAGE:-signaling-provisioner:local}"
PG_IMAGE="${POSTGRES_IMAGE:-postgres:17.6}"
RUN_VOL="$PREFIX-run"
PROBE="$PREFIX-probe"
SRC=/run/signaling-src
CA_DB="$SRC/_shared/ca-db.crt"
PG_SERVERS="pg-c001-primary pg-c001-standby pg-c002-primary pg-c002-standby pg-directory"
STORAGE_MIGRATIONS="$repo/signaling-storage/src/main/resources/db/migration"
DIRECTORY_MIGRATIONS="$repo/signaling-control-api/src/main/resources/db/directory/migration"
STARTED_AT="$(date +%s)"

say() { printf '%s\n' "$*"; }
abort() { printf 'test_db: ABORT %s\n' "$*" >&2; exit 1; }
upper() { printf '%s' "$1" | tr '[:lower:]' '[:upper:]'; }

# --- preconditions -----------------------------------------------------------------------

if ! command -v docker >/dev/null 2>&1; then
  if [ -z "${CI:-}" ]; then
    say "SKIP test_db.sh: docker is not installed (this test needs Docker)"
    exit 0
  fi
  abort "docker is not installed and CI is set"
fi
docker info >/dev/null 2>&1 || abort "the Docker daemon is not reachable"
docker image inspect "$PROV_IMAGE" >/dev/null 2>&1 ||
  abort "image $PROV_IMAGE missing; build it: docker build -t signaling-provisioner:local -f deploy/compose/provisioner/Dockerfile ."
docker image inspect "$PG_IMAGE" >/dev/null 2>&1 || abort "image $PG_IMAGE missing; docker pull $PG_IMAGE"

net_name() { printf '%s-%s' "$PREFIX" "$1"; }
net_subnet() {
  case "$1" in
    c001) printf '%s' "$NET_C001" ;;
    c002) printf '%s' "$NET_C002" ;;
    shared) printf '%s' "$NET_SHARED" ;;
  esac
}

# Stale resources of an earlier run of THIS test (our label) are removed; anything else that
# holds one of the names or overlaps one of the subnets aborts the run untouched.
remove_labelled() {
  docker ps -aq --filter "label=$LABEL" | xargs -r docker rm -f -v >/dev/null 2>&1 || true
  docker network ls -q --filter "label=$LABEL" | xargs -r docker network rm >/dev/null 2>&1 || true
  docker volume ls -q --filter "label=$LABEL" | xargs -r docker volume rm -f >/dev/null 2>&1 || true
}

check_networks_free() {
  local id name subnets n mine
  while read -r id; do
    name="$(docker network inspect -f '{{.Name}}' "$id")"
    subnets="$(docker network inspect -f '{{range .IPAM.Config}}{{.Subnet}} {{end}}' "$id")"
    for n in c001 c002 shared; do
      [ "$name" != "$(net_name "$n")" ] || abort "network $name already exists (not created by this test); remove it first"
      for mine in $subnets; do
        if python3 -c 'import ipaddress,sys; sys.exit(0 if ipaddress.ip_network(sys.argv[1],strict=False).overlaps(ipaddress.ip_network(sys.argv[2])) else 1)' "$mine" "$(net_subnet "$n")"; then
          abort "subnet $(net_subnet "$n") is in use by network $name (a running compose project? run its 'docker compose down' first)"
        fi
      done
    done
  done < <(docker network ls -q)
}

remove_labelled
check_networks_free
LOGS="$(mktemp -d)"
# on_exit: remove every labelled container, network and volume, keep the exit status.
# Debugging only: KEEP_LOGS=1 keeps the provisioner logs (already scanned for secrets),
# KEEP_CONTAINERS=1 leaves the environment running (the next run removes it first).
on_exit() {
  local rc=$?
  if [ "${KEEP_CONTAINERS:-0}" = 1 ]; then
    say "test_db: KEEP_CONTAINERS=1, environment left running (label $LABEL)"
  else
    remove_labelled
  fi
  if [ "${KEEP_LOGS:-0}" = 1 ]; then say "test_db: logs kept in $LOGS"; else rm -rf "$LOGS"; fi
  exit "$rc"
}
trap on_exit EXIT
trap 'exit 130' INT TERM

# --- environment -------------------------------------------------------------------------

for n in c001 c002 shared; do
  docker network create --label "$LABEL" --subnet "$(net_subnet "$n")" "$(net_name "$n")" >/dev/null
done
docker volume create --label "$LABEL" "$RUN_VOL" >/dev/null

PROV_MOUNTS=(
  -v "$compose:/opt/provisioner/compose:ro"
  -v "$STORAGE_MIGRATIONS:/opt/provisioner/migrations/storage:ro"
  -v "$DIRECTORY_MIGRATIONS:/opt/provisioner/migrations/directory:ro"
)
PROVISION=(bash /opt/provisioner/compose/provisioner/provision.sh)

# provisioner <log> <args...>: one-shot provisioner container attached to all three networks.
provisioner() {
  local log="$1" name="$PREFIX-provisioner" rc=0
  shift
  docker create --name "$name" --label "$LABEL" --network "$(net_name c001)" \
    -v "$RUN_VOL:$SRC" "${PROV_MOUNTS[@]}" "$PROV_IMAGE" "$@" >/dev/null
  docker network connect "$(net_name c002)" "$name"
  docker network connect "$(net_name shared)" "$name"
  docker start -a "$name" >"$log" 2>&1 || rc=$?
  docker rm -f "$name" >/dev/null
  return "$rc"
}

pg_kind_net() {
  case "$1" in
    pg-directory) printf 'shared' ;;
    pg-c001-*) printf 'c001' ;;
    pg-c002-*) printf 'c002' ;;
  esac
}

pg_ip() {
  case "$1" in
    pg-directory) ip_of pg-directory shared ;;
    *) ip_of "$1" cell ;;
  esac
}

# start_pg <service>: the server as compose runs it.
start_pg() {
  local s="$1" init_tmpfs=()
  case "$s" in *-standby) ;; *) init_tmpfs=(--tmpfs /docker-entrypoint-initdb.d) ;; esac
  docker volume create --label "$LABEL" "$PREFIX-data-$s" >/dev/null
  docker run -d --name "$PREFIX-$s" --label "$LABEL" \
    --network "$(net_name "$(pg_kind_net "$s")")" --ip "$(pg_ip "$s")" \
    --read-only --tmpfs /tmp --tmpfs /var/run/postgresql --tmpfs /run/pgtls "${init_tmpfs[@]}" \
    --cap-drop ALL --cap-add CHOWN --cap-add DAC_OVERRIDE --cap-add FOWNER --cap-add SETGID --cap-add SETUID \
    --security-opt no-new-privileges \
    --mount "type=volume,src=$RUN_VOL,dst=/run/signaling,volume-subpath=$s,readonly" \
    --mount "type=volume,src=$PREFIX-data-$s,dst=/var/lib/postgresql/data" \
    -e POSTGRES_USER=signaling_owner -e POSTGRES_PASSWORD_FILE=/run/signaling/owner.pass \
    --entrypoint bash "$PG_IMAGE" /run/signaling/start.sh >/dev/null
}

# The probe: psql/flyway/jq from the provisioner image, attached to all networks, the run volume
# read-only. Its pgpass and owner password file are written inside the container from
# db-passwords.env, so no password crosses the host's argv.
start_probe() {
  docker create --name "$PROBE" --label "$LABEL" --network "$(net_name c001)" \
    -v "$RUN_VOL:$SRC:ro" "${PROV_MOUNTS[@]}" "$PROV_IMAGE" bash -c '
      set -euo pipefail
      set -a; . /run/signaling-src/_shared/db-passwords.env; set +a
      umask 077
      {
        printf "*:*:*:signaling_owner:%s\n" "$DB_PASSWORD_OWNER"
        printf "*:*:*:signaling_app:%s\n" "$DB_PASSWORD_APP"
        printf "*:*:*:signaling_nodir:%s\n" "$DB_PASSWORD_NODIR"
        printf "*:*:*:source_owner:%s\n" "$DB_PASSWORD_SOURCE_OWNER"
        printf "*:*:*:replicator:%s\n" "$DB_PASSWORD_REPLICATOR"
      } >/tmp/pgpass
      printf "%s" "$DB_PASSWORD_OWNER" >/tmp/owner.pass
      unset DB_PASSWORD_OWNER DB_PASSWORD_APP DB_PASSWORD_NODIR DB_PASSWORD_SOURCE_OWNER DB_PASSWORD_REPLICATOR
      exec sleep infinity' >/dev/null
  docker network connect "$(net_name c002)" "$PROBE"
  docker network connect "$(net_name shared)" "$PROBE"
  docker start "$PROBE" >/dev/null
}

probe() { docker exec -e PGPASSFILE=/tmp/pgpass -e PGCONNECT_TIMEOUT=5 "$PROBE" "$@"; }

# q <host> <db> <user> <sql>: one query over verify-full TLS; prints unaligned tuples.
q() {
  probe psql -X -q -tA -v ON_ERROR_STOP=1 \
    -d "host=$1 port=$PG_PORT dbname=$2 user=$3 sslmode=verify-full sslrootcert=$CA_DB" -c "$4"
}
# qc <conninfo> <sql>: a query with a caller-built connection string (the TLS negatives).
qc() { probe psql -X -q -tA -v ON_ERROR_STOP=1 -d "$1" -c "$2"; }

wait_servers() {
  local deadline=$(($(date +%s) + 180)) s ok
  while :; do
    ok=1
    for s in $PG_SERVERS; do
      if [ "$(docker inspect -f '{{.State.Running}}' "$PREFIX-$s")" != true ]; then
        docker logs --tail 30 "$PREFIX-$s" >&2 || true
        abort "$s exited during start-up (log tail above)"
      fi
      probe pg_isready -q -h "$(pg_ip "$s")" -p "$PG_PORT" -t 2 || ok=0
    done
    [ "$ok" -eq 0 ] || return 0
    [ "$(date +%s)" -lt "$deadline" ] || abort "PostgreSQL servers not ready after 180 s"
    sleep 1
  done
}

say "test_db: stage 1"
STAGE1_RC=0
provisioner "$LOGS/stage1.log" "${PROVISION[@]}" stage1 || STAGE1_RC=$?
[ "$STAGE1_RC" -eq 0 ] || { cat "$LOGS/stage1.log" >&2; abort "stage 1 failed (rc $STAGE1_RC)"; }
start_probe
say "test_db: starting PostgreSQL servers"
for s in $PG_SERVERS; do start_pg "$s"; done
wait_servers
T_SERVERS="$(date +%s)"
say "test_db: stage 2"
STAGE2_RC=0
provisioner "$LOGS/stage2.log" "${PROVISION[@]}" stage2 || STAGE2_RC=$?
T_STAGE2="$(date +%s)"
SECOND_RC=0
provisioner "$LOGS/stage2-again.log" "${PROVISION[@]}" stage2 || SECOND_RC=$?
say "test_db: servers ready after $((T_SERVERS - STARTED_AT)) s, stage 2 took $((T_STAGE2 - T_SERVERS)) s"

PASSWORDS="$(probe cat "$SRC/_shared/db-passwords.env")"
pw_of() { local l; while IFS= read -r l; do [ "${l%%=*}" != "$1" ] || printf '%s' "${l#*=}"; done <<<"$PASSWORDS"; }
WAL_ENV="$(probe cat "$SRC/_shared/wal.env" 2>/dev/null || true)"
wal_of() { local l; while IFS= read -r l; do [ "${l%%=*}" != "$1" ] || printf '%s' "${l#*=}"; done <<<"$WAL_ENV"; }

# --- tests -------------------------------------------------------------------------------

stage2_succeeds() {
  if ! assert_eq "0" "$STAGE2_RC" "stage 2 exit status"; then
    sed 's/^/  stage2| /' "$LOGS/stage2.log" | tail -n 30 >&2
  fi
  assert_true ".stage2-done written" probe test -f "$SRC/_shared/.stage2-done"
  assert_true ".rendered written by the online render" probe test -f "$SRC/.rendered"
  assert_false "no .offline marker" probe test -e "$SRC/.offline"
  local s
  for s in $PG_SERVERS; do
    assert_eq "true" "$(docker inspect -f '{{.State.Running}}' "$PREFIX-$s")" "$s still running"
  done
}

second_stage2_is_refused() {
  assert_false "second stage 2 exits non-zero" test "$SECOND_RC" -eq 0
  assert_true "refusal names the marker" grep -q 'PROVISION_REFUSED.*\.stage2-done' "$LOGS/stage2-again.log"
  assert_true ".stage2-done still present" probe test -f "$SRC/_shared/.stage2-done"
}

cell_seed_matches_the_fixture() {
  local cell cu ip first last active
  for cell in $CELLS; do
    cu="$(upper "$cell")"
    ip="PG_${cu}_PRIMARY_IP"
    ip="${!ip}"
    if [ "$cell" = c001 ]; then first=0 last=$((SPLIT - 1)); else first=$SPLIT last=16383; fi
    active=$((last - first + 1))
    assert_eq "1|$cell|1|GROUPED|1|ACTIVE" "$(q "$ip" signaling signaling_owner 'SELECT * FROM cell_authority')" "$cell cell_authority"
    assert_eq "16384" "$(q "$ip" signaling signaling_owner 'SELECT count(*) FROM bucket_authority')" "$cell bucket_authority rows"
    assert_eq "$active|$first|$last" \
      "$(q "$ip" signaling signaling_owner "SELECT count(*),min(bucket_id),max(bucket_id) FROM bucket_authority WHERE status='ACTIVE'")" \
      "$cell ACTIVE buckets are its half"
    assert_eq "$((16384 - active))" \
      "$(q "$ip" signaling signaling_owner "SELECT count(*) FROM bucket_authority WHERE status='FROZEN'")" "$cell FROZEN buckets"
    assert_eq "0" "$(q "$ip" signaling signaling_owner 'SELECT count(*) FROM bucket_authority WHERE directory_epoch<>1 OR recovery_epoch<>1 OR transfer_id IS NOT NULL OR bucket_id NOT BETWEEN 0 AND 16383')" \
      "$cell bucket epochs"
    assert_eq "1024|0|1023|1024" \
      "$(q "$ip" signaling signaling_owner "SELECT count(*),min(group_id),max(group_id),count(*) FILTER (WHERE cell_id='$cell' AND ownership_hash_version=1 AND storage_epoch=1 AND group_epoch=1 AND lease_sequence=0 AND status='IDLE' AND owner_node IS NULL) FROM group_owner")" \
      "$cell group_owner rows"
  done
}

# Flyway CLI's own view: validate compares every recorded checksum with the files; the V016
# checksum is also recomputed the way the app's compatibility gate does (CRC32 over the lines).
app_crc32() {
  python3 -I -c '
import re, sys, zlib
text = open(sys.argv[1], "rb").read().decode("utf-8")
lines = re.split(r"\r\n|\r|\n", text)
if lines and lines[-1] == "":
    lines.pop()
if lines and lines[0].startswith("﻿"):
    lines[0] = lines[0][1:]
crc = 0
for line in lines:
    crc = zlib.crc32(line.encode("utf-8"), crc)
print(crc - (1 << 32) if crc >= (1 << 31) else crc)' "$1"
}

flyway_on() { # flyway_on <ip> <db> <location> <table> <command>
  probe bash -c 'FLYWAY_PASSWORD="$(< /tmp/owner.pass)" flyway -url="jdbc:postgresql://$1:$2/$3?sslmode=verify-full&sslrootcert=$4" \
    -user=signaling_owner -locations="filesystem:$5" -table="$6" "$7"' _ "$1" "$PG_PORT" "$2" "$CA_DB" "$3" "$4" "$5"
}

flyway_history_is_complete() {
  local cell cu ip expected
  expected="$(app_crc32 "$STORAGE_MIGRATIONS/V016__storage_domain_renewal_adoption.sql")"
  for cell in $CELLS; do
    cu="$(upper "$cell")"
    ip="PG_${cu}_PRIMARY_IP"
    ip="${!ip}"
    assert_eq "18|18|1|18" \
      "$(q "$ip" signaling signaling_owner 'SELECT count(*),count(*) FILTER (WHERE success),min(version::int),max(version::int) FROM flyway_schema_history')" \
      "$cell flyway_schema_history: 18 successful migrations V001-V018"
    assert_eq "$expected" \
      "$(q "$ip" signaling signaling_owner "SELECT checksum FROM flyway_schema_history WHERE success AND version IN ('16','016')")" \
      "$cell V016 checksum equals the file's CRC32"
    assert_true "$cell flyway validate (CLI checksums match the files)" \
      flyway_on "$ip" signaling /opt/provisioner/migrations/storage flyway_schema_history validate
  done
  assert_eq "2|2" \
    "$(q "$PG_DIRECTORY_IP" directory signaling_owner 'SELECT count(*),count(*) FILTER (WHERE success) FROM directory_schema_history')" \
    "directory_schema_history: V001-V002"
  assert_true "directory flyway validate" \
    flyway_on "$PG_DIRECTORY_IP" directory /opt/provisioner/migrations/directory directory_schema_history validate
}

directory_seed_matches_the_snapshot() {
  local snapshot ranges rows url1 url2
  snapshot="$(probe cat "$SRC/gateway-c001/directory.json")"
  ranges="$(jq -r '.buckets[] | "\(.cell)|\(.first)|\(.last)|\(.last - .first + 1)"' <<<"$snapshot")"
  rows="$(q "$PG_DIRECTORY_IP" directory signaling_owner \
    'SELECT cell_id,min(bucket_id),max(bucket_id),count(*) FROM directory_bucket GROUP BY cell_id ORDER BY 2')"
  assert_eq "$ranges" "$rows" "directory_bucket split equals directory.json ranges"
  assert_eq "c001|0|$((SPLIT - 1))|$SPLIT" "$(head -n1 <<<"$rows")" "c001 owns buckets below SPLIT"
  url1="wss://$HOSTNAME_C001:$GATEWAY_PUBLISH_C001/ws"
  url2="wss://$HOSTNAME_C002:$GATEWAY_PUBLISH_C002/ws"
  assert_eq "c001|$url1|$SPLIT
c002|$url2|$((16384 - SPLIT))" \
    "$(q "$PG_DIRECTORY_IP" directory signaling_owner 'SELECT cell_id,wss_url,count(*) FROM directory_bucket GROUP BY 1,2 ORDER BY 1')" \
    "directory wss_url per cell"
  assert_eq "16384" "$(q "$PG_DIRECTORY_IP" directory signaling_owner 'SELECT count(*) FROM directory_bucket WHERE directory_epoch=1')" \
    "every directory row at epoch 1"
}

source_database_is_left_to_the_platform() {
  assert_eq "source_owner" \
    "$(q "$PG_DIRECTORY_IP" postgres signaling_owner "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='source'")" \
    "source is owned by source_owner"
  assert_eq "0" "$(q "$PG_DIRECTORY_IP" source signaling_owner "SELECT count(*) FROM pg_tables WHERE schemaname='public'")" \
    "source holds no table (the platform migrates it)"
  assert_true "source_owner may create in source.public" \
    q "$PG_DIRECTORY_IP" source source_owner 'BEGIN; CREATE TABLE sp3_probe(i int); ROLLBACK'
  assert_false "signaling_app cannot connect to source" q "$PG_DIRECTORY_IP" source signaling_app 'SELECT 1'
  assert_false "source_owner cannot connect to directory" q "$PG_DIRECTORY_IP" directory source_owner 'SELECT 1'
}

quorum_standby_per_cell() {
  local cell cu ip sip
  for cell in $CELLS; do
    cu="$(upper "$cell")"
    ip="PG_${cu}_PRIMARY_IP"
    ip="${!ip}"
    sip="PG_${cu}_STANDBY_IP"
    sip="${!sip}"
    assert_eq "standby-$cell|streaming|quorum|$sip" \
      "$(q "$ip" postgres signaling_owner 'SELECT application_name,state,sync_state,client_addr FROM pg_stat_replication')" \
      "$cell: exactly one quorum standby"
    assert_eq "ANY 1 (\"standby-$cell\")" "$(q "$ip" postgres signaling_owner 'SHOW synchronous_standby_names')" \
      "$cell synchronous_standby_names"
    assert_eq "t|16384" "$(q "$sip" signaling signaling_owner 'SELECT pg_is_in_recovery(),(SELECT count(*) FROM bucket_authority)')" \
      "$cell standby is in recovery and holds the seed"
  done
}

server_settings() {
  local s ip expected
  for s in $PG_SERVERS; do
    ip="$(pg_ip "$s")"
    expected="on|TLSv1.3|400|replica|4|on|scram-sha-256|/run/pgtls/hba.conf"
    assert_eq "$expected" \
      "$(q "$ip" postgres signaling_owner "SELECT string_agg(setting, '|' ORDER BY ord) FROM unnest(ARRAY['ssl','ssl_min_protocol_version','max_connections','wal_level','max_wal_senders','synchronous_commit','password_encryption','hba_file']) WITH ORDINALITY AS u(n, ord) JOIN pg_settings ON name = n")" \
      "$s server settings"
  done
}

app_role_monitors_but_cannot_ddl() {
  local cell cu ip
  for cell in $CELLS; do
    cu="$(upper "$cell")"
    ip="PG_${cu}_PRIMARY_IP"
    ip="${!ip}"
    assert_eq "f|t" "$(q "$ip" signaling signaling_app "SELECT rolsuper, pg_has_role('signaling_app','pg_monitor','member') FROM pg_roles WHERE rolname=current_user")" \
      "$cell signaling_app: not superuser, member of pg_monitor"
    assert_eq "t|t|quorum" \
      "$(q "$ip" signaling signaling_app 'SELECT (pg_control_system()).system_identifier IS NOT NULL, pg_current_wal_flush_lsn() IS NOT NULL, (SELECT sync_state FROM pg_stat_replication)')" \
      "$cell signaling_app reads pg_control_system, the flush LSN and pg_stat_replication"
    assert_eq "$(wal_of "WAL_${cu}_SYSTEM_ID")|$(wal_of "WAL_${cu}_TIMELINE")" \
      "$(q "$ip" signaling signaling_app "SELECT system_identifier::text,('x'||substring(pg_walfile_name(pg_current_wal_flush_lsn()),1,8))::bit(32)::bigint FROM pg_control_system()")" \
      "$cell WAL facts query as signaling_app equals wal.env"
    assert_eq "16384|t|f" \
      "$(q "$ip" signaling signaling_app "SELECT (SELECT count(*) FROM bucket_authority), has_table_privilege('bucket_authority','INSERT,UPDATE,DELETE'), has_table_privilege('flyway_schema_history','INSERT')")" \
      "$cell signaling_app: DML on the schema, read-only history"
    assert_false "$cell signaling_app cannot CREATE TABLE" q "$ip" signaling signaling_app 'CREATE TABLE sp3_probe(i int)'
  done
  assert_eq "16384" "$(q "$PG_DIRECTORY_IP" directory signaling_app 'SELECT count(*) FROM directory_bucket')" \
    "signaling_app reads the directory"
  assert_false "signaling_app cannot CREATE TABLE in directory" \
    q "$PG_DIRECTORY_IP" directory signaling_app 'CREATE TABLE sp3_probe(i int)'
}

nodir_role_reaches_nothing() {
  local out
  out="$(q "$PG_DIRECTORY_IP" directory signaling_nodir 'SELECT 1' 2>&1 || true)"
  assert_true "signaling_nodir refused by privilege on directory (got: $out)" grep -q 'permission denied for database' <<<"$out"
  assert_false "signaling_nodir cannot connect to signaling (c001)" q "$PG_C001_PRIMARY_IP" signaling signaling_nodir 'SELECT 1'
  assert_eq "f|f|t" \
    "$(q "$PG_DIRECTORY_IP" postgres signaling_owner "SELECT rolsuper, pg_has_role('signaling_nodir','pg_monitor','member'), rolcanlogin FROM pg_roles WHERE rolname='signaling_nodir'")" \
    "signaling_nodir: login only, no pg_monitor"
}

tls_is_mandatory_and_verified() {
  local s ip base
  for s in $PG_SERVERS; do
    ip="$(pg_ip "$s")"
    base="host=$ip port=$PG_PORT dbname=postgres user=signaling_owner"
    assert_eq "TLSv1.3" "$(qc "$base sslmode=verify-full sslrootcert=$CA_DB" 'SELECT version FROM pg_stat_ssl WHERE pid=pg_backend_pid()')" \
      "$s positive control: verify-full over TLS 1.3"
    assert_false "$s refuses a TLS 1.2-only client" \
      qc "$base sslmode=verify-full sslrootcert=$CA_DB ssl_max_protocol_version=TLSv1.2" 'SELECT 1'
    assert_false "$s refuses a plaintext client" qc "$base sslmode=disable" 'SELECT 1'
    assert_false "$s is not trusted under another CA" \
      qc "$base sslmode=verify-full sslrootcert=$SRC/_shared/ca-workload.crt" 'SELECT 1'
  done
}

wal_env_holds_numeric_facts() {
  local cell cu sys tl ip
  for cell in $CELLS; do
    cu="$(upper "$cell")"
    sys="$(wal_of "WAL_${cu}_SYSTEM_ID")"
    tl="$(wal_of "WAL_${cu}_TIMELINE")"
    assert_true "WAL_${cu}_SYSTEM_ID numeric ($sys)" grep -Eq '^[0-9]{1,20}$' <<<"$sys"
    assert_true "WAL_${cu}_TIMELINE numeric and >= 1 ($tl)" grep -Eq '^[1-9][0-9]*$' <<<"$tl"
    ip="PG_${cu}_PRIMARY_IP"
    assert_eq "$sys" "$(q "${!ip}" signaling signaling_owner 'SELECT system_identifier::text FROM pg_control_system()')" \
      "WAL_${cu}_SYSTEM_ID is $cell's cluster"
    assert_eq "$sys|$tl" \
      "$(probe jq -r '.wal | "\(.systemIdentifier)|\(.timeline)"' "$SRC/actor-$cell-1/process.json")" \
      "actor-$cell-1 profile carries the WAL facts"
  done
  assert_false "the two cells are distinct clusters" test "$(wal_of WAL_C001_SYSTEM_ID)" = "$(wal_of WAL_C002_SYSTEM_ID)"
  assert_eq "4" "$(grep -c '^WAL_C00[12]_\(SYSTEM_ID\|TIMELINE\)=' <<<"$WAL_ENV" || true)" "wal.env has the four keys"
}

standby_conninfo_holds_no_password() {
  local cell conf pw
  pw="$(pw_of DB_PASSWORD_REPLICATOR)"
  for cell in $CELLS; do
    conf="$(docker exec -u postgres "$PREFIX-pg-$cell-standby" cat /var/lib/postgresql/data/pgdata/postgresql.auto.conf)"
    # pg_basebackup -R quotes values ('' inside the setting): application_name=''standby-c001''
    assert_true "$cell standby primary_conninfo names standby-$cell" grep -Eq "application_name='*standby-$cell'* " <<<"$conf"
    assert_true "$cell standby replicates over verify-full" grep -Eq "sslmode='*verify-full'* " <<<"$conf"
    assert_true "$cell standby asks for TLS 1.3 itself" grep -Eq "ssl_min_protocol_version='*TLSv1.3'* " <<<"$conf"
    assert_true "$cell standby reads its password from the tmpfs pgpass" grep -Eq "passfile='*/run/pgtls/pgpass'* " <<<"$conf"
    assert_false "$cell standby conninfo has no password=" grep -q 'password=' <<<"$conf"
    CHECKS=$((CHECKS + 1))
    [[ "$conf" != *"$pw"* ]] || _fail "$cell standby conninfo contains the replicator password"
  done
}

# contains_secret <text>: true when any role password occurs in <text> (bash matching, so the
# password never becomes an argument of a command).
contains_secret() {
  local key pw
  for key in DB_PASSWORD_OWNER DB_PASSWORD_APP DB_PASSWORD_NODIR DB_PASSWORD_REPLICATOR DB_PASSWORD_SOURCE_OWNER; do
    pw="$(pw_of "$key")"
    [ -n "$pw" ] || continue
    [[ "$1" != *"$pw"* ]] || return 0
  done
  return 1
}

no_secret_leaks() {
  local f s text
  assert_eq "5" "$(grep -c '^DB_PASSWORD_[A-Z_]*=[0-9a-f]\{48\}$' <<<"$PASSWORDS" || true)" "five role passwords read for the scan"
  for f in stage1 stage2 stage2-again; do
    text="$(<"$LOGS/$f.log")"
    assert_false "no password in the $f log" contains_secret "$text"
    assert_false "no private key in the $f log" grep -q 'PRIVATE KEY' "$LOGS/$f.log"
  done
  for s in $PG_SERVERS; do
    text="$(docker logs "$PREFIX-$s" 2>&1)"
    assert_false "no password in the $s log" contains_secret "$text"
    text="$(docker exec -u postgres "$PREFIX-$s" cat /proc/1/environ | tr '\0' '\n')"
    assert_false "$s postmaster environment holds no password" contains_secret "$text"
    assert_false "$s postmaster environment has no non-empty POSTGRES_PASSWORD" grep -q "^POSTGRES_PASSWORD=." <<<"$text"
    text="$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$PREFIX-$s")"
    assert_false "$s container config holds no password" contains_secret "$text"
  done
}

# A restart takes the "cluster exists" path: start.sh re-creates its tmpfs files (TLS, hba,
# the standby's pgpass), initialises nothing, and the standby streams as quorum again.
servers_survive_a_restart() {
  local s deadline state text
  docker restart "$PREFIX-pg-c001-primary" "$PREFIX-pg-c001-standby" >/dev/null
  deadline=$(($(date +%s) + 120))
  while :; do
    state="$(q "$PG_C001_PRIMARY_IP" postgres signaling_owner \
      "SELECT coalesce(string_agg(state || '/' || sync_state, ','), 'none') FROM pg_stat_replication WHERE application_name='standby-c001'" 2>/dev/null || true)"
    [ "$state" != "streaming/quorum" ] || break
    [ "$(date +%s)" -lt "$deadline" ] || break
    sleep 1
  done
  assert_eq "streaming/quorum" "$state" "standby-c001 is a quorum standby again after restarting both"
  assert_eq "1|c001|1|GROUPED|1|ACTIVE" "$(q "$PG_C001_STANDBY_IP" signaling signaling_owner 'SELECT * FROM cell_authority')" \
    "the restarted standby kept its data"
  for s in pg-c001-primary pg-c001-standby; do
    assert_true "$s skipped initialisation" grep -q 'cluster exists; skipping initialisation\|Skipping initialization' \
      <<<"$(docker logs --since 2m "$PREFIX-$s" 2>&1)"
    text="$(docker exec -u postgres "$PREFIX-$s" cat /proc/1/environ | tr '\0' '\n')"
    assert_false "$s restarted postmaster environment holds no password" contains_secret "$text"
  done
}

for t in stage2_succeeds second_stage2_is_refused cell_seed_matches_the_fixture flyway_history_is_complete \
  directory_seed_matches_the_snapshot source_database_is_left_to_the_platform quorum_standby_per_cell \
  server_settings app_role_monitors_but_cannot_ddl nodir_role_reaches_nothing tls_is_mandatory_and_verified \
  wal_env_holds_numeric_facts standby_conninfo_holds_no_password no_secret_leaks servers_survive_a_restart; do
  run_test "$t"
done
say "test_db: total $(($(date +%s) - STARTED_AT)) s"
finish
