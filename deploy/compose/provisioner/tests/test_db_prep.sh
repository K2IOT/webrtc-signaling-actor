#!/usr/bin/env bash
# Provisioner stage 1, PostgreSQL mount preparation: every pg-* mount gets start.sh, hba.conf,
# owner.pass and (cell servers) replicator.pass. Runs on the host; no Docker (the servers
# themselves are exercised by test_db.sh). LOCAL_TEST_ONLY / NOT_QUALIFIED.
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

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
RUN_ROOT="$TMP/run"
mkdir -p "$RUN_ROOT"
export RUN_ROOT
export IDENTITY_PUBLIC_KEY="$repo/signaling-identity/src/main/resources/keys/public.pem"
STAGE1_OUT="$TMP/stage1.out"

PG_SERVERS="pg-c001-primary pg-c001-standby pg-c002-primary pg-c002-standby pg-directory"
CELL_SERVERS="pg-c001-primary pg-c001-standby pg-c002-primary pg-c002-standby"

kv() { grep -E "^$2=" "$1" | head -n1 | cut -d= -f2-; }
upper() { printf '%s' "$1" | tr '[:lower:]' '[:upper:]'; }
# effective hba lines, whitespace normalised
hba_lines() { awk '!/^[[:space:]]*#/ && NF { $1 = $1; print }' "$1"; }

setup_run() {
  set +e
  bash "$prov/provision.sh" stage1 >"$STAGE1_OUT" 2>&1
  SETUP_RC=$?
  set -e
}

stage1_succeeds() {
  assert_eq "0" "$SETUP_RC" "stage1 exit status"
}

every_pg_mount_has_its_files() {
  local s
  for s in $PG_SERVERS; do
    assert_true "$s/start.sh" test -s "$RUN_ROOT/$s/start.sh"
    assert_true "$s/hba.conf" test -s "$RUN_ROOT/$s/hba.conf"
    assert_true "$s/owner.pass" test -s "$RUN_ROOT/$s/owner.pass"
  done
  for s in $CELL_SERVERS; do
    assert_true "$s/replicator.pass" test -s "$RUN_ROOT/$s/replicator.pass"
  done
  assert_false "pg-directory has no replicator.pass" test -e "$RUN_ROOT/pg-directory/replicator.pass"
}

password_files_hold_the_role_passwords() {
  local env="$RUN_ROOT/_shared/db-passwords.env" s owner repl
  owner="$(kv "$env" DB_PASSWORD_OWNER)"
  repl="$(kv "$env" DB_PASSWORD_REPLICATOR)"
  for s in $PG_SERVERS; do
    assert_eq "$owner" "$(cat "$RUN_ROOT/$s/owner.pass")" "$s/owner.pass is DB_PASSWORD_OWNER"
    assert_eq "$(printf '%s' "$owner" | wc -c | tr -d ' ')" "$(wc -c <"$RUN_ROOT/$s/owner.pass" | tr -d ' ')" \
      "$s/owner.pass has no trailing newline"
  done
  for s in $CELL_SERVERS; do
    assert_eq "$repl" "$(cat "$RUN_ROOT/$s/replicator.pass")" "$s/replicator.pass is DB_PASSWORD_REPLICATOR"
  done
}

mount_files_are_0440() {
  local s f
  for s in $PG_SERVERS; do
    for f in start.sh hba.conf owner.pass; do
      assert_eq "440" "$(stat -c '%a' "$RUN_ROOT/$s/$f")" "$s/$f mode"
    done
  done
}

start_scripts_are_complete_bash() {
  local s
  for s in $PG_SERVERS; do
    assert_true "$s/start.sh parses" bash -n "$RUN_ROOT/$s/start.sh"
    assert_false "$s/start.sh has no unresolved placeholder" grep -q '{{' "$RUN_ROOT/$s/start.sh"
    assert_eq '#!/usr/bin/env bash' "$(head -n1 "$RUN_ROOT/$s/start.sh")" "$s/start.sh shebang"
    assert_true "$s/start.sh names its server" grep -qF "'$s'" "$RUN_ROOT/$s/start.sh"
    assert_true "$s/start.sh hands over to the image entrypoint" grep -q 'exec docker-entrypoint.sh postgres' "$RUN_ROOT/$s/start.sh"
    assert_true "$s/start.sh enforces TLS 1.3" grep -q 'ssl_min_protocol_version=TLSv1.3' "$RUN_ROOT/$s/start.sh"
  done
}

start_scripts_carry_the_replication_topology() {
  local cell cu p ip
  for cell in $CELLS; do
    cu="$(upper "$cell")"
    p="$RUN_ROOT/pg-$cell-primary/start.sh"
    assert_true "pg-$cell-primary is synchronous to standby-$cell" \
      grep -qF "synchronous_standby_names=ANY 1 (\\\"standby-$cell\\\")" "$p"
    assert_false "pg-$cell-primary takes no base backup" grep -q 'pg_basebackup' "$p"
    p="$RUN_ROOT/pg-$cell-standby/start.sh"
    ip="PG_${cu}_PRIMARY_IP"
    assert_true "pg-$cell-standby backs up from its primary" grep -qF "host=${!ip} " "$p"
    assert_true "pg-$cell-standby application_name" grep -qF "application_name=standby-$cell " "$p"
    assert_true "pg-$cell-standby verifies the primary certificate" grep -qF 'sslmode=verify-full' "$p"
    assert_true "pg-$cell-standby bounds its connection attempts" grep -qF 'connect_timeout=5 ' "$p"
    assert_true "pg-$cell-standby only trusts a finished base backup" grep -qF 'standby.signal' "$p"
    assert_true "pg-$cell-standby is a hot standby" grep -q 'hot_standby=on' "$p"
    assert_false "pg-$cell-standby sets no synchronous names" grep -q 'synchronous_standby_names=' "$p"
  done
  p="$RUN_ROOT/pg-directory/start.sh"
  assert_false "pg-directory sets no synchronous names" grep -q 'synchronous_standby_names=' "$p"
  assert_false "pg-directory takes no base backup" grep -q 'pg_basebackup' "$p"
}

hba_matches_the_server_role() {
  local cell cu sip net expected
  for cell in $CELLS; do
    cu="$(upper "$cell")"
    sip="PG_${cu}_STANDBY_IP"
    net="NET_${cu}"
    expected="$(printf '%s\n' \
      "local all signaling_owner scram-sha-256" \
      "hostssl all signaling_owner ${!net} scram-sha-256" \
      "hostssl signaling signaling_app ${!net} scram-sha-256" \
      "hostssl replication replicator ${!sip}/32 scram-sha-256")"
    assert_eq "$expected" "$(hba_lines "$RUN_ROOT/pg-$cell-primary/hba.conf")" "pg-$cell-primary hba"
    expected="$(printf '%s\n' \
      "local all signaling_owner scram-sha-256" \
      "hostssl all signaling_owner ${!net} scram-sha-256")"
    assert_eq "$expected" "$(hba_lines "$RUN_ROOT/pg-$cell-standby/hba.conf")" "pg-$cell-standby hba"
  done
  expected="$(printf '%s\n' \
    "local all signaling_owner scram-sha-256" \
    "hostssl all signaling_owner $NET_SHARED scram-sha-256" \
    "hostssl directory signaling_app $CONTROL_IP/32 scram-sha-256" \
    "hostssl directory signaling_nodir $NET_SHARED scram-sha-256" \
    "hostssl source source_owner $PLATFORM_IP/32 scram-sha-256")"
  assert_eq "$expected" "$(hba_lines "$RUN_ROOT/pg-directory/hba.conf")" "pg-directory hba"
}

hba_has_no_weak_entry() {
  local s
  for s in $PG_SERVERS; do
    assert_eq "" "$(hba_lines "$RUN_ROOT/$s/hba.conf" | awk '$NF != "scram-sha-256"')" "$s: every method is scram-sha-256"
    assert_eq "" "$(hba_lines "$RUN_ROOT/$s/hba.conf" | awk '$1 != "local" && $1 != "hostssl"')" "$s: TCP only over TLS"
    assert_false "$s: no trust anywhere" grep -qiw 'trust' "$RUN_ROOT/$s/hba.conf"
  done
}

no_secret_in_rendered_scripts_or_output() {
  local env="$RUN_ROOT/_shared/db-passwords.env" key pw s
  for key in DB_PASSWORD_OWNER DB_PASSWORD_APP DB_PASSWORD_NODIR DB_PASSWORD_REPLICATOR DB_PASSWORD_SOURCE_OWNER; do
    pw="$(kv "$env" "$key")"
    assert_false "$key not on stage-1 output" grep -qF "$pw" "$STAGE1_OUT"
    for s in $PG_SERVERS; do
      assert_false "$key not in $s/start.sh" grep -qF "$pw" "$RUN_ROOT/$s/start.sh"
      assert_false "$key not in $s/hba.conf" grep -qF "$pw" "$RUN_ROOT/$s/hba.conf"
    done
  done
}

# The placeholder renderer itself: an unknown placeholder or a value outside the allowed
# characters is refused and nothing is written.
placeholder_renderer_fails_closed() {
  local t="$TMP/tmpl"
  printf 'a={{KNOWN}}\nb={{UNKNOWN}}\n' >"$t"
  assert_false "unknown placeholder refused" \
    bash -c 'source "$1/lib/pki.sh"; source "$1/lib/dbprep.sh"; render_placeholders "$2.out" "$2" -- KNOWN=x' _ "$prov" "$t"
  assert_false "nothing written on refusal" test -e "$t.out"
  printf 'a={{KNOWN}}\n' >"$t"
  assert_false "value with a shell metacharacter refused" \
    bash -c 'source "$1/lib/pki.sh"; source "$1/lib/dbprep.sh"; render_placeholders "$2.out" "$2" -- "KNOWN=x;y"' _ "$prov" "$t"
  assert_true "a valid value renders" \
    bash -c 'source "$1/lib/pki.sh"; source "$1/lib/dbprep.sh"; render_placeholders "$2.out" "$2" -- KNOWN=172.30.1.0/24' _ "$prov" "$t"
  assert_eq "a=172.30.1.0/24" "$(cat "$t.out" 2>/dev/null)" "rendered value"
}

setup_run
for t in stage1_succeeds every_pg_mount_has_its_files password_files_hold_the_role_passwords \
  mount_files_are_0440 start_scripts_are_complete_bash start_scripts_carry_the_replication_topology \
  hba_matches_the_server_role hba_has_no_weak_entry no_secret_in_rendered_scripts_or_output \
  placeholder_renderer_fails_closed; do
  run_test "$t"
done
finish
