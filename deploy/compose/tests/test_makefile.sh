#!/usr/bin/env bash
# Makefile (dry run, `make -n`) and guard.sh (stub docker). LOCAL_TEST_ONLY / NOT_QUALIFIED.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
compose="$(cd "$here/.." && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"
# shellcheck source=../lib/topology.sh
source "$compose/lib/topology.sh"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

dry() { make -n -C "$compose" "$@" 2>&1; }

line_no() { printf '%s\n' "$1" | grep -n -m1 -- "$2" | cut -d: -f1; }

up_runs_preflight_and_guard_before_compose_up() {
  local out p g u
  out="$(dry up)"
  p="$(line_no "$out" 'preflight.sh')"
  g="$(line_no "$out" 'guard.sh')"
  u="$(line_no "$out" 'up -d --wait')"
  assert_true "preflight present" test -n "$p"
  assert_true "guard present" test -n "$g"
  assert_true "compose up present" test -n "$u"
  assert_true "preflight before guard" test "${p:-99}" -lt "${g:-0}"
  assert_true "guard before compose up" test "${g:-99}" -lt "${u:-0}"
  assert_true "a generous wait timeout" grep -q -- '--wait-timeout 600' <<<"$out"
  assert_true "both env files" grep -q -- '--env-file .*topology.env --env-file .*\.env' <<<"$out"
  # the project name reaches the guard from compose.yaml
  assert_true "guard gets the project" grep -q 'COMPOSE_PROJECT=signaling-compose-local' <<<"$out"
}

build_shows_maven_and_five_image_builds() {
  local out n
  out="$(dry build)"
  assert_true "maven command" grep -q -- './mvnw -o -pl signaling-app,signaling-identity,signaling-local-platform -am package -DskipTests -Dspotless.check.skip=true' <<<"$out"
  n="$(printf '%s\n' "$out" | grep -c 'docker build' || true)"
  assert_eq "5" "$n" "number of docker builds"
  local img
  for img in signaling-app:local signaling-identity:local signaling-local-platform:local signaling-kube-api:local signaling-provisioner:local; do
    assert_true "builds $img" grep -q -- "-t $img" <<<"$out"
  done
  assert_eq "5" "$(printf '%s\n' "$out" | grep -c -- '--build-arg SOURCE_COMMIT=[0-9a-f]\{40\}' || true)" "SOURCE_COMMIT on every build"
  for f in 'Dockerfile \.' 'Dockerfile.identity \.' 'Dockerfile.local-platform \.' 'deploy/compose/kube-api/Dockerfile \.' 'deploy/compose/provisioner/Dockerfile \.'; do
    assert_true "dockerfile $f" grep -q -- "-f $f" <<<"$out"
  done
  # online build when asked
  out="$(dry build MVN_OFFLINE=0)"
  assert_true "online maven" grep -q -- './mvnw -pl signaling-app' <<<"$out"
}

down_removes_volumes() {
  local out
  out="$(dry down)"
  assert_true "down -v --remove-orphans" grep -q -- 'down -v --remove-orphans' <<<"$out"
  out="$(dry reset)"
  assert_true "reset downs with -v" grep -q -- 'down -v --remove-orphans' <<<"$out"
  assert_true "reset removes .smoke" grep -q -- 'rm -rf .*\.smoke' <<<"$out"
}

help_lists_all_targets() {
  local out t
  out="$(make -C "$compose" help 2>&1)"
  for t in build hosts preflight up smoke-ca smoke down reset lint test help; do
    assert_true "help lists $t" grep -q -E "^ *$t( |$)" <<<"$out"
  done
}

smoke_exports_material_then_runs_the_client() {
  local out s r
  out="$(dry smoke)"
  s="$(line_no "$out" 'smoke.mjs')"
  r="$(line_no "$out" 'ca-workload.crt')"
  assert_true "smoke.mjs runs" test -n "$s"
  assert_true "ca export first" test "${r:-99}" -lt "${s:-0}"
  for v in CA_FILE ADMIN_CERT_FILE ADMIN_KEY_FILE; do
    assert_true "smoke gets $v" grep -q "$v=" <<<"$out"
  done
}

lint_and_test_targets() {
  assert_true "lint target" grep -q 'compose_lint.py' <<<"$(dry lint)"
  assert_true "test target" grep -q 'run-tests.sh' <<<"$(dry test)"
}

# --- guard.sh with a stub docker -------------------------------------------------------------
# STUB_VOLUME=1        the run volume exists
# STUB_OFFLINE=1       /r/.offline exists in it
# STUB_RUNNING="a b"   services of the compose project that are running
make_guard_stub() {
  mkdir -p "$WORK/bin"
  cat >"$WORK/bin/docker" <<'EOF'
#!/usr/bin/env bash
case "$1 ${2:-}" in
  "volume inspect") [ "${STUB_VOLUME:-0}" = "1" ] ;;
  "ps --filter") for s in ${STUB_RUNNING:-}; do echo "$s"; done ;;
  "run --rm") [ "${STUB_OFFLINE:-0}" = "1" ] ;;
  *) echo "stub docker: unexpected: $*" >&2; exit 99 ;;
esac
EOF
  chmod +x "$WORK/bin/docker"
}
make_guard_stub

# --- smoke-ca with a stub docker -------------------------------------------------------------
# STUB_VOLUME=0 the run volume does not exist; STUB_FAIL_PATH=<substring>: `docker run ... cat <path>`
# fails when its path contains it, after writing a partial byte (like a broken pipe).
make_smoke_stub() {
  mkdir -p "$WORK/sbin"
  cat >"$WORK/sbin/docker" <<'STUB'
#!/usr/bin/env bash
case "$1 ${2:-}" in
  "volume inspect") [ "${STUB_VOLUME:-1}" = "1" ] ;;
  "run --rm")
    path="${*: -1}"
    if [ -n "${STUB_FAIL_PATH:-}" ] && [[ "$path" == *"$STUB_FAIL_PATH"* ]]; then printf 'part'; exit 1; fi
    printf 'TEST_ONLY %s\n' "$path" ;;
  *) echo "stub docker: unexpected: $*" >&2; exit 99 ;;
esac
STUB
  chmod +x "$WORK/sbin/docker"
}
make_smoke_stub

# run_smoke_ca <smoke-dir> [VAR=value...] -> OUT, RC (SMOKE_DIR is redirected to a temp directory)
run_smoke_ca() {
  local dir="$1"
  shift
  RC=0
  OUT="$(env PATH="$WORK/sbin:$PATH" "$@" make -C "$compose" smoke-ca SMOKE_DIR="$dir" 2>&1)" || RC=$?
}

mode_of() { stat -c '%a' "$1"; }
tmp_files() { find "$1" -maxdepth 1 -name '.tmp*' | wc -l | tr -d ' '; }

smoke_ca_success_path_is_unchanged() {
  local d="$WORK/smoke-ok" f
  run_smoke_ca "$d"
  assert_eq "0" "$RC" "all exports succeed"
  assert_eq "700" "$(mode_of "$d")" "directory mode"
  for f in ca-workload.crt admin-client.crt admin-client.key; do
    assert_true "$f exists" test -f "$d/$f"
    assert_eq "600" "$(mode_of "$d/$f")" "$f mode"
    assert_true "$f reported" grep -q "exported .smoke/$f" <<<"$OUT"
  done
  assert_eq "0" "$(tmp_files "$d")" "no tmp files left"
}

smoke_ca_fails_when_the_ca_export_fails() {
  local d="$WORK/smoke-ca-fail"
  mkdir -p "$d"
  echo STALE >"$d/ca-workload.crt"
  run_smoke_ca "$d" STUB_FAIL_PATH=ca-workload.crt
  assert_true "non-zero exit" test "$RC" -ne 0
  assert_false "no 'exported' for the CA" grep -q 'exported .smoke/ca-workload.crt' <<<"$OUT"
  assert_false "a stale CA is not left behind" test -e "$d/ca-workload.crt"
  assert_eq "0" "$(tmp_files "$d")" "no tmp files left"
}

smoke_ca_fails_when_only_the_admin_key_export_fails() {
  local d="$WORK/smoke-key-fail"
  run_smoke_ca "$d" STUB_FAIL_PATH=admin-client.key
  assert_true "non-zero exit" test "$RC" -ne 0
  assert_false "no 'exported' for the key" grep -q 'exported .smoke/admin-client.key' <<<"$OUT"
  assert_false "no key file" test -e "$d/admin-client.key"
  assert_eq "0" "$(tmp_files "$d")" "no tmp files left"
}

smoke_ca_refuses_without_the_volume() {
  run_smoke_ca "$WORK/smoke-novol" STUB_VOLUME=0
  assert_true "non-zero exit" test "$RC" -ne 0
}

all_jvm="$(load_topology; service_list | tr '\n' ' ')"

run_guard() {
  RC=0
  OUT="$(env PATH="$WORK/bin:$PATH" "$@" bash "$compose/guard.sh" 2>&1)" || RC=$?
}

guard_passes_when_the_volume_does_not_exist() {
  run_guard STUB_VOLUME=0
  assert_eq "0" "$RC" "fresh host passes"
}

guard_refuses_when_the_volume_exists_and_a_jvm_container_is_missing() {
  local partial
  partial="$(printf '%s\n' $all_jvm | grep -v '^actor-c002-3$' | tr '\n' ' ')"
  run_guard STUB_VOLUME=1 STUB_RUNNING="$partial"
  assert_eq "1" "$RC" "one missing container refuses"
  assert_true "message says make reset" grep -q 'make reset' <<<"$OUT"
  assert_true "message names the missing service" grep -q 'actor-c002-3' <<<"$OUT"
  run_guard STUB_VOLUME=1 STUB_RUNNING=""
  assert_eq "1" "$RC" "no container at all (down without -v) refuses"
}

guard_refuses_an_offline_marker() {
  run_guard STUB_VOLUME=1 STUB_OFFLINE=1 STUB_RUNNING="$all_jvm"
  assert_eq "1" "$RC" "offline render refuses even when everything runs"
  assert_true "message mentions the offline render" grep -q -i 'offline' <<<"$OUT"
  assert_true "message says make reset" grep -q 'make reset' <<<"$OUT"
}

guard_passes_when_the_volume_exists_and_all_11_run() {
  assert_eq "11" "$(printf '%s\n' $all_jvm | grep -c .)" "service_list has 11 names"
  run_guard STUB_VOLUME=1 STUB_RUNNING="$all_jvm"
  assert_eq "0" "$RC" "running stack passes"
}

run_test up_runs_preflight_and_guard_before_compose_up
run_test build_shows_maven_and_five_image_builds
run_test down_removes_volumes
run_test help_lists_all_targets
run_test smoke_exports_material_then_runs_the_client
run_test lint_and_test_targets
run_test smoke_ca_success_path_is_unchanged
run_test smoke_ca_fails_when_the_ca_export_fails
run_test smoke_ca_fails_when_only_the_admin_key_export_fails
run_test smoke_ca_refuses_without_the_volume
run_test guard_passes_when_the_volume_does_not_exist
run_test guard_refuses_when_the_volume_exists_and_a_jvm_container_is_missing
run_test guard_refuses_an_offline_marker
run_test guard_passes_when_the_volume_exists_and_all_11_run
finish
