#!/usr/bin/env bash
# preflight.sh: each failing condition ends in exactly one clear line, a quiet host passes.
# Everything is stubbed (docker on PATH, meminfo, loadavg, nproc by env). LOCAL_TEST_ONLY / NOT_QUALIFIED.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
compose="$(cd "$here/.." && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Stub docker: behaviour is driven by STUB_* variables.
#   STUB_COMPOSE_VERSION   output of `docker compose version --short` (default 2.40.0)
#   STUB_NETWORKS          file with lines "<network name> <subnet>"
#   STUB_MISSING_IMAGES    space-separated image references `docker image inspect` rejects
#   STUB_RUNNING           services that `docker ps --filter ...` reports as running
make_stub() {
  mkdir -p "$WORK/bin"
  cat >"$WORK/bin/docker" <<'EOF'
#!/usr/bin/env bash
case "$1 ${2:-}" in
  "compose version") echo "${STUB_COMPOSE_VERSION:-2.40.0}" ;;
  "network ls") [ -f "${STUB_NETWORKS:-/nonexistent}" ] && awk '{print $1}' "$STUB_NETWORKS" || true ;;
  "network inspect")
    name="$3"
    [ -f "${STUB_NETWORKS:-/nonexistent}" ] && awk -v n="$name" '$1 == n {print $2}' "$STUB_NETWORKS" || true ;;
  "ps --filter") for s in ${STUB_RUNNING:-}; do echo "$s"; done ;;
  "image inspect")
    for m in ${STUB_MISSING_IMAGES:-}; do
      [ "$3" = "$m" ] && { echo "Error: No such image: $3" >&2; exit 1; }
    done
    exit 0 ;;
  *) echo "stub docker: unexpected: $*" >&2; exit 99 ;;
esac
EOF
  chmod +x "$WORK/bin/docker"
}
make_stub

# A quiet host: 24 GiB available, load 0.5 on 16 cores.
printf 'MemTotal: 33554432 kB\nMemAvailable: 25165824 kB\n' >"$WORK/meminfo"
printf '0.50 0.40 0.30 1/100 1234\n' >"$WORK/loadavg"
: >"$WORK/networks"

# run_preflight [VAR=value...]  -> sets OUT and RC
run_preflight() {
  RC=0
  OUT="$(env PATH="$WORK/bin:$PATH" \
    PREFLIGHT_MEMINFO="$WORK/meminfo" PREFLIGHT_LOADAVG="$WORK/loadavg" PREFLIGHT_NPROC=16 \
    STUB_NETWORKS="$WORK/networks" "$@" bash "$compose/preflight.sh" 2>&1)" || RC=$?
}

lines_with() { printf '%s\n' "$OUT" | grep -c "$1" || true; }

passes_on_a_quiet_host_stub() {
  run_preflight
  assert_eq "0" "$RC" "exit status on a quiet host"
  assert_eq "PREFLIGHT_OK" "$(printf '%s\n' "$OUT" | tail -n 1)" "last line"
  assert_eq "0" "$(lines_with 'FAIL')" "no failure line"
}

fails_when_compose_is_too_old() {
  run_preflight STUB_COMPOSE_VERSION=2.25.9
  assert_eq "1" "$RC" "exit status"
  assert_eq "1" "$(lines_with 'FAIL compose')" "one compose line"
  assert_eq "0" "$(lines_with 'PREFLIGHT_OK')" "no OK line"
  run_preflight STUB_COMPOSE_VERSION=v2.26.0
  assert_eq "0" "$RC" "2.26.0 with a v prefix is enough"
  run_preflight STUB_COMPOSE_VERSION=5.6.0
  assert_eq "0" "$RC" "5.6.0 is enough"
}

fails_below_memory_floor() {
  printf 'MemAvailable: 8388608 kB\n' >"$WORK/meminfo.low"   # 8 GiB
  run_preflight PREFLIGHT_MEMINFO="$WORK/meminfo.low"
  assert_eq "1" "$RC" "exit status below 12 GiB"
  assert_eq "1" "$(lines_with 'FAIL memory')" "one memory line"
  run_preflight PREFLIGHT_MEMINFO="$WORK/meminfo.low" PREFLIGHT_MIN_MEM_GIB=4
  assert_eq "0" "$RC" "the floor is configurable"
}

fails_when_load_is_high() {
  printf '12.00 9.00 8.00 1/100 1234\n' >"$WORK/loadavg.high"   # 0.75 per core on 16 cores
  run_preflight PREFLIGHT_LOADAVG="$WORK/loadavg.high"
  assert_eq "1" "$RC" "exit status above 0.7 per core"
  assert_eq "1" "$(lines_with 'FAIL load')" "one load line"
  printf '11.00 9.00 8.00 1/100 1234\n' >"$WORK/loadavg.edge"   # 0.6875 per core
  run_preflight PREFLIGHT_LOADAVG="$WORK/loadavg.edge"
  assert_eq "0" "$RC" "0.6875 per core passes"
}

fails_when_a_foreign_network_uses_a_subnet() {
  printf 'bridge 172.17.0.0/16\nother_net 172.30.1.0/24\n' >"$WORK/networks.foreign"
  run_preflight STUB_NETWORKS="$WORK/networks.foreign"
  assert_eq "1" "$RC" "exit status"
  assert_eq "1" "$(lines_with 'FAIL subnets')" "one subnets line"
  assert_eq "1" "$(lines_with 'other_net')" "the line names the foreign network"
  # an enclosing network overlaps as well
  printf 'wide 172.30.0.0/16\n' >"$WORK/networks.wide"
  run_preflight STUB_NETWORKS="$WORK/networks.wide"
  assert_eq "1" "$RC" "an overlapping /16 is refused"
  # the project's own networks are fine
  printf 'signaling-compose-local_net-c001 172.30.1.0/24\nsignaling-compose-local_net-shared 172.30.0.0/24\n' >"$WORK/networks.own"
  run_preflight STUB_NETWORKS="$WORK/networks.own"
  assert_eq "0" "$RC" "project networks are not foreign"
}

fails_when_an_image_is_missing() {
  run_preflight STUB_MISSING_IMAGES="signaling-kube-api:local signaling-app:local"
  assert_eq "1" "$RC" "exit status"
  assert_eq "1" "$(lines_with 'FAIL images')" "one images line"
  assert_eq "1" "$(lines_with 'make build')" "the hint names make build"
  assert_eq "1" "$(lines_with 'signaling-kube-api:local')" "names the missing image"
}

skip_env_skips_a_check() {
  run_preflight STUB_COMPOSE_VERSION=2.1.0 PREFLIGHT_SKIP=compose
  assert_eq "0" "$RC" "compose check skipped"
  run_preflight STUB_COMPOSE_VERSION=2.1.0 STUB_MISSING_IMAGES="signaling-app:local" PREFLIGHT_SKIP=compose,images
  assert_eq "0" "$RC" "two checks skipped"
  run_preflight STUB_COMPOSE_VERSION=2.1.0 STUB_MISSING_IMAGES="signaling-app:local" PREFLIGHT_SKIP=compose
  assert_eq "1" "$RC" "only the named check is skipped"
  run_preflight PREFLIGHT_SKIP=bogus
  assert_eq "2" "$RC" "an unknown check name is a usage error"
}

all_jvm="$(
  source "$compose/lib/topology.sh"
  load_topology
  service_list | tr '\n' ' '
)"

running_stack_skips_memory_and_load_checks() {
  printf 'MemAvailable: 1048576 kB\n' >"$WORK/meminfo.busy" # 1 GiB: the stack itself uses the RAM
  printf '30.00 20.00 10.00 1/100 1234\n' >"$WORK/loadavg.busy"
  run_preflight PREFLIGHT_MEMINFO="$WORK/meminfo.busy" PREFLIGHT_LOADAVG="$WORK/loadavg.busy" STUB_RUNNING="$all_jvm"
  assert_eq "0" "$RC" "all 17 running: resource checks skipped"
  assert_eq "1" "$(lines_with 'PREFLIGHT_NOTE stack already running; memory/load not checked')" "the note"
  assert_eq "PREFLIGHT_OK" "$(printf '%s\n' "$OUT" | tail -n 1)" "still ends with OK"
  # the other checks still run on a running stack
  run_preflight STUB_RUNNING="$all_jvm" STUB_COMPOSE_VERSION=2.1.0
  assert_eq "1" "$RC" "compose check still runs"
  run_preflight STUB_RUNNING="$all_jvm" STUB_MISSING_IMAGES="signaling-app:local"
  assert_eq "1" "$RC" "image check still runs"
  printf 'foreign 172.30.2.0/24\n' >"$WORK/networks.foreign2"
  run_preflight STUB_RUNNING="$all_jvm" STUB_NETWORKS="$WORK/networks.foreign2"
  assert_eq "1" "$RC" "subnet check still runs"
  # no note on a stack that is not up
  run_preflight
  assert_eq "0" "$(lines_with 'PREFLIGHT_NOTE')" "no note when nothing runs"
}

sixteen_running_still_fails_memory_and_load() {
  local sixteen
  sixteen="$(printf '%s\n' $all_jvm | grep -v '^gateway-c002$' | tr '\n' ' ')"
  printf 'MemAvailable: 1048576 kB\n' >"$WORK/meminfo.busy"
  printf '30.00 20.00 10.00 1/100 1234\n' >"$WORK/loadavg.busy"
  run_preflight PREFLIGHT_MEMINFO="$WORK/meminfo.busy" PREFLIGHT_LOADAVG="$WORK/loadavg.busy" STUB_RUNNING="$sixteen"
  assert_eq "1" "$RC" "16 running: checks apply"
  assert_eq "1" "$(lines_with 'FAIL memory')" "memory fails"
  assert_eq "1" "$(lines_with 'FAIL load')" "load fails"
  assert_eq "0" "$(lines_with 'PREFLIGHT_NOTE')" "no note"
}

run_test passes_on_a_quiet_host_stub
run_test fails_when_compose_is_too_old
run_test fails_below_memory_floor
run_test fails_when_load_is_high
run_test fails_when_a_foreign_network_uses_a_subnet
run_test fails_when_an_image_is_missing
run_test skip_env_skips_a_check
run_test running_stack_skips_memory_and_load_checks
run_test sixteen_running_still_fails_memory_and_load
finish
