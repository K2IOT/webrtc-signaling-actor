#!/usr/bin/env bash
# Host checks before `make up`. Every failing check prints ONE line "preflight: FAIL <check>: <cause>";
# exit 0 prints PREFLIGHT_OK. LOCAL_TEST_ONLY / NOT_QUALIFIED.
#
# Checks (names for PREFLIGHT_SKIP): compose, memory, load, subnets, images.
#   compose  docker compose >= 2.26
#   memory   MemAvailable >= PREFLIGHT_MIN_MEM_GIB GiB (default 12; Task 10 measures and adjusts)
#   load     1-minute load average per core <= 0.7 (the 2 s database deadlines flake on a busy host)
#   subnets  172.30.0.0/24, 172.30.1.0/24, 172.30.2.0/24 are free or belong to this compose project
#   images   the five own images exist locally (`make build`)
# Environment: PREFLIGHT_SKIP=name1,name2 skips checks; PREFLIGHT_MIN_MEM_GIB; test overrides
# PREFLIGHT_MEMINFO (default /proc/meminfo), PREFLIGHT_LOADAVG (default /proc/loadavg),
# PREFLIGHT_NPROC (default `nproc`).
# A re-run `make up` on a stack whose 17 JVM services all run skips memory and load (the stack
# itself uses that RAM and CPU) and prints PREFLIGHT_NOTE; the other checks still run.
# Exit: 0 ok, 1 a check failed, 2 usage.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/topology.sh
source "$here/lib/topology.sh"
# shellcheck source=lib/compose_state.sh
source "$here/lib/compose_state.sh"
load_topology

PROJECT_PREFIX="signaling-compose-local_"
MIN_MEM_GIB="${PREFLIGHT_MIN_MEM_GIB:-12}"
MEMINFO="${PREFLIGHT_MEMINFO:-/proc/meminfo}"
LOADAVG="${PREFLIGHT_LOADAVG:-/proc/loadavg}"
KNOWN_CHECKS="compose memory load subnets images"
SKIP=",${PREFLIGHT_SKIP:-},"

case "$MIN_MEM_GIB" in
  '' | *[!0-9]*) echo "preflight: PREFLIGHT_MIN_MEM_GIB must be a whole number" >&2; exit 2 ;;
esac
for s in $(printf '%s' "${PREFLIGHT_SKIP:-}" | tr ',' ' '); do
  case " $KNOWN_CHECKS " in
    *" $s "*) ;;
    *) echo "preflight: unknown check '$s' in PREFLIGHT_SKIP (known: $KNOWN_CHECKS)" >&2; exit 2 ;;
  esac
done

failed=0
fail() { # fail <check> <message>
  printf 'preflight: FAIL %s: %s\n' "$1" "$2"
  failed=1
}
skipped() { case "$SKIP" in *",$1,"*) return 0 ;; *) return 1 ;; esac; }

# Value of KEY from .env, else .env.example, else the environment/default.
env_value() { # env_value KEY default
  local f v
  for f in "$here/.env" "$here/.env.example"; do
    [ -r "$f" ] || continue
    v="$(sed -n "s/^$1=//p" "$f" | tail -n 1)"
    if [ -n "$v" ]; then printf '%s' "$v"; return; fi
  done
  printf '%s' "$2"
}

check_compose() {
  local v
  v="$(docker compose version --short 2>/dev/null || true)"
  v="${v#v}"
  if [ -z "$v" ]; then
    fail compose "docker compose is not available (need >= 2.26)"
    return
  fi
  if [ "$(printf '%s\n2.26\n' "$v" | sort -V | head -n 1)" != "2.26" ]; then
    fail compose "docker compose $v is older than 2.26 (service volume subpath needs 2.26)"
  fi
}

check_memory() {
  local kb need
  kb="$(awk '/^MemAvailable:/ {print $2}' "$MEMINFO" 2>/dev/null || true)"
  if [ -z "$kb" ]; then
    fail memory "cannot read MemAvailable from $MEMINFO"
    return
  fi
  need=$((MIN_MEM_GIB * 1048576))
  if [ "$kb" -lt "$need" ]; then
    fail memory "$((kb / 1048576)) GiB available, at least $MIN_MEM_GIB GiB needed (17 JVMs and 5 PostgreSQL)"
  fi
}

check_load() {
  local load cores
  load="$(awk '{print $1}' "$LOADAVG" 2>/dev/null || true)"
  cores="${PREFLIGHT_NPROC:-$(nproc 2>/dev/null || echo 1)}"
  if [ -z "$load" ] || [ "$cores" -lt 1 ] 2>/dev/null; then
    fail load "cannot read the load average from $LOADAVG"
    return
  fi
  if awk -v l="$load" -v c="$cores" 'BEGIN { exit !(l / c > 0.7) }'; then
    fail load "1-minute load average $load on $cores cores is above 0.7 per core; wait for a quiet host (database deadlines are 500 ms to 2 s)"
  fi
}

check_subnets() {
  local want net subs foreign=""
  want="$NET_SHARED $NET_C001 $NET_C002"
  while IFS= read -r net; do
    [ -n "$net" ] || continue
    case "$net" in "$PROJECT_PREFIX"*) continue ;; esac
    subs="$(docker network inspect "$net" --format '{{range .IPAM.Config}}{{.Subnet}} {{end}}' 2>/dev/null || true)"
    if overlaps "$want" "$subs"; then
      foreign="$foreign $net($(echo $subs))"
    fi
  done < <(docker network ls --format '{{.Name}}' 2>/dev/null || true)
  if [ -n "$foreign" ]; then
    fail subnets "Docker network(s)${foreign} use a subnet of $want; remove them or change the NET_* values in topology.env"
  fi
}

# overlaps "<wanted cidrs>" "<found cidrs>": exit 0 when any found cidr overlaps a wanted one.
overlaps() {
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$1" "$2" <<'PY'
import ipaddress, sys
want = [ipaddress.ip_network(c) for c in sys.argv[1].split()]
for c in sys.argv[2].split():
    try:
        n = ipaddress.ip_network(c, strict=False)
    except ValueError:
        continue
    if any(n.version == w.version and n.overlaps(w) for w in want):
        sys.exit(0)
sys.exit(1)
PY
  else
    local w f
    for f in $2; do
      for w in $1; do [ "$f" = "$w" ] && return 0; done
    done
    return 1
  fi
}

check_images() {
  local key img missing=""
  for key in APP_IMAGE IDENTITY_IMAGE PLATFORM_IMAGE PROVISIONER_IMAGE KUBE_API_IMAGE; do
    case "$key" in
      APP_IMAGE) img="$(env_value APP_IMAGE signaling-app:local)" ;;
      IDENTITY_IMAGE) img="$(env_value IDENTITY_IMAGE signaling-identity:local)" ;;
      PLATFORM_IMAGE) img="$(env_value PLATFORM_IMAGE signaling-local-platform:local)" ;;
      PROVISIONER_IMAGE) img="$(env_value PROVISIONER_IMAGE signaling-provisioner:local)" ;;
      KUBE_API_IMAGE) img="$(env_value KUBE_API_IMAGE signaling-kube-api:local)" ;;
    esac
    docker image inspect "$img" >/dev/null 2>&1 || missing="$missing $img"
  done
  if [ -n "$missing" ]; then
    fail images "missing local image(s):$missing; run 'make build'"
  fi
}

stack_up=0
if [ -z "$(missing_jvm_services)" ]; then
  stack_up=1
  if ! skipped memory || ! skipped load; then
    echo "PREFLIGHT_NOTE stack already running; memory/load not checked"
  fi
fi

for c in $KNOWN_CHECKS; do
  skipped "$c" && continue
  if [ "$stack_up" -eq 1 ] && { [ "$c" = memory ] || [ "$c" = load ]; }; then
    continue
  fi
  "check_$c"
done

if [ "$failed" -ne 0 ]; then
  exit 1
fi
echo PREFLIGHT_OK
