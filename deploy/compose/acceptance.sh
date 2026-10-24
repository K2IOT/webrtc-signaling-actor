#!/usr/bin/env bash
# acceptance.sh -- automates spec section 9 (criteria 1-4 and 6) of the two-cell compose
# environment, plus the network isolation probe and the resource measurements.
# LOCAL_TEST_ONLY / NOT_QUALIFIED: a pass establishes LOCAL_QUALIFIED at most, never anything about
# production, HA, fencing, N-1 or capacity.
#
#   bash deploy/compose/acceptance.sh [phase ...]
#     phases (default: all, in this order): up smoke actor standby refusal isolation secrets measure
#       up        make down; make up; every service healthy, all actors running, /ready 200
#       smoke     make smoke passes (cross-cell call and token revoke); RSS/time measured after it
#       actor     docker stop the last c001 actor: the cell stays ready (quorum is a majority), a fresh smoke passes; start -> healthy
#       standby   docker stop pg-c001-standby: a c001 INVITE does not commit, synchronous_standby_names
#                 unchanged; start -> quorum back, control and the c001 actors ready again; the
#                 gateway's lapsed boot lease is fenced by design (INFO), so the stack is
#                 re-provisioned (make down; make up) and the smoke passes again
#       refusal   bad fingerprint, foreign SPIFFE leaf, missing contract: one container each
#       isolation /dev/tcp probes between the cell networks and to the cell PostgreSQL servers
#       secrets   git, docker inspect env and the logs hold no secret of this run
#     A phase other than `up` expects the stack to be running (ACCEPTANCE_KEEP_UP=1 from a prior run).
#
# Every check prints `PASS <criterion> <check>: <evidence>` or `FAIL ...`; the run ends with
# `ACCEPTANCE_PASSED` (exit 0) or `ACCEPTANCE_FAILED <n> failure(s)` (exit 1). Evidence is counts,
# status codes, close/error codes and fixed diagnostic lines: never a token, password or key.
# Environment:
#   ACCEPTANCE_KEEP_UP=1   leave the stack running at the end (default: make down)
#   ACCEPTANCE_OUT=<dir>   logs (default deploy/compose/.smoke/acceptance/<UTC time>; git-ignored)
#   ACCEPTANCE_KEEP_LOGS=1 keep the full `docker compose logs` dump the secrets phase scans
#   ACCEPTANCE_GIT_BASE    commit the git checks compare against (default c6370f8, the SP3 plan)
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
# shellcheck source=lib/topology.sh
source "$here/lib/topology.sh"
load_topology

PROJECT="$(sed -n 's/^name: *//p' "$here/compose.yaml")"
MAKE=(make --no-print-directory -C "$here")
COMPOSE=(docker compose --project-directory "$here" --env-file "$here/topology.env"
  --env-file "$here/.env" -f "$here/compose.yaml")
OUT="${ACCEPTANCE_OUT:-$here/.smoke/acceptance/$(date -u +%Y%m%dT%H%M%SZ)}"
READY_WAIT_S=120      # the cell must be ready again within this after losing one actor
RESTART_WAIT_S=300    # a restarted container must be healthy within this
REFUSAL_LIMIT_S=60    # a refusing container must exit within this
SCRATCH_LABEL="io.webrtc.signaling.test=acceptance"
failures=0
passes=0

mkdir -p "$OUT"
chmod 0700 "$here/.smoke" "$OUT" 2>/dev/null || true

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
pass() { passes=$((passes + 1)); printf 'PASS %s %s: %s\n' "$1" "$2" "$3" | tee -a "$OUT/results.txt"; }
fail() { failures=$((failures + 1)); printf 'FAIL %s %s: %s\n' "$1" "$2" "$3" | tee -a "$OUT/results.txt"; }
check() { # check <criterion> <name> <evidence> <condition-exit-code>
  if [ "$4" -eq 0 ]; then pass "$1" "$2" "$3"; else fail "$1" "$2" "$3"; fi
}

# --- docker helpers ------------------------------------------------------------------------------

cid() { # container id of a compose service (running or not)
  docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" \
    --filter "label=com.docker.compose.service=$1" | head -n 1
}
health() { docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$(cid "$1")" 2>/dev/null; }
state() { docker inspect -f '{{.State.Status}}' "$(cid "$1")" 2>/dev/null; }

wait_health() { # wait_health <service> <want> <seconds>; prints the seconds taken
  local s="$1" want="$2" limit="$3" t0 now
  t0=$(date +%s)
  while :; do
    now=$(date +%s)
    [ "$(health "$s")" = "$want" ] && { echo $((now - t0)); return 0; }
    [ $((now - t0)) -ge "$limit" ] && { echo "$limit"; return 1; }
    sleep 2
  done
}

# HTTP status of GET <path> on <ip>:<port>, fetched with bash /dev/tcp inside <service>'s container.
PROBE='exec 3<>"/dev/tcp/$1/$2" || { echo 000; exit 0; }
printf "GET %s HTTP/1.0\r\nHost: %s\r\n\r\n" "$3" "$1" >&3
IFS= read -r -t 4 line <&3 || { echo 000; exit 0; }
set -- $line; echo "${2:-000}"'
http_status() { # http_status <from-service> <ip> <port> <path>
  timeout 8 docker exec "$(cid "$1")" bash -c "$PROBE" probe "$2" "$3" "$4" 2>/dev/null | tr -d '\r' || echo 000
}
# tcp_open <from-service> <ip> <port>: exit 0 when a TCP connection opens within 3 s
tcp_open() {
  timeout 8 docker exec "$(cid "$1")" timeout 3 bash -c 'exec 3<>"/dev/tcp/$1/$2"' tcp "$2" "$3" >/dev/null 2>&1
}

health_ip() { # the /ready address of a plane or local-platform
  case "$1" in
    actor-*) ip_of "$1" cell ;;
    *) ip_of "$1" shared ;;
  esac
}

ACTORS=$(for c in c001 c002; do for n in $(seq 1 "$ACTORS_PER_CELL"); do printf 'actor-%s-%s ' "$c" "$n"; done; done)
# The last c001 actor is the one the actor, standby and refusal phases stop or replace; the one before
# it is the "sibling" whose leaf the sibling-identity case borrows.
SPARE="actor-c001-$ACTORS_PER_CELL"
SIBLING="actor-c001-$((ACTORS_PER_CELL - 1))"
_v="ACTOR_C001_${ACTORS_PER_CELL}_CELL_IP"; SPARE_CELL_IP="${!_v}"
_v="ACTOR_C001_${ACTORS_PER_CELL}_SHARED_IP"; SPARE_SHARED_IP="${!_v}"
N_ACTORS=$((ACTORS_PER_CELL * 2))
# actors + 2 gateways + control + identity + local-platform + 5 PostgreSQL + 2 kube-api + 2 provisioners
N_CONTAINERS=$((N_ACTORS + 2 + 1 + 1 + 1 + 5 + 2 + 2))
PLANES="$ACTORS gateway-c001 gateway-c002 control"

ready_all() { # ready_all <criterion> <label> <services...>: /ready 200 on each, from itself
  local crit="$1" label="$2" s code bad="" ok=0
  shift 2
  for s in "$@"; do
    code="$(http_status "$s" "$(health_ip "$s")" "$HEALTH_PORT" /ready)"
    if [ "$code" = 200 ]; then ok=$((ok + 1)); else bad="$bad $s=$code"; fi
  done
  check "$crit" "$label" "/ready 200 on $ok of $#${bad:+ (not:$bad)}" "$([ -z "$bad" ] && echo 0 || echo 1)"
  local b
  for b in $bad; do # evidence for an unready plane: its metrics and logs (git-ignored output)
    s="${b%%=*}"
    timeout 10 docker exec "$(cid "$s")" bash -c 'exec 3<>"/dev/tcp/$1/$2" && printf "GET /metrics HTTP/1.0\r\nHost: %s\r\n\r\n" "$1" >&3 && timeout 5 cat <&3' \
      metrics "$(health_ip "$s")" "$HEALTH_PORT" >"$OUT/unready-$crit-$s.metrics" 2>&1 || true
    docker logs "$(cid "$s")" >"$OUT/unready-$crit-$s.log" 2>&1 || true
  done
}

# psql on a cell primary as signaling_owner over the container-private socket. The password is read
# inside the container and never leaves it.
pg_sql() { # pg_sql <service> <database> <sql>
  timeout 20 docker exec "$(cid "$1")" bash -c \
    'PGPASSWORD="$(cat /run/signaling/owner.pass)" PGCONNECT_TIMEOUT=5 psql -h /var/run/postgresql -U signaling_owner -d "$1" -XAtqc "$2"' \
    sql "$2" "$3" 2>/dev/null
}

run_smoke() { # run_smoke <log-name>: exit status of make smoke; the STEP/SMOKE lines go to the log
  local f="$OUT/$1.log"
  timeout 360 "${MAKE[@]}" smoke >"$f" 2>&1
  local rc=$?
  grep -E '^(STEP|SMOKE_|WARNING)' "$f" | sed 's/^/    /'
  return "$rc"
}
smoke_summary() { grep -E '^(SMOKE_PASSED|SMOKE_FAILED)' "$OUT/$1.log" | head -n 1 | cut -c1-200; }

# --- criterion 1: make down; make up -------------------------------------------------------------

record_health_order() { # background: first time each service is seen healthy, until stopped
  local t0="$1" seen=" " line s
  while :; do
    while IFS='|' read -r s line; do
      case "$line" in *"(healthy)"*) ;; *) continue ;; esac
      case "$seen" in *" $s "*) continue ;; esac
      seen="$seen$s "
      printf '%s %s\n' "$(($(date +%s) - t0))" "$s"
    done < <(docker ps --filter "label=com.docker.compose.project=$PROJECT" \
      --format '{{.Label "com.docker.compose.service"}}|{{.Status}}' 2>/dev/null)
    sleep 2
  done
}

phase_up() {
  log "criterion 1: make down; make up (logs: $OUT/up.log)"
  "${MAKE[@]}" down >"$OUT/down-before.log" 2>&1
  local t0 rc poller
  t0=$(date +%s)
  record_health_order "$t0" >"$OUT/healthy-order.txt" &
  poller=$!
  "${MAKE[@]}" up >"$OUT/up.log" 2>&1
  rc=$?
  kill "$poller" 2>/dev/null; wait "$poller" 2>/dev/null
  UP_SECONDS=$(($(date +%s) - t0))
  echo "$UP_SECONDS" >"$OUT/time-to-healthy.txt"
  check 1 make-up "make up exit $rc after ${UP_SECONDS}s (wait timeout 600s)" "$rc"
  "${COMPOSE[@]}" ps -a --format '{{.Service}}|{{.State}}|{{.Health}}|{{.ExitCode}}' | sort >"$OUT/ps-after-up.txt"
  local total healthy oneshots actors
  total=$(wc -l <"$OUT/ps-after-up.txt")
  healthy=$(grep -c '|running|healthy|' "$OUT/ps-after-up.txt")
  oneshots=$(grep -cE '^provision-[12]\|exited\|\|0$' "$OUT/ps-after-up.txt")
  actors=$(grep -cE '^actor-c00[12]-[1-9]\|running\|healthy\|' "$OUT/ps-after-up.txt")
  check 1 all-healthy "$healthy of $((total - 2)) long-running services healthy, $oneshots of 2 provisioners exited 0, $total containers" \
    "$([ "$total" -eq "$N_CONTAINERS" ] && [ "$healthy" -eq $((N_CONTAINERS - 2)) ] && [ "$oneshots" -eq 2 ] && echo 0 || echo 1)"
  check 1 all-actors "$actors of $N_ACTORS actors running and healthy" "$([ "$actors" -eq "$N_ACTORS" ] && echo 0 || echo 1)"
  # shellcheck disable=SC2086
  ready_all 1 ready-planes $PLANES
  ready_all 1 ready-platform local-platform
  if [ "$rc" -ne 0 ]; then
    COMPOSE_CMD="${COMPOSE[*]}" bash "$here/status.sh" failures >"$OUT/up-failures.log" 2>&1 || true
    return 1
  fi
}

# --- criterion 2: smoke --------------------------------------------------------------------------

phase_smoke() {
  log "criterion 2: make smoke"
  local rc
  run_smoke smoke-1; rc=$?
  check 2 smoke "$(smoke_summary smoke-1)" "$rc"
}

# --- criterion 3a: lose one actor of c001 --------------------------------------------------------

C001_REST=$(for n in $(seq 1 $((ACTORS_PER_CELL - 1))); do printf 'actor-c001-%s ' "$n"; done)

wait_quiet() { # waits (at most 600 s) until make up's own load guard would pass; never bypasses it
  local t0 load cores
  cores="${PREFLIGHT_NPROC:-$(nproc 2>/dev/null || echo 1)}"
  t0=$(date +%s)
  while :; do
    load="$(cut -d' ' -f1 /proc/loadavg)"
    echo "$(($(date +%s) - t0))s load1=$load"
    awk -v l="$load" -v c="$cores" 'BEGIN { exit !(l / c <= 0.65) }' && return 0
    [ $(($(date +%s) - t0)) -ge 600 ] && return 1
    sleep 10
  done
}

ready_within() { # ready_within <seconds> <services...>: polls until every /ready is 200
  local limit="$1" t0 s all
  shift
  t0=$(date +%s)
  while :; do
    all=1
    for s in "$@"; do
      [ "$(http_status "$s" "$(health_ip "$s")" "$HEALTH_PORT" /ready)" = 200 ] || { all=0; break; }
    done
    [ "$all" = 1 ] && { echo $(($(date +%s) - t0)); return 0; }
    [ $(($(date +%s) - t0)) -ge "$limit" ] && { echo "$limit"; return 1; }
    sleep 3
  done
}

phase_actor() {
  log "criterion 3a: docker stop $SPARE"
  local took rc
  docker stop -t 90 "$(cid "$SPARE")" >/dev/null
  check 3a stopped "$SPARE state $(state "$SPARE")" "$([ "$(state "$SPARE")" = exited ] && echo 0 || echo 1)"
  sleep 10 # let the cell observe the loss before asking it
  # shellcheck disable=SC2086
  took=$(ready_within "$READY_WAIT_S" gateway-c001 control $C001_REST); rc=$?
  check 3a cell-ready "gateway-c001, control and the other c001 actors /ready 200 ${took}s after the 10s settle" "$rc"
  run_smoke smoke-3a-down; rc=$?
  check 3a smoke-one-actor-down "$(smoke_summary smoke-3a-down)" "$rc"
  docker start "$(cid "$SPARE")" >/dev/null
  took=$(wait_health "$SPARE" healthy "$RESTART_WAIT_S"); rc=$?
  check 3a restart "$SPARE healthy ${took}s after docker start" "$rc"
  ready_all 3a ready-after-restart "$SPARE" gateway-c001 control
  run_smoke smoke-3a-after; rc=$?
  check 3a smoke-after-restart "$(smoke_summary smoke-3a-after)" "$rc"
}

# --- criterion 3b: lose the c001 standby ---------------------------------------------------------

phase_standby() {
  log "criterion 3b: docker stop pg-c001-standby"
  local sync_before sync_after calls_before calls_after calls_back took rc
  sync_before="$(pg_sql pg-c001-primary postgres 'SHOW synchronous_standby_names')"
  calls_before="$(pg_sql pg-c001-primary signaling 'SELECT count(*) FROM call_state')"
  docker stop -t 30 "$(cid pg-c001-standby)" >/dev/null
  check 3b stopped "pg-c001-standby state $(state pg-c001-standby); primary synchronous_standby_names=$sync_before; call_state rows $calls_before" \
    "$([ "$(state pg-c001-standby)" = exited ] && echo 0 || echo 1)"
  SMOKE_TIMEOUT_MS=20000 run_smoke smoke-3b-down
  rc=$?
  local line
  line="$(smoke_summary smoke-3b-down)"
  check 3b no-commit "smoke must fail before a committed INVITE: $line" \
    "$([ "$rc" -ne 0 ] && ! grep -q '^STEP 4 ok' "$OUT/smoke-3b-down.log" && echo 0 || echo 1)"
  sync_after="$(pg_sql pg-c001-primary postgres 'SHOW synchronous_standby_names')"
  calls_after="$(pg_sql pg-c001-primary signaling 'SELECT count(*) FROM call_state')"
  pg_sql pg-c001-primary postgres "SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'SyncRep'" >"$OUT/syncrep-waiters.txt"
  check 3b no-async-fallback "synchronous_standby_names before=$sync_before after=$sync_after; call_state rows before=$calls_before after=$calls_after (locally visible); backends waiting on SyncRep=$(cat "$OUT/syncrep-waiters.txt")" \
    "$([ -n "$sync_before" ] && [ "$sync_before" = "$sync_after" ] && echo 0 || echo 1)"
  docker start "$(cid pg-c001-standby)" >/dev/null
  took=$(wait_health pg-c001-standby healthy "$RESTART_WAIT_S"); rc=$?
  check 3b standby-back "pg-c001-standby healthy ${took}s after docker start" "$rc"
  local t0 st=""
  t0=$(date +%s)
  while [ $(($(date +%s) - t0)) -lt 120 ]; do
    st="$(pg_sql pg-c001-primary postgres "SELECT sync_state FROM pg_stat_replication WHERE application_name LIKE '%standby%'")"
    [ "$st" = quorum ] || [ "$st" = sync ] && break
    sleep 2
  done
  check 3b quorum-back "pg_stat_replication sync_state=${st:-none} $(($(date +%s) - t0))s after healthy" \
    "$([ "$st" = quorum ] || [ "$st" = sync ] && echo 0 || echo 1)"
  # shellcheck disable=SC2086
  took=$(ready_within "$READY_WAIT_S" control $C001_REST "$SPARE"); rc=$?
  check 3b cell-ready "control and the c001 actors /ready 200 ${took}s after quorum" "$rc"
  # The gateway's 15 s boot lease cannot be renewed while commits wait for the lost standby, and an
  # expired boot is never revived (fencing). Its boot id is pinned at provisioning, so it stays
  # unready until re-provisioned: that is the designed fail-closed result, recorded, not judged.
  local gw
  gw="$(http_status gateway-c001 "$(health_ip gateway-c001)" "$HEALTH_PORT" /ready)"
  printf 'INFO 3b gateway-fenced: gateway-c001 /ready=%s; boot lease %s\n' "$gw" \
    "$(pg_sql pg-c001-primary signaling "SELECT CASE WHEN lease_until>clock_timestamp() THEN 'live' ELSE 'lapsed' END FROM gateway_lease")" |
    tee -a "$OUT/results.txt"
  log "criterion 3b: re-provision (make down; make up) to restore the fenced gateway boot"
  "${MAKE[@]}" down >"$OUT/down-3b.log" 2>&1
  wait_quiet >"$OUT/quiet-3b.log"
  "${MAKE[@]}" up >"$OUT/up-3b.log" 2>&1
  rc=$?
  check 3b reprovisioned "make up exit $rc after the standby case" "$rc"
  run_smoke smoke-3b-after
  rc=$?
  calls_back="$(pg_sql pg-c001-primary signaling 'SELECT count(*) FROM call_state')"
  check 3b smoke-after "$(smoke_summary smoke-3b-after); call_state rows now $calls_back" "$rc"
}

# --- criterion 4: refusals -----------------------------------------------------------------------

SECRET_RE='eyJ[A-Za-z0-9_-]{8,}\.|BEGIN [A-Z ]*PRIVATE KEY|password=|PASSWORD='

plane_env() { # docker run -e flags of an actor of c001 (compose's x-plane-env plus the actor keys)
  printf -- '-e\nSIGNALING_DEPLOYMENT_MODE=local-minikube\n-e\nSIGNALING_LOCAL_ACKNOWLEDGEMENT=LOCAL_TEST_ONLY\n'
  printf -- '-e\nSIGNALING_NATIVE_PROVIDER_CONTRACT=/run/signaling/provider.json\n'
  printf -- '-e\nSIGNALING_LOCAL_PROCESS_PROFILE=/run/signaling/process.json\n'
  printf -- '-e\nSIGNALING_RUNTIME_CONTRACT=/run/signaling/runtime.yaml\n'
  printf -- '-e\nSIGNALING_IDENTITY_ISSUER=%s\n-e\nSIGNALING_IDENTITY_AUDIENCE=%s\n' "$ISSUER" "$AUDIENCE"
  printf -- '-e\nSPRING_PROFILES_ACTIVE=actor\n-e\nSIGNALING_CELL_ID=c001\n-e\nSIGNALING_FORMATION_PROFILE=initial-six\n-e\nSIGNALING_LOCAL_CELL_ACTORS=%s\n' "$ACTORS_PER_CELL"
  printf -- '-e\nJVM_OPTS=%s\n' "$(sed -n 's/^JVM_OPTS=//p' "$here/.env" | tail -n 1)"
}

app_image() { sed -n 's/^APP_IMAGE=//p' "$here/.env" | tail -n 1; }

# refuse_case <pass|info> <name> <expected-line-ERE> <docker run args...>
# Runs one container (detached, labelled), waits up to REFUSAL_LIMIT_S for it to exit, and checks:
# non-zero exit in time, a fixed PLATFORM ERROR line matching the ERE, no secret-looking text in its
# output. `info` cases print INFO (recorded, not counted): behaviour the spec does not require.
refuse_case() {
  local mode="$1" name="$2" want="$3" c t0 took code logf="$OUT/refusal-$2.log"
  shift 3
  local report=check
  [ "$mode" = info ] && report=info_check
  c="$(docker run -d --label "$SCRATCH_LABEL" --name "acceptance-$name" "$@")" || {
    "$report" 4 "$name" "docker run failed" 1
    return
  }
  t0=$(date +%s)
  while [ "$(docker inspect -f '{{.State.Status}}' "$c")" = running ] && [ $(($(date +%s) - t0)) -lt "$REFUSAL_LIMIT_S" ]; do
    sleep 1
  done
  took=$(($(date +%s) - t0))
  docker logs "$c" >"$logf" 2>&1
  if [ "$(docker inspect -f '{{.State.Status}}' "$c")" = running ]; then
    "$report" 4 "$name" "still running after ${REFUSAL_LIMIT_S}s (no startup refusal); log lines: $(wc -l <"$logf")" 1
    docker rm -f "$c" >/dev/null
    return
  fi
  code="$(docker inspect -f '{{.State.ExitCode}}' "$c")"
  docker rm -f "$c" >/dev/null
  local line leaks traces
  line="$(grep -oE '\{"component":"PLATFORM","level":"ERROR","errorCode":"[A-Z_]+"[^}]*\}' "$logf" | head -n 1)"
  leaks="$(grep -cE "$SECRET_RE" "$logf")"
  traces="$(grep -cE '^\s+at ' "$logf")"
  "$report" 4 "$name" "exit $code after ${took}s; diagnostic: ${line:-none}; stack frames: $traces; secret-looking lines: $leaks" \
    "$([ "$code" -ne 0 ] && grep -qE "$want" <<<"$line" && [ "$leaks" -eq 0 ] && echo 0 || echo 1)"
}
info_check() { printf 'INFO %s %s: %s (%s)\n' "$1" "$2" "$3" "$([ "$4" -eq 0 ] && echo refused || echo not-refused)" | tee -a "$OUT/results.txt"; }

scratch_mount() { # scratch_mount <volume> <service> [leaf-from-service|-] [file-to-remove]: a copy of a mount
  local vol="$1" svc="$2" leaf="${3:--}" drop="${4:-}" prov
  prov="$(sed -n 's/^PROVISIONER_IMAGE=//p' "$here/.env" | tail -n 1)"
  docker volume create --label "$SCRATCH_LABEL" "$vol" >/dev/null
  docker run --rm --label "$SCRATCH_LABEL" --network none -v "${PROJECT}_run:/src:ro" -v "$vol:/dst" \
    --entrypoint bash "$prov" -c 'cp -a "/src/$1/." /dst/ &&
      if [ "$2" != - ]; then cp -a "/src/$2/leaf.crt" "/src/$2/leaf.key" /dst/; fi &&
      if [ -n "$3" ]; then rm -f "/dst/$3"; fi' copy "$svc" "$leaf" "$drop" >/dev/null
}

REJECTED='"errorCode":"LOCAL_RUNTIME_REJECTED"'

phase_refusal() {
  log "criterion 4: refusal cases (one container each)"
  local img env_args wrong
  img="$(app_image)"
  mapfile -t env_args < <(plane_env)
  env_args+=(-e "SIGNALING_CLUSTER_FINGERPRINT=$CLUSTER_FINGERPRINT")
  local base=(--user 10001:10001 --read-only --cap-drop ALL --security-opt no-new-privileges:true
    --tmpfs /tmp:size=128m,nosuid --memory 1g)
  local hardening=("${base[@]}" --network none)
  # (a) missing contract: the last c001 actor's mount without provider.json, compose's actor environment.
  scratch_mount acceptance-no-contract "$SPARE" - provider.json
  refuse_case pass missing-contract "$REJECTED,\"role\":\"contract\",\"category\":\"MISSING\"" "${hardening[@]}" \
    --mount "type=volume,src=acceptance-no-contract,dst=/run/signaling,readonly" \
    "${env_args[@]}" --entrypoint bash "$img" /run/signaling/launch.sh
  docker volume rm acceptance-no-contract >/dev/null
  # (a') an empty mount: no launch.sh either, so java runs directly with compose's environment and
  # the placeholder values launch.sh would export; runtime.yaml is missing too.
  refuse_case pass empty-mount '"errorCode":"(LOCAL_RUNTIME_REJECTED|CONFIGURATION_REJECTED)"' "${hardening[@]}" \
    --tmpfs /run/signaling:size=1m "${env_args[@]}" -e SIGNALING_POD_UID=00000000-0000-4000-8000-000000000000 \
    -e SIGNALING_REMOTING_KEYSTORE_PASSWORD=TEST_ONLY_PLACEHOLDER -e SIGNALING_REMOTING_KEY_PASSWORD=TEST_ONLY_PLACEHOLDER \
    -e SIGNALING_REMOTING_TRUSTSTORE_PASSWORD=TEST_ONLY_PLACEHOLDER "$img"
  # (b) foreign SPIFFE id: the last c001 actor's mount with gateway-c001's leaf (key and certificate).
  scratch_mount acceptance-foreign-leaf "$SPARE" gateway-c001
  refuse_case pass foreign-spiffe-gateway "$REJECTED,\"role\":\"identity\",\"category\":\"IDENTITY_MISMATCH\"" \
    "${hardening[@]}" --mount "type=volume,src=acceptance-foreign-leaf,dst=/run/signaling,readonly" \
    "${env_args[@]}" --entrypoint bash "$img" /run/signaling/launch.sh
  docker volume rm acceptance-foreign-leaf >/dev/null
  # (b') a sibling's per-pod SPIFFE id (the previous actor's leaf in the last actor's mount): recorded only;
  # startup checks the cell-level actor URI, the per-pod id is the source's concern.
  scratch_mount acceptance-sibling-leaf "$SPARE" "$SIBLING"
  refuse_case info foreign-spiffe-sibling "$REJECTED" "${hardening[@]}" \
    --mount "type=volume,src=acceptance-sibling-leaf,dst=/run/signaling,readonly" \
    "${env_args[@]}" --entrypoint bash "$img" /run/signaling/launch.sh
  docker volume rm acceptance-sibling-leaf >/dev/null
  # (c) a fingerprint that is not 64 lower-case hex: refused at startup.
  refuse_case pass fingerprint-malformed "$REJECTED,\"role\":\"identity\",\"category\":\"INVALID\"" "${hardening[@]}" \
    --mount "type=volume,src=${PROJECT}_run,dst=/run/signaling,readonly,volume-subpath=$SPARE" \
    "${env_args[@]}" -e "SIGNALING_CLUSTER_FINGERPRINT=NOT-A-FINGERPRINT" --entrypoint bash "$img" /run/signaling/launch.sh
  # (d) a well-formed but different fingerprint, in the last c001 actor's place on the live cell.
  wrong="$(printf 'TEST_ONLY_ACCEPTANCE_WRONG_FINGERPRINT' | sha256sum | cut -c1-64)"
  mapfile -t env_args < <(plane_env)
  fingerprint_mismatch "$img" "$wrong" "${base[@]}" "${env_args[@]}"
}

# The product validates only the fingerprint's form at startup; a different 64-hex value is a
# cluster join property (FingerprintCompatibilityChecker). This case records what really happens to
# such a container in the last c001 actor's place: does it exit, join, or become ready?
fingerprint_mismatch() {
  local img="$1" wrong="$2" c t0 took code st ready="000" logf="$OUT/refusal-fingerprint-mismatch.log"
  shift 2
  docker stop -t 90 "$(cid "$SPARE")" >/dev/null
  c="$(docker run -d --label "$SCRATCH_LABEL" --name acceptance-fingerprint-mismatch "$@" \
    --network "name=${PROJECT}_net-c001,ip=$SPARE_CELL_IP" \
    --network "name=${PROJECT}_net-shared,ip=$SPARE_SHARED_IP" \
    --mount "type=volume,src=${PROJECT}_run,dst=/run/signaling,readonly,volume-subpath=$SPARE" \
    -e "SIGNALING_CLUSTER_FINGERPRINT=$wrong" --entrypoint bash "$img" /run/signaling/launch.sh)" || {
    fail 4 fingerprint-mismatch "docker run failed"
    docker start "$(cid "$SPARE")" >/dev/null
    return
  }
  t0=$(date +%s)
  while [ $(($(date +%s) - t0)) -lt 150 ]; do
    st="$(docker inspect -f '{{.State.Status}}' "$c")"
    [ "$st" = running ] || break
    ready="$(timeout 8 docker exec "$c" bash -c "$PROBE" probe "$SPARE_CELL_IP" "$HEALTH_PORT" /ready 2>/dev/null | tr -d '\r')"
    [ "$ready" = 200 ] && break
    sleep 3
  done
  took=$(($(date +%s) - t0))
  st="$(docker inspect -f '{{.State.Status}}' "$c")"
  code="$(docker inspect -f '{{.State.ExitCode}}' "$c")"
  docker logs "$c" >"$logf" 2>&1
  docker rm -f "$c" >/dev/null
  local line leaks incompat
  line="$(grep -oE '\{"component":"PLATFORM","level":"ERROR","errorCode":"[A-Z_]+"[^}]*\}' "$logf" | head -n 1)"
  leaks="$(grep -cE "$SECRET_RE" "$logf")"
  incompat="$(grep -ciE 'incompatible|configuration-compatibility|ConfigCompat' "$logf")"
  # The criterion: exit non-zero within REFUSAL_LIMIT_S with the fixed LOCAL_RUNTIME_REJECTED line.
  # Observed up to 150 s so the real behaviour is recorded even when it is slower or generic.
  local how
  if [ "$st" = running ] && [ "$ready" = 200 ]; then
    how="BECAME READY after ${took}s with a foreign fingerprint"
  elif [ "$st" = running ]; then
    how="still running, /ready=$ready after ${took}s"
  else
    how="exited $code after ${took}s, never ready"
  fi
  check 4 fingerprint-mismatch "$how; diagnostic: ${line:-none}; incompatible-config log lines: $incompat; secret-looking lines: $leaks" \
    "$([ "$st" != running ] && [ "$code" -ne 0 ] && [ "$took" -le "$REFUSAL_LIMIT_S" ] && grep -q "$REJECTED" <<<"$line" && echo 0 || echo 1)"
  docker start "$(cid "$SPARE")" >/dev/null
  local rc
  took=$(wait_health "$SPARE" healthy "$RESTART_WAIT_S"); rc=$?
  check 4 actor-back "$SPARE healthy ${took}s after the case" "$rc"
  run_smoke smoke-4-after; rc=$?
  check 4 smoke-after "$(smoke_summary smoke-4-after)" "$rc"
}

# --- isolation (spec 8 network row; criterion 5 itself is the SP1/SP2 ITs) ---------------------

phase_isolation() {
  log "isolation probes"
  local from=actor-c001-1 rc
  tcp_open "$from" "$PG_C001_PRIMARY_IP" "$PG_PORT"; rc=$?
  check iso own-cell-pg "$from -> $PG_C001_PRIMARY_IP:$PG_PORT open (probe works)" "$rc"
  for target in "$ACTOR_C002_1_CELL_IP:25520" "$ACTOR_C002_1_CELL_IP:8558" "$PG_C002_PRIMARY_IP:$PG_PORT" \
    "$ACTOR_C002_1_SHARED_IP:25520" "$KUBE_C002_IP:$KUBE_API_PORT"; do
    tcp_open "$from" "${target%:*}" "${target#*:}"
    rc=$?
    check iso "c001-to-${target}" "$from -> $target $([ "$rc" -eq 0 ] && echo OPEN || echo refused/timeout)" "$([ "$rc" -ne 0 ] && echo 0 || echo 1)"
  done
  local s both=""
  for s in $ACTORS gateway-c001 gateway-c002 control identity local-platform kube-api-c001 kube-api-c002 \
    pg-c001-primary pg-c001-standby pg-c002-primary pg-c002-standby pg-directory; do
    if tcp_open "$s" "$PG_C001_PRIMARY_IP" "$PG_PORT" && tcp_open "$s" "$PG_C002_PRIMARY_IP" "$PG_PORT"; then
      both="$both $s"
    fi
  done
  check iso both-cell-pgs "containers reaching both cell primaries:${both:- none}" "$([ "$both" = " control" ] && echo 0 || echo 1)"
}

# --- criterion 6: secrets ------------------------------------------------------------------------

# Every password/key-file value of this run, read inside a provisioner container, compared in
# python against docker inspect env and the logs. Only counts are printed.
# key_files <ref>: tracked files at <ref> holding a PEM private key BODY (a BEGIN line followed by a
# base64 line), as opposed to code or tests that only name the marker.
key_files() {
  local f
  git -C "$repo" grep -lE 'BEGIN [A-Z ]*PRIVATE KEY-----' "$1" -- . 2>/dev/null | sed "s/^$1://" | while read -r f; do
    git -C "$repo" show "$1:$f" | grep -A1 -E 'BEGIN [A-Z ]*PRIVATE KEY-----' | grep -qE '^[A-Za-z0-9+/]{40,}={0,2}$' &&
      printf '%s\n' "$f"
  done | sort
}

phase_secrets() {
  log "criterion 6: secrets"
  local prov new
  prov="$(sed -n 's/^PROVISIONER_IMAGE=//p' "$here/.env" | tail -n 1)"
  # SP3_BASE: the last commit before the compose environment work (the SP3 plan commit).
  local base="${ACCEPTANCE_GIT_BASE:-c6370f8}"
  new="$(comm -13 <(key_files "$base") <(key_files HEAD) | tr '\n' ' ')"
  check 6 git-private-keys "tracked files with a PEM private key body: $(key_files HEAD | wc -l) at HEAD, added since $base: ${new:-none}" \
    "$([ -z "$new" ] && echo 0 || echo 1)"
  local added
  added="$(git -C "$repo" diff "$base" HEAD -- deploy/compose | grep -E '^\+' | grep -v '^+++' |
    grep -ciE "(password|secret|token)[a-z_]*[=:][[:space:]]*[\"']?[A-Za-z0-9+_-]{16,}[\"']?[[:space:]]*\$" || true)"
  check 6 git-secret-literals "added lines under deploy/compose since $base with a long password/secret/token literal: $added" \
    "$([ "$added" = 0 ] && echo 0 || echo 1)"
  local untracked
  untracked="$(git -C "$repo" status --porcelain --untracked-files=all | grep -E '\.(key|p12|pass|pem)$|db-passwords' || true)"
  check 6 git-status "untracked/modified key, store or password files: ${untracked:-none}" "$([ -z "$untracked" ] && echo 0 || echo 1)"
  "${COMPOSE[@]}" logs --no-color >"$OUT/compose-logs.txt" 2>&1
  docker inspect $(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT") >"$OUT/inspect.json"
  docker run --rm --network none -v "${PROJECT}_run:/r:ro" --entrypoint bash "$prov" -c \
    'find /r -type f \( -name "*.pass" -o -name "db-passwords.env" -o -name "kube-token" -o -name "*.key" \) -print0 |
       while IFS= read -r -d "" f; do case "$f" in *.env) sed -n "s/^[A-Z_]*=//p" "$f" ;;
         *.key) if grep -aq "^-----BEGIN" "$f"; then grep -av "^-----" "$f" | tr -d "\n"; else base64 -w0 "$f"; fi; echo ;;
         *) cat "$f"; echo ;; esac; done' |
    python3 -c '
import json, re, sys
secrets = {line.strip() for line in sys.stdin if len(line.strip()) >= 8}
inspect = json.load(open(sys.argv[1]))
logs = open(sys.argv[2], errors="replace").read()
env_hits = sum(1 for c in inspect for e in (c["Config"].get("Env") or []) for s in secrets if s in e)
log_hits = sum(1 for s in secrets if s in logs)
jwts = len(re.findall(r"eyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.", logs))
known = len(re.findall(r"admin123|mod123", logs))
print(f"{len(secrets)} {env_hits} {log_hits} {jwts} {known}")
' "$OUT/inspect.json" "$OUT/compose-logs.txt" >"$OUT/secret-counts.txt"
  local n env_hits log_hits jwts known
  read -r n env_hits log_hits jwts known <"$OUT/secret-counts.txt"
  check 6 inspect-env "$n secret values of this run (passwords, tokens, key bodies); found in docker inspect env: $env_hits" \
    "$([ "${n:-0}" -gt 0 ] && [ "$env_hits" = 0 ] && echo 0 || echo 1)"
  check 6 logs "compose logs ($(wc -l <"$OUT/compose-logs.txt") lines): secret values $log_hits, JWTs $jwts, TEST_ONLY account passwords $known" \
    "$([ "$log_hits" = 0 ] && [ "$jwts" = 0 ] && [ "$known" = 0 ] && echo 0 || echo 1)"
  # The full log dump is gigabytes after a few hours of uptime; keep it only on request.
  [ "${ACCEPTANCE_KEEP_LOGS:-0}" = 1 ] || rm -f "$OUT/compose-logs.txt"
  local labels="" i
  for i in APP_IMAGE IDENTITY_IMAGE PLATFORM_IMAGE; do
    labels="$labels $(docker image inspect -f '{{index .Config.Labels "io.webrtc.signaling.release.status"}}' "$(sed -n "s/^$i=//p" "$here/.env" | tail -n 1)")"
  done
  check 6 image-labels "release.status label of app, identity, local-platform images:$labels" \
    "$([ "$labels" = " NOT_QUALIFIED NOT_QUALIFIED NOT_QUALIFIED" ] && echo 0 || echo 1)"
  rm -f "$OUT/inspect.json"
}

# --- measurements --------------------------------------------------------------------------------

phase_measure() {
  log "measurements (steady state)"
  docker stats --no-stream --format '{{.Name}}|{{.MemUsage}}|{{.CPUPerc}}' \
    $(docker ps -q --filter "label=com.docker.compose.project=$PROJECT") | sort >"$OUT/docker-stats.txt"
  local s rss
  : >"$OUT/jvm-rss.txt"
  for s in $(service_list); do
    rss="$(docker exec "$(cid "$s")" awk '/^VmRSS/ { print $2 }' /proc/1/status 2>/dev/null)"
    printf '%s %s\n' "$s" "${rss:-?}" >>"$OUT/jvm-rss.txt"
  done
  python3 - "$OUT/docker-stats.txt" "$OUT/jvm-rss.txt" <<'PY' | tee "$OUT/measure-summary.txt"
import re, sys
unit = {"B": 1 / 2**20, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024}
rows = []
for line in open(sys.argv[1]):
    name, mem, _ = line.strip().split("|")
    m = re.match(r"([\d.]+)(\w+)", mem.split("/")[0].strip())
    rows.append((name, float(m.group(1)) * unit[m.group(2)]))
jvm = [(n, int(r) / 1024) for n, r in (l.split() for l in open(sys.argv[2])) if r.isdigit()]
total = sum(v for _, v in rows)
print(f"MEASURE docker-stats containers={len(rows)} total_mib={total:.0f} max_mib={max(v for _, v in rows):.0f} ({max(rows, key=lambda r: r[1])[0]})")
print(f"MEASURE jvm-rss jvms={len(jvm)} total_mib={sum(v for _, v in jvm):.0f} max_mib={max(v for _, v in jvm):.0f} ({max(jvm, key=lambda r: r[1])[0]}) min_mib={min(v for _, v in jvm):.0f}")
PY
  [ -s "$OUT/time-to-healthy.txt" ] && echo "MEASURE time-to-healthy $(cat "$OUT/time-to-healthy.txt")s (make up, including preflight and provisioning)"
  cpu_window 30
}

# cpu_window <seconds>: CPU cores used per container (cgroup v2 cpu.stat usage_usec) and by the
# whole host (/proc/stat) over the window. Skipped when the cgroup layout is not the systemd one.
cpu_window() {
  local w="$1" id f n before after
  local -A start
  local ids
  ids="$(docker ps -q --no-trunc --filter "label=com.docker.compose.project=$PROJECT")"
  for id in $ids; do
    f="/sys/fs/cgroup/system.slice/docker-$id.scope/cpu.stat"
    [ -r "$f" ] || { echo "MEASURE cpu skipped (no $f)"; return 0; }
    start[$id]="$(awk '/^usage_usec/ { print $2 }' "$f")"
  done
  before="$(awk '/^cpu / { print $2 + $3 + $4 + $6 + $7 + $8 }' /proc/stat)"
  sleep "$w"
  after="$(awk '/^cpu / { print $2 + $3 + $4 + $6 + $7 + $8 }' /proc/stat)"
  for id in $ids; do
    n="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.service"}}' "$id")"
    printf '%s %s\n' "$n" "$(($(awk '/^usage_usec/ { print $2 }' "/sys/fs/cgroup/system.slice/docker-$id.scope/cpu.stat") - start[$id]))"
  done | sort >"$OUT/cpu-usec.txt"
  awk -v w="$w" -v host="$(( (after - before) ))" -v hz="$(getconf CLK_TCK)" '
    { c = $2 / 1e6 / w; total += c; if ($1 ~ /^actor-/) { actors += c; if (c > amax) amax = c } }
    END { printf "MEASURE cpu-cores window=%ss compose_total=%.2f actors_total=%.2f actor_max=%.2f host_busy=%.2f of %d\n",
          w, total, actors, amax, host / hz / w, '"$(nproc)"' }' "$OUT/cpu-usec.txt"
  printf 'MEASURE cpu-pressure %s\n' "$(head -n 1 /proc/pressure/cpu 2>/dev/null || echo unavailable)"
}

# --- main ----------------------------------------------------------------------------------------

finish() {
  local rc=$?
  docker rm -f $(docker ps -aq --filter "label=$SCRATCH_LABEL") >/dev/null 2>&1 || true
  docker volume rm $(docker volume ls -q --filter "label=$SCRATCH_LABEL") >/dev/null 2>&1 || true
  if [ "${ACCEPTANCE_KEEP_UP:-0}" != 1 ]; then
    log "make down (ACCEPTANCE_KEEP_UP=1 keeps the stack)"
    "${MAKE[@]}" down >"$OUT/down-after.log" 2>&1 || true
  fi
  exit "$rc"
}
trap finish EXIT

phases=("$@")
[ "${#phases[@]}" -gt 0 ] || phases=(up smoke measure actor standby refusal isolation secrets)
log "acceptance (LOCAL_TEST_ONLY / NOT_QUALIFIED) phases: ${phases[*]}; commit $(git -C "$repo" rev-parse --short HEAD); logs $OUT"
for p in "${phases[@]}"; do
  case "$p" in
    up) phase_up || { log "stack did not come up; later phases skipped"; break; } ;;
    smoke | actor | standby | refusal | isolation | secrets | measure) "phase_$p" ;;
    *) echo "acceptance: unknown phase $p" >&2; exit 64 ;;
  esac
done
if [ "$failures" -eq 0 ]; then
  log "ACCEPTANCE_PASSED $passes checks (LOCAL_TEST_ONLY / NOT_QUALIFIED)"
  exit 0
fi
log "ACCEPTANCE_FAILED $failures failure(s), $passes passed"
exit 1
